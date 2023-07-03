package rannift

import (
	"context"
	ranniftpb "github.com/TianLuan99/RanniKV.git/internal/grpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"math/rand"
	"sync"
	"time"
)

// NodeState is the type for RanniNode state.
type NodeState int

// NodeState constants.
const (
	Follower = iota
	Candidate
	Leader
)

// RanniNodeConfig is the config for RanniNode.
type RanniNodeConfig struct {
	// Peers' addresses
	Peers []string `yaml:"peers"`
	// Index of current node in peers
	NodeIndex int `yaml:"node_index"`
	// Election timeout range
	MinElectionTimeout int `yaml:"min_election_timeout"`
	MaxElectionTimeout int `yaml:"max_election_timeout"`
	// Heartbeat interval
	HeartbeatInterval int `yaml:"heartbeat_interval"`
}

// RanniNode is the struct that represents a node in RanniKV.
type RanniNode struct {
	// RPC server
	ranniftpb.UnimplementedRanniftServiceServer

	// Logger
	log *zap.Logger

	// gRPC connections to peers
	conns []*grpc.ClientConn

	// Persistent states on node
	currentTerm int32
	votedFor    int32
	logs        []*ranniftpb.LogEntry

	// Volatile states on node
	commitIndex int32
	lastApplied int32

	// Fields only used if current node is leader
	nextIndex  []int32
	matchIndex []int32

	// Node's state
	state NodeState

	// Peers' addresses
	peers []string
	// Index of current node in peers
	nodeIndex int32

	// Election timeout range
	minElectionTimeout int
	maxElectionTimeout int
	// Election timer
	electionTimer *time.Ticker

	// Heartbeat interval
	heartbeatInterval int
	// Heartbeat timer
	heartbeatTimer *time.Ticker

	// Random number generator
	rand *rand.Rand

	// Votes count and votes needed for election
	votesCount  int32
	votesNeeded int32

	// TODO: Use finer-grained locking
	// Mutex for locking
	mu sync.Mutex

	// Stop channel
	stopCh chan struct{}
}

// NewRanniNode creates a new RanniNode.
func NewRanniNode(rnCfg *RanniNodeConfig) (*RanniNode, error) {
	// TODO: Change to production logger.
	logger, err := zap.NewDevelopment()
	if err != nil {
		return nil, err
	}

	// Initialize gRPC connections to peers
	var conns []*grpc.ClientConn
	for i, peer := range rnCfg.Peers {
		if i == rnCfg.NodeIndex { // Skip current node
			conns = append(conns, nil)
			continue
		}
		conn, err := grpc.Dial(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		conns = append(conns, conn)
	}

	// Initialize nextIndex and matchIndex
	nextIndex := make([]int32, len(rnCfg.Peers))
	matchIndex := make([]int32, len(rnCfg.Peers))

	// Initialize RanniNode
	rn := &RanniNode{
		log:                logger,
		conns:              conns,
		currentTerm:        0,
		votedFor:           -1,
		logs:               []*ranniftpb.LogEntry{},
		commitIndex:        0,
		lastApplied:        0,
		nextIndex:          nextIndex,
		matchIndex:         matchIndex,
		state:              Follower,
		peers:              rnCfg.Peers,
		nodeIndex:          int32(rnCfg.NodeIndex),
		minElectionTimeout: rnCfg.MinElectionTimeout,
		maxElectionTimeout: rnCfg.MaxElectionTimeout,
		electionTimer:      time.NewTicker(time.Millisecond * time.Duration(rand.Intn(rnCfg.MaxElectionTimeout-rnCfg.MinElectionTimeout)+rnCfg.MinElectionTimeout)),
		heartbeatInterval:  rnCfg.HeartbeatInterval,
		heartbeatTimer:     time.NewTicker(time.Millisecond * time.Duration(rnCfg.HeartbeatInterval)),
		rand:               rand.New(rand.NewSource(time.Now().UnixNano())),
		votesCount:         0,
		votesNeeded:        int32(len(rnCfg.Peers)/2 + 1),
		mu:                 sync.Mutex{},
		stopCh:             make(chan struct{}),
	}
	return rn, nil
}

// RequestVote is the RPC handler for RequestVote RPC.
func (rn *RanniNode) RequestVote(_ context.Context, req *ranniftpb.RequestVoteRequest) (*ranniftpb.RequestVoteResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := &ranniftpb.RequestVoteResponse{}
	if req.Term < rn.currentTerm { // Reject if candidate's term is smaller than current term
		resp.VoteGranted = false
	} else if rn.votedFor != -1 && rn.votedFor != req.CandidateId { // Reject if already voted for another candidate
		resp.VoteGranted = false
	} else if rn.lastApplied > req.LastLogIndex { // Reject if candidate's log is not up-to-date
		resp.VoteGranted = false
	} else if rn.lastApplied == req.LastLogIndex && rn.logs[rn.lastApplied].Term > req.LastLogTerm {
		resp.VoteGranted = false
	} else { // Grant vote in other cases
		resp.VoteGranted = true
		rn.currentTerm = req.Term
		rn.votedFor = req.CandidateId
		rn.ResetElectionTimer()
	}
	resp.Term = rn.currentTerm
	return resp, nil
}

// AppendEntries is the RPC handler for AppendEntries RPC.
func (rn *RanniNode) AppendEntries(_ context.Context, req *ranniftpb.AppendEntriesRequest) (*ranniftpb.AppendEntriesResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if req == nil { // If request is nil, which means it's a heartbeat, reset election timer
		rn.ResetElectionTimer()
		return nil, nil
	}
	// TODO: Implement AppendEntries
	resp := &ranniftpb.AppendEntriesResponse{
		Term:    rn.currentTerm,
		Success: true,
	}
	return resp, nil
}

// Heartbeat sends heartbeat to all peers to maintain leadership.
func (rn *RanniNode) Heartbeat() {
	for {
		select {
		case <-rn.heartbeatTimer.C: // If heartbeat timer expires, send heartbeat
			if rn.state != Leader { // Only leader can send heartbeat
				return
			}
			rn.broadcastHeartBeat()
		case <-rn.stopCh: // If stopCh is closed, stop heartbeat
			return
		}
	}
}

// broadcastHeartBeat sends heartbeat to all peers.
func (rn *RanniNode) broadcastHeartBeat() {
	// wg is used to wait for all goroutines to finish
	var wg sync.WaitGroup
	// Send heartbeat to all peers
	for i := range rn.peers {
		if i == int(rn.nodeIndex) { // Skip current node
			continue
		}
		wg.Add(1)
		go func(connIndex int) { // Send heartbeat to peers concurrently
			defer wg.Done()
			// Initialize gRPC client
			c := ranniftpb.NewRanniftServiceClient(rn.conns[connIndex])
			// Set timeout to heartbeat interval
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(rn.heartbeatInterval))
			defer cancel()
			// Send nil request to indicate heartbeat
			_, err := c.AppendEntries(ctx, nil)
			if err != nil {
				rn.log.Error("failed to send heartbeat", zap.Error(err))
			}
		}(i)
	}
	wg.Wait()
}

// Election starts election if election timer expires.
func (rn *RanniNode) Election() {
	for {
		select {
		case <-rn.electionTimer.C: // If election timer expires, start election
			rn.log.Info("election timer expires, start election")
			rn.startElection()
		case <-rn.stopCh: // If stopCh is closed, stop election
			return
		}
	}
}

// startElection starts election.
func (rn *RanniNode) startElection() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.state == Leader { // If current node is leader, reset election timer
		rn.ResetElectionTimer()
		return
	}
	// If current node is not leader, start election
	rn.state = Candidate       // Convert state to candidate
	rn.currentTerm++           // Increment current term
	rn.votedFor = rn.nodeIndex // Vote for self
	rn.ResetElectionTimer()    // Reset election timer

	rn.votesCount = 1 // Vote for self
	// Send RequestVote RPC to all peers
	for i := range rn.peers {
		if i == int(rn.nodeIndex) { // Skip current node
			continue
		}
		go func(connIndex int) { // Send RequestVote RPC to peers concurrently
			// Initialize gRPC client
			c := ranniftpb.NewRanniftServiceClient(rn.conns[connIndex])
			// Set timeout to heartbeat interval
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(rn.heartbeatInterval))
			defer cancel()

			rn.mu.Lock() // Lock before sending RequestVote RPC
			req := &ranniftpb.RequestVoteRequest{
				Term:         rn.currentTerm,
				CandidateId:  rn.nodeIndex,
				LastLogIndex: rn.lastApplied,
				LastLogTerm:  rn.logs[rn.lastApplied].Term,
			}
			rn.mu.Unlock() // Unlock after sending RequestVote RPC
			// Send RequestVote RPC to peer
			resp, err := c.RequestVote(ctx, req)
			if err != nil { // If error occurs, return
				rn.log.Error("failed to send RequestVote RPC", zap.Error(err))
				return
			}
			if resp.VoteGranted { // If vote is granted, increment votesCount
				rn.mu.Lock() // Lock before incrementing votesCount and checking state
				rn.votesCount++
				if rn.votesCount > rn.votesNeeded && rn.state == Candidate { // If votesCount > votesNeeded, become leader
					rn.state = Leader
					rn.mu.Unlock() // Unlock before calling broadcastHeartBeat to avoid deadlock
					rn.log.Info("become leader")
					rn.ResetElectionTimer()
					rn.broadcastHeartBeat()
					return
				}
				rn.mu.Unlock()
			}
		}(i)
	}
}

// ResetElectionTimer resets election timer to a random time between minElectionTimeout and maxElectionTimeout.
func (rn *RanniNode) ResetElectionTimer() {
	// Set random seed
	rn.rand.Seed(time.Now().UnixNano())
	// Generate a random time between minElectionTimeout and maxElectionTimeout
	electTime := rn.rand.Intn(rn.maxElectionTimeout-rn.minElectionTimeout) + rn.minElectionTimeout
	rn.electionTimer.Reset(time.Millisecond * time.Duration(electTime))
}

// Run starts the RanniNode.
func (rn *RanniNode) Run() {
	go rn.Heartbeat()
	go rn.Election()
}

// Close closes the RanniNode.
func (rn *RanniNode) Close() {
	rn.electionTimer.Stop()
	rn.heartbeatTimer.Stop()
	rn.stopCh <- struct{}{}
	for _, conn := range rn.conns {
		err := conn.Close()
		if err != nil {
			rn.log.Error("failed to close connection", zap.Error(err))
		}
	}
}
