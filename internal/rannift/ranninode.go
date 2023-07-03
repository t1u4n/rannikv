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
	// Election timeout
	ElectionTimeout int `yaml:"election_timeout"`
	// Election float range
	ElectionFloatRange int `yaml:"election_float_range"`
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
	nodeIndex int

	// Election timer
	electionTimer *time.Ticker

	// Heartbeat timer
	heartbeatTimer *time.Ticker

	// Mutex for locking
	mu sync.Mutex
}

// RequestVote is the RPC handler for RequestVote RPC.
func (rn *RanniNode) RequestVote(context.Context, *ranniftpb.RequestVoteRequest) (*ranniftpb.RequestVoteResponse, error) {
	resp := &ranniftpb.RequestVoteResponse{
		Term:        rn.currentTerm,
		VoteGranted: true,
	}
	return resp, nil
}

// AppendEntries is the RPC handler for AppendEntries RPC.
func (rn *RanniNode) AppendEntries(_ context.Context, req *ranniftpb.AppendEntriesRequest) (*ranniftpb.AppendEntriesResponse, error) {
	if req == nil { // If request is nil, which means it's a heartbeat, reset election timer
		rn.ResetElectionTimer()
		return nil, nil
	}

	resp := &ranniftpb.AppendEntriesResponse{
		Term:    rn.currentTerm,
		Success: true,
	}
	return resp, nil
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
		log:            logger,
		conns:          conns,
		currentTerm:    0,
		votedFor:       0,
		logs:           []*ranniftpb.LogEntry{},
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      nextIndex,
		matchIndex:     matchIndex,
		state:          0,
		peers:          rnCfg.Peers,
		nodeIndex:      rnCfg.NodeIndex,
		electionTimer:  time.NewTicker(time.Millisecond * time.Duration(rnCfg.ElectionTimeout+rand.Intn(rnCfg.ElectionFloatRange))),
		heartbeatTimer: time.NewTicker(time.Millisecond * time.Duration(rnCfg.HeartbeatInterval)),
	}
	return rn, nil
}

// Heartbeat sends heartbeat to all peers to maintain leadership.
func (rn *RanniNode) Heartbeat() {
	select {
	case <-rn.heartbeatTimer.C: // If heartbeat timer expires, send heartbeat
		if rn.state != Leader { // Only leader can send heartbeat
			return
		}

		// wg is used to wait for all goroutines to finish
		var wg sync.WaitGroup
		// Send heartbeat to all peers
		for i := range rn.peers {
			if i == rn.nodeIndex { // Skip current node
				continue
			}
			wg.Add(1)
			go func(connIndex int) { // Send heartbeat to peers concurrently
				defer wg.Done()
				// Initialize gRPC client
				c := ranniftpb.NewRanniftServiceClient(rn.conns[connIndex])
				// Set timeout to 50ms
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
				defer cancel()
				// Send nil request to indicate heartbeat
				c.AppendEntries(ctx, nil)
			}(i)
		}
		wg.Wait()
	}
}

// TODO: Implement election.
// Election starts election if election timer expires.
func (rn *RanniNode) Election() {
	select {
	case <-rn.electionTimer.C: // If election timer expires, start election
		rn.log.Info("election timer expires, start election")
	}
}

// ResetElectionTimer resets election timer to a random time between 150ms and 300ms.
func (rn *RanniNode) ResetElectionTimer() {
	// Generate a random number generator, make sure it's not the same every time
	randGen := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Generate a random time between 150ms and 300ms
	electTime := randGen.Intn(150) + 150
	rn.electionTimer.Reset(time.Millisecond * time.Duration(electTime))
}

// Run starts the RanniNode.
func (rn *RanniNode) Run() {
	go rn.Heartbeat()
	go rn.Election()
}
