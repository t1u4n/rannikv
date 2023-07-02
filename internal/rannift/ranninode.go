package rannift

import (
	"context"
	ranniftpb "github.com/TianLuan99/RanniKV.git/internal/grpc"
	"go.uber.org/zap"
)

// NodeState is the type for RanniNode state.
type NodeState int

// NodeState constants.
const (
	Follower = iota
	Candidate
	Leader
)

// RanniNode is the struct that represents a node in RanniKV.
type RanniNode struct {
	// RPC server
	ranniftpb.UnimplementedRanniftServiceServer

	// Logger
	log *zap.Logger

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
func (rn *RanniNode) AppendEntries(context.Context, *ranniftpb.AppendEntriesRequest) (*ranniftpb.AppendEntriesResponse, error) {
	resp := &ranniftpb.AppendEntriesResponse{
		Term:    rn.currentTerm,
		Success: true,
	}
	return resp, nil
}

// NewRanniNode creates a new RanniNode.
func NewRanniNode() (*RanniNode, error) {
	// TODO: Change to production logger.
	logger, err := zap.NewDevelopment()
	if err != nil {
		return nil, err
	}

	rn := &RanniNode{
		log:         logger,
		currentTerm: 0,
		votedFor:    0,
		logs:        []*ranniftpb.LogEntry{},
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   []int32{},
		matchIndex:  []int32{},
		state:       0,
	}
	return rn, nil
}
