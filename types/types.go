package types

import (
	"encoding/json"
	"time"
)

// NodeID represents a unique identifier for a Raft node
type NodeID string

// Term represents a Raft term number
type Term uint64

// LogIndex represents an index in the Raft log
type LogIndex uint64

// NodeState represents the current state of a Raft node
type NodeState int

const (
	// Follower is the initial state - nodes start as followers
	Follower NodeState = iota
	// Candidate state - node is requesting votes to become leader
	Candidate
	// Leader state - node is the current leader
	Leader
)

// String returns string representation of NodeState
func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry represents a single entry in the Raft log
// Each entry contains a command to be applied to the state machine
type LogEntry struct {
	Term Term `json:"term"`
	// Index position in log
	Index LogIndex `json:"index"`
	// Command to apply to state machine (key-value operations)
	Command Command `json:"command"`
	// Timestamp when entry was created
	Timestamp time.Time `json:"timestamp"`
}

// Command represents a state machine command (key-value operations)
type Command struct {
	// Type of operation: "set", "delete", "get"
	Type string `json:"type"`
	// Key for the operation
	Key string `json:"key"`
	// Value for set operations (empty for delete/get)
	Value string `json:"value,omitempty"`
	// ClientID for tracking client requests
	ClientID string `json:"client_id,omitempty"`
	// RequestID for deduplication
	RequestID string `json:"request_id,omitempty"`
}

// CommandResult represents the result of applying a command
type CommandResult struct {
	// Success indicates if command was applied successfully
	Success bool `json:"success"`
	// Value returned by get operations
	Value string `json:"value,omitempty"`
	// Error message if command failed
	Error string `json:"error,omitempty"`
	// Index of the log entry where command was applied
	Index LogIndex `json:"index"`
}

// PersistentState represents state that must survive server restarts
// This is stored on disk and loaded on startup
type PersistentState struct {
	// CurrentTerm - latest term server has seen
	CurrentTerm Term `json:"current_term"`
	// VotedFor - candidateId that received vote in current term (null if none)
	VotedFor *NodeID `json:"voted_for"`
	// Log - log entries; each entry contains command for state machine
	Log []LogEntry `json:"log"`
}

// VolatileState represents state that doesn't need to persist across restarts
type VolatileState struct {
	// CommitIndex - index of highest log entry known to be committed
	CommitIndex LogIndex `json:"commit_index"`
	// LastApplied - index of highest log entry applied to state machine
	LastApplied LogIndex `json:"last_applied"`
	// Current state of this node
	State NodeState `json:"state"`
}

// LeaderState represents volatile state maintained only by leaders
// Reinitialized after each election
type LeaderState struct {
	// NextIndex - for each server, index of next log entry to send
	NextIndex map[NodeID]LogIndex `json:"next_index"`
	// MatchIndex - for each server, index of highest log entry known to be replicated
	MatchIndex map[NodeID]LogIndex `json:"match_index"`
}

// VoteRequest represents RequestVote RPC request
type VoteRequest struct {
	// Term - candidate's term
	Term Term `json:"term"`
	// CandidateID - candidate requesting vote
	CandidateID NodeID `json:"candidate_id"`
	// LastLogIndex - index of candidate's last log entry
	LastLogIndex LogIndex `json:"last_log_index"`
	// LastLogTerm - term of candidate's last log entry
	LastLogTerm Term `json:"last_log_term"`
}

// VoteResponse represents RequestVote RPC response
type VoteResponse struct {
	// Term - currentTerm, for candidate to update itself
	Term Term `json:"term"`
	// VoteGranted - true means candidate received vote
	VoteGranted bool `json:"vote_granted"`
}

// AppendEntriesRequest represents AppendEntries RPC request
type AppendEntriesRequest struct {
	// Term - leader's term
	Term Term `json:"term"`
	// LeaderID - so follower can redirect clients
	LeaderID NodeID `json:"leader_id"`
	// PrevLogIndex - index of log entry immediately preceding new ones
	PrevLogIndex LogIndex `json:"prev_log_index"`
	// PrevLogTerm - term of prevLogIndex entry
	PrevLogTerm Term `json:"prev_log_term"`
	// Entries - log entries to store (empty for heartbeat)
	Entries []LogEntry `json:"entries"`
	// LeaderCommit - leader's commitIndex
	LeaderCommit LogIndex `json:"leader_commit"`
}

// AppendEntriesResponse represents AppendEntries RPC response
type AppendEntriesResponse struct {
	// Term - currentTerm, for leader to update itself
	Term Term `json:"term"`
	// Success - true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool `json:"success"`
	// ConflictIndex - optimization: index where log conflict begins
	ConflictIndex LogIndex `json:"conflict_index,omitempty"`
	// ConflictTerm - optimization: term of conflicting entry
	ConflictTerm Term `json:"conflict_term,omitempty"`
}

// ClientRequest represents a client request to the key-value store
type ClientRequest struct {
	// Command to execute
	Command Command `json:"command"`
	// Timeout for the request
	Timeout time.Duration `json:"timeout,omitempty"`
}

// ClientResponse represents response to client request
type ClientResponse struct {
	// Result of the command execution
	Result CommandResult `json:"result"`
	// LeaderID if this node is not the leader (for client redirection)
	LeaderID *NodeID `json:"leader_id,omitempty"`
	// Error message if request failed
	Error string `json:"error,omitempty"`
}

// NodeInfo represents information about a cluster node
type NodeInfo struct {
	// ID of the node
	ID NodeID `json:"id"`
	// Address where node can be reached
	Address string `json:"address"`
	// Current state of the node
	State NodeState `json:"state"`
	// Current term
	Term Term `json:"term"`
	// Whether this node is the leader
	IsLeader bool `json:"is_leader"`
}

// ClusterInfo represents information about the entire cluster
type ClusterInfo struct {
	// Information about all nodes
	Nodes []NodeInfo `json:"nodes"`
	// ID of current leader (if any)
	LeaderID *NodeID `json:"leader_id"`
	// Current term across the cluster
	Term Term `json:"term"`
	// Number of committed entries
	CommitIndex LogIndex `json:"commit_index"`
}

// Interfaces define contracts between modules

// StateMachine defines the interface for the key-value state machine
type StateMachine interface {
	// Apply applies a command to the state machine and returns the result
	Apply(cmd Command) CommandResult
	
	// Snapshot creates a snapshot of current state machine state
	Snapshot() ([]byte, error)
	
	// Restore restores state machine from a snapshot
	Restore(snapshot []byte) error
	
	// GetState returns a copy of the current state (for debugging/monitoring)
	GetState() map[string]string
}

// LogStore defines the interface for persistent log storage
type LogStore interface {
	// StoreEntry stores a single log entry
	StoreEntry(entry LogEntry) error
	
	// StoreEntries stores multiple log entries atomically
	StoreEntries(entries []LogEntry) error
	
	// GetEntry retrieves a log entry by index
	GetEntry(index LogIndex) (*LogEntry, error)
	
	// GetEntries retrieves log entries in range [start, end]
	GetEntries(start, end LogIndex) ([]LogEntry, error)
	
	// LastIndex returns the index of the last stored entry
	LastIndex() LogIndex
	
	// FirstIndex returns the index of the first stored entry
	FirstIndex() LogIndex
	
	// DeleteRange deletes entries in range [start, end]
	DeleteRange(start, end LogIndex) error
}

// StableStore defines interface for storing persistent state
type StableStore interface {
	// Set stores a key-value pair
	Set(key string, value []byte) error
	
	// Get retrieves value for a key
	Get(key string) ([]byte, error)
	
	// Delete removes a key
	Delete(key string) error
	
	// Has checks if key exists
	Has(key string) bool
}

// Transport defines the interface for network communication between nodes
type Transport interface {
	// SendVoteRequest sends RequestVote RPC to target node
	SendVoteRequest(target NodeID, req VoteRequest) (*VoteResponse, error)
	
	// SendAppendEntries sends AppendEntries RPC to target node
	SendAppendEntries(target NodeID, req AppendEntriesRequest) (*AppendEntriesResponse, error)
	
	// Start starts the transport layer
	Start() error
	
	// Stop stops the transport layer
	Stop() error
	
	// LocalAddr returns the local address this transport is bound to
	LocalAddr() string
}

// RaftNode defines the main interface for a Raft node
type RaftNode interface {
	// Start starts the Raft node
	Start() error
	
	// Stop stops the Raft node
	Stop() error
	
	// Apply applies a command to the distributed state machine
	Apply(cmd Command, timeout time.Duration) (*CommandResult, error)
	
	// GetState returns current node state information
	GetState() NodeInfo
	
	// GetClusterInfo returns information about the entire cluster
	GetClusterInfo() ClusterInfo
	
	// IsLeader returns true if this node is currently the leader
	IsLeader() bool
	
	// GetLeader returns the current leader ID (if known)
	GetLeader() *NodeID
	
	// AddPeer adds a new peer to the cluster configuration
	AddPeer(nodeID NodeID, address string) error
	
	// RemovePeer removes a peer from the cluster configuration
	RemovePeer(nodeID NodeID) error
}

// Utility functions for working with types

// MarshalJSON marshals LogEntry to JSON
func (le LogEntry) MarshalJSON() ([]byte, error) {
	type Alias LogEntry
	return json.Marshal(&struct {
		Alias
		Timestamp string `json:"timestamp"`
	}{
		Alias:     (Alias)(le),
		Timestamp: le.Timestamp.Format(time.RFC3339Nano),
	})
}

// UnmarshalJSON unmarshals LogEntry from JSON
func (le *LogEntry) UnmarshalJSON(data []byte) error {
	type Alias LogEntry
	aux := &struct {
		*Alias
		Timestamp string `json:"timestamp"`
	}{
		Alias: (*Alias)(le),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	
	if aux.Timestamp != "" {
		t, err := time.Parse(time.RFC3339Nano, aux.Timestamp)
		if err != nil {
			return err
		}
		le.Timestamp = t
	}
	
	return nil
}

// NewLogEntry creates a new log entry with current timestamp
func NewLogEntry(term Term, index LogIndex, cmd Command) LogEntry {
	return LogEntry{
		Term:      term,
		Index:     index,
		Command:   cmd,
		Timestamp: time.Now(),
	}
}

// NewSetCommand creates a new SET command
func NewSetCommand(key, value, clientID, requestID string) Command {
	return Command{
		Type:      "set",
		Key:       key,
		Value:     value,
		ClientID:  clientID,
		RequestID: requestID,
	}
}

// NewGetCommand creates a new GET command
func NewGetCommand(key, clientID, requestID string) Command {
	return Command{
		Type:      "get",
		Key:       key,
		ClientID:  clientID,
		RequestID: requestID,
	}
}

// NewDeleteCommand creates a new DELETE command
func NewDeleteCommand(key, clientID, requestID string) Command {
	return Command{
		Type:      "delete",
		Key:       key,
		ClientID:  clientID,
		RequestID: requestID,
	}
}