package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"log"

	"github.com/ismael144/distributed-kv-store/storage"
	"github.com/ismael144/distributed-kv-store/transport"
	"github.com/ismael144/distributed-kv-store/types"
)

// Node implements the core Raft consensus algorithm
// This is where all the Raft magic happens - leader election, log replication, etc.
type Node struct {
	mu sync.RWMutex

	// Node identification
	id    types.NodeID
	peers map[types.NodeID]string // NodeID -> Address mapping

	// Raft state (as defined in the paper)
	// Persistent state on all servers
	currentTerm types.Term
	votedFor    *types.NodeID
	log         types.LogStore

	// Volatile state on all servers
	commitIndex types.LogIndex
	lastApplied types.LogIndex
	state       types.NodeState

	// Volatile state on leaders (reinitialized after election)
	nextIndex  map[types.NodeID]types.LogIndex
	matchIndex map[types.NodeID]types.LogIndex

	// Storage components
	stableStore  types.StableStore
	stateMachine types.StateMachine
	transport    types.Transport

	// Leader information
	currentLeader *types.NodeID
	leaderCommit  types.LogIndex

	// Election and heartbeat timers
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// Election timeout configuration (randomized between min and max)
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration

	// Channels for coordination
	shutdownCh chan struct{}
	applyCh    chan applyMsg

	// Client request handling
	pendingRequests map[string]*pendingRequest
	requestsMu      sync.RWMutex

	// Metrics and monitoring
	stats NodeStats

	// Configuration
	config NodeConfig

	// Background goroutine control
	running    int32 // atomic
	shutdownWg sync.WaitGroup
}

// NodeConfig holds configuration for a Raft node
type NodeConfig struct {
	// Election timeout bounds (randomized between these values)
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// Heartbeat interval (should be much less than election timeout)
	HeartbeatInterval time.Duration

	// Maximum number of log entries to send in a single AppendEntries RPC
	MaxEntriesPerMessage int

	// Apply command timeout
	ApplyTimeout time.Duration

	// Enable/disable various optimizations
	EnableBatching       bool
	EnablePipelining     bool
	EnableLeaderSticking bool
}

// DefaultConfig returns a default node configuration
func DefaultConfig() NodeConfig {
	return NodeConfig{
		ElectionTimeoutMin:   150 * time.Millisecond,
		ElectionTimeoutMax:   300 * time.Millisecond,
		HeartbeatInterval:    50 * time.Millisecond,
		MaxEntriesPerMessage: 100,
		ApplyTimeout:         5 * time.Second,
		EnableBatching:       true,
		EnablePipelining:     false, // Advanced feature
		EnableLeaderSticking: true,
	}
}

// NodeStats tracks various metrics about the node
type NodeStats struct {
	// Basic stats
	CurrentTerm  types.Term      `json:"current_term"`
	State        types.NodeState `json:"state"`
	CommitIndex  types.LogIndex  `json:"commit_index"`
	LastApplied  types.LogIndex  `json:"last_applied"`
	LastLogIndex types.LogIndex  `json:"last_log_index"`
	LastLogTerm  types.Term      `json:"last_log_term"`

	// Election stats
	ElectionsStarted uint64 `json:"elections_started"`
	ElectionsWon     uint64 `json:"elections_won"`
	VotesReceived    uint64 `json:"votes_received"`
	VotesCast        uint64 `json:"votes_cast"`

	// Replication stats
	EntriesAppended    uint64 `json:"entries_appended"`
	EntriesCommitted   uint64 `json:"entries_committed"`
	EntriesApplied     uint64 `json:"entries_applied"`
	HeartbeatsSent     uint64 `json:"heartbeats_sent"`
	HeartbeatsReceived uint64 `json:"heartbeats_received"`

	// Client request stats
	ClientRequests        uint64 `json:"client_requests"`
	ClientRequestsSuccess uint64 `json:"client_requests_success"`
	ClientRequestsFailed  uint64 `json:"client_requests_failed"`

	// Error stats
	ReplicationErrors uint64 `json:"replication_errors"`
	TransportErrors   uint64 `json:"transport_errors"`

	// Timing stats
	LastElectionTime  time.Time `json:"last_election_time"`
	LastHeartbeatTime time.Time `json:"last_heartbeat_time"`
	BecameLeaderTime  time.Time `json:"became_leader_time"`
}

// applyMsg represents a message to apply to the state machine
type applyMsg struct {
	Index    types.LogIndex
	Command  types.Command
	ResultCh chan types.CommandResult
}

// pendingRequest tracks a client request waiting for completion
type pendingRequest struct {
	Command   types.Command
	Index     types.LogIndex
	Term      types.Term
	ResultCh  chan types.CommandResult
	Timeout   time.Duration
	StartTime time.Time
}

// NewNode creates a new Raft node
func NewNode(
	id types.NodeID,
	peers map[types.NodeID]string,
	logStore types.LogStore,
	stableStore types.StableStore,
	stateMachine types.StateMachine,
	transport types.Transport,
	config NodeConfig,
) (*Node, error) {

	node := &Node{
		id:           id,
		peers:        make(map[types.NodeID]string),
		log:          logStore,
		stableStore:  stableStore,
		stateMachine: stateMachine,
		transport:    transport,
		config:       config,

		// Initialize Raft state
		state:       types.Follower,
		commitIndex: 0,
		lastApplied: 0,

		// Initialize leader state
		nextIndex:  make(map[types.NodeID]types.LogIndex),
		matchIndex: make(map[types.NodeID]types.LogIndex),

        // Initialize channels
        shutdownCh:      make(chan struct{}),
        applyCh:         make(chan applyMsg, 100),
        pendingRequests: make(map[string]*pendingRequest),

        // Initialize timers - THIS IS THE CHANGE
        electionTimer:      time.NewTimer(0), // Initialize with a duration of 0
        heartbeatTimer:     time.NewTimer(0),
        electionTimeoutMin: config.ElectionTimeoutMin,
        electionTimeoutMax: config.ElectionTimeoutMax,
        heartbeatInterval:  config.HeartbeatInterval,
    }

    // Immediately stop the timers after creation, as they are not yet running.
    node.electionTimer.Stop()
    node.heartbeatTimer.Stop()

	// Copy peers
	for nodeID, addr := range peers {
		node.peers[nodeID] = addr
	}

	// Load persistent state
	if err := node.loadPersistentState(); err != nil {
		return nil, fmt.Errorf("failed to load persistent state: %w", err)
	}

	// Set up transport handlers
	node.setupTransportHandlers()

	return node, nil
}

func (n *Node) Start() error {
	if !atomic.CompareAndSwapInt32(&n.running, 0, 1) {
		return fmt.Errorf("node is already running")
	}

	// Start transport
	if err := n.transport.Start(); err != nil {
		atomic.StoreInt32(&n.running, 0)
		return fmt.Errorf("failed to start transport: %w", err)
	}

	// ✅ Initialize election timer before goroutines
	n.resetElectionTimer()


	// Start background goroutines
	n.shutdownWg.Add(3)
	go n.runMainLoop()
	go n.runApplyLoop()
	go n.runRequestCleanup()

	return nil
}

// Stop stops the Raft node
func (n *Node) Stop() error {
	if !atomic.CompareAndSwapInt32(&n.running, 1, 0) {
		return fmt.Errorf("node is not running")
	}

	// Signal shutdown
	close(n.shutdownCh)

	// Stop timers
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}

	// Wait for background goroutines to finish
	n.shutdownWg.Wait()

	// Stop transport
	return n.transport.Stop()
}

// setupTransportHandlers configures transport message handlers
func (n *Node) setupTransportHandlers() {
	if httpTransport, ok := n.transport.(*transport.HTTPTransport); ok {
		httpTransport.SetVoteHandler(n.handleRequestVote)
		httpTransport.SetAppendEntriesHandler(n.handleAppendEntries)
	}
}

// loadPersistentState loads persistent state from storage
func (n *Node) loadPersistentState() error {
	// Load current term
	currentTerm, err := storage.GetCurrentTerm(n.stableStore)
	if err != nil {
		return fmt.Errorf("failed to load current term: %w", err)
	}
	n.currentTerm = currentTerm

	// Load voted for
	votedFor, err := storage.GetVotedFor(n.stableStore)
	if err != nil {
		return fmt.Errorf("failed to load voted for: %w", err)
	}
	n.votedFor = votedFor

	return nil
}

// savePersistentState saves persistent state to storage
func (n *Node) savePersistentState() error {
	// Save current term
	if err := storage.StoreCurrentTerm(n.stableStore, n.currentTerm); err != nil {
		return fmt.Errorf("failed to save current term: %w", err)
	}

	// Save voted for
	if err := storage.StoreVotedFor(n.stableStore, n.votedFor); err != nil {
		return fmt.Errorf("failed to save voted for: %w", err)
	}

	return nil
}

// Main event loop - handles timers and state transitions
func (n *Node) runMainLoop() {
	defer n.shutdownWg.Done()

	if n.electionTimer.C == nil {
		log.Fatal("electionTimer is nil")
		return
	}

	for {
		select {
		case <-n.shutdownCh:
			return
		case <-n.electionTimer.C:
			n.handleElectionTimeout()
			n.resetElectionTimer()
		case <-n.heartbeatTimer.C:
			n.handleHeartbeatTimeout()
			n.resetHeartbeatTimer()
		}
	}
}

// Apply loop - applies committed entries to state machine
func (n *Node) runApplyLoop() {
	defer n.shutdownWg.Done()

	for {
		select {
		case <-n.shutdownCh:
			return
		case msg := <-n.applyCh:
			result := n.stateMachine.Apply(msg.Command)
			result.Index = msg.Index

			select {
			case msg.ResultCh <- result:
			case <-time.After(1 * time.Second):
				// Timeout sending result
			}

			n.mu.Lock()
			n.lastApplied = msg.Index
			n.stats.EntriesApplied++
			n.mu.Unlock()
		}
	}
}

// Cleanup expired client requests
func (n *Node) runRequestCleanup() {
	defer n.shutdownWg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.shutdownCh:
			return
		case <-ticker.C:
			n.cleanupExpiredRequests()
		}
	}
}

// cleanupExpiredRequests removes expired pending requests
func (n *Node) cleanupExpiredRequests() {
	n.requestsMu.Lock()
	defer n.requestsMu.Unlock()

	now := time.Now()
	for key, req := range n.pendingRequests {
		if now.Sub(req.StartTime) > req.Timeout {
			// Send timeout error
			select {
			case req.ResultCh <- types.CommandResult{
				Success: false,
				Error:   "request timeout",
			}:
			default:
			}

			delete(n.pendingRequests, key)
		}
	}
}

// resetElectionTimer resets the election timeout with a random duration
func (n *Node) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}

	// Randomize election timeout to prevent split votes
	timeout := n.electionTimeoutMin + time.Duration(rand.Int63n(int64(n.electionTimeoutMax-n.electionTimeoutMin)))
	n.electionTimer = time.NewTimer(timeout)
}

// resetHeartbeatTimer resets the heartbeat timer (only for leaders)
func (n *Node) resetHeartbeatTimer() {
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	n.heartbeatTimer = time.NewTimer(n.heartbeatInterval)
}

// handleElectionTimeout handles election timeout - start new election
func (n *Node) handleElectionTimeout() {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Only followers and candidates should start elections
	if n.state == types.Leader {
		return
	}

	// Start new election
	n.startElection()
}

// handleHeartbeatTimeout handles heartbeat timeout - send heartbeats to followers
func (n *Node) handleHeartbeatTimeout() {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Only leaders send heartbeats
	if n.state != types.Leader {
		return
	}

	// Send heartbeats to all followers
	n.sendHeartbeats()

	// Reset heartbeat timer
	n.resetHeartbeatTimer()
}

// startElection initiates a new leader election
func (n *Node) startElection() {
	// Increment current term
	n.currentTerm++
	n.votedFor = &n.id
	n.state = types.Candidate

	// Update stats
	n.stats.ElectionsStarted++
	n.stats.LastElectionTime = time.Now()

	// Save persistent state
	if err := n.savePersistentState(); err != nil {
		// Log error in production
		fmt.Printf("Failed to save persistent state during election: %v\n", err)
	}

	// Reset election timer
	n.resetElectionTimer()

	// Vote for self
	votes := 1
	needed := len(n.peers)/2 + 1

	// If we're the only node, become leader immediately
	if len(n.peers) == 0 {
		n.becomeLeader()
		return
	}

	// Send RequestVote RPCs to all other nodes
	lastLogIndex := n.log.LastIndex()
	lastLogTerm := types.Term(0)
	if lastLogIndex > 0 {
		if entry, err := n.log.GetEntry(lastLogIndex); err == nil {
			lastLogTerm = entry.Term
		}
	}

	voteReq := types.VoteRequest{
		Term:         n.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// Send vote requests concurrently
	var wg sync.WaitGroup
	var mu sync.Mutex

	for peerID := range n.peers {
		wg.Add(1)
		go func(peer types.NodeID) {
			defer wg.Done()

			resp, err := n.transport.SendVoteRequest(peer, voteReq)
			if err != nil {
				n.stats.TransportErrors++
				return
			}

			mu.Lock()
			defer mu.Unlock()

			// Check if we're still a candidate and in the same term
			if n.state != types.Candidate || n.currentTerm != voteReq.Term {
				return
			}

			// If response term is higher, step down
			if resp.Term > n.currentTerm {
				n.stepDown(resp.Term)
				return
			}

			// Count vote if granted
			if resp.VoteGranted {
				votes++
				n.stats.VotesReceived++

				// Check if we have majority
				if votes >= needed {
					n.becomeLeader()
				}
			}
		}(peerID)
	}

	go func() {
		wg.Wait()
		// Election completed
	}()
}

// becomeLeader transitions node to leader state
func (n *Node) becomeLeader() {
	if n.state != types.Candidate {
		return
	}

	n.state = types.Leader
	n.currentLeader = &n.id
	n.stats.ElectionsWon++
	n.stats.BecameLeaderTime = time.Now()

	// Initialize leader state
	lastLogIndex := n.log.LastIndex()
	for peerID := range n.peers {
		n.nextIndex[peerID] = lastLogIndex + 1
		n.matchIndex[peerID] = 0
	}

	// Stop election timer and start heartbeat timer
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	n.resetHeartbeatTimer()

	// Send initial heartbeats
	n.sendHeartbeats()

	fmt.Printf("Node %s became leader for term %d\n", n.id, n.currentTerm)
}

// stepDown transitions node to follower state
func (n *Node) stepDown(term types.Term) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = nil
	}

	n.state = types.Follower
	n.currentLeader = nil

	// Stop heartbeat timer and reset election timer
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	n.resetElectionTimer()

	// Save persistent state
	if err := n.savePersistentState(); err != nil {
		fmt.Printf("Failed to save persistent state during step down: %v\n", err)
	}
}

// sendHeartbeats sends heartbeat messages to all followers
func (n *Node) sendHeartbeats() {
	n.stats.HeartbeatsSent++

	for peerID := range n.peers {
		go n.sendAppendEntries(peerID, true) // true = heartbeat
	}
}

// sendAppendEntries sends AppendEntries RPC to a specific peer
func (n *Node) sendAppendEntries(peerID types.NodeID, heartbeat bool) {
	n.mu.RLock()

	if n.state != types.Leader {
		n.mu.RUnlock()
		return
	}

	nextIndex := n.nextIndex[peerID]
	prevLogIndex := nextIndex - 1
	prevLogTerm := types.Term(0)

	// Get previous log term
	if prevLogIndex > 0 {
		if entry, err := n.log.GetEntry(prevLogIndex); err == nil {
			prevLogTerm = entry.Term
		} else {
			n.mu.RUnlock()
			return
		}
	}

	// Prepare entries to send
	var entries []types.LogEntry
	if !heartbeat {
		lastLogIndex := n.log.LastIndex()
		if nextIndex <= lastLogIndex {
			// Calculate how many entries to send
			endIndex := nextIndex + types.LogIndex(n.config.MaxEntriesPerMessage) - 1
			if endIndex > lastLogIndex {
				endIndex = lastLogIndex
			}

			// Get entries
			if entriesSlice, err := n.log.GetEntries(nextIndex, endIndex); err == nil {
				entries = entriesSlice
			}
		}
	}

	req := types.AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}

	n.mu.RUnlock()

	// Send the request
	resp, err := n.transport.SendAppendEntries(peerID, req)
	if err != nil {
		n.mu.Lock()
		n.stats.TransportErrors++
		n.mu.Unlock()
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Check if we're still leader and in the same term
	if n.state != types.Leader || n.currentTerm != req.Term {
		return
	}

	// If response term is higher, step down
	if resp.Term > n.currentTerm {
		n.stepDown(resp.Term)
		return
	}

	if resp.Success {
		// Update next and match indices
		if len(entries) > 0 {
			n.nextIndex[peerID] = entries[len(entries)-1].Index + 1
			n.matchIndex[peerID] = entries[len(entries)-1].Index
			n.updateCommitIndex()
		}
	} else {
		// Decrement nextIndex and retry
		if n.nextIndex[peerID] > 1 {
			n.nextIndex[peerID]--
		}

		// Optimization: use conflict information if provided
		if resp.ConflictIndex > 0 {
			n.nextIndex[peerID] = resp.ConflictIndex
		}

		n.stats.ReplicationErrors++

		// Retry immediately for failed append
		go n.sendAppendEntries(peerID, false)
	}
}

// updateCommitIndex updates the commit index based on majority replication
func (n *Node) updateCommitIndex() {
	if n.state != types.Leader {
		return
	}

	// Find the highest index that's replicated on majority
	lastLogIndex := n.log.LastIndex()

	for index := n.commitIndex + 1; index <= lastLogIndex; index++ {
		// Count how many nodes have this index
		count := 1 // Count self
		for _, matchIndex := range n.matchIndex {
			if matchIndex >= index {
				count++
			}
		}

		// Check if majority has this index
		if count > len(n.peers)/2 {
			// Verify the entry is from current term (Raft safety requirement)
			if entry, err := n.log.GetEntry(index); err == nil && entry.Term == n.currentTerm {
				n.commitIndex = index
				n.stats.EntriesCommitted++

				// Apply committed entries
				for i := n.lastApplied + 1; i <= n.commitIndex; i++ {
					if entry, err := n.log.GetEntry(i); err == nil {
						applyMsg := applyMsg{
							Index:    i,
							Command:  entry.Command,
							ResultCh: make(chan types.CommandResult, 1),
						}

						select {
						case n.applyCh <- applyMsg:
							// Wait for result and notify client if needed
							go n.handleApplyResult(applyMsg)
						default:
							// Apply channel full
						}
					}
				}
			}
		} else {
			break // No point checking higher indices
		}
	}
}

// handleApplyResult handles the result of applying a command
func (n *Node) handleApplyResult(msg applyMsg) {
	select {
	case result := <-msg.ResultCh:
		// Notify waiting client request if any
		n.requestsMu.Lock()
		requestKey := fmt.Sprintf("%s:%s", msg.Command.ClientID, msg.Command.RequestID)
		if req, exists := n.pendingRequests[requestKey]; exists {
			select {
			case req.ResultCh <- result:
			default:
			}
			delete(n.pendingRequests, requestKey)
		}
		n.requestsMu.Unlock()

	case <-time.After(5 * time.Second):
		// Timeout waiting for apply result
	}
}

// RequestVote RPC handler
func (n *Node) handleRequestVote(req types.VoteRequest) types.VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := types.VoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	// Reply false if term < currentTerm
	if req.Term < n.currentTerm {
		return resp
	}

	// If RPC request or response contains term T > currentTerm, set currentTerm = T and convert to follower
	if req.Term > n.currentTerm {
		n.stepDown(req.Term)
	}

	// Update response term
	resp.Term = n.currentTerm

	// Vote for candidate if:
	// 1. We haven't voted for anyone else in this term
	// 2. Candidate's log is at least as up-to-date as ours
	if (n.votedFor == nil || *n.votedFor == req.CandidateID) && n.isLogUpToDate(req.LastLogIndex, req.LastLogTerm) {
		resp.VoteGranted = true
		n.votedFor = &req.CandidateID
		n.stats.VotesCast++

		// Reset election timer since we granted a vote
		n.resetElectionTimer()

		// Save persistent state
		if err := n.savePersistentState(); err != nil {
			fmt.Printf("Failed to save persistent state after voting: %v\n", err)
		}
	}

	return resp
}

// AppendEntries RPC handler
func (n *Node) handleAppendEntries(req types.AppendEntriesRequest) types.AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := types.AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
	}

	// Reply false if term < currentTerm
	if req.Term < n.currentTerm {
		return resp
	}

	// If RPC request contains term T > currentTerm, set currentTerm = T and convert to follower
	if req.Term > n.currentTerm {
		n.stepDown(req.Term)
	}

	// Update response term
	resp.Term = n.currentTerm

	// Reset election timer - we heard from leader
	n.resetElectionTimer()
	n.currentLeader = &req.LeaderID
	n.stats.HeartbeatsReceived++

	// Ensure we're a follower
	if n.state != types.Follower {
		n.state = types.Follower
	}

	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > n.log.LastIndex() {
			// We don't have enough entries
			resp.ConflictIndex = n.log.LastIndex() + 1
			return resp
		}

		if entry, err := n.log.GetEntry(req.PrevLogIndex); err != nil || entry.Term != req.PrevLogTerm {
			// Find conflict term
			conflictTerm := types.Term(0)
			if err == nil {
				conflictTerm = entry.Term
			}

			// Find first index of conflicting term
			conflictIndex := req.PrevLogIndex
			for conflictIndex > 1 {
				if entry, err := n.log.GetEntry(conflictIndex - 1); err != nil || entry.Term != conflictTerm {
					break
				}
				conflictIndex--
			}

			resp.ConflictIndex = conflictIndex
			resp.ConflictTerm = conflictTerm
			return resp
		}
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	for i, entry := range req.Entries {
		entryIndex := req.PrevLogIndex + types.LogIndex(i) + 1

		if existingEntry, err := n.log.GetEntry(entryIndex); err == nil {
			if existingEntry.Term != entry.Term {
				// Delete conflicting entry and all following entries
				lastIndex := n.log.LastIndex()
				if entryIndex <= lastIndex {
					n.log.DeleteRange(entryIndex, lastIndex)
				}
				break
			}
		}
	}

	// Append any new entries not already in the log
	for i, entry := range req.Entries {
		entryIndex := req.PrevLogIndex + types.LogIndex(i) + 1

		if entryIndex > n.log.LastIndex() {
			// This is a new entry
			if err := n.log.StoreEntry(entry); err != nil {
				fmt.Printf("Failed to store log entry: %v\n", err)
				return resp
			}
			n.stats.EntriesAppended++
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = req.LeaderCommit
		if lastEntryIndex := n.log.LastIndex(); n.commitIndex > lastEntryIndex {
			n.commitIndex = lastEntryIndex
		}

		// Apply committed entries
		for i := n.lastApplied + 1; i <= n.commitIndex; i++ {
			if entry, err := n.log.GetEntry(i); err == nil {
				applyMsg := applyMsg{
					Index:    i,
					Command:  entry.Command,
					ResultCh: make(chan types.CommandResult, 1),
				}

				select {
				case n.applyCh <- applyMsg:
				default:
					// Apply channel full
				}
			}
		}
	}

	resp.Success = true
	return resp
}

// isLogUpToDate checks if candidate's log is at least as up-to-date as ours
func (n *Node) isLogUpToDate(lastLogIndex types.LogIndex, lastLogTerm types.Term) bool {
	ourLastIndex := n.log.LastIndex()
	ourLastTerm := types.Term(0)

	if ourLastIndex > 0 {
		if entry, err := n.log.GetEntry(ourLastIndex); err == nil {
			ourLastTerm = entry.Term
		}
	}

	// Candidate's log is up-to-date if:
	// 1. Last log term is greater than ours, OR
	// 2. Last log terms are equal and last log index is >= ours
	return lastLogTerm > ourLastTerm || (lastLogTerm == ourLastTerm && lastLogIndex >= ourLastIndex)
}

// Public interface methods

// Apply applies a command to the distributed state machine
func (n *Node) Apply(cmd types.Command, timeout time.Duration) (*types.CommandResult, error) {
	n.mu.RLock()

	// Only leaders can accept client requests
	if n.state != types.Leader {
		leader := n.currentLeader
		n.mu.RUnlock()

		return &types.CommandResult{
			Success: false,
			Error:   "not leader",
		}, fmt.Errorf("not leader, current leader: %v", leader)
	}

	// Create log entry
	entry := types.NewLogEntry(n.currentTerm, n.log.LastIndex()+1, cmd)

	n.mu.RUnlock()

	// Store entry in log
	if err := n.log.StoreEntry(entry); err != nil {
		return &types.CommandResult{
			Success: false,
			Error:   "failed to store log entry",
		}, fmt.Errorf("failed to store log entry: %w", err)
	}

	// Create pending request
	resultCh := make(chan types.CommandResult, 1)
	requestKey := fmt.Sprintf("%s:%s", cmd.ClientID, cmd.RequestID)

	n.requestsMu.Lock()
	n.pendingRequests[requestKey] = &pendingRequest{
		Command:   cmd,
		Index:     entry.Index,
		Term:      entry.Term,
		ResultCh:  resultCh,
		Timeout:   timeout,
		StartTime: time.Now(),
	}
	n.requestsMu.Unlock()

	// Update stats
	n.mu.Lock()
	n.stats.ClientRequests++
	n.mu.Unlock()

	// Start replication to followers
	n.mu.RLock()
	for peerID := range n.peers {
		go n.sendAppendEntries(peerID, false)
	}
	n.mu.RUnlock()

	// Wait for result or timeout
	select {
	case result := <-resultCh:
		n.mu.Lock()
		if result.Success {
			n.stats.ClientRequestsSuccess++
		} else {
			n.stats.ClientRequestsFailed++
		}
		n.mu.Unlock()
		return &result, nil

	case <-time.After(timeout):
		// Clean up pending request
		n.requestsMu.Lock()
		delete(n.pendingRequests, requestKey)
		n.requestsMu.Unlock()

		n.mu.Lock()
		n.stats.ClientRequestsFailed++
		n.mu.Unlock()

		return &types.CommandResult{
			Success: false,
			Error:   "timeout",
		}, fmt.Errorf("command application timeout")
	}
}

// GetState returns current node state information
func (n *Node) GetState() types.NodeInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return types.NodeInfo{
		ID:       n.id,
		Address:  n.transport.LocalAddr(),
		State:    n.state,
		Term:     n.currentTerm,
		IsLeader: n.state == types.Leader,
	}
}

// GetClusterInfo returns information about the entire cluster
func (n *Node) GetClusterInfo() types.ClusterInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()

	var nodes []types.NodeInfo

	// Add self
	nodes = append(nodes, types.NodeInfo{
		ID:       n.id,
		Address:  n.transport.LocalAddr(),
		State:    n.state,
		Term:     n.currentTerm,
		IsLeader: n.state == types.Leader,
	})

	// Add peers (we don't know their state, so we'll mark them as unknown)
	for peerID, addr := range n.peers {
		nodes = append(nodes, types.NodeInfo{
			ID:       peerID,
			Address:  addr,
			State:    types.Follower, // Assumption
			Term:     n.currentTerm,  // Assumption
			IsLeader: false,
		})
	}

	return types.ClusterInfo{
		Nodes:       nodes,
		LeaderID:    n.currentLeader,
		Term:        n.currentTerm,
		CommitIndex: n.commitIndex,
	}
}

// IsLeader returns true if this node is currently the leader
func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state == types.Leader
}

// GetLeader returns the current leader ID (if known)
func (n *Node) GetLeader() *types.NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentLeader
}

// AddPeer adds a new peer to the cluster configuration
func (n *Node) AddPeer(nodeID types.NodeID, address string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.peers[nodeID] = address

	// Add to transport
	if httpTransport, ok := n.transport.(*transport.HTTPTransport); ok {
		httpTransport.AddPeer(nodeID, address)
	}

	// Initialize leader state if we're leader
	if n.state == types.Leader {
		n.nextIndex[nodeID] = n.log.LastIndex() + 1
		n.matchIndex[nodeID] = 0
	}

	return nil
}

// RemovePeer removes a peer from the cluster configuration
func (n *Node) RemovePeer(nodeID types.NodeID) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.peers, nodeID)

	// Remove from transport
	if httpTransport, ok := n.transport.(*transport.HTTPTransport); ok {
		httpTransport.RemovePeer(nodeID)
	}

	// Clean up leader state
	delete(n.nextIndex, nodeID)
	delete(n.matchIndex, nodeID)

	return nil
}

// GetStats returns current node statistics
func (n *Node) GetStats() NodeStats {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Update current stats
	n.stats.CurrentTerm = n.currentTerm
	n.stats.State = n.state
	n.stats.CommitIndex = n.commitIndex
	n.stats.LastApplied = n.lastApplied
	n.stats.LastLogIndex = n.log.LastIndex()

	if n.stats.LastLogIndex > 0 {
		if entry, err := n.log.GetEntry(n.stats.LastLogIndex); err == nil {
			n.stats.LastLogTerm = entry.Term
		}
	}

	return n.stats
}
