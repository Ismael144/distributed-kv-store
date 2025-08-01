package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/ismael144/distributed-kv-store/types"
)

// HTTPTransport implements the Transport interface using HTTP
// This handles all network communication between Raft nodes
type HTTPTransport struct {
	mu sync.RWMutex
	
	// Local node information
	localAddr string
	localID   types.NodeID
	
	// HTTP server for receiving requests
	server *http.Server
	mux    *http.ServeMux
	
	// HTTP client for sending requests
	client *http.Client
	
	// Peer address mapping (NodeID -> Address)
	peers map[types.NodeID]string
	
	// Request handlers
	voteHandler          func(types.VoteRequest) types.VoteResponse
	appendEntriesHandler func(types.AppendEntriesRequest) types.AppendEntriesResponse
	
	// Metrics
	requestCount  uint64
	responseCount uint64
	errorCount    uint64
	
	// Shutdown channel
	shutdownCh chan struct{}
}

// NewHTTPTransport creates a new HTTP-based transport
func NewHTTPTransport(localAddr string, localID types.NodeID) *HTTPTransport {
	transport := &HTTPTransport{
		localAddr:  localAddr,
		localID:    localID,
		peers:      make(map[types.NodeID]string),
		mux:        http.NewServeMux(),
		shutdownCh: make(chan struct{}),
		client: &http.Client{
			Timeout: 5 * time.Second, // 5 second timeout for requests
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     30 * time.Second,
			},
		},
	}
	
	// Set up HTTP routes
	transport.setupRoutes()
	
	// Create HTTP server
	transport.server = &http.Server{
		Addr:         localAddr,
		Handler:      transport.mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	
	return transport
}

// setupRoutes configures HTTP routes for Raft RPCs
func (t *HTTPTransport) setupRoutes() {
	// RequestVote RPC endpoint
	t.mux.HandleFunc("/raft/vote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		
		t.handleVoteRequest(w, r)
	})
	
	// AppendEntries RPC endpoint
	t.mux.HandleFunc("/raft/append", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		
		t.handleAppendEntriesRequest(w, r)
	})
	
	// Health check endpoint
	t.mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	
	// Statistics endpoint
	t.mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		t.handleStatsRequest(w, r)
	})
}

// Start starts the HTTP transport
func (t *HTTPTransport) Start() error {
	go func() {
		if err := t.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Log error in production
			fmt.Printf("HTTP server error: %v\n", err)
		}
	}()
	
	// Wait a moment for server to start
	time.Sleep(100 * time.Millisecond)
	
	return nil
}

// Stop stops the HTTP transport
func (t *HTTPTransport) Stop() error {
	close(t.shutdownCh)
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	return t.server.Shutdown(ctx)
}

// LocalAddr returns the local address this transport is bound to
func (t *HTTPTransport) LocalAddr() string {
	return t.localAddr
}

// SetVoteHandler sets the handler for RequestVote RPCs
func (t *HTTPTransport) SetVoteHandler(handler func(types.VoteRequest) types.VoteResponse) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.voteHandler = handler
}

// SetAppendEntriesHandler sets the handler for AppendEntries RPCs
func (t *HTTPTransport) SetAppendEntriesHandler(handler func(types.AppendEntriesRequest) types.AppendEntriesResponse) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.appendEntriesHandler = handler
}

// AddPeer adds a peer to the transport's peer list
func (t *HTTPTransport) AddPeer(nodeID types.NodeID, address string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[nodeID] = address
}

// RemovePeer removes a peer from the transport's peer list
func (t *HTTPTransport) RemovePeer(nodeID types.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, nodeID)
}

// SendVoteRequest sends RequestVote RPC to target node
func (t *HTTPTransport) SendVoteRequest(target types.NodeID, req types.VoteRequest) (*types.VoteResponse, error) {
	t.mu.RLock()
	addr, exists := t.peers[target]
	t.mu.RUnlock()
	
	if !exists {
		t.errorCount++
		return nil, fmt.Errorf("unknown peer: %s", target)
	}
	
	// Serialize request
	reqData, err := json.Marshal(req)
	if err != nil {
		t.errorCount++
		return nil, fmt.Errorf("failed to marshal vote request: %w", err)
	}
	
	// Send HTTP request
	url := fmt.Sprintf("http://%s/raft/vote", addr)
	resp, err := t.client.Post(url, "application/json", bytes.NewReader(reqData))
	if err != nil {
		t.errorCount++
		return nil, fmt.Errorf("failed to send vote request: %w", err)
	}
	defer resp.Body.Close()
	
	t.requestCount++
	
	if resp.StatusCode != http.StatusOK {
		t.errorCount++
		return nil, fmt.Errorf("vote request failed with status: %d", resp.StatusCode)
	}
	
	// Deserialize response
	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		t.errorCount++
		return nil, fmt.Errorf("failed to read vote response: %w", err)
	}
	
	var voteResp types.VoteResponse
	if err := json.Unmarshal(respData, &voteResp); err != nil {
		t.errorCount++
		return nil, fmt.Errorf("failed to unmarshal vote response: %w", err)
	}
	
	t.responseCount++
	return &voteResp, nil
}

// SendAppendEntries sends AppendEntries RPC to target node
func (t *HTTPTransport) SendAppendEntries(target types.NodeID, req types.AppendEntriesRequest) (*types.AppendEntriesResponse, error) {
	t.mu.RLock()
	addr, exists := t.peers[target]
	t.mu.RUnlock()
	
	if !exists {
		t.errorCount++
		return nil, fmt.Errorf("unknown peer: %s", target)
	}
	
	// Serialize request
	reqData, err := json.Marshal(req)
	if err != nil {
		t.errorCount++
		return nil, fmt.Errorf("failed to marshal append entries request: %w", err)
	}
	
	// Send HTTP request
	url := fmt.Sprintf("http://%s/raft/append", addr)
	resp, err := t.client.Post(url, "application/json", bytes.NewReader(reqData))
	if err != nil {
		t.errorCount++
		return nil, fmt.Errorf("failed to send append entries request: %w", err)
	}
	defer resp.Body.Close()
	
	t.requestCount++
	
	if resp.StatusCode != http.StatusOK {
		t.errorCount++
		return nil, fmt.Errorf("append entries request failed with status: %d", resp.StatusCode)
	}
	
	// Deserialize response
	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		t.errorCount++
		return nil, fmt.Errorf("failed to read append entries response: %w", err)
	}
	
	var appendResp types.AppendEntriesResponse
	if err := json.Unmarshal(respData, &appendResp); err != nil {
		t.errorCount++
		return nil, fmt.Errorf("failed to unmarshal append entries response: %w", err)
	}
	
	t.responseCount++
	return &appendResp, nil
}

// HTTP request handlers

// handleVoteRequest handles incoming RequestVote RPCs
func (t *HTTPTransport) handleVoteRequest(w http.ResponseWriter, r *http.Request) {
	// Read request body
	reqData, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	
	// Deserialize request
	var voteReq types.VoteRequest
	if err := json.Unmarshal(reqData, &voteReq); err != nil {
		http.Error(w, "Failed to unmarshal request", http.StatusBadRequest)
		return
	}
	
	// Get handler
	t.mu.RLock()
	handler := t.voteHandler
	t.mu.RUnlock()
	
	if handler == nil {
		http.Error(w, "Vote handler not set", http.StatusInternalServerError)
		return
	}
	
	// Process request
	voteResp := handler(voteReq)
	
	// Serialize response
	respData, err := json.Marshal(voteResp)
	if err != nil {
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}
	
	// Send response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(respData)
}

// handleAppendEntriesRequest handles incoming AppendEntries RPCs
func (t *HTTPTransport) handleAppendEntriesRequest(w http.ResponseWriter, r *http.Request) {
	// Read request body
	reqData, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	
	// Deserialize request
	var appendReq types.AppendEntriesRequest
	if err := json.Unmarshal(reqData, &appendReq); err != nil {
		http.Error(w, "Failed to unmarshal request", http.StatusBadRequest)
		return
	}
	
	// Get handler
	t.mu.RLock()
	handler := t.appendEntriesHandler
	t.mu.RUnlock()
	
	if handler == nil {
		http.Error(w, "Append entries handler not set", http.StatusInternalServerError)
		return
	}
	
	// Process request
	appendResp := handler(appendReq)
	
	// Serialize response
	respData, err := json.Marshal(appendResp)
	if err != nil {
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}
	
	// Send response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(respData)
}

// handleStatsRequest handles statistics requests
func (t *HTTPTransport) handleStatsRequest(w http.ResponseWriter, r *http.Request) {
	t.mu.RLock()
	stats := map[string]interface{}{
		"local_addr":     t.localAddr,
		"local_id":       t.localID,
		"peer_count":     len(t.peers),
		"request_count":  t.requestCount,
		"response_count": t.responseCount,
		"error_count":    t.errorCount,
		"peers":          t.peers,
	}
	t.mu.RUnlock()
	
	respData, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		http.Error(w, "Failed to marshal stats", http.StatusInternalServerError)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(respData)
}

// GetStats returns transport statistics
func (t *HTTPTransport) GetStats() map[string]interface{} {
	t.mu.RLock()
	defer t.mu.RUnlock()
	
	return map[string]interface{}{
		"local_addr":     t.localAddr,
		"local_id":       t.localID,
		"peer_count":     len(t.peers),
		"request_count":  t.requestCount,
		"response_count": t.responseCount,
		"error_count":    t.errorCount,
	}
}

// Advanced Transport Features

// BatchTransport allows sending multiple requests concurrently
type BatchTransport struct {
	*HTTPTransport
}

// NewBatchTransport creates a transport optimized for batch operations
func NewBatchTransport(localAddr string, localID types.NodeID) *BatchTransport {
	httpTransport := NewHTTPTransport(localAddr, localID)
	
	// Optimize for batch operations
	httpTransport.client.Transport.(*http.Transport).MaxIdleConnsPerHost = 50
	
	return &BatchTransport{HTTPTransport: httpTransport}
}

// SendVoteRequestBatch sends vote requests to multiple nodes concurrently
func (bt *BatchTransport) SendVoteRequestBatch(targets []types.NodeID, req types.VoteRequest) map[types.NodeID]*types.VoteResponse {
	results := make(map[types.NodeID]*types.VoteResponse)
	var wg sync.WaitGroup
	var mu sync.Mutex
	
	for _, target := range targets {
		wg.Add(1)
		go func(nodeID types.NodeID) {
			defer wg.Done()
			
			resp, err := bt.SendVoteRequest(nodeID, req)
			
			mu.Lock()
			if err == nil {
				results[nodeID] = resp
			} else {
				// Log error in production
				fmt.Printf("Vote request to %s failed: %v\n", nodeID, err)
			}
			mu.Unlock()
		}(target)
	}
	
	wg.Wait()
	return results
}

// SendAppendEntriesBatch sends append entries to multiple nodes concurrently
func (bt *BatchTransport) SendAppendEntriesBatch(targets []types.NodeID, reqs map[types.NodeID]types.AppendEntriesRequest) map[types.NodeID]*types.AppendEntriesResponse {
	results := make(map[types.NodeID]*types.AppendEntriesResponse)
	var wg sync.WaitGroup
	var mu sync.Mutex
	
	for _, target := range targets {
		req, exists := reqs[target]
		if !exists {
			continue
		}
		
		wg.Add(1)
		go func(nodeID types.NodeID, request types.AppendEntriesRequest) {
			defer wg.Done()
			
			resp, err := bt.SendAppendEntries(nodeID, request)
			
			mu.Lock()
			if err == nil {
				results[nodeID] = resp
			} else {
				// Log error in production
				fmt.Printf("Append entries to %s failed: %v\n", nodeID, err)
			}
			mu.Unlock()
		}(target, req)
	}
	
	wg.Wait()
	return results
}

// Connection Pool Management

// ConnectionPool manages persistent connections to peers
type ConnectionPool struct {
	mu          sync.RWMutex
	connections map[types.NodeID]*http.Client
	maxConns    int
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(maxConns int) *ConnectionPool {
	return &ConnectionPool{
		connections: make(map[types.NodeID]*http.Client),
		maxConns:    maxConns,
	}
}

// GetConnection returns a connection for the given node
func (cp *ConnectionPool) GetConnection(nodeID types.NodeID) *http.Client {
	cp.mu.RLock()
	client, exists := cp.connections[nodeID]
	cp.mu.RUnlock()
	
	if exists {
		return client
	}
	
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	// Double-check after acquiring write lock
	if client, exists := cp.connections[nodeID]; exists {
		return client
	}
	
	// Create new connection
	client = &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 5,
			IdleConnTimeout:     30 * time.Second,
		},
	}
	
	// Add to pool if we haven't reached the limit
	if len(cp.connections) < cp.maxConns {
		cp.connections[nodeID] = client
	}
	
	return client
}

// CloseConnection closes and removes a connection
func (cp *ConnectionPool) CloseConnection(nodeID types.NodeID) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	if client, exists := cp.connections[nodeID]; exists {
		// Close idle connections
		client.CloseIdleConnections()
		delete(cp.connections, nodeID)
	}
}

// CloseAll closes all connections in the pool
func (cp *ConnectionPool) CloseAll() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	for nodeID, client := range cp.connections {
		client.CloseIdleConnections()
		delete(cp.connections, nodeID)
	}
}

// Transport with Connection Pooling

// PooledHTTPTransport extends HTTPTransport with connection pooling
type PooledHTTPTransport struct {
	*HTTPTransport
	connPool *ConnectionPool
}

// NewPooledHTTPTransport creates a transport with connection pooling
func NewPooledHTTPTransport(localAddr string, localID types.NodeID, maxConns int) *PooledHTTPTransport {
	httpTransport := NewHTTPTransport(localAddr, localID)
	
	return &PooledHTTPTransport{
		HTTPTransport: httpTransport,
		connPool:      NewConnectionPool(maxConns),
	}
}

// SendVoteRequest sends a vote request using a pooled connection
func (pt *PooledHTTPTransport) SendVoteRequest(target types.NodeID, req types.VoteRequest) (*types.VoteResponse, error) {
	pt.mu.RLock()
	addr, exists := pt.peers[target]
	pt.mu.RUnlock()
	
	if !exists {
		pt.errorCount++
		return nil, fmt.Errorf("unknown peer: %s", target)
	}
	
	// Get pooled connection
	client := pt.connPool.GetConnection(target)
	
	// Serialize request
	reqData, err := json.Marshal(req)
	if err != nil {
		pt.errorCount++
		return nil, fmt.Errorf("failed to marshal vote request: %w", err)
	}
	
	// Send HTTP request using pooled connection
	url := fmt.Sprintf("http://%s/raft/vote", addr)
	resp, err := client.Post(url, "application/json", bytes.NewReader(reqData))
	if err != nil {
		pt.errorCount++
		return nil, fmt.Errorf("failed to send vote request: %w", err)
	}
	defer resp.Body.Close()
	
	pt.requestCount++
	
	if resp.StatusCode != http.StatusOK {
		pt.errorCount++
		return nil, fmt.Errorf("vote request failed with status: %d", resp.StatusCode)
	}
	
	// Deserialize response
	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		pt.errorCount++
		return nil, fmt.Errorf("failed to read vote response: %w", err)
	}
	
	var voteResp types.VoteResponse
	if err := json.Unmarshal(respData, &voteResp); err != nil {
		pt.errorCount++
		return nil, fmt.Errorf("failed to unmarshal vote response: %w", err)
	}
	
	pt.responseCount++
	return &voteResp, nil
}

// Stop closes all pooled connections before stopping the transport
func (pt *PooledHTTPTransport) Stop() error {
	pt.connPool.CloseAll()
	return pt.HTTPTransport.Stop()
}