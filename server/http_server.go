package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ismael144/distributed-kv-store/raft"
	"github.com/ismael144/distributed-kv-store/types"
)

// HTTPServer provides a REST API for the distributed key-value store
// This is the main interface that clients use to interact with the cluster
type HTTPServer struct {
	addr     string
	server   *http.Server
	mux      *http.ServeMux
	raftNode types.RaftNode

	// Configuration
	config ServerConfig

	// Statistics
	requestCount  uint64
	successCount  uint64
	errorCount    uint64
	redirectCount uint64
}

// ServerConfig holds configuration for the HTTP server
type ServerConfig struct {
	// Request timeout for client operations
	DefaultTimeout time.Duration `json:"default_timeout"`

	// Enable cross-origin requests
	EnableCORS bool `json:"enable_cors"`

	// Maximum request body size
	MaxRequestSize int64 `json:"max_request_size"`

	// Enable detailed error messages
	VerboseErrors bool `json:"verbose_errors"`

	// Enable request/response logging
	EnableLogging bool `json:"enable_logging"`
}

// DefaultServerConfig returns default server configuration
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		DefaultTimeout: 5 * time.Second,
		EnableCORS:     true,
		MaxRequestSize: 1024 * 1024, // 1MB
		VerboseErrors:  false,
		EnableLogging:  true,
	}
}

// NewHTTPServer creates a new HTTP server
func NewHTTPServer(addr string, raftNode types.RaftNode, config ServerConfig) *HTTPServer {
	server := &HTTPServer{
		addr:     addr,
		raftNode: raftNode,
		config:   config,
		mux:      http.NewServeMux(),
	}

	// Instead of setting up routes directly on mux here, let's create a temporary mux
	// and then apply the middleware to it.

	// Step 1: Create a mux for all your routes
	routesMux := http.NewServeMux()
	server.setupRoutes(routesMux)

	// Step 2: Wrap the entire routes mux with your middleware
	// The middleware's 'next' handler will now be the routesMux itself
	wrappedHandler := server.middleware(routesMux)

	// Step 3: Assign the wrapped handler to the main server
	server.server = &http.Server{
		Addr:           addr,
		Handler:        wrappedHandler, // Use the wrapped handler
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20, // 1MB
	}

	return server
}

// setupRoutes configures HTTP routes
// It now takes a mux as an argument, and only registers handlers on it.
func (s *HTTPServer) setupRoutes(mux *http.ServeMux) {
	// Key-value operations
	mux.HandleFunc("/kv/", s.handleKeyValue)

	// Batch operations
	mux.HandleFunc("/batch", s.handleBatch)

	// Cluster management
	mux.HandleFunc("/cluster/info", s.handleClusterInfo)
	mux.HandleFunc("/cluster/leader", s.handleLeaderInfo)
	mux.HandleFunc("/cluster/nodes", s.handleNodes)
	mux.HandleFunc("/cluster/add-node", s.handleAddNode)
	mux.HandleFunc("/cluster/remove-node", s.handleRemoveNode)

	// Node status and statistics
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/health", s.handleHealth)

	// Administrative operations
	mux.HandleFunc("/admin/snapshot", s.handleSnapshot)
	mux.HandleFunc("/admin/logs", s.handleLogs)

	// The previous incorrect line has been removed.
	// s.mux.Handle("/", s.middleware(s.mux))
}

// middleware applies common middleware to all requests
func (s *HTTPServer) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// CORS headers
		if s.config.EnableCORS {
			s.setCORSHeaders(w)
		}

		// Handle preflight requests
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Request size limit
		if r.ContentLength > s.config.MaxRequestSize {
			s.errorResponse(w, http.StatusRequestEntityTooLarge, "Request too large")
			return
		}

		// Set content type for JSON responses
		w.Header().Set("Content-Type", "application/json")

		// Call next handler
		next.ServeHTTP(w, r)

		// Log request if enabled
		if s.config.EnableLogging {
			duration := time.Since(start)
			fmt.Printf("[%s] %s %s - %v\n",
				time.Now().Format("2006-01-02 15:04:05"),
				r.Method, r.URL.Path, duration)
		}
	})
}

// setCORSHeaders sets CORS headers for cross-origin requests
func (s *HTTPServer) setCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.Header().Set("Access-Control-Max-Age", "86400")
}

// Start starts the HTTP server
func (s *HTTPServer) Start() error {
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP server error: %v\n", err)
		}
	}()

	fmt.Printf("HTTP server listening on %s\n", s.addr)
	return nil
}

// Stop stops the HTTP server
func (s *HTTPServer) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

// Key-Value Operations Handlers

// handleKeyValue handles key-value operations: GET, PUT, DELETE
func (s *HTTPServer) handleKeyValue(w http.ResponseWriter, r *http.Request) {
	s.requestCount++

	// Extract key from URL path (/kv/mykey)
	path := strings.TrimPrefix(r.URL.Path, "/kv/")
	if path == "" {
		s.errorResponse(w, http.StatusBadRequest, "Key is required")
		return
	}

	key := path

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodPut, http.MethodPost:
		s.handleSet(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		s.errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleGet handles GET requests for retrieving values
func (s *HTTPServer) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	// Generate client ID and request ID for tracking
	clientID := s.getClientID(r)
	requestID := s.generateRequestID()

	// Create GET command
	cmd := types.NewGetCommand(key, clientID, requestID)

	// Apply command to Raft
	result, err := s.raftNode.Apply(cmd, s.config.DefaultTimeout)
	if err != nil {
		if s.isNotLeaderError(err) {
			s.handleRedirect(w, r)
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	if result.Success {
		s.successCount++
		response := map[string]interface{}{
			"key":   key,
			"value": result.Value,
			"index": result.Index,
		}
		s.jsonResponse(w, http.StatusOK, response)
	} else {
		s.errorCount++
		if result.Error == "key not found" {
			s.errorResponse(w, http.StatusNotFound, "Key not found")
		} else {
			s.errorResponse(w, http.StatusInternalServerError, result.Error)
		}
	}
}

// handleSet handles PUT/POST requests for setting values
func (s *HTTPServer) handleSet(w http.ResponseWriter, r *http.Request, key string) {
	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Failed to read request body")
		return
	}
	defer r.Body.Close()

	// Parse request body
	var req struct {
		Value string `json:"value"`
	}

	// Try JSON first, then plain text
	if err := json.Unmarshal(body, &req); err != nil {
		// Treat as plain text value
		req.Value = string(body)
	}

	if req.Value == "" {
		s.errorResponse(w, http.StatusBadRequest, "Value is required")
		return
	}

	// Generate client ID and request ID
	clientID := s.getClientID(r)
	requestID := s.generateRequestID()

	// Create SET command
	cmd := types.NewSetCommand(key, req.Value, clientID, requestID)

	// Apply command to Raft
	result, err := s.raftNode.Apply(cmd, s.config.DefaultTimeout)
	if err != nil {
		if s.isNotLeaderError(err) {
			s.handleRedirect(w, r)
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	if result.Success {
		s.successCount++
		response := map[string]interface{}{
			"key":            key,
			"value":          req.Value,
			"previous_value": result.Value,
			"index":          result.Index,
		}
		s.jsonResponse(w, http.StatusOK, response)
	} else {
		s.errorCount++
		s.errorResponse(w, http.StatusInternalServerError, result.Error)
	}
}

// handleDelete handles DELETE requests for removing keys
func (s *HTTPServer) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	// Generate client ID and request ID
	clientID := s.getClientID(r)
	requestID := s.generateRequestID()

	// Create DELETE command
	cmd := types.NewDeleteCommand(key, clientID, requestID)

	// Apply command to Raft
	result, err := s.raftNode.Apply(cmd, s.config.DefaultTimeout)
	if err != nil {
		if s.isNotLeaderError(err) {
			s.handleRedirect(w, r)
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	if result.Success {
		s.successCount++
		response := map[string]interface{}{
			"key":           key,
			"deleted_value": result.Value,
			"index":         result.Index,
		}
		s.jsonResponse(w, http.StatusOK, response)
	} else {
		s.errorCount++
		if result.Error == "key not found" {
			s.errorResponse(w, http.StatusNotFound, "Key not found")
		} else {
			s.errorResponse(w, http.StatusInternalServerError, result.Error)
		}
	}
}

// handleBatch handles batch operations
func (s *HTTPServer) handleBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	s.requestCount++

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Failed to read request body")
		return
	}
	defer r.Body.Close()

	// Parse batch request
	var batchReq struct {
		Operations []struct {
			Type  string `json:"type"`
			Key   string `json:"key"`
			Value string `json:"value,omitempty"`
		} `json:"operations"`
	}

	if err := json.Unmarshal(body, &batchReq); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Invalid JSON")
		return
	}

	if len(batchReq.Operations) == 0 {
		s.errorResponse(w, http.StatusBadRequest, "No operations specified")
		return
	}

	// Generate client ID and request ID
	clientID := s.getClientID(r)

	// Execute operations sequentially (could be optimized for true batch processing)
	var results []map[string]interface{}

	for i, op := range batchReq.Operations {
		requestID := fmt.Sprintf("%s-%d", s.generateRequestID(), i)

		var cmd types.Command
		switch op.Type {
		case "set":
			cmd = types.NewSetCommand(op.Key, op.Value, clientID, requestID)
		case "get":
			cmd = types.NewGetCommand(op.Key, clientID, requestID)
		case "delete":
			cmd = types.NewDeleteCommand(op.Key, clientID, requestID)
		default:
			results = append(results, map[string]interface{}{
				"success": false,
				"error":   fmt.Sprintf("Unknown operation type: %s", op.Type),
			})
			continue
		}

		// Apply command
		result, err := s.raftNode.Apply(cmd, s.config.DefaultTimeout)
		if err != nil {
			if s.isNotLeaderError(err) {
				s.handleRedirect(w, r)
				return
			}
			results = append(results, map[string]interface{}{
				"success": false,
				"error":   err.Error(),
			})
			continue
		}

		// Add result
		opResult := map[string]interface{}{
			"success": result.Success,
			"key":     op.Key,
			"index":   result.Index,
		}

		if result.Success {
			if op.Type == "get" || op.Type == "delete" {
				opResult["value"] = result.Value
			}
		} else {
			opResult["error"] = result.Error
		}

		results = append(results, opResult)
	}

	s.successCount++
	response := map[string]interface{}{
		"results": results,
		"count":   len(results),
	}
	s.jsonResponse(w, http.StatusOK, response)
}

// Cluster Management Handlers

// handleClusterInfo returns information about the entire cluster
func (s *HTTPServer) handleClusterInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	info := s.raftNode.GetClusterInfo()
	s.jsonResponse(w, http.StatusOK, info)
}

// handleLeaderInfo returns information about the current leader
func (s *HTTPServer) handleLeaderInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	leader := s.raftNode.GetLeader()
	response := map[string]interface{}{
		"is_leader": s.raftNode.IsLeader(),
		"leader_id": leader,
	}

	s.jsonResponse(w, http.StatusOK, response)
}

// handleNodes returns information about cluster nodes
func (s *HTTPServer) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	clusterInfo := s.raftNode.GetClusterInfo()
	response := map[string]interface{}{
		"nodes": clusterInfo.Nodes,
		"count": len(clusterInfo.Nodes),
	}

	s.jsonResponse(w, http.StatusOK, response)
}

// handleAddNode adds a new node to the cluster
func (s *HTTPServer) handleAddNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Only leaders can add nodes
	if !s.raftNode.IsLeader() {
		s.handleRedirect(w, r)
		return
	}

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Failed to read request body")
		return
	}
	defer r.Body.Close()

	// Parse request
	var req struct {
		NodeID  string `json:"node_id"`
		Address string `json:"address"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Invalid JSON")
		return
	}

	if req.NodeID == "" || req.Address == "" {
		s.errorResponse(w, http.StatusBadRequest, "node_id and address are required")
		return
	}

	// Add node to cluster
	if err := s.raftNode.AddPeer(types.NodeID(req.NodeID), req.Address); err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Node %s added to cluster", req.NodeID),
	}
	s.jsonResponse(w, http.StatusOK, response)
}

// handleRemoveNode removes a node from the cluster
func (s *HTTPServer) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Only leaders can remove nodes
	if !s.raftNode.IsLeader() {
		s.handleRedirect(w, r)
		return
	}

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Failed to read request body")
		return
	}
	defer r.Body.Close()

	// Parse request
	var req struct {
		NodeID string `json:"node_id"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Invalid JSON")
		return
	}

	if req.NodeID == "" {
		s.errorResponse(w, http.StatusBadRequest, "node_id is required")
		return
	}

	// Remove node from cluster
	if err := s.raftNode.RemovePeer(types.NodeID(req.NodeID)); err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Node %s removed from cluster", req.NodeID),
	}
	s.jsonResponse(w, http.StatusOK, response)
}

// Status and Statistics Handlers

// handleStatus returns current node status
func (s *HTTPServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	nodeInfo := s.raftNode.GetState()
	s.jsonResponse(w, http.StatusOK, nodeInfo)
}

// handleStats returns detailed statistics
func (s *HTTPServer) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Get node stats (if available)
	var nodeStats interface{}
	if statsNode, ok := s.raftNode.(*raft.Node); ok {
		nodeStats = statsNode.GetStats()
	} else {
		nodeStats = "Statistics not available"
	}

	response := map[string]interface{}{
		"node": s.raftNode.GetState(),
		"raft": nodeStats,
		"server": map[string]interface{}{
			"request_count":  s.requestCount,
			"success_count":  s.successCount,
			"error_count":    s.errorCount,
			"redirect_count": s.redirectCount,
		},
	}

	s.jsonResponse(w, http.StatusOK, response)
}

// handleHealth returns health status
func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
		"is_leader": s.raftNode.IsLeader(),
	}
	s.jsonResponse(w, http.StatusOK, response)
}

// Administrative Handlers

// handleSnapshot triggers state machine snapshot creation
func (s *HTTPServer) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Only leaders should handle snapshots
	if !s.raftNode.IsLeader() {
		s.handleRedirect(w, r)
		return
	}

	// This would trigger snapshot creation in a real implementation
	response := map[string]interface{}{
		"success": true,
		"message": "Snapshot creation triggered",
	}
	s.jsonResponse(w, http.StatusOK, response)
}

// handleLogs returns recent log entries (for debugging)
func (s *HTTPServer) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Parse query parameters
	limitStr := r.URL.Query().Get("limit")
	limit := 10 // default
	if limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	response := map[string]interface{}{
		"message": fmt.Sprintf("Would return last %d log entries", limit),
		"note":    "Log inspection not implemented in this example",
	}
	s.jsonResponse(w, http.StatusOK, response)
}

// Utility methods

// handleRedirect redirects client to current leader
func (s *HTTPServer) handleRedirect(w http.ResponseWriter, r *http.Request) {
	s.redirectCount++

	leader := s.raftNode.GetLeader()
	if leader == nil {
		s.errorResponse(w, http.StatusServiceUnavailable, "No leader available")
		return
	}

	// In a real implementation, you'd need to maintain a mapping of node IDs to addresses
	response := map[string]interface{}{
		"error":     "not leader",
		"leader_id": *leader,
		"message":   "Redirect to leader required",
	}

	w.WriteHeader(http.StatusTemporaryRedirect)
	s.jsonResponse(w, http.StatusTemporaryRedirect, response)
}

// isNotLeaderError checks if error indicates this node is not the leader
func (s *HTTPServer) isNotLeaderError(err error) bool {
	return strings.Contains(err.Error(), "not leader")
}

// getClientID extracts or generates client ID from request
func (s *HTTPServer) getClientID(r *http.Request) string {
	// Try to get from header first
	if clientID := r.Header.Get("X-Client-ID"); clientID != "" {
		return clientID
	}

	// Generate based on remote address
	return fmt.Sprintf("client-%s", r.RemoteAddr)
}

// generateRequestID generates a unique request ID
func (s *HTTPServer) generateRequestID() string {
	return fmt.Sprintf("req-%d", time.Now().UnixNano())
}

// jsonResponse sends a JSON response
func (s *HTTPServer) jsonResponse(w http.ResponseWriter, status int, data interface{}) {
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		fmt.Printf("Failed to encode JSON response: %v\n", err)
	}
}

// errorResponse sends an error response
func (s *HTTPServer) errorResponse(w http.ResponseWriter, status int, message string) {
	s.errorCount++

	response := map[string]interface{}{
		"error":     message,
		"status":    status,
		"timestamp": time.Now().Unix(),
	}

	if s.config.VerboseErrors {
		response["details"] = "Additional error details would go here"
	}

	w.WriteHeader(status)
	json.NewEncoder(w).Encode(response)
}

// GetStats returns server statistics
func (s *HTTPServer) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"request_count":  s.requestCount,
		"success_count":  s.successCount,
		"error_count":    s.errorCount,
		"redirect_count": s.redirectCount,
	}
}
