package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client provides a simple interface to interact with the distributed key-value store
type Client struct {
	// List of server addresses to try
	servers []string
	
	// HTTP client for making requests
	httpClient *http.Client
	
	// Client configuration
	config ClientConfig
	
	// Current leader (for optimization)
	currentLeader string
	
	// Client ID for request tracking
	clientID string
}

// ClientConfig holds configuration for the client
type ClientConfig struct {
	// Timeout for requests
	Timeout time.Duration `json:"timeout"`
	
	// Number of retries for failed requests
	MaxRetries int `json:"max_retries"`
	
	// Whether to follow redirects automatically
	FollowRedirects bool `json:"follow_redirects"`
	
	// Whether to enable verbose logging
	Verbose bool `json:"verbose"`
}

// DefaultClientConfig returns default client configuration
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		Timeout:         5 * time.Second,
		MaxRetries:      3,
		FollowRedirects: true,
		Verbose:         false,
	}
}

// NewClient creates a new client
func NewClient(servers []string, config ClientConfig) *Client {
	return &Client{
		servers: servers,
		config:  config,
		httpClient: &http.Client{
			Timeout: config.Timeout,
		},
		clientID: fmt.Sprintf("client-%d", time.Now().UnixNano()),
	}
}

// Response represents a response from the key-value store
type Response struct {
	Success       bool        `json:"success"`
	Key           string      `json:"key,omitempty"`
	Value         string      `json:"value,omitempty"`
	PreviousValue string      `json:"previous_value,omitempty"`
	DeletedValue  string      `json:"deleted_value,omitempty"`
	Index         uint64      `json:"index,omitempty"`
	Error         string      `json:"error,omitempty"`
	LeaderID      string      `json:"leader_id,omitempty"`
}

// BatchOperation represents a single operation in a batch
type BatchOperation struct {
	Type  string `json:"type"`  // "get", "set", "delete"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// BatchResponse represents the response from a batch operation
type BatchResponse struct {
	Results []map[string]interface{} `json:"results"`
	Count   int                      `json:"count"`
}

// ClusterInfo represents information about the cluster
type ClusterInfo struct {
	Nodes       []NodeInfo `json:"nodes"`
	LeaderID    *string    `json:"leader_id"`
	Term        uint64     `json:"term"`
	CommitIndex uint64     `json:"commit_index"`
}

// NodeInfo represents information about a single node
type NodeInfo struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	State    string `json:"state"`
	Term     uint64 `json:"term"`
	IsLeader bool   `json:"is_leader"`
}

// Key-Value Operations

// Get retrieves the value for a given key
func (c *Client) Get(key string) (*Response, error) {
	if key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}
	
	url := fmt.Sprintf("/kv/%s", key)
	return c.makeRequest("GET", url, nil)
}

// Set stores a key-value pair
func (c *Client) Set(key, value string) (*Response, error) {
	if key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}
	
	requestBody := map[string]string{
		"value": value,
	}
	
	url := fmt.Sprintf("/kv/%s", key)
	return c.makeRequest("PUT", url, requestBody)
}

// Delete removes a key-value pair
func (c *Client) Delete(key string) (*Response, error) {
	if key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}
	
	url := fmt.Sprintf("/kv/%s", key)
	return c.makeRequest("DELETE", url, nil)
}

// Batch executes multiple operations in a single request
func (c *Client) Batch(operations []BatchOperation) (*BatchResponse, error) {
	if len(operations) == 0 {
		return nil, fmt.Errorf("no operations specified")
	}
	
	requestBody := map[string]interface{}{
		"operations": operations,
	}
	
	resp, err := c.makeRequest("POST", "/batch", requestBody)
	if err != nil {
		return nil, err
	}
	
	// Parse batch response
	var batchResp BatchResponse
	if resp.Success {
		// The actual parsing would depend on the server response format
		batchResp.Count = len(operations)
	}
	
	return &batchResp, nil
}

// Cluster Management Operations

// GetClusterInfo retrieves information about the cluster
func (c *Client) GetClusterInfo() (*ClusterInfo, error) {
	resp, err := c.makeRawRequest("GET", "/cluster/info", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	
	var clusterInfo ClusterInfo
	if err := json.Unmarshal(body, &clusterInfo); err != nil {
		return nil, fmt.Errorf("failed to parse cluster info: %w", err)
	}
	
	return &clusterInfo, nil
}

// GetLeader returns information about the current leader
func (c *Client) GetLeader() (map[string]interface{}, error) {
	resp, err := c.makeRawRequest("GET", "/cluster/leader", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse leader info: %w", err)
	}
	
	return result, nil
}

// AddNode adds a new node to the cluster
func (c *Client) AddNode(nodeID, address string) error {
	requestBody := map[string]string{
		"node_id": nodeID,
		"address": address,
	}
	
	resp, err := c.makeRequest("POST", "/cluster/add-node", requestBody)
	if err != nil {
		return err
	}
	
	if !resp.Success {
		return fmt.Errorf("failed to add node: %s", resp.Error)
	}
	
	return nil
}

// RemoveNode removes a node from the cluster
func (c *Client) RemoveNode(nodeID string) error {
	requestBody := map[string]string{
		"node_id": nodeID,
	}
	
	resp, err := c.makeRequest("POST", "/cluster/remove-node", requestBody)
	if err != nil {
		return err
	}
	
	if !resp.Success {
		return fmt.Errorf("failed to remove node: %s", resp.Error)
	}
	
	return nil
}

// Monitoring and Status Operations

// GetStatus retrieves the status of a specific node
func (c *Client) GetStatus() (map[string]interface{}, error) {
	resp, err := c.makeRawRequest("GET", "/status", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse status: %w", err)
	}
	
	return result, nil
}

// GetStats retrieves detailed statistics
func (c *Client) GetStats() (map[string]interface{}, error) {
	resp, err := c.makeRawRequest("GET", "/stats", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse stats: %w", err)
	}
	
	return result, nil
}

// Health checks if the service is healthy
func (c *Client) Health() (bool, error) {
	resp, err := c.makeRawRequest("GET", "/health", nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	
	return resp.StatusCode == http.StatusOK, nil
}

// Internal helper methods

// makeRequest makes an HTTP request and handles retries and redirects
func (c *Client) makeRequest(method, path string, body interface{}) (*Response, error) {
	var lastErr error
	
	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		resp, err := c.makeRawRequest(method, path, body)
		if err != nil {
			lastErr = err
			if c.config.Verbose {
				fmt.Printf("Attempt %d failed: %v\n", attempt+1, err)
			}
			continue
		}
		defer resp.Body.Close()
		
		// Read response body
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			lastErr = fmt.Errorf("failed to read response body: %w", err)
			continue
		}
		
		// Handle different status codes
		switch resp.StatusCode {
		case http.StatusOK:
			// Success - parse response
			var response Response
			if err := json.Unmarshal(respBody, &response); err != nil {
				lastErr = fmt.Errorf("failed to parse response: %w", err)
				continue
			}
			response.Success = true
			return &response, nil
			
		case http.StatusNotFound:
			// Key not found
			return &Response{
				Success: false,
				Error:   "key not found",
			}, nil
			
		case http.StatusTemporaryRedirect:
			// Handle redirect to leader
			if !c.config.FollowRedirects {
				var errorResp map[string]interface{}
				json.Unmarshal(respBody, &errorResp)
				return &Response{
					Success:  false,
					Error:    "redirect required",
					LeaderID: fmt.Sprintf("%v", errorResp["leader_id"]),
				}, nil
			}
			
			// Try to extract leader information and retry
			var redirectResp map[string]interface{}
			if err := json.Unmarshal(respBody, &redirectResp); err == nil {
				if leaderID, ok := redirectResp["leader_id"].(string); ok {
					c.currentLeader = leaderID
					if c.config.Verbose {
						fmt.Printf("Redirecting to leader: %s\n", leaderID)
					}
				}
			}
			
			// Retry with a different server
			lastErr = fmt.Errorf("redirect to leader required")
			continue
			
		default:
			// Other errors
			var errorResp map[string]interface{}
			json.Unmarshal(respBody, &errorResp)
			
			errorMsg := fmt.Sprintf("HTTP %d", resp.StatusCode)
			if msg, ok := errorResp["error"].(string); ok {
				errorMsg = msg
			}
			
			return &Response{
				Success: false,
				Error:   errorMsg,
			}, nil
		}
	}
	
	return nil, fmt.Errorf("request failed after %d attempts: %v", c.config.MaxRetries+1, lastErr)
}

// makeRawRequest makes a raw HTTP request to one of the servers
func (c *Client) makeRawRequest(method, path string, body interface{}) (*http.Response, error) {
	// Serialize request body if provided
	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewReader(jsonBody)
	}
	
	// Try current leader first if known
	if c.currentLeader != "" {
		for _, server := range c.servers {
			if server == c.currentLeader {
				return c.makeRawRequestToServer(method, server, path, reqBody)
			}
		}
	}
	
	// Try all servers
	var lastErr error
	for _, server := range c.servers {
		resp, err := c.makeRawRequestToServer(method, server, path, reqBody)
		if err != nil {
			lastErr = err
			if c.config.Verbose {
				fmt.Printf("Request to %s failed: %v\n", server, err)
			}
			continue
		}
		
		// Update current leader if this request succeeded
		if resp.StatusCode == http.StatusOK {
			c.currentLeader = server
		}
		
		return resp, nil
	}
	
	return nil, fmt.Errorf("all servers failed, last error: %v", lastErr)
}

// makeRawRequestToServer makes a raw HTTP request to a specific server
func (c *Client) makeRawRequestToServer(method, server, path string, body io.Reader) (*http.Response, error) {
	url := fmt.Sprintf("http://%s%s", server, path)
	
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Client-ID", c.clientID)
	
	if c.config.Verbose {
		fmt.Printf("Making %s request to %s\n", method, url)
	}
	
	return c.httpClient.Do(req)
}

// Convenience methods for common operations

// Exists checks if a key exists in the store
func (c *Client) Exists(key string) (bool, error) {
	resp, err := c.Get(key)
	if err != nil {
		return false, err
	}
	
	return resp.Success, nil
}

// SetIfNotExists sets a key only if it doesn't already exist
func (c *Client) SetIfNotExists(key, value string) (bool, error) {
	exists, err := c.Exists(key)
	if err != nil {
		return false, err
	}
	
	if exists {
		return false, nil // Key already exists
	}
	
	resp, err := c.Set(key, value)
	if err != nil {
		return false, err
	}
	
	return resp.Success, nil
}

// GetMultiple retrieves multiple keys in a single batch operation
func (c *Client) GetMultiple(keys []string) (map[string]string, error) {
	if len(keys) == 0 {
		return make(map[string]string), nil
	}
	
	// Create batch operations
	operations := make([]BatchOperation, len(keys))
	for i, key := range keys {
		operations[i] = BatchOperation{
			Type: "get",
			Key:  key,
		}
	}
	
	// Execute batch
	batchResp, err := c.Batch(operations)
	if err != nil {
		return nil, err
	}
	
	// Parse results
	result := make(map[string]string)
	for i, res := range batchResp.Results {
		if i < len(keys) {
			if success, ok := res["success"].(bool); ok && success {
				if value, ok := res["value"].(string); ok {
					result[keys[i]] = value
				}
			}
		}
	}
	
	return result, nil
}

// SetMultiple stores multiple key-value pairs in a single batch operation
func (c *Client) SetMultiple(pairs map[string]string) error {
	if len(pairs) == 0 {
		return nil
	}
	
	// Create batch operations
	operations := make([]BatchOperation, 0, len(pairs))
	for key, value := range pairs {
		operations = append(operations, BatchOperation{
			Type:  "set",
			Key:   key,
			Value: value,
		})
	}
	
	// Execute batch
	_, err := c.Batch(operations)
	return err
}

// DeleteMultiple removes multiple keys in a single batch operation
func (c *Client) DeleteMultiple(keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	
	// Create batch operations
	operations := make([]BatchOperation, len(keys))
	for i, key := range keys {
		operations[i] = BatchOperation{
			Type: "delete",
			Key:  key,
		}
	}
	
	// Execute batch
	_, err := c.Batch(operations)
	return err
}

// Wait for cluster to be ready (useful for testing)
func (c *Client) WaitForCluster(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	
	for time.Now().Before(deadline) {
		if healthy, _ := c.Health(); healthy {
			// Also check if there's a leader
			if leader, err := c.GetLeader(); err == nil {
				if isLeader, ok := leader["is_leader"].(bool); ok && isLeader {
					return nil
				}
				if leaderID, ok := leader["leader_id"].(string); ok && leaderID != "" {
					return nil
				}
			}
		}
		
		time.Sleep(100 * time.Millisecond)
	}
	
	return fmt.Errorf("cluster not ready within timeout")
}

// Configuration methods

// SetTimeout updates the client timeout
func (c *Client) SetTimeout(timeout time.Duration) {
	c.config.Timeout = timeout
	c.httpClient.Timeout = timeout
}

// SetVerbose enables or disables verbose logging
func (c *Client) SetVerbose(verbose bool) {
	c.config.Verbose = verbose
}

// SetMaxRetries sets the maximum number of retries
func (c *Client) SetMaxRetries(maxRetries int) {
	c.config.MaxRetries = maxRetries
}

// AddServer adds a server to the list of servers to try
func (c *Client) AddServer(server string) {
	for _, existing := range c.servers {
		if existing == server {
			return // Already exists
		}
	}
	c.servers = append(c.servers, server)
}

// RemoveServer removes a server from the list
func (c *Client) RemoveServer(server string) {
	for i, existing := range c.servers {
		if existing == server {
			c.servers = append(c.servers[:i], c.servers[i+1:]...)
			if c.currentLeader == server {
				c.currentLeader = ""
			}
			return
		}
	}
}

// GetServers returns the list of configured servers
func (c *Client) GetServers() []string {
	result := make([]string, len(c.servers))
	copy(result, c.servers)
	return result
}

// GetCurrentLeader returns the currently known leader
func (c *Client) GetCurrentLeader() string {
	return c.currentLeader
}

// Example usage and testing helpers

// Example demonstrates basic client usage
func Example() {
	// Create client with multiple server addresses
	servers := []string{
		"localhost:8001",
		"localhost:8002", 
		"localhost:8003",
	}
	
	config := DefaultClientConfig()
	config.Verbose = true
	
	client := NewClient(servers, config)
	
	// Wait for cluster to be ready
	fmt.Println("Waiting for cluster...")
	if err := client.WaitForCluster(10 * time.Second); err != nil {
		fmt.Printf("Cluster not ready: %v\n", err)
		return
	}
	
	// Set some values
	fmt.Println("Setting values...")
	if _, err := client.Set("hello", "world"); err != nil {
		fmt.Printf("Set failed: %v\n", err)
		return
	}
	
	if _, err := client.Set("foo", "bar"); err != nil {
		fmt.Printf("Set failed: %v\n", err)
		return
	}
	
	// Get values
	fmt.Println("Getting values...")
	if resp, err := client.Get("hello"); err != nil {
		fmt.Printf("Get failed: %v\n", err)
	} else {
		fmt.Printf("hello = %s\n", resp.Value)
	}
	
	// Batch operations
	fmt.Println("Batch operations...")
	pairs := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}
	
	if err := client.SetMultiple(pairs); err != nil {
		fmt.Printf("Batch set failed: %v\n", err)
		return
	}
	
	values, err := client.GetMultiple([]string{"key1", "key2", "key3"})
	if err != nil {
		fmt.Printf("Batch get failed: %v\n", err)
		return
	}
	
	for key, value := range values {
		fmt.Printf("%s = %s\n", key, value)
	}
	
	// Get cluster info
	fmt.Println("Cluster info...")
	if info, err := client.GetClusterInfo(); err != nil {
		fmt.Printf("Failed to get cluster info: %v\n", err)
	} else {
		fmt.Printf("Cluster has %d nodes\n", len(info.Nodes))
		if info.LeaderID != nil {
			fmt.Printf("Leader: %s\n", *info.LeaderID)
		}
	}
}