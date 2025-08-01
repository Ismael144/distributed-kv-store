package statemachine

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ismael144/distributed-kv-store/types"
)

// KVStateMachine implements a key-value store state machine
// This is where the actual business logic of our distributed system lives
type KVStateMachine struct {
	mu sync.RWMutex
	
	// The actual key-value store
	data map[string]string
	
	// Track applied commands for deduplication
	// In a real system, you'd want to periodically clean this up
	appliedCommands map[string]commandResult
	
	// Statistics for monitoring
	stats Statistics
}

// commandResult stores the result of a previously applied command
// Used for client request deduplication
type commandResult struct {
	Result    types.CommandResult
	Timestamp time.Time
}

// Statistics tracks various metrics about the state machine
type Statistics struct {
	// Total number of commands applied
	TotalCommands uint64 `json:"total_commands"`
	
	// Commands by type
	SetCommands    uint64 `json:"set_commands"`
	GetCommands    uint64 `json:"get_commands"`
	DeleteCommands uint64 `json:"delete_commands"`
	
	// Current state
	KeyCount        int       `json:"key_count"`
	LastAppliedTime time.Time `json:"last_applied_time"`
	
	// Error counts
	ErrorCount uint64 `json:"error_count"`
}

// NewKVStateMachine creates a new key-value state machine
func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{
		data:            make(map[string]string),
		appliedCommands: make(map[string]commandResult),
		stats:           Statistics{},
	}
}

// Apply applies a command to the state machine and returns the result
// This method must be deterministic - given the same command, it should always
// produce the same result regardless of when or where it's executed
func (kv *KVStateMachine) Apply(cmd types.Command) types.CommandResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Update statistics
	kv.stats.TotalCommands++
	kv.stats.LastAppliedTime = time.Now()

	// Check for duplicate command (idempotency)
	if cmd.ClientID != "" && cmd.RequestID != "" {
		commandKey := fmt.Sprintf("%s:%s", cmd.ClientID, cmd.RequestID)
		if cached, exists := kv.appliedCommands[commandKey]; exists {
			// Return cached result for duplicate command
			return cached.Result
		}
		
		// Store command result for future deduplication
		defer func(key string, result types.CommandResult) {
			kv.appliedCommands[key] = commandResult{
				Result:    result,
				Timestamp: time.Now(),
			}
			// Clean up old commands periodically (simple approach)
			if len(kv.appliedCommands) > 10000 {
				kv.cleanupOldCommands()
			}
		}(commandKey, types.CommandResult{})
	}

	// Apply the command based on its type
	switch cmd.Type {
	case "set":
		return kv.applySet(cmd)
	case "get":
		return kv.applyGet(cmd)
	case "delete":
		return kv.applyDelete(cmd)
	default:
		kv.stats.ErrorCount++
		return types.CommandResult{
			Success: false,
			Error:   fmt.Sprintf("unknown command type: %s", cmd.Type),
		}
	}
}

// applySet handles SET commands
func (kv *KVStateMachine) applySet(cmd types.Command) types.CommandResult {
	if cmd.Key == "" {
		kv.stats.ErrorCount++
		return types.CommandResult{
			Success: false,
			Error:   "key cannot be empty",
		}
	}

	// Store the key-value pair
	oldValue, existed := kv.data[cmd.Key]
	kv.data[cmd.Key] = cmd.Value

	// Update statistics
	kv.stats.SetCommands++
	if !existed {
		kv.stats.KeyCount++
	}

	return types.CommandResult{
		Success: true,
		Value:   oldValue, // Return previous value if any
	}
}

// applyGet handles GET commands
func (kv *KVStateMachine) applyGet(cmd types.Command) types.CommandResult {
	if cmd.Key == "" {
		kv.stats.ErrorCount++
		return types.CommandResult{
			Success: false,
			Error:   "key cannot be empty",
		}
	}

	// Retrieve the value
	value, exists := kv.data[cmd.Key]

	// Update statistics
	kv.stats.GetCommands++

	if !exists {
		return types.CommandResult{
			Success: false,
			Error:   "key not found",
		}
	}

	return types.CommandResult{
		Success: true,
		Value:   value,
	}
}

// applyDelete handles DELETE commands
func (kv *KVStateMachine) applyDelete(cmd types.Command) types.CommandResult {
	if cmd.Key == "" {
		kv.stats.ErrorCount++
		return types.CommandResult{
			Success: false,
			Error:   "key cannot be empty",
		}
	}

	// Delete the key
	oldValue, existed := kv.data[cmd.Key]
	if existed {
		delete(kv.data, cmd.Key)
		kv.stats.KeyCount--
	}

	// Update statistics
	kv.stats.DeleteCommands++

	if !existed {
		return types.CommandResult{
			Success: false,
			Error:   "key not found",
		}
	}

	return types.CommandResult{
		Success: true,
		Value:   oldValue, // Return the deleted value
	}
}

// Snapshot creates a snapshot of the current state machine state
// This is used for log compaction and new node initialization
func (kv *KVStateMachine) Snapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	// Create a snapshot containing all data and statistics
	snapshot := struct {
		Data            map[string]string    `json:"data"`
		Stats           Statistics           `json:"statistics"`
		AppliedCommands map[string]time.Time `json:"applied_commands"` // Only store timestamps
		SnapshotTime    time.Time            `json:"snapshot_time"`
	}{
		Data:            kv.data,
		Stats:           kv.stats,
		AppliedCommands: make(map[string]time.Time),
		SnapshotTime:    time.Now(),
	}

	// Include only recent applied commands (last hour) to prevent snapshot bloat
	cutoff := time.Now().Add(-1 * time.Hour)
	for key, cmd := range kv.appliedCommands {
		if cmd.Timestamp.After(cutoff) {
			snapshot.AppliedCommands[key] = cmd.Timestamp
		}
	}

	return json.MarshalIndent(snapshot, "", "  ")
}

// Restore restores the state machine from a snapshot
func (kv *KVStateMachine) Restore(snapshotData []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var snapshot struct {
		Data            map[string]string    `json:"data"`
		Stats           Statistics           `json:"statistics"`
		AppliedCommands map[string]time.Time `json:"applied_commands"`
		SnapshotTime    time.Time            `json:"snapshot_time"`
	}

	if err := json.Unmarshal(snapshotData, &snapshot); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	// Restore data
	kv.data = make(map[string]string)
	for k, v := range snapshot.Data {
		kv.data[k] = v
	}

	// Restore statistics
	kv.stats = snapshot.Stats

	// Restore applied commands (create empty results since we only stored timestamps)
	kv.appliedCommands = make(map[string]commandResult)
	for key, timestamp := range snapshot.AppliedCommands {
		kv.appliedCommands[key] = commandResult{
			Result:    types.CommandResult{Success: true}, // Placeholder
			Timestamp: timestamp,
		}
	}

	return nil
}

// GetState returns a copy of the current state (for debugging/monitoring)
func (kv *KVStateMachine) GetState() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	// Return a copy to prevent external modification
	stateCopy := make(map[string]string)
	for k, v := range kv.data {
		stateCopy[k] = v
	}

	return stateCopy
}

// GetStatistics returns current statistics
func (kv *KVStateMachine) GetStatistics() Statistics {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return kv.stats
}

// GetKeyCount returns the current number of keys in the store
func (kv *KVStateMachine) GetKeyCount() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return len(kv.data)
}

// HasKey checks if a key exists in the store
func (kv *KVStateMachine) HasKey(key string) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	_, exists := kv.data[key]
	return exists
}

// ListKeys returns all keys currently in the store (for debugging)
func (kv *KVStateMachine) ListKeys() []string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	keys := make([]string, 0, len(kv.data))
	for k := range kv.data {
		keys = append(keys, k)
	}

	return keys
}

// cleanupOldCommands removes old applied commands to prevent memory bloat
func (kv *KVStateMachine) cleanupOldCommands() {
	cutoff := time.Now().Add(-24 * time.Hour) // Keep commands for 24 hours
	
	for key, cmd := range kv.appliedCommands {
		if cmd.Timestamp.Before(cutoff) {
			delete(kv.appliedCommands, key)
		}
	}
}

// Reset clears all data in the state machine (useful for testing)
func (kv *KVStateMachine) Reset() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.data = make(map[string]string)
	kv.appliedCommands = make(map[string]commandResult)
	kv.stats = Statistics{}
}

// Size returns the approximate memory size of the state machine in bytes
func (kv *KVStateMachine) Size() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	size := 0
	
	// Estimate size of data map
	for k, v := range kv.data {
		size += len(k) + len(v) + 32 // Rough estimate including map overhead
	}
	
	// Estimate size of applied commands
	size += len(kv.appliedCommands) * 64 // Rough estimate
	
	return size
}

// Advanced Operations

// CompareAndSwap atomically compares and swaps a value if it matches expected
func (kv *KVStateMachine) CompareAndSwap(key, expected, newValue string) types.CommandResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	current, exists := kv.data[key]
	
	if !exists && expected != "" {
		return types.CommandResult{
			Success: false,
			Error:   "key not found",
		}
	}
	
	if exists && current != expected {
		return types.CommandResult{
			Success: false,
			Error:   "value mismatch",
			Value:   current,
		}
	}
	
	// Perform the swap
	kv.data[key] = newValue
	if !exists {
		kv.stats.KeyCount++
	}
	
	kv.stats.SetCommands++
	kv.stats.TotalCommands++
	
	return types.CommandResult{
		Success: true,
		Value:   current,
	}
}

// BatchOperation represents a batch of operations to execute atomically
type BatchOperation struct {
	Operations []types.Command `json:"operations"`
}

// ApplyBatch applies multiple commands atomically
// This is an advanced feature that could be exposed via the Raft interface
func (kv *KVStateMachine) ApplyBatch(ops []types.Command) []types.CommandResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	results := make([]types.CommandResult, len(ops))
	
	// In a real implementation, you might want to support rollback on partial failure
	// For simplicity, we'll apply all operations and return individual results
	for i, cmd := range ops {
		// Apply each command (without the outer lock since we already have it)
		switch cmd.Type {
		case "set":
			results[i] = kv.applySet(cmd)
		case "get":
			results[i] = kv.applyGet(cmd)  
		case "delete":
			results[i] = kv.applyDelete(cmd)
		default:
			results[i] = types.CommandResult{
				Success: false,
				Error:   fmt.Sprintf("unknown command type: %s", cmd.Type),
			}
		}
	}
	
	return results
}