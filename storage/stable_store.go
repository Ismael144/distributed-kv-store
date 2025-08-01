package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/ismael144/distributed-kv-store/types"
)

// FileStableStore implements StableStore interface using file-based storage
// This stores critical Raft state that must survive node restarts
type FileStableStore struct {
	mu      sync.RWMutex
	dataDir string
	// Cache for frequently accessed values
	cache map[string][]byte
}

// NewFileStableStore creates a new file-based stable store
func NewFileStableStore(dataDir string) (*FileStableStore, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	return &FileStableStore{
		dataDir: dataDir,
		cache:   make(map[string][]byte),
	}, nil
}

// Set stores a key-value pair persistently
func (fs *FileStableStore) Set(key string, value []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Write to file
	filename := filepath.Join(fs.dataDir, key)
	if err := os.WriteFile(filename, value, 0644); err != nil {
		return fmt.Errorf("failed to write stable store entry: %w", err)
	}

	// Update cache
	fs.cache[key] = make([]byte, len(value))
	copy(fs.cache[key], value)

	return nil
}

// Get retrieves value for a key
func (fs *FileStableStore) Get(key string) ([]byte, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Check cache first
	if value, exists := fs.cache[key]; exists {
		result := make([]byte, len(value))
		copy(result, value)
		return result, nil
	}

	// Read from file
	filename := filepath.Join(fs.dataDir, key)
	value, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("key not found: %s", key)
		}
		return nil, fmt.Errorf("failed to read stable store entry: %w", err)
	}

	// Update cache
	fs.cache[key] = make([]byte, len(value))
	copy(fs.cache[key], value)

	// Return copy
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

// Delete removes a key
func (fs *FileStableStore) Delete(key string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Remove file
	filename := filepath.Join(fs.dataDir, key)
	if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete stable store entry: %w", err)
	}

	// Remove from cache
	delete(fs.cache, key)

	return nil
}

// Has checks if key exists
func (fs *FileStableStore) Has(key string) bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Check cache first
	if _, exists := fs.cache[key]; exists {
		return true
	}

	// Check file
	filename := filepath.Join(fs.dataDir, key)
	if _, err := os.Stat(filename); err == nil {
		return true
	}

	return false
}

// MemoryStableStore implements StableStore interface using in-memory storage
// This is useful for testing when persistence is not required
type MemoryStableStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemoryStableStore creates a new in-memory stable store
func NewMemoryStableStore() *MemoryStableStore {
	return &MemoryStableStore{
		data: make(map[string][]byte),
	}
}

// Set stores a key-value pair in memory
func (ms *MemoryStableStore) Set(key string, value []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Store a copy to prevent external modification
	ms.data[key] = make([]byte, len(value))
	copy(ms.data[key], value)

	return nil
}

// Get retrieves value for a key
func (ms *MemoryStableStore) Get(key string) ([]byte, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	value, exists := ms.data[key]
	if !exists {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	// Return a copy
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

// Delete removes a key
func (ms *MemoryStableStore) Delete(key string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	delete(ms.data, key)
	return nil
}

// Has checks if key exists
func (ms *MemoryStableStore) Has(key string) bool {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	_, exists := ms.data[key]
	return exists
}

// Helper functions for storing/retrieving Raft-specific data

// Constants for Raft persistent state keys
const (
	CurrentTermKey = "current_term"
	VotedForKey    = "voted_for"
)

// StoreCurrentTerm stores the current term
func StoreCurrentTerm(store types.StableStore, term types.Term) error {
	data, err := json.Marshal(term)
	if err != nil {
		return fmt.Errorf("failed to marshal current term: %w", err)
	}
	return store.Set(CurrentTermKey, data)
}

// GetCurrentTerm retrieves the current term
func GetCurrentTerm(store types.StableStore) (types.Term, error) {
	data, err := store.Get(CurrentTermKey)
	if err != nil {
		// If term doesn't exist, start with term 0
		return 0, nil
	}

	var term types.Term
	if err := json.Unmarshal(data, &term); err != nil {
		return 0, fmt.Errorf("failed to unmarshal current term: %w", err)
	}

	return term, nil
}

// StoreVotedFor stores the voted for candidate
func StoreVotedFor(store types.StableStore, votedFor *types.NodeID) error {
	data, err := json.Marshal(votedFor)
	if err != nil {
		return fmt.Errorf("failed to marshal voted for: %w", err)
	}
	return store.Set(VotedForKey, data)
}

// GetVotedFor retrieves the voted for candidate
func GetVotedFor(store types.StableStore) (*types.NodeID, error) {
	data, err := store.Get(VotedForKey)
	if err != nil {
		// If voted for doesn't exist, return nil
		return nil, nil
	}

	var votedFor *types.NodeID
	if err := json.Unmarshal(data, &votedFor); err != nil {
		return nil, fmt.Errorf("failed to unmarshal voted for: %w", err)
	}

	return votedFor, nil
}

// StorePersistentState stores all persistent state atomically
func StorePersistentState(store types.StableStore, state types.PersistentState) error {
	// Store current term
	if err := StoreCurrentTerm(store, state.CurrentTerm); err != nil {
		return fmt.Errorf("failed to store current term: %w", err)
	}

	// Store voted for
	if err := StoreVotedFor(store, state.VotedFor); err != nil {
		return fmt.Errorf("failed to store voted for: %w", err)
	}

	// Note: Log entries are stored separately via LogStore interface

	return nil
}

// LoadPersistentState loads all persistent state
func LoadPersistentState(store types.StableStore) (types.PersistentState, error) {
	var state types.PersistentState

	// Load current term
	term, err := GetCurrentTerm(store)
	if err != nil {
		return state, fmt.Errorf("failed to load current term: %w", err)
	}
	state.CurrentTerm = term

	// Load voted for
	votedFor, err := GetVotedFor(store)
	if err != nil {
		return state, fmt.Errorf("failed to load voted for: %w", err)
	}
	state.VotedFor = votedFor

	// Note: Log entries are loaded separately via LogStore interface

	return state, nil
}

// ClusterConfiguration represents the cluster membership configuration
type ClusterConfiguration struct {
	// Nodes in the cluster
	Nodes map[types.NodeID]string `json:"nodes"` // NodeID -> Address mapping
	// Configuration index in the log where this config was committed
	Index types.LogIndex `json:"index"`
}

// StoreClusterConfiguration stores the cluster configuration
func StoreClusterConfiguration(store types.StableStore, config ClusterConfiguration) error {
	data, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal cluster configuration: %w", err)
	}
	return store.Set("cluster_config", data)
}

// LoadClusterConfiguration loads the cluster configuration
func LoadClusterConfiguration(store types.StableStore) (*ClusterConfiguration, error) {
	data, err := store.Get("cluster_config")
	if err != nil {
		// If configuration doesn't exist, return nil
		return nil, nil
	}

	var config ClusterConfiguration
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cluster configuration: %w", err)
	}

	return &config, nil
}

// Snapshot metadata for state machine snapshots
type SnapshotMetadata struct {
	// Index of last log entry included in snapshot
	Index types.LogIndex `json:"index"`
	// Term of last log entry included in snapshot
	Term types.Term `json:"term"`
	// Cluster configuration at the time of snapshot
	Configuration ClusterConfiguration `json:"configuration"`
	// Timestamp when snapshot was created
	Timestamp int64 `json:"timestamp"`
}

// StoreSnapshotMetadata stores snapshot metadata
func StoreSnapshotMetadata(store types.StableStore, metadata SnapshotMetadata) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot metadata: %w", err)
	}
	return store.Set("snapshot_metadata", data)
}

// LoadSnapshotMetadata loads snapshot metadata
func LoadSnapshotMetadata(store types.StableStore) (*SnapshotMetadata, error) {
	data, err := store.Get("snapshot_metadata")
	if err != nil {
		// If metadata doesn't exist, return nil
		return nil, nil
	}

	var metadata SnapshotMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot metadata: %w", err)
	}

	return &metadata, nil
}

// Utility functions for debugging and monitoring

// ListAllKeys returns all keys in the stable store (for debugging)
func ListAllKeys(store types.StableStore) []string {
	var keys []string

	// For file store, we can list directory contents
	if fs, ok := store.(*FileStableStore); ok {
		fs.mu.RLock()
		defer fs.mu.RUnlock()

		entries, err := os.ReadDir(fs.dataDir)
		if err != nil {
			return keys
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				keys = append(keys, entry.Name())
			}
		}
	}

	// For memory store, we can list map keys
	if ms, ok := store.(*MemoryStableStore); ok {
		ms.mu.RLock()
		defer ms.mu.RUnlock()

		for key := range ms.data {
			keys = append(keys, key)
		}
	}

	return keys
}

// GetStorageSize returns approximate storage size in bytes
func GetStorageSize(store types.StableStore) int64 {
	var size int64

	// For file store, calculate directory size
	if fs, ok := store.(*FileStableStore); ok {
		filepath.Walk(fs.dataDir, func(path string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				size += info.Size()
			}
			return nil
		})
	}

	// For memory store, estimate based on data
	if ms, ok := store.(*MemoryStableStore); ok {
		ms.mu.RLock()
		defer ms.mu.RUnlock()

		for key, value := range ms.data {
			size += int64(len(key) + len(value))
		}
	}

	return size
}