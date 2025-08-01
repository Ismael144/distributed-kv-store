package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/ismael144/distributed-kv-store/types"
)

// FileLogStore implements LogStore interface using file-based storage
// Each log entry is stored as a separate file for simplicity
// In production, you'd want to use more efficient storage like a log-structured file
type FileLogStore struct {
	mu      sync.RWMutex
	dataDir string
	// Cache frequently accessed entries to improve performance
	cache     map[types.LogIndex]*types.LogEntry
	cacheSize int
	// Track the range of stored entries
	firstIndex types.LogIndex
	lastIndex  types.LogIndex
}

// NewFileLogStore creates a new file-based log store
func NewFileLogStore(dataDir string) (*FileLogStore, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	ls := &FileLogStore{
		dataDir:   dataDir,
		cache:     make(map[types.LogIndex]*types.LogEntry),
		cacheSize: 1000, // Cache up to 1000 entries
	}

	// Load existing entries to determine index range
	if err := ls.loadIndexRange(); err != nil {
		return nil, fmt.Errorf("failed to load index range: %w", err)
	}

	return ls, nil
}

// StoreEntry stores a single log entry to disk
func (ls *FileLogStore) StoreEntry(entry types.LogEntry) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	return ls.storeEntryUnlocked(entry)
}

// storeEntryUnlocked stores an entry without acquiring the lock (internal use)
func (ls *FileLogStore) storeEntryUnlocked(entry types.LogEntry) error {
	// Serialize entry to JSON
	data, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal log entry: %w", err)
	}

	// Write to file
	filename := ls.entryFilename(entry.Index)
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write log entry to file: %w", err)
	}

	// Update cache
	ls.updateCache(entry.Index, &entry)

	// Update index range
	if ls.firstIndex == 0 || entry.Index < ls.firstIndex {
		ls.firstIndex = entry.Index
	}
	if entry.Index > ls.lastIndex {
		ls.lastIndex = entry.Index
	}

	return nil
}

// StoreEntries stores multiple log entries atomically
func (ls *FileLogStore) StoreEntries(entries []types.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	ls.mu.Lock()
	defer ls.mu.Unlock()

	// Store each entry - in a real implementation, you'd want true atomicity
	// For simplicity, we'll store them one by one
	for _, entry := range entries {
		if err := ls.storeEntryUnlocked(entry); err != nil {
			return fmt.Errorf("failed to store entry at index %d: %w", entry.Index, err)
		}
	}

	return nil
}

// GetEntry retrieves a log entry by index
func (ls *FileLogStore) GetEntry(index types.LogIndex) (*types.LogEntry, error) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	// Check cache first
	if entry, exists := ls.cache[index]; exists {
		// Return a copy to prevent external modification
		entryCopy := *entry
		return &entryCopy, nil
	}

	// Load from disk
	entry, err := ls.loadEntryFromDisk(index)
	if err != nil {
		return nil, err
	}

	// Update cache
	ls.updateCache(index, entry)

	// Return a copy
	entryCopy := *entry
	return &entryCopy, nil
}

// GetEntries retrieves log entries in range [start, end] (inclusive)
func (ls *FileLogStore) GetEntries(start, end types.LogIndex) ([]types.LogEntry, error) {
	if start > end {
		return nil, fmt.Errorf("invalid range: start (%d) > end (%d)", start, end)
	}

	ls.mu.RLock()
	defer ls.mu.RUnlock()

	var entries []types.LogEntry

	for i := start; i <= end; i++ {
		// Try cache first
		if entry, exists := ls.cache[i]; exists {
			entries = append(entries, *entry)
			continue
		}

		// Load from disk
		entry, err := ls.loadEntryFromDisk(i)
		if err != nil {
			// If entry doesn't exist, we've hit a gap in the log
			if os.IsNotExist(err) {
				return entries, fmt.Errorf("log entry at index %d not found", i)
			}
			return entries, fmt.Errorf("failed to load entry at index %d: %w", i, err)
		}

		// Update cache and add to results
		ls.updateCache(i, entry)
		entries = append(entries, *entry)
	}

	return entries, nil
}

// LastIndex returns the index of the last stored entry
func (ls *FileLogStore) LastIndex() types.LogIndex {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return ls.lastIndex
}

// FirstIndex returns the index of the first stored entry
func (ls *FileLogStore) FirstIndex() types.LogIndex {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return ls.firstIndex
}

// DeleteRange deletes entries in range [start, end] (inclusive)
func (ls *FileLogStore) DeleteRange(start, end types.LogIndex) error {
	if start > end {
		return fmt.Errorf("invalid range: start (%d) > end (%d)", start, end)
	}

	ls.mu.Lock()
	defer ls.mu.Unlock()

	// Delete files and update cache
	for i := start; i <= end; i++ {
		filename := ls.entryFilename(i)
		
		// Remove file (ignore if doesn't exist)
		if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to delete entry file at index %d: %w", i, err)
		}

		// Remove from cache
		delete(ls.cache, i)
	}

	// Update index range if necessary
	if start <= ls.firstIndex {
		ls.firstIndex = end + 1
		if ls.firstIndex > ls.lastIndex {
			// No entries left
			ls.firstIndex = 0
			ls.lastIndex = 0
		}
	}

	return nil
}

// Helper methods

// entryFilename generates filename for a log entry
func (ls *FileLogStore) entryFilename(index types.LogIndex) string {
	return filepath.Join(ls.dataDir, fmt.Sprintf("entry_%010d.json", index))
}

// loadEntryFromDisk loads a log entry from disk
func (ls *FileLogStore) loadEntryFromDisk(index types.LogIndex) (*types.LogEntry, error) {
	filename := ls.entryFilename(index)
	
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var entry types.LogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("failed to unmarshal log entry: %w", err)
	}

	return &entry, nil
}

// updateCache updates the LRU cache with a new entry
func (ls *FileLogStore) updateCache(index types.LogIndex, entry *types.LogEntry) {
	// Simple cache management - remove oldest entries if cache is full
	if len(ls.cache) >= ls.cacheSize {
		// Find the smallest index to remove (simple LRU approximation)
		var minIndex types.LogIndex
		first := true
		for idx := range ls.cache {
			if first || idx < minIndex {
				minIndex = idx
				first = false
			}
		}
		delete(ls.cache, minIndex)
	}

	// Add/update entry in cache
	entryCopy := *entry
	ls.cache[index] = &entryCopy
}

// loadIndexRange scans the data directory to determine the range of stored entries
func (ls *FileLogStore) loadIndexRange() error {
	entries, err := os.ReadDir(ls.dataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	var indices []types.LogIndex

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Parse filename to extract index
		name := entry.Name()
		if !strings.HasPrefix(name, "entry_") || !strings.HasSuffix(name, ".json") {
			continue
		}

		// Extract index from filename (entry_0000000001.json -> 1)
		indexStr := strings.TrimPrefix(name, "entry_")
		indexStr = strings.TrimSuffix(indexStr, ".json")
		
		index, err := strconv.ParseUint(indexStr, 10, 64)
		if err != nil {
			// Skip invalid filenames
			continue
		}

		indices = append(indices, types.LogIndex(index))
	}

	// Sort indices to find range
	if len(indices) > 0 {
		sort.Slice(indices, func(i, j int) bool {
			return indices[i] < indices[j]
		})
		ls.firstIndex = indices[0]
		ls.lastIndex = indices[len(indices)-1]
	}

	return nil
}

// In-Memory Log Store for testing and development

// MemoryLogStore implements LogStore interface using in-memory storage
// This is useful for testing and when persistence is not required
type MemoryLogStore struct {
	mu      sync.RWMutex
	entries map[types.LogIndex]types.LogEntry
}

// NewMemoryLogStore creates a new in-memory log store
func NewMemoryLogStore() *MemoryLogStore {
	return &MemoryLogStore{
		entries: make(map[types.LogIndex]types.LogEntry),
	}
}

// StoreEntry stores a single log entry in memory
func (ls *MemoryLogStore) StoreEntry(entry types.LogEntry) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	// Store a copy to prevent external modification
	ls.entries[entry.Index] = entry
	return nil
}

// StoreEntries stores multiple log entries in memory
func (ls *MemoryLogStore) StoreEntries(entries []types.LogEntry) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	for _, entry := range entries {
		ls.entries[entry.Index] = entry
	}
	return nil
}

// GetEntry retrieves a log entry by index
func (ls *MemoryLogStore) GetEntry(index types.LogIndex) (*types.LogEntry, error) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	entry, exists := ls.entries[index]
	if !exists {
		return nil, fmt.Errorf("log entry at index %d not found", index)
	}

	// Return a copy
	entryCopy := entry
	return &entryCopy, nil
}

// GetEntries retrieves log entries in range [start, end]
func (ls *MemoryLogStore) GetEntries(start, end types.LogIndex) ([]types.LogEntry, error) {
	if start > end {
		return nil, fmt.Errorf("invalid range: start (%d) > end (%d)", start, end)
	}

	ls.mu.RLock()
	defer ls.mu.RUnlock()

	var entries []types.LogEntry

	for i := start; i <= end; i++ {
		entry, exists := ls.entries[i]
		if !exists {
			return entries, fmt.Errorf("log entry at index %d not found", i)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// LastIndex returns the index of the last stored entry
func (ls *MemoryLogStore) LastIndex() types.LogIndex {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	var maxIndex types.LogIndex
	for index := range ls.entries {
		if index > maxIndex {
			maxIndex = index
		}
	}
	return maxIndex
}

// FirstIndex returns the index of the first stored entry
func (ls *MemoryLogStore) FirstIndex() types.LogIndex {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	if len(ls.entries) == 0 {
		return 0
	}

	var minIndex types.LogIndex
	first := true
	for index := range ls.entries {
		if first || index < minIndex {
			minIndex = index
			first = false
		}
	}
	return minIndex
}

// DeleteRange deletes entries in range [start, end]
func (ls *MemoryLogStore) DeleteRange(start, end types.LogIndex) error {
	if start > end {
		return fmt.Errorf("invalid range: start (%d) > end (%d)", start, end)
	}

	ls.mu.Lock()
	defer ls.mu.Unlock()

	for i := start; i <= end; i++ {
		delete(ls.entries, i)
	}

	return nil
}

// GetAllEntries returns all stored entries (useful for debugging)
func (ls *MemoryLogStore) GetAllEntries() []types.LogEntry {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	var entries []types.LogEntry
	var indices []types.LogIndex

	// Collect and sort indices
	for index := range ls.entries {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool {
		return indices[i] < indices[j]
	})

	// Build sorted entries list
	for _, index := range indices {
		entries = append(entries, ls.entries[index])
	}

	return entries
}