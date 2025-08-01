package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ismael144/distributed-kv-store/client"
	"github.com/ismael144/distributed-kv-store/raft"
	"github.com/ismael144/distributed-kv-store/server"
	"github.com/ismael144/distributed-kv-store/statemachine"
	"github.com/ismael144/distributed-kv-store/storage"
	"github.com/ismael144/distributed-kv-store/transport"
	"github.com/ismael144/distributed-kv-store/types"
)

// Configuration for a single node
type NodeConfig struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raft_addr"`
	HTTPAddr string `json:"http_addr"`
	DataDir  string `json:"data_dir"`
	Peers    string `json:"peers"` // Comma-separated list of peer addresses
}

func main() {
	// Command line flags
	var (
		nodeID   = flag.String("id", "node1", "Node ID")
		raftAddr = flag.String("raft-addr", "localhost:9001", "Raft transport address")
		httpAddr = flag.String("http-addr", "localhost:8001", "HTTP server address")
		dataDir  = flag.String("data-dir", "./data", "Data directory")
		peers    = flag.String("peers", "", "Comma-separated list of peer addresses (raft addresses)")
		mode     = flag.String("mode", "server", "Mode: server, client, or demo")
		servers  = flag.String("servers", "localhost:8001,localhost:8002,localhost:8003", "Server addresses for client mode")
	)
	flag.Parse()

	switch *mode {
	case "server":
		runServer(*nodeID, *raftAddr, *httpAddr, *dataDir, *peers)
	case "client":
		runClient(*servers)
	case "demo":
		runDemo()
	default:
		fmt.Printf("Unknown mode: %s\n", *mode)
		fmt.Println("Available modes: server, client, demo")
		os.Exit(1)
	}
}

// runServer starts a single Raft node with HTTP API
func runServer(nodeID, raftAddr, httpAddr, dataDir, peersStr string) {
	fmt.Printf("Starting Raft node %s...\n", nodeID)
	fmt.Printf("Raft address: %s\n", raftAddr)
	fmt.Printf("HTTP address: %s\n", httpAddr)
	fmt.Printf("Data directory: %s\n", dataDir)

	// Parse peers
	var peerAddrs []string
	if peersStr != "" {
		peerAddrs = strings.Split(peersStr, ",")
	}
	fmt.Printf("Peers: %v\n", peerAddrs)

	// Create data directory
	nodeDataDir := filepath.Join(dataDir, nodeID)
	if err := os.MkdirAll(nodeDataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Initialize storage components
	logStore, err := storage.NewFileLogStore(filepath.Join(nodeDataDir, "logs"))
	if err != nil {
		log.Fatalf("Failed to create log store: %v", err)
	}

	stableStore, err := storage.NewFileStableStore(filepath.Join(nodeDataDir, "stable"))
	if err != nil {
		log.Fatalf("Failed to create stable store: %v", err)
	}

	// Initialize state machine
	stateMachine := statemachine.NewKVStateMachine()

	// Initialize transport
	raftTransport := transport.NewHTTPTransport(raftAddr, types.NodeID(nodeID))

	// Build peer map (NodeID -> Address)
	peers := make(map[types.NodeID]string)
	for i, peerAddr := range peerAddrs {
		peerID := fmt.Sprintf("node%d", i+2) // Simple peer ID generation
		if peerAddr != raftAddr { // Don't include self
			peers[types.NodeID(peerID)] = peerAddr
		}
	}

	// Create and configure Raft node
	raftConfig := raft.DefaultConfig()
	raftConfig.ElectionTimeoutMin = 150 * time.Millisecond
	raftConfig.ElectionTimeoutMax = 300 * time.Millisecond
	raftConfig.HeartbeatInterval = 50 * time.Millisecond

	raftNode, err := raft.NewNode(
		types.NodeID(nodeID),
		peers,
		logStore,
		stableStore,
		stateMachine,
		raftTransport,
		raftConfig,
	)

	if err != nil {
		log.Fatalf("Failed to create Raft node: %v", err)
	}

	// Add peers to transport
	for peerID, peerAddr := range peers {
		raftTransport.AddPeer(peerID, peerAddr)
	}

	// Create HTTP server
	serverConfig := server.DefaultServerConfig()
	serverConfig.EnableLogging = true
	serverConfig.VerboseErrors = true

	httpServer := server.NewHTTPServer(httpAddr, raftNode, serverConfig)

	// Start components
	fmt.Println("Starting transport...")
	if err := raftTransport.Start(); err != nil {
		log.Fatalf("Failed to start transport: %v", err)
	}

	fmt.Println("Starting Raft node...")
	if err := raftNode.Start(); err != nil {
		log.Fatalf("Failed to start Raft node: %v", err)
	}

	log.Println("This is running")
	fmt.Println("Starting HTTP server...")
	if err := httpServer.Start(); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}

	fmt.Printf("Node %s started successfully!\n", nodeID)
	fmt.Printf("HTTP API available at http://%s\n", httpAddr)
	fmt.Println("\nExample commands:")
	fmt.Printf("  curl -X PUT http://%s/kv/hello -d '{\"value\":\"world\"}'\n", httpAddr)
	fmt.Printf("  curl -X GET http://%s/kv/hello\n", httpAddr)
	fmt.Printf("  curl -X GET http://%s/status\n", httpAddr)
	fmt.Printf("  curl -X GET http://%s/cluster/info\n", httpAddr)


	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")

	// Graceful shutdown
	if err := httpServer.Stop(); err != nil {
		fmt.Printf("Error stopping HTTP server: %v\n", err)
	}

	if err := raftNode.Stop(); err != nil {
		fmt.Printf("Error stopping Raft node: %v\n", err)
	}

	if err := raftTransport.Stop(); err != nil {
		fmt.Printf("Error stopping transport: %v\n", err)
	}

	fmt.Println("Shutdown complete")
}

// runClient demonstrates client usage
func runClient(serversStr string) {
	fmt.Println("Running client example...")

	// Parse server addresses
	serverAddrs := strings.Split(serversStr, ",")
	fmt.Printf("Connecting to servers: %v\n", serverAddrs)

	// Create client
	config := client.DefaultClientConfig()
	config.Verbose = true
	config.MaxRetries = 3

	kvClient := client.NewClient(serverAddrs, config)

	// Wait for cluster to be ready
	fmt.Println("Waiting for cluster to be ready...")
	if err := kvClient.WaitForCluster(30 * time.Second); err != nil {
		log.Fatalf("Cluster not ready: %v", err)
	}
	fmt.Println("Cluster is ready!")

	// Demonstrate basic operations
	fmt.Println("\n=== Basic Operations ===")

	// Set some values
	fmt.Println("Setting values...")
	if resp, err := kvClient.Set("hello", "world"); err != nil {
		fmt.Printf("Error setting hello: %v\n", err)
	} else {
		fmt.Printf("Set hello=world (index: %d)\n", resp.Index)
	}

	if resp, err := kvClient.Set("foo", "bar"); err != nil {
		fmt.Printf("Error setting foo: %v\n", err)
	} else {
		fmt.Printf("Set foo=bar (index: %d)\n", resp.Index)
	}

	// Get values
	fmt.Println("Getting values...")
	if resp, err := kvClient.Get("hello"); err != nil {
		fmt.Printf("Error getting hello: %v\n", err)
	} else {
		fmt.Printf("hello = %s\n", resp.Value)
	}

	if resp, err := kvClient.Get("foo"); err != nil {
		fmt.Printf("Error getting foo: %v\n", err)
	} else {
		fmt.Printf("foo = %s\n", resp.Value)
	}

	// Try to get non-existent key
	if resp, err := kvClient.Get("nonexistent"); err != nil {
		fmt.Printf("Error getting nonexistent: %v\n", err)
	} else {
		fmt.Printf("nonexistent key result: success=%v, error=%s\n", resp.Success, resp.Error)
	}

	// Demonstrate batch operations
	fmt.Println("\n=== Batch Operations ===")

	// Set multiple values
	pairs := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	fmt.Println("Setting multiple values...")
	if err := kvClient.SetMultiple(pairs); err != nil {
		fmt.Printf("Error in batch set: %v\n", err)
	} else {
		fmt.Println("Batch set successful")
	}

	// Get multiple values
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	fmt.Println("Getting multiple values...")
	if values, err := kvClient.GetMultiple(keys); err != nil {
		fmt.Printf("Error in batch get: %v\n", err)
	} else {
		for key, value := range values {
			fmt.Printf("  %s = %s\n", key, value)
		}
	}

	// Demonstrate cluster information
	fmt.Println("\n=== Cluster Information ===")

	// Get cluster info
	if info, err := kvClient.GetClusterInfo(); err != nil {
		fmt.Printf("Error getting cluster info: %v\n", err)
	} else {
		fmt.Printf("Cluster has %d nodes\n", len(info.Nodes))
		fmt.Printf("Current term: %d\n", info.Term)
		fmt.Printf("Commit index: %d\n", info.CommitIndex)
		
		if info.LeaderID != nil {
			fmt.Printf("Leader: %s\n", *info.LeaderID)
		} else {
			fmt.Println("No leader currently")
		}

		fmt.Println("Nodes:")
		for _, node := range info.Nodes {
			fmt.Printf("  %s (%s) - %s, term %d, leader: %v\n",
				node.ID, node.Address, node.State, node.Term, node.IsLeader)
		}
	}

	// Get node statistics
	if stats, err := kvClient.GetStats(); err != nil {
		fmt.Printf("Error getting stats: %v\n", err)
	} else {
		fmt.Println("Node statistics:")
		fmt.Printf("  %+v\n", stats)
	}

	// Demonstrate error handling
	fmt.Println("\n=== Error Handling ===")

	// Try operations with all servers down (simulate)
	fmt.Println("Testing error handling...")
	
	// Delete some keys
	fmt.Println("Deleting keys...")
	if resp, err := kvClient.Delete("key1"); err != nil {
		fmt.Printf("Error deleting key1: %v\n", err)
	} else {
		fmt.Printf("Deleted key1, previous value: %s\n", resp.DeletedValue)
	}

	// Verify deletion
	if resp, err := kvClient.Get("key1"); err != nil {
		fmt.Printf("Error getting deleted key: %v\n", err)
	} else {
		fmt.Printf("Get deleted key result: success=%v, error=%s\n", resp.Success, resp.Error)
	}

	fmt.Println("\nClient demo completed!")
}

// runDemo runs a comprehensive demonstration
func runDemo() {
	fmt.Println("=== Raft Distributed Key-Value Store Demo ===")
	fmt.Println()
	fmt.Println("This demo shows how to set up and use the distributed key-value store.")
	fmt.Println("In a real deployment, you would run multiple nodes on different machines.")
	fmt.Println()

	// Instructions for manual setup
	fmt.Println("To run a 3-node cluster manually:")
	fmt.Println()
	fmt.Println("Terminal 1 (Node 1):")
	fmt.Println("  go run cmd/main.go -mode=server -id=node1 \\")
	fmt.Println("    -raft-addr=localhost:9001 -http-addr=localhost:8001 \\")
	fmt.Println("    -data-dir=./data -peers=localhost:9002,localhost:9003")
	fmt.Println()
	fmt.Println("Terminal 2 (Node 2):")
	fmt.Println("  go run cmd/main.go -mode=server -id=node2 \\")
	fmt.Println("    -raft-addr=localhost:9002 -http-addr=localhost:8002 \\")
	fmt.Println("    -data-dir=./data -peers=localhost:9001,localhost:9003")
	fmt.Println()
	fmt.Println("Terminal 3 (Node 3):")
	fmt.Println("  go run cmd/main.go -mode=server -id=node3 \\")
	fmt.Println("    -raft-addr=localhost:9003 -http-addr=localhost:8003 \\")
	fmt.Println("    -data-dir=./data -peers=localhost:9001,localhost:9002")
	fmt.Println()
	fmt.Println("Terminal 4 (Client):")
	fmt.Println("  go run cmd/main.go -mode=client")
	fmt.Println()

	// API examples
	fmt.Println("=== HTTP API Examples ===")
	fmt.Println()
	fmt.Println("Basic key-value operations:")
	fmt.Println("  # Set a value")
	fmt.Println("  curl -X PUT http://localhost:8001/kv/hello -d '{\"value\":\"world\"}'")
	fmt.Println()
	fmt.Println("  # Get a value")
	fmt.Println("  curl -X GET http://localhost:8001/kv/hello")
	fmt.Println()
	fmt.Println("  # Delete a value")
	fmt.Println("  curl -X DELETE http://localhost:8001/kv/hello")
	fmt.Println()

	fmt.Println("Batch operations:")
	fmt.Println("  # Batch operations")
	fmt.Println("  curl -X POST http://localhost:8001/batch -d '{")
	fmt.Println("    \"operations\": [")
	fmt.Println("      {\"type\": \"set\", \"key\": \"key1\", \"value\": \"value1\"},")
	fmt.Println("      {\"type\": \"set\", \"key\": \"key2\", \"value\": \"value2\"},")
	fmt.Println("      {\"type\": \"get\", \"key\": \"key1\"}")
	fmt.Println("    ]")
}