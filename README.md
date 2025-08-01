# Distributed Key-Value Store with Raft Consensus

A production-ready distributed key-value store implementation using the Raft consensus algorithm, built in Go for educational and learning purposes.

## 🎯 Overview

This project implements a complete distributed key-value store that demonstrates core concepts of distributed systems:

- **Consensus Algorithm**: Raft consensus for leader election and log replication
- **Strong Consistency**: All operations go through the leader ensuring linearizability
- **Fault Tolerance**: Survives minority node failures (up to (N-1)/2 in N-node cluster)
- **Persistent Storage**: Data survives node restarts and failures
- **HTTP API**: RESTful interface for easy integration
- **Client Library**: Go client with automatic leader discovery and retries

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    HTTP API Layer                          │
│  GET /kv/{key}  PUT /kv/{key}  DELETE /kv/{key}           │
│  POST /batch    GET /cluster/info  GET /stats             │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                    Raft Node                               │
│  • Leader Election    • Log Replication                   │
│  • Client Redirection • Request Processing               │
└─────────────┬─────────────────────────┬─────────────────────┘
              │                         │
┌─────────────▼─────────────┐ ┌─────────▼─────────────────────┐
│     State Machine         │ │      Storage Layer          │
│  • Key-Value Store        │ │  • Persistent Log           │
│  • Command Application    │ │  • Stable State             │
│  • Snapshots             │ │  • File/Memory Backend      │
└───────────────────────────┘ └─────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                Network Transport                            │
│  • HTTP-based RPC        • Peer Communication             │
│  • Vote Requests         • Append Entries                 │
│  • Connection Pooling    • Error Handling                 │
└─────────────────────────────────────────────────────────────┘
```

## 📁 Project Structure

```
├── types/
│   └── types.go              # Core data types and interfaces
├── storage/
│   ├── log_store.go          # Persistent log storage (file & memory)
│   └── stable_store.go       # Persistent state storage
├── statemachine/
│   └── kv_store.go           # Key-value state machine implementation
├── transport/
│   └── http_transport.go     # HTTP-based network transport
├── raft/
│   └── node.go               # Core Raft consensus algorithm
├── server/
│   └── http_server.go        # REST API server
├── client/
│   └── client.go             # Client library with automatic failover
├── cmd/
│   └── main.go               # Main application and examples
└── README.md                 # This file
```

## 🚀 Quick Start

### Prerequisites

- Go 1.19 or later
- No external dependencies (uses only Go standard library)

### Running a 3-Node Cluster

**Terminal 1 (Node 1 - will become leader):**
```bash
go run cmd/main.go -mode=server -id=node1 \
  -raft-addr=localhost:9001 -http-addr=localhost:8001 \
  -data-dir=./data -peers=localhost:9002,localhost:9003
```

**Terminal 2 (Node 2):**
```bash
go run cmd/main.go -mode=server -id=node2 \
  -raft-addr=localhost:9002 -http-addr=localhost:8002 \
  -data-dir=./data -peers=localhost:9001,localhost:9003
```

**Terminal 3 (Node 3):**
```bash
go run cmd/main.go -mode=server -id=node3 \
  -raft-addr=localhost:9003 -http-addr=localhost:8003 \
  -data-dir=./data -peers=localhost:9001,localhost:9002
```

**Terminal 4 (Client):**
```bash
go run cmd/main.go -mode=client
```

### Using the HTTP API

```bash
# Set a value
curl -X PUT http://localhost:8001/kv/hello -d '{"value":"world"}'

# Get a value
curl -X GET http://localhost:8001/kv/hello

# Delete a value
curl -X DELETE http://localhost:8001/kv/hello

# Batch operations
curl -X POST http://localhost:8001/batch -d '{
  "operations": [
    {"type": "set", "key": "key1", "value": "value1"},
    {"type": "set", "key": "key2", "value": "value2"},
    {"type": "get", "key": "key1"}
  ]
}'

# Cluster information
curl -X GET http://localhost:8001/cluster/info

# Node status
curl -X GET http://localhost:8001/status

# Health check
curl -X GET http://localhost:8001/health
```

## 🔧 Configuration

### Command Line Options

- `-mode`: Operation mode (`server`, `client`, `demo`)
- `-id`: Unique node identifier
- `-raft-addr`: Address for Raft peer communication
- `-http-addr`: Address for HTTP API server
- `-data-dir`: Directory for persistent data storage
- `-peers`: Comma-separated list of peer Raft addresses

### Raft Configuration

```go
config := raft.DefaultConfig()
config.ElectionTimeoutMin = 150 * time.Millisecond
config.ElectionTimeoutMax = 300 * time.Millisecond
config.HeartbeatInterval = 50 * time.Millisecond
config.MaxEntriesPerMessage = 100
```

## 📚 Core Concepts

### Raft Consensus Algorithm

The implementation follows the Raft paper specifications:

1. **Leader Election**
   - Nodes start as followers
   - Election timeout triggers candidate state
   - Majority vote required to become leader
   - Split vote prevention through randomized timeouts

2. **Log Replication**
   - All writes go through the current leader
   - Leader replicates entries to followers
   - Entries committed when majority acknowledges
   - Followers redirect clients to leader

3. **Safety Properties**
   - Election Safety: At most one leader per term
   - Leader Append-Only: Leaders never overwrite entries
   - Log Matching: Logs are consistent across nodes
   - Leader Completeness: Committed entries appear in future leaders
   - State Machine Safety: Applied entries are identical across nodes

### State Machine

The key-value store state machine provides:

- **Operations**: GET, SET, DELETE
- **Consistency**: All operations are deterministic and idempotent
- **Persistence**: State survives restarts through log replay
- **Snapshots**: Compact representation for log truncation (planned)

### Storage Layer

- **Log Store**: Persistent storage for Raft log entries
- **Stable Store**: Persistent storage for Raft metadata (term, votedFor)
- **Backends**: File-based and in-memory implementations
- **Durability**: fsync ensures data reaches disk before acknowledgment

## 🔍 Monitoring and Debugging

### Status Endpoints

- `GET /status` - Current node status
- `GET /stats` - Detailed statistics
- `GET /cluster/info` - Cluster membership and state
- `GET /health` - Health check

### Key Metrics

- **Raft Metrics**: Term, commit index, election count
- **Performance**: Request latency, throughput
- **Errors**: Failed elections, replication errors
- **Network**: Message counts, transport errors

### Debugging Tips

1. **Check Leader**: Ensure cluster has elected a leader
2. **Log Entries**: Verify log replication across nodes
3. **Network**: Check connectivity between peers
4. **Storage**: Verify persistent data integrity
5. **Timeouts**: Adjust based on network latency

## 🧪 Testing Scenarios

### Basic Functionality
```bash
# Test basic operations
curl -X PUT http://localhost:8001/kv/test -d '{"value":"data"}'
curl -X GET http://localhost:8001/kv/test
curl -X DELETE http://localhost:8001/kv/test
```

### Fault Tolerance
```bash
# 1. Start 3-node cluster
# 2. Kill the leader node
# 3. Verify new leader election
# 4. Verify data consistency
# 5. Restart killed node
```

### Network Partitions
```bash
# 1. Use iptables to simulate network partitions
# 2. Verify minority partition becomes read-only
# 3. Verify majority partition continues operating
# 4. Heal partition and verify reconciliation
```

## 🚀 Advanced Features

### Planned Enhancements

1. **Log Compaction**
   - State machine snapshots
   - Automatic log truncation
   - Snapshot transfer for new nodes

2. **Dynamic Membership**
   - Add/remove nodes safely
   - Joint consensus implementation
   - Automatic node discovery

3. **Performance Optimizations**
   - Batching of log entries
   - Pipeline replication
   - Read-only query optimization

4. **Operational Features**
   - Metrics export (Prometheus)
   - Distributed tracing
   - Configuration hot-reload

### Production Considerations

1. **Security**
   - TLS encryption for all communication
   - Authentication and authorization
   - Certificate management

2. **Deployment**
   - Container orchestration (Kubernetes)
   - Service discovery integration
   - Load balancer configuration

3. **Monitoring**
   - Comprehensive metrics
   - Alerting rules
   - Performance dashboards

## 🐛 Troubleshooting

### Common Issues

**Split Brain / No Leader**
- Check network connectivity between nodes
- Verify node addresses are reachable
- Ensure majority of nodes are running

**Data Inconsistency**
- Verify all operations go through leader
- Check for network partitions
- Validate log replication

**Performance Issues**
- Monitor election frequency
- Check network latency between nodes
- Tune timeout values

**Storage Problems**
- Verify disk space availability
- Check file permissions
- Monitor I/O performance

### Debug Commands

```bash
# Check cluster status
curl -s http://localhost:8001/cluster/info | jq

# Monitor statistics
watch -n 1 'curl -s http://localhost:8001/stats | jq'

# Check individual node status
for port in 8001 8002 8003; do
  echo "Node on port $port:"
  curl -s http://localhost:$port/status | jq .state
done
```

## 📖 Learning Resources

- **Original Paper**: ["In Search of an Understandable Consensus Algorithm" by Diego Ongaro and John Ousterhout](https://raft.github.io/raft.pdf)
- **Raft Visualization**: [https://raft.github.io/