# Distributed Key-Value Store Database

A fault-tolerant, distributed key-value database built in Python with consistent hashing, configurable replication, and multiple consistency levels.

## Features

- **Consistent Hashing**: Automatic data distribution across nodes with minimal data movement when nodes join/leave
- **Configurable Replication**: Choose replication factor and consistency levels (`one`, `quorum`, `all`)
- **Fault Tolerance**: Data survives node failures through intelligent replication
- **HTTP REST API**: Simple HTTP interface for integration with any application
- **Interactive Shell**: User-friendly command-line interface for database operations
- **Real-time Cluster Management**: Dynamic node joining and cluster state synchronization

## Architecture

This system implements core distributed database concepts:

- **Hash Ring**: Uses MD5 hashing to distribute keys across nodes consistently
- **Replication Strategy**: Stores multiple copies of data on different nodes for fault tolerance
- **Consistency Levels**: 
  - `one`: Fast writes/reads, eventual consistency
  - `quorum`: Balanced performance, majority consensus
  - `all`: Strong consistency, all nodes must agree
- **Node-to-Node Communication**: Peer-to-peer networking for data replication

## Quick Start

### Prerequisites

- Python 3.7+
- `aiohttp` library

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/distributed-key-value-store.git
cd distributed-key-value-store
```

2. Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Mac/Linux
# On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Start the database:
```bash
python main.py
```

5. Choose option 1 to start cluster + interactive shell

**Note**: Always activate the virtual environment (`source venv/bin/activate`) before running the project. When you're done, deactivate it with:
```bash
deactivate
```

### Basic Usage

Once the shell is running, you can use these commands:

```bash
# Store data with different consistency levels
kvstore[1]> put user1 Alice one      # Fast write, 1 replica
kvstore[1]> put user2 Bob quorum     # Balanced, 2 replicas  
kvstore[1]> put user3 Charlie all    # Safe write, 3 replicas

# Retrieve data
kvstore[1]> get user1                # Fast read from any replica
kvstore[1]> get user2 quorum         # Read from majority
kvstore[1]> get user3 all            # Strong consistency read

# Delete data
kvstore[1]> delete user1 quorum

# Check cluster status
kvstore[1]> status                   # Current node status
kvstore[1]> status 2                 # Node 2 status
kvstore[1]> nodes                    # List all nodes

# Switch between nodes
kvstore[1]> switch 2                 # Connect to node 2
kvstore[2]> status                   # Now querying node 2

# Get help
kvstore[1]> help
kvstore[1]> quit
```

## Advanced Usage

### Running Cluster Only (Background Mode)

```bash
python main.py
# Choose option 2 for cluster-only mode
```

Then in another terminal:
```bash
python shell.py
```

### Using the REST API

The system exposes a REST API on ports 8001, 8002, 8003:

```bash
# Store data
curl -X PUT http://localhost:8001/keys/user1 \
  -H 'Content-Type: application/json' \
  -d '{"value": "Alice", "consistency": "all"}'

# Retrieve data  
curl "http://localhost:8001/keys/user1?consistency=quorum"

# Delete data
curl -X DELETE "http://localhost:8001/keys/user1?consistency=quorum"

# Check node status
curl http://localhost:8001/admin/status
```

### Command Line Options

```bash
python main.py --cluster    # Start cluster only
python main.py --shell      # Start cluster + shell
python main.py --help       # Show help
```

## Configuration

### Consistency Levels

| Level | Writes To | Reads From | Use Case |
|-------|-----------|------------|----------|
| `one` | 1 node | 1 node | High performance, eventual consistency |
| `quorum` | 2 nodes | 2 nodes | Balanced performance and consistency |
| `all` | 3 nodes | 3 nodes | Strong consistency, highest durability |

### Replication Factor

Default replication factor is 3, meaning each key is stored on 3 different nodes. This can be modified when creating nodes:

```python
node = DistributedNode("node1", port=8001, replication_factor=3)
```

## Implementation Details

### Consistent Hashing

The system uses MD5 hashing to place both nodes and keys on a virtual ring. Keys are assigned to the next N nodes clockwise from their hash position, where N is the replication factor.

### Replication Protocol

1. **Write Path**: Client sends write to any node → Node determines replica set → Replicates to required nodes based on consistency level
2. **Read Path**: Client sends read to any node → Node checks if it has the data locally → If not, queries appropriate replica nodes
3. **Consistency**: System supports tunable consistency with read/write quorum options

### Node Discovery

Nodes join the cluster by contacting any existing node. The contact node:
1. Adds the new node to its peer list and hash ring
2. Notifies all existing peers about the new node
3. Returns the complete peer list to the joining node

## Performance Characteristics

- **Write Latency**: Depends on consistency level (one < quorum < all)
- **Read Latency**: Fastest for `one`, slower for `quorum` and `all`
- **Availability**: Can survive (N-1)/2 node failures for reads with `quorum` consistency
- **Partition Tolerance**: Designed for network partition scenarios

## Limitations

- **In-Memory Storage**: Data is not persisted to disk (easily extendable)
- **No Automatic Rebalancing**: Adding/removing nodes doesn't trigger data migration
- **Basic Conflict Resolution**: Uses last-write-wins for concurrent updates
- **Single Datacenter**: Designed for nodes in same network segment

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make your changes and add tests
4. Commit your changes: `git commit -m 'Add feature'`
5. Push to the branch: `git push origin feature-name`
6. Submit a pull request

## Acknowledgments

This project implements concepts from:
- Amazon DynamoDB's consistent hashing
- Apache Cassandra's replication strategies  
- Redis Cluster's distributed architecture

Built as a learning project to understand distributed systems fundamentals.
