# Distributed Database with FlexiRaft Consensus

A toy distributed database implementing FlexiRaft consensus with sharding and replication.

## Features

- **8 Shard Groups**: One per language (zh, ja, ko, ms, fil, id, km, th)
- **4 Replicas per Group**: 32 total SQLite databases
- **FlexiRaft Consensus**: Dynamic quorum (2/4 requirement)
- **Ghost Entries**: Failed writes leave uncommitted log entries
- **Parallel Operations**: Elections and writes use concurrent execution
- **Raft Log Recovery**: Pull-based synchronization for lagging replicas
- **Detailed Metrics**: Progress reports every 250 entries

## Quick Start

### 1. Test Components

```bash
python3 test_components.py
```

This verifies all core components work:
- System initialization (32 shards)
- CSV generation and loading
- Database operations
- Raft log management
- State management

### 2. Run Full System (Simple Test - 100 records)

```bash
# Generate sample data and populate
python3 main.py --generate-sample --num-records 100
```

**Note**: The full system with elections may take time. For testing, use the component test above.

### 3. Reset System

```bash
python3 reset_system.py --yes
```

## Architecture

```
32 Shards = 8 Languages × 4 Replicas

Languages: zh, ja, ko, ms, fil, id, km, th
Nodes: node_0 through node_7

Example for Chinese (zh):
  Replicas: node_0, node_7, node_1, node_2
  Quorum: Any 2 of 4 replicas must agree for writes
```

## Key Components

### Core Modules

- **db/raft_log_manager.py**: Raft log operations with ghost entry support
- **db/state_manager.py**: State management (sync for leader, async for followers)
- **db/db_schema.py**: SQLite schema and operations
- **db/csv_loader.py**: CSV parsing and sample data generation
- **db/initialize_system.py**: System initialization (32 shards)
- **db/flexiraft_coordinator.py**: FlexiRaft consensus bridge
- **db/election_manager.py**: Parallel leader elections
- **main.py**: Main orchestrator with metrics
- **reset_system.py**: System reset utility

### FlexiRaft Integration

Uses the existing FlexiRaft implementation:
- **wrapper/flexiraft.py**: Core FlexiRaft algorithm
- **wrapper/flexiraft_helper.py**: Algorithm 2 (getPotentialNextLeaders)

## File Structure

```
db/
  node_0/ through node_7/     # 8 nodes
    zh/, ja/, ko/, ...        # Language shards
      novel.db                # SQLite database
      state.json              # Raft state
      raft_log.json           # Operation log
  node_config.json            # System configuration
```

## Testing

### Component Tests
```bash
python3 test_components.py
```

### Reset and Reinitialize
```bash
python3 reset_system.py --yes
python3 main.py --generate-sample --num-records 1000
```

## Configuration

Edit `db/node_config.json` to modify:
- Languages supported
- Node assignments
- Replica distribution

## Requirements

- Python 3.8+
- SQLite3
- No external dependencies (uses standard library)

## Technical Details

### Quorum System
- **Mode**: Dynamic (FlexiRaft)
- **Requirement**: 2 out of 4 replicas
- **Write Process**:
  1. Leader appends to raft log
  2. Parallel append to all replicas
  3. Check quorum (2/4)
  4. If quorum: apply to databases
  5. If failed: retry 5 times, then leave ghost entries

### Ghost Entries
- Uncommitted raft log entries from failed writes
- Not rolled back (left in place)
- Overwritten on next successful write
- Standard Raft log reconciliation

### Metrics
- Progress reports every 250 entries
- Average quorum success rate
- Average latency per batch
- Per-language statistics
- Shard distribution

## Troubleshooting

### Elections Hang
The election process uses infinite retries. For testing, use:
```bash
python3 test_components.py  # Tests without elections
```

### Database Locked
Reset the system:
```bash
python3 reset_system.py --yes
```

### Import Errors
Type checker warnings are normal and don't affect runtime. Run tests to verify:
```bash
python3 test_components.py
```
