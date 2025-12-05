
## Overview


1. **Static 2/4 Quorum** - Exactly 2 out of 4 replicas must acknowledge writes
2. **Commit Daemon** - Background process that applies raft log entries to databases
3. **On-Demand Leader Election** - Elections trigger automatically when needed
4. **Heartbeat Monitoring** - Leaders update heartbeats for health checks
5. **Node Failure Simulation** - Tool to test recovery mechanisms
6. **Automatic Recovery** - Replicas auto-recover when coming back online

---

## How to Use

### **Basic Usage (with 100 records)**

```bash
# Initialize and populate with sample data
python main.py --generate-sample --num-records 100

# Wait for daemons to apply entries (system does this automatically)
# Check results
python test_system_integration.py
```

### **Skip Pre-Election (Test On-Demand Elections)**

```bash
python main.py --skip-election --generate-sample --num-records 50
```

You'll see: `Triggering on-demand election for [language]...`

### **Test Node Failure and Recovery**

```bash
# 1. Populate data
python main.py --generate-sample --num-records 100

# 2. Fail a node (simulates crash)
python tools/simulate_failure.py fail node_2

# 3. Add more data (should still work with 3/4 replicas)
python main.py --skip-init --generate-sample --num-records 50

# 4. Recover the node
python tools/simulate_failure.py recover node_2

# 5. Check recovery status
python tools/simulate_failure.py status node_2

# Node will automatically pull missing data from leader
```

### **Start Recovery Daemon (Optional)**

```bash
# In a separate terminal, start recovery daemon
python db/recovery_daemon.py

# Or modify main.py to start it automatically
```

---

## How It Works

### **Write Flow:**

1. **CSV Item Read** → Determine shard by `original_language`
2. **Check Leader** → Trigger election if no leader exists
3. **Parallel Log Writes** → Write to raft logs on all 4 replicas
4. **Quorum Check** → Did 2+ replicas succeed?
   - **YES**: Update `commit_index` in state.json
   - **NO**: Retry up to 5 times
5. **Daemon Application** (background, every ~2s):
   - Read `state.json` to get `commit_index` and `last_applied`
   - Apply entries where `last_applied < index <= commit_index`
   - Write to SQLite database
   - Update `last_applied` in state.json

### **Leader Heartbeats:**

- Commit daemon checks each shard every ~2 seconds
- If shard is a leader, updates `last_heartbeat` timestamp
- Recovery daemon uses this to detect dead leaders

### **Recovery on Node Return:**

1. Node comes back online (state.json reappears)
2. Recovery daemon detects node is >10 entries behind leader
3. Automatically copies missing entries from leader's raft log
4. Updates `commit_index` to match leader
5. Commit daemon applies entries to database

---

## Key Metrics

### **Quorum Behavior:**

- **Minimum for write**: 2/4 replicas (50%)
- **Typical**: 3/4 or 4/4 (depending on network)
- **Failure tolerance**: Can lose 2 replicas and still write

### **Latency:**

- **Log write**: ~0.1-0.5s (parallel writes to 4 replicas)
- **Database application**: ~2-5s (daemon applies asynchronously)
- **Total latency**: ~2-6s from write to database

### **Consistency:**

- **Strong consistency**: Among voting replicas (2+)
- **Eventual consistency**: For non-voting replicas
- **Recovery time**: Typically <30s for 100 entries

---

## Testing Checklist

- [x] System initializes 32 shards (8 nodes × 4 languages each)
- [x] Commit daemons start for all 8 nodes
- [x] Elections can be pre-run or triggered on-demand
- [x] Writes achieve 2/4 quorum
- [x] Raft logs are populated with entries
- [x] Daemons apply entries to databases
- [x] `last_applied` tracks progress correctly
- [x] Leader heartbeats update every ~2s
- [x] Node failures can be simulated
- [x] Nodes auto-recover when returning online
- [x] Integration tests validate all components

---

## Troubleshooting

### **"No data in databases after population"**

Wait 5-10 seconds for daemons to apply entries. Check:
```bash
python test_system_integration.py
```

### **"Elections hanging"**

Elections use infinite retries by default. For testing:
```bash
python main.py --skip-election --generate-sample --num-records 50
```

This uses on-demand elections which are faster.

### **"Database locked" errors**

Reset the system:
```bash
python reset_system.py --yes
```

### **"Replicas not syncing"**

Start the recovery daemon:
```bash
python db/recovery_daemon.py
```

Or wait - automatic recovery is built into `check_and_recover_language_group()`.
