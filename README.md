# Distributed Databases

## Features

- **8 Shard Groups**: One per language (zh, ja, ko, ms, fil, id, km, th)
- **4 Replicas per Group**: 32 total SQLite databases
- **FlexiRaft Consensus**: Dynamic quorum (2/4 requirement)
- **Ghost Entries**: Failed writes leave uncommitted log entries
- **Parallel Operations**: Elections and writes use concurrent execution
- **Raft Log Recovery**: Pull-based synchronization for lagging replicas
- **Detailed Metrics**: Progress reports every 250 entries

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
- **Failure tolerance**: Can lose 1 replica and still write

### **Consistency:**

- **Strong consistency**: Among voting replicas (2+)
- **Eventual consistency**: For non-voting replicas
- **Recovery time**: Typically <30s for 100 entries


---

### Project Architecture

```
[CSV File] 
    ↓
[main.py] → Reads novels, determines shard by language
    ↓
[Election Manager] → Ensures leader exists (on-demand)
    ↓
[Write to Raft Logs] → Parallel writes to 4 replicas
    ↓
[Check 2/4 Quorum] → Did 2+ replicas succeed?
    ↓ YES
[Update commit_index] → Mark entries as committed
    ↓
[Commit Daemon] → (Background, every ~2s)
    ├─ Reads commit_index and last_applied
    ├─ Applies entries to SQLite
    └─ Updates last_applied
```

