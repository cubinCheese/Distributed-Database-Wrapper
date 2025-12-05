# Useful Commands

### Run System with Sample Data
```bash
python main.py --generate-sample --num-records 100
```

### Verify Everything Works
```bash
python test_system_integration.py
```
## Test On-Demand Elections

Skip pre-election and let system elect leaders as needed:

```bash
# Reset first
python reset_system.py --yes

# Run without pre-election
python main.py --skip-election --generate-sample --num-records 50
```

### Simulate a Node Crash
```bash
# 1. Populate initial data
python main.py --generate-sample --num-records 100

# 2. Crash node_2 (simulates offline)
python tools/simulate_failure.py fail node_2
```

### System Continues Working
```bash
# 3. Add more data (works with only 3/4 replicas)
python main.py --skip-init --generate-sample --num-records 50
```

### Node Comes Back Online
```bash
# 4. Bring node_2 back online
python tools/simulate_failure.py recover node_2
```
### Verify Recovery
```bash
# 5. Check if node recovered
python tools/simulate_failure.py status node_2
```

## Check Database Contents

### Query a Specific Shard
```bash
sqlite3 db/node_0/zh/novel.db "SELECT COUNT(*) FROM novels;"
```

### Check All Chinese Replicas
```bash
for node in node_0 node_1 node_2 node_7; do
  echo -n "$node: "
  sqlite3 db/$node/zh/novel.db "SELECT COUNT(*) FROM novels WHERE original_language='zh';" 2>/dev/null || echo "N/A"
done
```


## Utility Commands
### Reset Everything
```bash
python reset_system.py --yes
```

### Run with Different Record Counts
```bash
# Small test (50 records)
python main.py --generate-sample --num-records 50

# Medium test (1000 records)
python main.py --generate-sample --num-records 1000

# Large test (10000 records)
python main.py --generate-sample --num-records 10000
```

### Check System Status
```bash
# List all databases
find db -name "novel.db" -exec echo {} \; -exec sqlite3 {} "SELECT COUNT(*) FROM novels;" \;

# List all state files
find db -name "state.json" -exec echo {} \; -exec python3 -c "import json, sys; s=json.load(open(sys.argv[1])); print(f\"role={s['role']}, commit={s['commit_index']}, applied={s['last_applied']}\")" {} \;
```

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

---

# Useful Commands

### Run System with Sample Data
```bash
python main.py --generate-sample --num-records 100
```

### Verify Everything Works
```bash
python test_system_integration.py
```
## Test On-Demand Elections

Skip pre-election and let system elect leaders as needed:

```bash
# Reset first
python reset_system.py --yes

# Run without pre-election
python main.py --skip-election --generate-sample --num-records 50
```

### Simulate a Node Crash
```bash
# 1. Populate initial data
python main.py --generate-sample --num-records 100

# 2. Crash node_2 (simulates offline)
python tools/simulate_failure.py fail node_2
```

### System Continues Working
```bash
# 3. Add more data (works with only 3/4 replicas)
python main.py --skip-init --generate-sample --num-records 50
```

### Node Comes Back Online
```bash
# 4. Bring node_2 back online
python tools/simulate_failure.py recover node_2
```
### Verify Recovery
```bash
# 5. Check if node recovered
python tools/simulate_failure.py status node_2
```

## Check Database Contents

### Query a Specific Shard
```bash
sqlite3 db/node_0/zh/novel.db "SELECT COUNT(*) FROM novels;"
```

### Check All Chinese Replicas
```bash
for node in node_0 node_1 node_2 node_7; do
  echo -n "$node: "
  sqlite3 db/$node/zh/novel.db "SELECT COUNT(*) FROM novels WHERE original_language='zh';" 2>/dev/null || echo "N/A"
done
```


## Utility Commands
### Reset Everything
```bash
python reset_system.py --yes
```

### Run with Different Record Counts
```bash
# Small test (50 records)
python main.py --generate-sample --num-records 50

# Medium test (1000 records)
python main.py --generate-sample --num-records 1000

# Large test (10000 records)
python main.py --generate-sample --num-records 10000
```

### Check System Status
```bash
# List all databases
find db -name "novel.db" -exec echo {} \; -exec sqlite3 {} "SELECT COUNT(*) FROM novels;" \;

# List all state files
find db -name "state.json" -exec echo {} \; -exec python3 -c "import json, sys; s=json.load(open(sys.argv[1])); print(f\"role={s['role']}, commit={s['commit_index']}, applied={s['last_applied']}\")" {} \;
```


