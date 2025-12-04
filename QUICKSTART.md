# 🚀 Quick Start Guide

## Complete Implementation with Static 2/4 Quorum

All changes have been successfully implemented! Your distributed database now has:

✅ **Static 2/4 quorum** (exactly 2 out of 4 replicas required)  
✅ **Commit daemon** (applies raft logs to databases asynchronously)  
✅ **On-demand leader election** (triggers automatically when needed)  
✅ **Heartbeat monitoring** (leaders update every ~2 seconds)  
✅ **Node failure simulation** (test recovery mechanisms)  
✅ **Automatic recovery** (replicas sync when coming back online)

---

## 🎯 Quick Test (3 minutes)

### Step 1: Run System with Sample Data
```bash
python main.py --generate-sample --num-records 100
```

**What happens:**
- Initializes 32 shards (8 nodes × 4 languages each)
- Starts 8 commit daemons (one per node)
- Triggers leader elections (or uses on-demand)
- Populates 100 novels using 2/4 quorum
- Waits 5 seconds for daemons to apply to databases

**Expected output:**
```
[1/5] Initializing system...
[2/5] Starting commit daemons for all nodes...
✓ Started 8 commit daemons
[3/5] Pre-electing leaders...
✓ Elections completed
[4/5] Loading CSV data...
✓ Loaded 100 novels
[5/5] Populating shards (2/4 quorum, daemon-based writes)...
✓ Quorum 2/4, 0.15s
⏳ Waiting 5 seconds for daemons to apply entries...
```

### Step 2: Verify Everything Works
```bash
python test_system_integration.py
```

**Expected output:**
```
=== TEST 1: Verify 2/4 Quorum ===
  ✓ zh: commit_index=12, role=leader
  ✓ ja: commit_index=13, role=follower
  ✓ ko: commit_index=11, role=leader

=== TEST 2: Verify Daemon Applied Entries ===
  ✓ zh: 12 novels in database
  ✓ ja: 13 novels in database
  ✓ ko: 11 novels in database

=== TEST 3: Verify last_applied Tracking ===
  ✓ zh: last_applied=12, commit_index=12 (synced)
  ...

ALL TESTS COMPLETE
```

---

## 🧪 Test On-Demand Elections

Skip pre-election and let system elect leaders as needed:

```bash
# Reset first
python reset_system.py --yes

# Run without pre-election
python main.py --skip-election --generate-sample --num-records 50
```

**You'll see:**
```
⚡ Triggering on-demand election for zh...
⚡ Triggering on-demand election for ja...
...
```

---

## 🔥 Test Node Failure & Recovery

### Simulate a Node Crash
```bash
# 1. Populate initial data
python main.py --generate-sample --num-records 100

# 2. Crash node_2 (simulates offline)
python tools/simulate_failure.py fail node_2
```

**Output:**
```
✓ Failed 4 shards in node_2
  Node node_2 is now simulating an offline state
```

### System Continues Working
```bash
# 3. Add more data (works with only 3/4 replicas)
python main.py --skip-init --generate-sample --num-records 50
```

**Result:** Writes still succeed because 3/4 > 2/4 quorum

### Node Comes Back Online
```bash
# 4. Bring node_2 back online
python tools/simulate_failure.py recover node_2
```

**Output:**
```
✓ Recovered 4 shards in node_2
  Node node_2 is now back online
  Recovery daemon will automatically pull missing data
```

### Verify Recovery
```bash
# 5. Check if node recovered
python tools/simulate_failure.py status node_2
```

**Expected:**
```
=== Node node_2 Status ===
Online shards:  4
Offline shards: 0

Shard Details:
  ✓ ja     : ONLINE
  ✓ ko     : ONLINE
  ✓ ms     : ONLINE
  ✓ zh     : ONLINE
```

---

## 📊 Check Database Contents

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

**Expected:**
```
node_0: 12
node_1: 12
node_2: 12
node_7: 12
```

All replicas should have the same count (eventual consistency).

---

## 🔍 Monitor Daemon Activity

### Check Leader Heartbeats
```bash
python3 -c "
import json
import time
with open('db/node_0/zh/state.json') as f:
    state = json.load(f)
    if state['role'] == 'leader':
        age = time.time() - state.get('last_heartbeat', 0)
        print(f'Leader heartbeat: {age:.1f}s ago')
"
```

### Check last_applied Progress
```bash
python3 -c "
import json
with open('db/node_0/zh/state.json') as f:
    state = json.load(f)
    print(f\"commit_index: {state['commit_index']}\")
    print(f\"last_applied: {state['last_applied']}\")
"
```

If `last_applied < commit_index`, daemon is still processing.

---

## 🛠️ Common Commands

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

## 📚 Understanding the System

### Architecture Overview

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

### Key Differences from Before

| Aspect | Before | After |
|--------|--------|-------|
| Quorum | Hardcoded `>= 2` | FlexiRaft static 2/4 |
| Writes | Direct to database | Raft log → Daemon → Database |
| Elections | Only at startup | On-demand when needed |
| Recovery | Manual | Automatic detection |
| Heartbeats | None | Leaders update every ~2s |

---

## 🎓 Next Steps

1. **Run the basic test** to verify everything works
2. **Test node failures** to see recovery in action
3. **Monitor daemon logs** to understand the flow
4. **Experiment with larger datasets** (1000+ records)
5. **Read IMPLEMENTATION_SUMMARY.md** for detailed changes

---

## 🆘 Need Help?

### System not working?

1. Check syntax: `python3 -m py_compile main.py`
2. Reset system: `python reset_system.py --yes`
3. Run tests: `python test_system_integration.py`
4. Check logs: Look for error messages in terminal output

### Databases empty after population?

- Wait 5-10 seconds for daemons to apply entries
- Check `last_applied` in state.json files
- Daemons run every ~2 seconds, so large datasets take time

### Elections hanging?

- Use `--skip-election` to use on-demand elections
- On-demand elections are faster for testing

---

## ✅ Success Indicators

Your system is working correctly if:

- ✅ Daemons start without errors
- ✅ Elections complete successfully
- ✅ Population shows "Quorum 2/4" or "Quorum 3/4" or "Quorum 4/4"
- ✅ Databases contain novels after 5-10 seconds
- ✅ `last_applied == commit_index` in state files
- ✅ Integration tests pass
- ✅ Node recovery works after simulated failures

---

## 🎉 You're All Set!

Your distributed database with static 2/4 quorum is ready to use!

**Start testing:** `python main.py --generate-sample --num-records 100`

For detailed technical documentation, see `IMPLEMENTATION_SUMMARY.md`.
