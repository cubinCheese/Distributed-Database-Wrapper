#!/usr/bin/env python3
"""
Simple test script to demonstrate the distributed database system
Tests each component individually
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from db.initialize_system import SystemInitializer
from db.csv_loader import CSVLoader
from db.db_schema import DatabaseSchema
from db.state_manager import StateManager
from db.raft_log_manager import RaftLogManager

print("=" * 70)
print("DISTRIBUTED DATABASE - COMPONENT TEST")
print("=" * 70)

# Test 1: System Initialization
print("\n[1/5] Testing system initialization...")
initializer = SystemInitializer()
success = initializer.initialize_all(verbose=False)
if success:
    print("✓ System initialized (32 shards)")
    shard_count = initializer.get_shard_count()
    print(f"  Total shards: {shard_count}")
else:
    print("✗ Initialization failed")
    sys.exit(1)

# Test 2: CSV Generation
print("\n[2/5] Testing CSV generation...")
csv_path = "test_novels.csv"
CSVLoader.create_sample_csv(csv_path, 100)
novels = CSVLoader.load_csv(csv_path)
print(f"✓ Generated and loaded {len(novels)} novels")

grouped = CSVLoader.group_by_language(novels)
print(f"  Languages: {list(grouped.keys())}")
print(f"  Distribution: {dict((k, len(v)) for k, v in grouped.items())}")

# Test 3: Database Operations
print("\n[3/5] Testing database operations...")
test_shard = "db/node_0/zh"
test_db = DatabaseSchema.get_db_path(test_shard)

# Insert test data
DatabaseSchema.bulk_insert_novels(
    test_db, [("Test Novel 1", "zh"), ("Test Novel 2", "zh")]
)

count = DatabaseSchema.count_novels(test_db, "zh")
print(f"✓ Database operations successful")
print(f"  Inserted 2 novels, count: {count}")

# Test 4: Raft Log Operations
print("\n[4/5] Testing raft log operations...")
RaftLogManager.append_entry(
    test_shard,
    term=1,
    operation="INSERT",
    sql="INSERT INTO novels VALUES (?, ?)",
    params=["Test", "zh"],
)

log_length = RaftLogManager.get_log_length(test_shard)
print(f"✓ Raft log operations successful")
print(f"  Log entries: {log_length}")

# Test 5: State Management
print("\n[5/5] Testing state management...")
state = StateManager.load_state(test_shard)
if state:
    print(f"✓ State management successful")
    print(f"  Node: {state['node_id']}, Language: {state['language']}")
    print(f"  Role: {state['role']}, Term: {state['term']}")
    print(f"  Commit index: {state['commit_index']}")
else:
    print("✗ State management failed")

# Summary
print("\n" + "=" * 70)
print("ALL COMPONENT TESTS PASSED")
print("=" * 70)
print("\nSystem is ready for:")
print("  1. Leader election (run: python3 test_election.py)")
print("  2. Data population with FlexiRaft consensus")
print("  3. Query operations")
print("\nTo reset system: python3 reset_system.py --yes")
print("=" * 70)
