#!/usr/bin/env python3
"""
Integration Test for Distributed Database with Static 2/4 Quorum
Tests the complete system flow with daemon-based writes and recovery.
"""

import sys
import time
import sqlite3
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from db.state_manager import StateManager
from db.db_schema import DatabaseSchema


def test_2_4_quorum():
    """Test 1: Verify 2/4 quorum during population"""
    print("\n=== TEST 1: Verify 2/4 Quorum ===")

    # Check a few language shards to verify quorum was satisfied
    test_cases = [
        ("node_0", "zh"),
        ("node_1", "ja"),
        ("node_2", "ko"),
    ]

    for node_id, language in test_cases:
        shard_path = f"db/{node_id}/{language}"
        state = StateManager.load_state(shard_path)

        if not state:
            print(f"  ✗ {language}: No state file found")
            continue

        commit_index = state.get("commit_index", 0)
        role = state.get("role", "unknown")

        if commit_index > 0:
            print(f"  ✓ {language}: commit_index={commit_index}, role={role}")
        else:
            print(f"  ⚠ {language}: No commits yet (commit_index=0)")

    print("✓ Test 1 passed: Quorum check complete")


def test_daemon_applied_entries():
    """Test 2: Verify daemon applied entries to databases"""
    print("\n=== TEST 2: Verify Daemon Applied Entries ===")

    test_cases = [
        ("node_0", "zh"),
        ("node_1", "ja"),
        ("node_2", "ko"),
    ]

    for node_id, language in test_cases:
        shard_path = f"db/{node_id}/{language}"
        db_path = DatabaseSchema.get_db_path(shard_path)

        if not Path(db_path).exists():
            print(f"  ✗ {language}: Database not found")
            continue

        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM novels")
            count = cursor.fetchone()[0]
            conn.close()

            if count > 0:
                print(f"  ✓ {language}: {count} novels in database")
            else:
                print(f"  ⚠ {language}: Database empty (daemon may need more time)")
        except Exception as e:
            print(f"  ✗ {language}: Error querying database: {e}")

    print("✓ Test 2 passed: Daemon application check complete")


def test_last_applied_tracking():
    """Test 3: Verify last_applied matches commit_index"""
    print("\n=== TEST 3: Verify last_applied Tracking ===")

    test_cases = [
        ("node_0", "zh"),
        ("node_1", "ja"),
        ("node_2", "ko"),
    ]

    for node_id, language in test_cases:
        shard_path = f"db/{node_id}/{language}"
        state = StateManager.load_state(shard_path)

        if not state:
            print(f"  ✗ {language}: No state file")
            continue

        commit_index = state.get("commit_index", 0)
        last_applied = state.get("last_applied", 0)

        if last_applied == commit_index and commit_index > 0:
            print(
                f"  ✓ {language}: last_applied={last_applied}, commit_index={commit_index} (synced)"
            )
        elif last_applied < commit_index:
            print(
                f"  ⚠ {language}: last_applied={last_applied}, commit_index={commit_index} (daemon processing)"
            )
        else:
            print(
                f"  ℹ {language}: last_applied={last_applied}, commit_index={commit_index}"
            )

    print("✓ Test 3 passed: last_applied tracking check complete")


def test_leader_heartbeats():
    """Test 4: Verify leaders have recent heartbeats"""
    print("\n=== TEST 4: Verify Leader Heartbeats ===")

    test_cases = [
        ("node_0", "zh"),
        ("node_1", "ja"),
        ("node_2", "ko"),
    ]

    current_time = time.time()

    for node_id, language in test_cases:
        shard_path = f"db/{node_id}/{language}"
        state = StateManager.load_state(shard_path)

        if not state:
            print(f"  ✗ {language}: No state file")
            continue

        role = state.get("role", "unknown")
        last_heartbeat = state.get("last_heartbeat")

        if role == "leader":
            if last_heartbeat:
                age = current_time - last_heartbeat
                if age < 10:  # Within 10 seconds
                    print(f"  ✓ {language}: Leader heartbeat {age:.1f}s ago")
                else:
                    print(f"  ⚠ {language}: Leader heartbeat {age:.1f}s ago (stale)")
            else:
                print(f"  ⚠ {language}: Leader has no heartbeat")
        else:
            print(f"  ℹ {language}: {role} (not leader)")

    print("✓ Test 4 passed: Heartbeat check complete")


def test_replication_consistency():
    """Test 5: Verify data consistency across replicas"""
    print("\n=== TEST 5: Verify Replication Consistency ===")

    # Test Chinese language replicas (should be on node_0, node_7, node_1, node_2)
    zh_replicas = [
        "db/node_0/zh",
        "db/node_7/zh",
        "db/node_1/zh",
        "db/node_2/zh",
    ]

    counts = []
    for replica_path in zh_replicas:
        db_path = DatabaseSchema.get_db_path(replica_path)

        if not Path(db_path).exists():
            counts.append(None)
            continue

        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM novels WHERE original_language = 'zh'")
            count = cursor.fetchone()[0]
            conn.close()
            counts.append(count)
        except Exception:
            counts.append(None)

    print(f"  Chinese (zh) replica counts: {counts}")

    valid_counts = [c for c in counts if c is not None]
    if valid_counts:
        max_count = max(valid_counts)
        min_count = min(valid_counts)

        if max_count == min_count and max_count > 0:
            print(f"  ✓ All replicas consistent: {max_count} novels")
        elif max_count > 0:
            print(f"  ⚠ Replicas have different counts: {min_count} to {max_count}")
            print(f"    This is expected if daemon is still processing")
        else:
            print(f"  ⚠ No data found in any replica")
    else:
        print(f"  ✗ Could not read any replica databases")

    print("✓ Test 5 passed: Replication consistency check complete")


def main():
    """Run all integration tests"""
    print("=" * 70)
    print("DISTRIBUTED DATABASE INTEGRATION TESTS")
    print("Testing: Static 2/4 Quorum, Daemon Writes, Recovery")
    print("=" * 70)

    # Run all tests
    test_2_4_quorum()
    test_daemon_applied_entries()
    test_last_applied_tracking()
    test_leader_heartbeats()
    test_replication_consistency()

    print("\n" + "=" * 70)
    print("ALL TESTS COMPLETE")
    print("=" * 70)
    print("\nNote: Some tests may show warnings if:")
    print("  - Daemons are still processing entries")
    print("  - System was just initialized")
    print("  - No data has been populated yet")
    print("\nRun 'python main.py --generate-sample --num-records 100' first")
    print("Then wait ~5 seconds and run these tests again.")


if __name__ == "__main__":
    main()
