#!/usr/bin/env python3
"""
Crash Recovery Test
Tests system behavior when nodes crash during population
and verifies that recovery daemon brings nodes back up to date.

Features:
- Random crash count (1-3 nodes)
- Random crash timing (20%-90% of records)
- Random recovery timing (always mid-run)
- Random recovery daemon interval (2-8 seconds)
- Shard death detection and graceful termination

Leader Crash Test (--leader-crash):
- Tests leader failure causing shard death
- Crashes 1 follower at 30% progress
- Crashes current leader at 50% progress
- Expected: Shard death (only 2/4 replicas, cannot elect new leader)
"""

import sys
import time
import random
import threading
import argparse
from pathlib import Path
from typing import Dict, Tuple

sys.path.insert(0, str(Path(__file__).parent))

from main import DistributedDatabaseOrchestrator
from db.csv_loader import CSVLoader
from db.consistency_checker import (
    generate_consistency_report,
    write_consistency_report,
    write_test_header,
    write_test_result_summary,
)
from db.flexiraft_coordinator import FlexiRaftCoordinator
from db.state_manager import StateManager


class CrashRecoveryTest:
    """Test crash recovery functionality with random failures"""

    def __init__(
        self,
        num_records: int = 500,
        crash_count: int = None,
        leader_crash_test: bool = False,
    ):
        self.num_records = num_records
        self.crash_count = crash_count  # None = random (1-3), or specific count
        self.leader_crash_test = leader_crash_test  # Test leader crash scenario
        self.allow_recovery = (
            not leader_crash_test
        )  # Disable recovery in leader crash test
        self.orchestrator = None
        self.crash_events = []  # List of {node_id, crash_at, recover_at}
        self.crashed_nodes = set()
        self.recovery_interval = random.randint(2, 8)  # Random 2-8 seconds
        self.target_shard = None  # Track target shard for leader crash test
        self.coordinator = FlexiRaftCoordinator(config_path="db/node_config.json")
        self.languages = self.coordinator.get_all_languages()
        self.election_failures = {}  # Track election failures: shard -> count
        self.stdout_capture = []  # Capture stdout for parsing election failures

    def track_election_failures(self) -> Dict[str, int]:
        """
        Parse stdout to extract election failure counts.
        Returns dict mapping shard_name -> max_attempt_count
        """
        import re

        # Join all captured output
        stdout_text = "\n".join(self.stdout_capture) if self.stdout_capture else ""

        # Parse election failures: "Election attempt X for Y failed"
        pattern = r"Election attempt (\d+) for (\w+) failed"
        matches = re.findall(pattern, stdout_text)

        election_failures = {}
        for attempt_str, shard_name in matches:
            attempt = int(attempt_str)
            if shard_name not in election_failures:
                election_failures[shard_name] = attempt
            else:
                election_failures[shard_name] = max(
                    election_failures[shard_name], attempt
                )

        return election_failures

    def generate_crash_plan(self):
        """
        Generate random crash plan (1-3 nodes, random timing)
        Each crash happens at a random point between 20%-90% of total
        Recovery always happens mid-run (after crash + at least 50 entries)
        """
        # Use specified crash count or randomly pick 1, 2, or 3 nodes to crash
        if self.crash_count is not None:
            crash_count = self.crash_count
        else:
            crash_count = random.choice([1, 2, 3])

        # Pick unique random nodes
        nodes = random.sample([f"node_{i}" for i in range(8)], crash_count)

        crash_events = []
        for node_id in nodes:
            # Random crash point (20%-70% of records to ensure room for recovery)
            crash_at = random.randint(
                int(self.num_records * 0.2), int(self.num_records * 0.7)
            )

            # Recovery always mid-run: after crash + at least 10% more entries, before 95%
            min_recover = crash_at + max(10, int(self.num_records * 0.1))
            max_recover = min(crash_at + 200, int(self.num_records * 0.95))

            # Ensure valid range
            if min_recover > max_recover:
                max_recover = int(self.num_records * 0.95)
                min_recover = min(min_recover, max_recover)

            recover_at = random.randint(min_recover, max_recover)

            crash_events.append(
                {"node_id": node_id, "crash_at": crash_at, "recover_at": recover_at}
            )

        # Sort by crash timing for display
        crash_events.sort(key=lambda x: x["crash_at"])
        return crash_events

    def generate_leader_crash_plan(self, leaders: Dict[str, Tuple[str, int]]):
        """
        Generate crash plan targeting a leader to cause shard death.
        - Randomly select a target shard
        - Crash 1: Random follower at 30% progress
        - Crash 2: Current leader at 50% progress
        - Result: Only 2/4 replicas remain, cannot elect new leader (need 3/4 votes)

        Args:
            leaders: Dictionary mapping language to (leader_id, term) tuples

        Returns:
            (crash_events, target_shard, replicas)
        """
        # 1. Randomly pick a target shard from 8 languages
        target_shard = random.choice(self.languages)

        # 2. Get replicas for this shard
        replicas = self.coordinator.get_replica_nodes(target_shard)

        # 3. Get current leader from the leaders dictionary
        leader_info = leaders.get(target_shard)
        leader_id = leader_info[0] if leader_info else None

        if not leader_id:
            # Fallback: use first replica as "leader"
            leader_id = replicas[0]

        # 4. Get followers (replicas that are not leader)
        followers = [r for r in replicas if r != leader_id]

        # 5. Randomly select one follower to crash first
        follower_to_crash = random.choice(followers)

        # 6. Create crash events
        # For leader crash test, crash BEFORE population starts (crash_at = 0)
        # This ensures immediate shard death detection
        crash_events = [
            {
                "node_id": follower_to_crash,
                "crash_at": 0,  # Crash before population
                "recover_at": 999999,  # Never recover
                "role": "follower",
            },
            {
                "node_id": leader_id,
                "crash_at": 1,  # Crash right after follower
                "recover_at": 999999,  # Never recover
                "role": "leader",
            },
        ]

        return crash_events, target_shard, replicas

    def crash_monitor_thread(self):
        """Background thread that triggers crashes/recoveries based on entry count"""
        while True:
            current_entries = self.orchestrator.metrics.total_entries

            # Check for crash triggers
            for event in self.crash_events:
                node_id = event["node_id"]

                # Trigger crash
                if (
                    current_entries >= event["crash_at"]
                    and node_id not in self.crashed_nodes
                ):
                    print(f"\nCRASH EVENT at entry {current_entries}: {node_id} PAUSED")
                    success, shards = self.orchestrator.pause_node(node_id)
                    if success:
                        print(f"  Affected shards: {', '.join(shards)}")
                        self.crashed_nodes.add(node_id)

                # Trigger recovery (only if recovery is allowed)
                if (
                    self.allow_recovery
                    and current_entries >= event["recover_at"]
                    and node_id in self.crashed_nodes
                ):
                    print(
                        f"\nRECOVERY EVENT at entry {current_entries}: {node_id} RESUMED"
                    )
                    success, shards = self.orchestrator.resume_node(node_id)
                    if success:
                        print(f"  Affected shards: {', '.join(shards)}")
                        print(
                            f"  Recovery daemon will detect lag in next cycle ({self.recovery_interval}s)"
                        )
                        self.crashed_nodes.discard(node_id)

            # Exit if all events processed and population done
            if current_entries >= self.num_records and not self.crashed_nodes:
                break

            time.sleep(0.1)

    def run_test(self):
        """Execute full crash recovery test"""

        print("=" * 70)
        print("CRASH RECOVERY TEST")
        print("=" * 70)

        # Initialize orchestrator
        self.orchestrator = DistributedDatabaseOrchestrator()

        # Initialize
        print("\n[1/7] Initializing system...")
        self.orchestrator.initializer.initialize_all(verbose=False)
        print("System initialized (32 shards)")

        # Start daemons
        print("\n[2/7] Starting daemons...")
        self.orchestrator.start_commit_daemons()
        print(f"Started {len(self.orchestrator.daemons)} commit daemons")

        # Start recovery daemon with random interval
        print("\n[3/7] Starting recovery daemon...")
        self.orchestrator.start_recovery_daemon(check_interval=self.recovery_interval)
        print(f"Recovery daemon started (check interval: {self.recovery_interval}s)")
        time.sleep(1)

        # Elect leaders
        print("\n[4/7] Electing leaders...")
        leaders = self.orchestrator.election_manager.elect_all_leaders_parallel()
        print(f"Elected leaders for {len(leaders)} languages")

        # Generate crash plan
        if self.leader_crash_test:
            # Leader crash test mode - crash nodes BEFORE starting population
            print(f"\n[DEBUG] Leaders dict: {leaders}")
            self.crash_events, self.target_shard, target_replicas = (
                self.generate_leader_crash_plan(leaders)
            )
            crash_count = len(self.crash_events)

            print(f"\n[5/7] Planning LEADER CRASH test:")
            print(f"  Target shard: {self.target_shard}")
            print(f"  Replicas: {', '.join(target_replicas)}")
            print()

            for i, event in enumerate(self.crash_events, 1):
                node_id = event["node_id"]
                role = event["role"]

                daemon = self.orchestrator.get_daemon(node_id)
                shards = daemon.get_shards() if daemon else []

                print(f"  Crash {i} ({role}): {node_id}")
                print(f"    Affected shards: {', '.join(shards)}")

            print()
            print(f"  After crashes: {self.target_shard} will have only 2/4 replicas")
            print(f"  Expected: Shard death (cannot elect leader with only 2/4 votes)")
            print()

            # Keep recovery daemon running to trigger elections, but don't recover crashed nodes
            # The recovery daemon will detect the dead leader and trigger elections
            # but those elections should FAIL with only 2/4 votes available
            print("[5a/7] Keeping recovery daemon running (will trigger elections)...")
            print("  Note: Crashed nodes will NOT be recovered (pause is permanent)")

            # Crash both nodes NOW (before population)
            print("[5b/7] Crashing nodes now...")
            for i, event in enumerate(self.crash_events, 1):
                node_id = event["node_id"]
                role = event["role"]
                print(f"  Crashing {node_id} ({role})...")
                success, shards = self.orchestrator.pause_node(node_id)
                if success:
                    self.crashed_nodes.add(node_id)
                    print(f"    ✓ Crashed - affects: {', '.join(shards)}")

            # Don't start monitor thread - nodes are already crashed
            monitor = None
        else:
            # Original random crash mode
            self.crash_events = self.generate_crash_plan()
            crash_count = len(self.crash_events)

            print(f"\n[5/7] Planning crashes ({crash_count} nodes):")
            for i, event in enumerate(self.crash_events, 1):
                node_id = event["node_id"]
                crash_pct = int((event["crash_at"] / self.num_records) * 100)
                recover_pct = int((event["recover_at"] / self.num_records) * 100)

                # Get shards for this node
                daemon = self.orchestrator.get_daemon(node_id)
                shards = daemon.get_shards() if daemon else []

                print(
                    f"  Crash {i}: {node_id} will crash at entry {event['crash_at']}/{self.num_records} ({crash_pct}%)"
                )
                print(f"    Affected shards: {', '.join(shards)}")
                print(
                    f"    Will recover at entry {event['recover_at']} ({recover_pct}%)"
                )

            # Start crash monitor thread for random crash mode
            monitor = threading.Thread(target=self.crash_monitor_thread, daemon=True)
            monitor.start()

        # Load data
        print(
            f"\n[6/7] Populating {self.num_records} records (crashes will occur mid-run)..."
        )
        csv_path = "test_crash_dataset.csv"
        CSVLoader.create_sample_csv(csv_path, self.num_records)
        novels = CSVLoader.load_csv(csv_path)
        grouped = CSVLoader.group_by_language(novels)

        # Populate (crashes happen during this)
        start_time = time.time()
        try:
            results = self.orchestrator.populate_all_parallel(grouped, leaders)
            elapsed = time.time() - start_time

            print(f"\nPopulation complete ({elapsed:.2f}s)")
            print(
                f"  Successfully written: {sum(count for _, count, _ in results.values())} entries"
            )
        except SystemExit:
            # Shard death detected, test will report failure
            print("\nTest terminated due to shard death")
            return False

        # For leader crash test, wait for election timeout to trigger automatic reelection
        if self.leader_crash_test:
            print(f"\n[6a/7] Waiting for election timeout on {self.target_shard}...")
            print(f"  Available replicas: 2/4 (need 3/4 for election quorum)")
            print(f"  Expected: Election should FAIL (not enough votes)")
            print(f"  Waiting 15s for election timeout and attempts...")
            time.sleep(15)
            print(f"  ✓ Timeout complete - check logs for election failures")

        # Recover any remaining crashed nodes (only if recovery is allowed)
        if self.allow_recovery:
            print("\n[7/7] Recovering any remaining crashed nodes...")
            for event in self.crash_events:
                if event["node_id"] in self.crashed_nodes:
                    node_id = event["node_id"]
                    print(f"\nRECOVERY EVENT: {node_id} RESUMED")
                    success, shards = self.orchestrator.resume_node(node_id)
                    if success:
                        print(f"  Affected shards: {', '.join(shards)}")
                        self.crashed_nodes.discard(node_id)
        else:
            print(
                "\n[7/7] Recovery disabled for leader crash test - nodes remain crashed"
            )

        # Wait for daemons and recovery
        print("\nWaiting 5s for active daemons to apply entries...")
        time.sleep(5)

        # Write test header (overwrite mode)
        test_params = {
            "num_records": self.num_records,
            "crash_count": len(self.crash_events) if self.crash_events else 0,
            "leader_crash": self.leader_crash_test,
            "recovery_interval": f"{self.recovery_interval}s",
        }
        write_test_header("report.log", "CRASH RECOVERY TEST", test_params, mode="w")

        # Before recovery consistency check
        print("\nGenerating consistency report (BEFORE full recovery)...")
        report_before = generate_consistency_report()
        write_consistency_report(
            report_before, "report.log", mode="a", section_header="BEFORE RECOVERY"
        )

        # Wait for recovery cycles
        wait_time = self.recovery_interval * 2 + 10  # At least 2 cycles plus buffer
        print(f"\nWaiting {wait_time}s for recovery daemon cycles...")
        time.sleep(wait_time)

        # After recovery consistency check
        print("\nGenerating consistency report (AFTER recovery)...")
        report_after = generate_consistency_report()
        write_consistency_report(
            report_after, "report.log", mode="a", section_header="AFTER RECOVERY"
        )

        # Track election failures and write summary
        test_duration = time.time() - start_time
        election_failures = self.track_election_failures()
        write_test_result_summary(
            "report.log",
            report_after,
            test_duration,
            self.crash_events,
            election_failures,
            mode="a",
        )

        # Verify success
        fully_consistent = report_after["summary"]["fully_consistent"]
        total_shards = report_after["summary"]["total_shards"]

        print("\n" + "=" * 70)
        print("TEST RESULT")
        print("=" * 70)

        if fully_consistent == total_shards:
            print("Status: PASSED")
            print(f"  All {total_shards} shards are fully consistent after recovery")
        else:
            print("Status: PARTIAL")
            print(f"  {fully_consistent}/{total_shards} shards fully consistent")

        print(f"\nCrash Details:")
        print(f"  Crashed nodes: {[e['node_id'] for e in self.crash_events]}")
        print(f"  Recovery daemon interval: {self.recovery_interval}s")
        print(f"  Test duration: {test_duration:.1f}s")
        print("=" * 70)
        print("\nLogged: report.log")

        return fully_consistent == total_shards


def main():
    parser = argparse.ArgumentParser(description="Crash Recovery Test")
    parser.add_argument(
        "--num-records",
        type=int,
        default=500,
        help="Number of records to populate (default: 500)",
    )
    parser.add_argument(
        "--crash-count",
        type=int,
        default=None,
        help="Number of nodes to crash (default: random 1-3, ignored if --leader-crash is used)",
    )
    parser.add_argument(
        "--leader-crash",
        action="store_true",
        help="Test leader crash scenario (2 sequential crashes including leader)",
    )
    args = parser.parse_args()

    test = CrashRecoveryTest(
        num_records=args.num_records,
        crash_count=args.crash_count if not args.leader_crash else None,
        leader_crash_test=args.leader_crash,
    )
    success = test.run_test()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
