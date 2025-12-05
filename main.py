#!/usr/bin/env python3
"""
Distributed Database with FlexiRaft Consensus
Main orchestrator for initialization, election, and data population with detailed metrics.
"""

import argparse
import sys
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from db.initialize_system import SystemInitializer
from db.election_manager import ElectionManager
from db.csv_loader import CSVLoader
from db.db_schema import DatabaseSchema
from db.raft_log_manager import RaftLogManager
from db.state_manager import StateManager
from db.flexiraft_coordinator import FlexiRaftCoordinator
from db.db_daemon import Daemon
from db.shard_monitor import ShardMonitor
from db.consistency_checker import (
    generate_consistency_report,
    write_consistency_report,
    write_test_header,
)


class DistributedDatabaseOrchestrator:
    """Main orchestrator for the distributed database system"""

    def __init__(self, config_path: str = "db/node_config.json"):
        self.config_path = config_path
        self.initializer = SystemInitializer(config_path)
        self.election_manager = ElectionManager(config_path)
        self.coordinator = FlexiRaftCoordinator(config_path)
        self.metrics = PopulationMetrics()
        self.daemons = []  # Track daemon instances
        self.shard_monitor = ShardMonitor(config_path)  # Track shard health
        self.recovery_daemon = None  # Recovery daemon instance

    def start_commit_daemons(self) -> List[Daemon]:
        """
        Start commit daemons for all 8 nodes.
        Daemons will apply raft log entries to databases asynchronously.

        Returns: List of daemon instances
        """
        node_ids = [
            "node_0",
            "node_1",
            "node_2",
            "node_3",
            "node_4",
            "node_5",
            "node_6",
            "node_7",
        ]

        for node_id in node_ids:
            daemon = Daemon(node_id)
            daemon.start()
            self.daemons.append(daemon)

        return self.daemons

    def stop_daemons(self):
        """Stop all running daemons"""
        for daemon in self.daemons:
            daemon.stop()

    def get_daemon(self, node_id: str):
        """Get daemon instance by node_id"""
        for daemon in self.daemons:
            if daemon._node_id == node_id:
                return daemon
        return None

    def pause_node(self, node_id: str):
        """Pause daemon for a specific node (simulate crash)"""
        daemon = self.get_daemon(node_id)
        if daemon:
            shards = daemon.get_shards()
            daemon.pause()
            self.shard_monitor.register_crash(node_id)
            self._check_for_shard_death()  # Check immediately
            return True, shards
        return False, []

    def resume_node(self, node_id: str):
        """Resume daemon for a specific node (simulate recovery)"""
        daemon = self.get_daemon(node_id)
        if daemon:
            shards = daemon.get_shards()
            daemon.resume()
            self.shard_monitor.register_recovery(node_id)
            return True, shards
        return False, []

    def _check_for_shard_death(self):
        """Check if any shards have died and terminate if so"""
        dead_shards = self.shard_monitor.get_dead_shards()

        if dead_shards:
            # Print to terminal
            report = self.shard_monitor.format_death_report(dead_shards)
            print("\n" + report)

            # Write to report.log
            with open("report.log", "w") as f:
                f.write(report)
            print("\nShard death report written to: report.log")

            # Gracefully terminate
            print("\nShutting down system...")
            self.stop_daemons()
            if self.recovery_daemon:
                self.stop_recovery_daemon()

            sys.exit(1)

    def start_recovery_daemon(self, check_interval: int = 8):
        """Start recovery daemon to auto-detect and recover crashed nodes"""
        from db.recovery_daemon import RecoveryDaemon

        self.recovery_daemon = RecoveryDaemon(
            config_path=self.config_path,
            check_interval=check_interval,
            leader_timeout=10,
            shard_monitor=self.shard_monitor,
        )
        self.recovery_daemon.start()
        return self.recovery_daemon

    def stop_recovery_daemon(self):
        """Stop recovery daemon"""
        if self.recovery_daemon:
            self.recovery_daemon.stop()

    def populate_language_group(
        self, language: str, novels: List[Dict], leader_id: Optional[str] = None
    ) -> Tuple[bool, int, str]:
        """
        Populate a single language group with data using 2/4 quorum.
        Writes only to raft logs; daemon applies to databases asynchronously.

        Args:
            language: Language code
            novels: List of novel dictionaries
            leader_id: Leader node ID (if None, will trigger on-demand election)

        Returns: (success, count, message)
        """
        if not novels:
            return (True, 0, "No data")

        # ON-DEMAND ELECTION: Ensure leader exists
        if not leader_id:
            leader_id, term = self.election_manager.ensure_leader_for_language(language)

        replicas = self.coordinator.get_replica_nodes(language)
        leader_path = self.coordinator.get_shard_path(leader_id, language)

        # Get current term
        term = StateManager.get_term(leader_path)

        # Prepare raft log entries
        entries = []
        for novel in novels:
            sql, params = DatabaseSchema.generate_insert_sql(
                novel["title"], novel["original_language"]
            )
            entries.append({"operation": "INSERT", "sql": sql, "params": params})

        # Try write with retries
        max_retries = 5
        retry_interval = 2.0

        latency = 0.0
        success_count = 0

        for attempt in range(max_retries):
            start_time = time.time()

            # Append to raft logs in parallel
            success_count = 0
            successful_replicas = []
            failed_nodes = []

            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {}

                for node_id in replicas:
                    shard_path = self.coordinator.get_shard_path(node_id, language)
                    future = executor.submit(
                        RaftLogManager.append_entries_bulk, shard_path, term, entries
                    )
                    futures[future] = node_id

                for future in as_completed(futures):
                    node_id = futures[future]
                    try:
                        future.result()
                        success_count += 1
                        successful_replicas.append(node_id)
                    except Exception as e:
                        failed_nodes.append(node_id)

            latency = time.time() - start_time

            # Check 2/4 quorum using coordinator
            if self.coordinator.check_write_quorum(language, successful_replicas):
                # Quorum reached! Update commit_index for successful replicas
                # The daemon will apply these entries to the databases
                leader_commit_index = StateManager.get_commit_index(leader_path)
                new_commit_index = leader_commit_index + len(entries)

                # Update commit_index for replicas that successfully wrote to log
                with ThreadPoolExecutor(max_workers=4) as executor:
                    for node_id in successful_replicas:
                        shard_path = self.coordinator.get_shard_path(node_id, language)
                        executor.submit(
                            StateManager.update_commit_index_sync,
                            shard_path,
                            new_commit_index,
                        )

                # Record metrics
                self.metrics.record_write(
                    language=language,
                    entry_count=len(entries),
                    success=True,
                    latency=latency,
                    quorum_size=success_count,
                    total_replicas=len(replicas),
                )

                return (True, len(novels), f"Quorum {success_count}/4, {latency:.2f}s")

            # Quorum not reached - check if shard is dead before retrying
            is_healthy, avail_count, avail_nodes = (
                self.shard_monitor.check_shard_health(language)
            )

            if not is_healthy:
                print(
                    f"\nCRITICAL: Shard '{language}' is dead ({avail_count}/4 replicas available)"
                )
                self._check_for_shard_death()  # This will terminate

            if attempt < max_retries - 1:
                print(
                    f"    Retry {attempt + 1}/{max_retries} for {language} (quorum: {success_count}/4)"
                )
                time.sleep(retry_interval)

        # Failed after all retries
        self.metrics.record_write(
            language=language,
            entry_count=len(entries),
            success=False,
            latency=latency,
            quorum_size=success_count,
            total_replicas=len(replicas),
        )

        return (False, 0, f"Quorum failed after {max_retries} retries")

    def populate_all_parallel(
        self,
        grouped_novels: Dict[str, List[Dict]],
        leaders: Optional[Dict[str, Tuple[str, int]]] = None,
    ) -> Dict[str, Tuple[bool, int, str]]:
        """
        Populate all language groups in parallel.
        If leaders dict is not provided, will trigger on-demand elections.

        Args:
            grouped_novels: {language: [novels]}
            leaders: Optional {language: (leader_id, term)} dict

        Returns: {language: (success, count, message)}
        """
        results = {}
        leaders = leaders or {}  # Default to empty dict if None

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {}

            for language, novels in grouped_novels.items():
                # Get leader if available, otherwise None (will trigger on-demand election)
                leader_id = (
                    leaders.get(language, (None, 0))[0] if language in leaders else None
                )

                future = executor.submit(
                    self.populate_language_group, language, novels, leader_id
                )
                futures[future] = language

            for future in as_completed(futures):
                lang = futures[future]
                try:
                    result = future.result()
                    results[lang] = result

                    # Print progress every 250 entries (approximation)
                    if self.metrics.should_print_progress():
                        self.metrics.print_progress()

                except Exception as e:
                    results[lang] = (False, 0, str(e))

        return results


class PopulationMetrics:
    """Track and report population metrics"""

    def __init__(self):
        self.total_entries = 0
        self.successful_entries = 0
        self.failed_entries = 0
        self.quorum_successes = 0
        self.quorum_failures = 0
        self.latencies = []
        self.per_language = {}
        self.last_print = 0

    def record_write(
        self,
        language: str,
        entry_count: int,
        success: bool,
        latency: float,
        quorum_size: int,
        total_replicas: int,
    ):
        self.total_entries += entry_count

        if success:
            self.successful_entries += entry_count
            self.quorum_successes += 1
        else:
            self.failed_entries += entry_count
            self.quorum_failures += 1

        self.latencies.append((entry_count, latency))

        if language not in self.per_language:
            self.per_language[language] = {"success": 0, "failed": 0}

        if success:
            self.per_language[language]["success"] += entry_count
        else:
            self.per_language[language]["failed"] += entry_count

    def should_print_progress(self) -> bool:
        if self.total_entries - self.last_print >= 250:
            self.last_print = self.total_entries
            return True
        return False

    def print_progress(self):
        if not self.latencies:
            return

        avg_latency = sum(lat for _, lat in self.latencies) / len(self.latencies)
        quorum_rate = (
            self.quorum_successes / (self.quorum_successes + self.quorum_failures) * 100
            if (self.quorum_successes + self.quorum_failures) > 0
            else 0
        )

        print(f"\n--- Progress: {self.total_entries} entries ---")
        print(
            f"  Cumulative: {self.successful_entries} success, {self.failed_entries} failed"
        )
        print(
            f"  Quorum rate: {quorum_rate:.1f}%, Avg latency: {avg_latency * 1000:.1f}ms/batch"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Distributed Database with FlexiRaft Consensus"
    )
    parser.add_argument(
        "--csv",
        type=str,
        default="novels_dataset.csv",
        help="Path to CSV file with novel data",
    )
    parser.add_argument(
        "--generate-sample",
        action="store_true",
        help="Generate sample CSV if it doesn't exist",
    )
    parser.add_argument(
        "--num-records", type=int, default=10000, help="Number of records in sample CSV"
    )
    parser.add_argument(
        "--skip-init", action="store_true", help="Skip database initialization"
    )
    parser.add_argument(
        "--skip-election", action="store_true", help="Skip leader election"
    )

    args = parser.parse_args()

    print("=" * 70)
    print("DISTRIBUTED DATABASE WITH FLEXIRAFT")
    print("Configuration: 8 languages, 4 replicas each, 2/4 quorum (STATIC)")
    print("=" * 70)

    orchestrator = DistributedDatabaseOrchestrator()

    # STEP 1: INITIALIZATION
    if not args.skip_init:
        print("\n[1/5] Initializing system...")
        success = orchestrator.initializer.initialize_all(verbose=True)
        if not success:
            print("ERROR: Initialization failed")
            return 1

    # STEP 2: START COMMIT DAEMONS
    print("\n[2/5] Starting commit daemons for all nodes...")
    daemons = orchestrator.start_commit_daemons()
    print(f"  Started {len(daemons)} commit daemons (will apply logs to databases)")
    print("  Daemon check interval: ~2 seconds")

    # Give daemons time to initialize
    time.sleep(1)

    # STEP 3: LEADER ELECTIONS (OPTIONAL - will auto-elect if needed)
    leaders = {}
    if not args.skip_election:
        print(
            "\n[3/5] Pre-electing leaders (optional, will auto-elect on-demand if needed)..."
        )
        start_time = time.time()

        leaders = orchestrator.election_manager.elect_all_leaders_parallel()

        elapsed = time.time() - start_time
        print(f"  Elections completed in {elapsed:.2f}s")
        print("\nElected Leaders:")
        for lang in sorted(leaders.keys()):
            leader_id, term = leaders[lang]
            print(f"  {lang:6s} → {leader_id:8s} (term {term})")
    else:
        # Load existing leaders if available
        for lang in orchestrator.coordinator.get_all_languages():
            leader_id = orchestrator.coordinator.get_leader_for_language(lang)
            if leader_id:
                shard_path = orchestrator.coordinator.get_shard_path(leader_id, lang)
                term = StateManager.get_term(shard_path)
                leaders[lang] = (leader_id, term)

        if not leaders:
            print(
                "  Note: No pre-elected leaders. Will elect on-demand during population."
            )

    # STEP 4: CSV LOADING
    print("\n[4/5] Loading CSV data...")
    csv_path = args.csv

    if args.generate_sample or not Path(csv_path).exists():
        print(f"  Generating sample CSV: {csv_path}")
        CSVLoader.create_sample_csv(csv_path, args.num_records)

    if not Path(csv_path).exists():
        print(f"ERROR: CSV file not found: {csv_path}")
        return 1

    novels = CSVLoader.load_csv(csv_path)
    print(f"  Loaded {len(novels)} novels from CSV")

    grouped = CSVLoader.group_by_language(novels)
    print(f"  Distribution: {dict((k, len(v)) for k, v in grouped.items())}")

    # STEP 5: DATA POPULATION
    print("\n[5/5] Populating shards (parallel, 2/4 quorum, daemon-based writes)...")
    print("  Writes go to raft logs; daemons apply to databases asynchronously")
    print("  Progress will be reported every ~250 entries\n")

    start_time = time.time()
    results = orchestrator.populate_all_parallel(grouped, leaders)
    elapsed = time.time() - start_time

    # Wait for daemons to apply all entries (active polling)
    print("\nWaiting for daemons to apply entries to databases...")
    max_wait = 60  # Max 60 seconds
    check_interval = 3  # Check every 3 seconds (aligned with daemon interval)

    def check_all_shards_applied(coordinator):
        """
        Check if all shards have applied all committed entries.
        Returns (all_synced, synced_count, total_count)
        """
        languages = coordinator.get_all_languages()
        total_shards = 0
        synced_shards = 0

        for language in languages:
            replicas = coordinator.config.get("replicas", {}).get(language, [])
            for node_id in replicas:
                shard_path = coordinator.get_shard_path(node_id, language)
                state = StateManager.load_state(shard_path)

                if state:
                    total_shards += 1
                    commit_index = state.get("commit_index", 0)
                    last_applied = state.get("last_applied", 0)

                    # Shard is synced if last_applied caught up to commit_index
                    if last_applied >= commit_index:
                        synced_shards += 1

        return synced_shards == total_shards, synced_shards, total_shards

    # Active polling loop
    for i in range(max_wait // check_interval):
        time.sleep(check_interval)

        all_synced, synced, total = check_all_shards_applied(orchestrator.coordinator)

        if all_synced:
            elapsed_wait = (i + 1) * check_interval
            print(f"  ✓ All {total} shards synced after {elapsed_wait}s")
            break
        else:
            print(f"  Progress: {synced}/{total} shards synced...")
    else:
        # Timeout reached
        all_synced, synced, total = check_all_shards_applied(orchestrator.coordinator)
        print(f"  ⚠ Timeout after {max_wait}s: {synced}/{total} shards synced")
        if synced < total:
            print(f"  Warning: {total - synced} shards may still be applying entries")

    # FINAL SUMMARY
    print("\n" + "=" * 70)
    print("POPULATION COMPLETE - FINAL METRICS")
    print("=" * 70)

    total_success = sum(count for _, count, _ in results.values())
    total_failed = sum(
        len(grouped[lang]) for lang, (success, _, _) in results.items() if not success
    )

    print(f"\nOverall Statistics:")
    print(f"  Total entries attempted:  {len(novels):,}")
    print(f"  Successfully written:     {total_success:,}")
    print(f"  Failed:                   {total_failed:,}")
    print(f"  Total elapsed time:       {elapsed:.2f}s")
    if total_success > 0:
        print(f"  Throughput:               {total_success / elapsed:.1f} entries/sec")

    print(f"\nPer-Language Breakdown:")
    print(f"  {'Language':<10} {'Attempted':<10} {'Success':<10} {'Status':<10}")
    print(f"  {'-' * 50}")

    for lang in sorted(results.keys()):
        success, count, message = results[lang]
        attempted = len(grouped.get(lang, []))
        status = "P" if success else "NP"
        print(f"  {lang:<10} {attempted:<10,} {count:<10,} {status:<10}")

    # CONSISTENCY CHECK
    print("\nGenerating consistency report...")

    # Write test header
    test_params = {
        "csv_file": args.csv,
        "num_records": len(novels),
        "skip_election": args.skip_election,
        "throughput": f"{total_success / elapsed:.1f} entries/sec"
        if total_success > 0 and elapsed > 0
        else "N/A",
    }
    write_test_header("report.log", "DATA POPULATION TEST", test_params, mode="w")

    # Write consistency report
    report = generate_consistency_report(orchestrator.config_path)
    write_consistency_report(
        report, "report.log", mode="a", section_header="AFTER POPULATION"
    )
    print("Logged: report.log")

    print("\n" + "=" * 70)
    print("SYSTEM READY")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    sys.exit(main())
