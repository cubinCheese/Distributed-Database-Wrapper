"""Recovery Daemon
Monitors leader health and recovers lagging replicas by pulling from raft logs.
Runs in background with configurable interval.
"""

import time
import threading
import sys
import os
from typing import Dict, List, Set, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.flexiraft_coordinator import FlexiRaftCoordinator
from db.state_manager import StateManager
from db.raft_log_manager import RaftLogManager
from db.db_schema import DatabaseSchema
from db.election_manager import ElectionManager


class RecoveryDaemon:
    """Background daemon for recovery and health monitoring"""

    def __init__(
        self,
        config_path: str = "db/node_config.json",
        check_interval: int = 30,
        leader_timeout: int = 10,
        shard_monitor=None,
    ):
        """
        Initialize recovery daemon

        Args:
            config_path: Path to node configuration
            check_interval: Seconds between recovery checks
            leader_timeout: Seconds before declaring leader dead
            shard_monitor: Optional ShardMonitor instance to check for crashed nodes
        """
        self.config_path = config_path
        self.check_interval = check_interval
        self.leader_timeout = leader_timeout
        self.coordinator = FlexiRaftCoordinator(
            config_path, shard_monitor=shard_monitor
        )
        self.election_manager = ElectionManager(
            config_path, shard_monitor=shard_monitor
        )
        self.running = False
        self.thread = None
        self.executor = ThreadPoolExecutor(max_workers=8)

    def check_leader_health(self, language: str) -> bool:
        """
        Check if leader for a language group is healthy

        Args:
            language: Language code

        Returns: True if leader is healthy, False otherwise
        """
        leader_id = self.coordinator.get_leader_for_language(language)

        if not leader_id:
            print(f"[{language}] No leader for {language}")
            return False

        # Check leader's last heartbeat
        shard_path = self.coordinator.get_shard_path(leader_id, language)
        last_heartbeat = StateManager.get_last_heartbeat(shard_path)

        if last_heartbeat is None:
            print(f"[{language}] No heartbeat recorded for leader {leader_id}")
            return False

        elapsed = time.time() - last_heartbeat
        if elapsed > self.leader_timeout:
            print(
                f"[{language}] Leader {leader_id} timeout: {elapsed:.1f}s since last heartbeat"
            )
            return False

        return True

    def recover_lagging_replica(
        self, language: str, replica_id: str, leader_id: str
    ) -> int:
        """
        Recover a lagging replica by pulling from leader's raft log

        Args:
            language: Language code
            replica_id: Replica to recover
            leader_id: Leader to pull from

        Returns: Number of entries recovered
        """
        replica_path = self.coordinator.get_shard_path(replica_id, language)
        leader_path = self.coordinator.get_shard_path(leader_id, language)

        # Get last_applied indices
        replica_last_applied = StateManager.get_last_applied(replica_path)
        leader_last_applied = StateManager.get_last_applied(leader_path)

        if replica_last_applied >= leader_last_applied:
            return 0  # Already up to date

        # Get commit indices
        leader_commit = StateManager.get_commit_index(leader_path)

        lag = leader_commit - replica_last_applied

        if lag <= 0:
            return 0

        print(f"  [{language}] Pulling {lag} entries: {leader_id} -> {replica_id}")

        # Copy missing entries from leader's log
        recovered = RaftLogManager.copy_entries_to_follower(
            leader_path=leader_path,
            follower_path=replica_path,
            start_index=replica_last_applied + 1,
            end_index=leader_commit,
        )

        # Update replica's commit_index to match leader
        if recovered > 0:
            StateManager.update_commit_index_sync(replica_path, leader_commit)
            print(f"  [{language}] Copied {recovered} log entries to {replica_id}")
            print(
                f"  [{language}] commit_index updated: {replica_last_applied} -> {leader_commit}"
            )
            print(f"  [{language}] Daemon will apply entries to database")

        return recovered

    def check_and_recover_language_group(self, language: str) -> Dict[str, int]:
        """
        Check and recover all replicas in a language group

        Args:
            language: Language code

        Returns: Dict of {replica_id: entries_recovered}
        """
        results = {}

        # Detect recently recovered nodes
        recovered_nodes = self.detect_recovered_nodes(language)
        if recovered_nodes:
            print(f"\n[{language}] Detected recovered nodes: {recovered_nodes}")

        # Check leader health
        if not self.check_leader_health(language):
            # Leader unhealthy, trigger reelection
            print(f"[{language}] Triggering reelection for {language}")
            try:
                new_leader, term = self.election_manager.trigger_reelection(
                    language, reason="leader_timeout"
                )
                print(f"[{language}] New leader elected: {new_leader} (term {term})")
            except Exception as e:
                print(f"[{language}] Reelection failed: {e}")
                return results

        leader_id = self.coordinator.get_leader_for_language(language)
        if not leader_id:
            return results

        leader_path = self.coordinator.get_shard_path(leader_id, language)
        leader_commit = StateManager.get_commit_index(leader_path)

        # Check each replica
        replicas = self.coordinator.get_replica_nodes(language)

        for replica_id in replicas:
            if replica_id == leader_id:
                continue

            replica_path = self.coordinator.get_shard_path(replica_id, language)
            replica_commit = StateManager.get_commit_index(replica_path)

            lag = leader_commit - replica_commit

            if lag > 0:
                print(
                    f"\n[{language}] Recovering {replica_id}: {lag} entries behind leader"
                )
                recovered = self.recover_lagging_replica(
                    language, replica_id, leader_id
                )
                results[replica_id] = recovered

        return results

    def detect_recovered_nodes(self, language: str) -> List[str]:
        """
        Detect replicas that have recently come back online
        (commit_index > last_applied, indicating daemon was paused)

        Returns: List of recovered node IDs
        """
        leader_id = self.coordinator.get_leader_for_language(language)
        if not leader_id:
            return []

        leader_path = self.coordinator.get_shard_path(leader_id, language)
        leader_last_applied = StateManager.get_last_applied(leader_path)

        recovered = []
        replicas = self.coordinator.get_replica_nodes(language)

        for replica_id in replicas:
            if replica_id == leader_id:
                continue

            replica_path = self.coordinator.get_shard_path(replica_id, language)
            state = StateManager.load_state(replica_path)

            if not state:
                continue  # Still offline

            replica_commit = state.get("commit_index", 0)
            replica_last_applied = state.get("last_applied", 0)

            # Detect if commit_index exists but last_applied is behind
            # This indicates daemon was paused/crashed
            lag = replica_commit - replica_last_applied

            # Consider "recovered" if more than 3 entries not applied (lowered threshold)
            if lag > 3:
                recovered.append(replica_id)

        return recovered

    def recovery_cycle(self) -> None:
        """Run one recovery cycle for all language groups"""
        languages = self.coordinator.get_all_languages()

        print(f"\n=== Recovery Cycle at {time.strftime('%H:%M:%S')} ===")

        # Check all language groups in parallel
        future_to_lang = {
            self.executor.submit(self.check_and_recover_language_group, lang): lang
            for lang in languages
        }

        total_recovered = 0
        for future in as_completed(future_to_lang):
            lang = future_to_lang[future]
            try:
                results = future.result()
                if results:
                    total_recovered += sum(results.values())
            except Exception as e:
                print(f"[{lang}] Recovery failed: {e}")

        if total_recovered > 0:
            print(f"Recovery complete: {total_recovered} total entries recovered")
        else:
            print("All replicas up to date")

    def run(self) -> None:
        """Main daemon loop"""
        print(
            f"Recovery daemon started (check_interval={self.check_interval}s, leader_timeout={self.leader_timeout}s)"
        )

        while self.running:
            try:
                self.recovery_cycle()
            except Exception as e:
                print(f"Recovery cycle error: {e}")

            # Sleep until next cycle
            time.sleep(self.check_interval)

        print("Recovery daemon stopped")

    def start(self) -> None:
        """Start daemon in background thread"""
        if self.running:
            print("Daemon already running")
            return

        self.running = True
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()
        print(f"Recovery daemon started in background (PID: {os.getpid()})")

    def stop(self) -> None:
        """Stop daemon"""
        if not self.running:
            print("Daemon not running")
            return

        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        print("Recovery daemon stopped")


# Convenience functions
def start_recovery_daemon(
    config_path: str = "db/node_config.json",
    check_interval: int = 30,
    leader_timeout: int = 10,
) -> RecoveryDaemon:
    """Start recovery daemon in background"""
    daemon = RecoveryDaemon(config_path, check_interval, leader_timeout)
    daemon.start()
    return daemon


if __name__ == "__main__":
    # Example usage
    daemon = start_recovery_daemon(check_interval=10)

    try:
        print("Press Ctrl+C to stop daemon...")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping daemon...")
        daemon.stop()
