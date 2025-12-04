"""Raft Log Manager
Manages raft log operations for the distributed database.
Uses newline-delimited JSON format compatible with existing router.py.
Supports ghost entries (uncommitted entries that can be overwritten).
"""

import json
import os
from typing import Dict, List, Optional, Tuple
import fcntl


class RaftLogManager:
    """Manages raft log operations with ghost entry support"""

    @staticmethod
    def get_log_path(shard_path: str) -> str:
        """Get path to raft_log.json for a shard"""
        return os.path.join(shard_path, "raft_log.json")

    @staticmethod
    def ensure_log_exists(shard_path: str) -> None:
        """Create empty raft_log.json if it doesn't exist"""
        log_path = RaftLogManager.get_log_path(shard_path)
        if not os.path.exists(log_path):
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            with open(log_path, "w") as f:
                pass  # Create empty file

    @staticmethod
    def append_entry(
        shard_path: str, term: int, operation: str, sql: str, params: List
    ) -> int:
        """
        Append entry to raft_log.json
        Format: {"term": int, "index": int, "operation": str, "sql": str, "params": list}

        NOTE: Does NOT check for duplicates - allows ghost entries

        Returns: entry index
        """
        RaftLogManager.ensure_log_exists(shard_path)
        log_path = RaftLogManager.get_log_path(shard_path)

        # Get next index
        last_index = RaftLogManager.get_last_log_index(shard_path)
        next_index = last_index + 1

        # Create entry
        entry = {
            "term": term,
            "index": next_index,
            "operation": operation,
            "sql": sql,
            "params": params,
        }

        # Append to log (with file locking for safety)
        with open(log_path, "a") as f:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                f.write(json.dumps(entry) + "\n")
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except IOError:
                # If locking fails, write anyway (best effort)
                f.write(json.dumps(entry) + "\n")

        return next_index

    @staticmethod
    def append_entries_bulk(
        shard_path: str, term: int, entries: List[Dict]
    ) -> Tuple[int, int]:
        """
        Append multiple entries in bulk (more efficient)
        Each entry should have: {"operation": str, "sql": str, "params": list}

        Returns: (start_index, end_index)
        """
        RaftLogManager.ensure_log_exists(shard_path)
        log_path = RaftLogManager.get_log_path(shard_path)

        last_index = RaftLogManager.get_last_log_index(shard_path)
        start_index = last_index + 1

        # Prepare all entries with indices
        log_entries = []
        for i, entry_data in enumerate(entries):
            entry = {
                "term": term,
                "index": start_index + i,
                "operation": entry_data["operation"],
                "sql": entry_data["sql"],
                "params": entry_data["params"],
            }
            log_entries.append(json.dumps(entry) + "\n")

        # Write all entries
        with open(log_path, "a") as f:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                f.writelines(log_entries)
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except IOError:
                f.writelines(log_entries)

        end_index = start_index + len(entries) - 1
        return start_index, end_index

    @staticmethod
    def read_log(
        shard_path: str, start_index: int = 0, end_index: Optional[int] = None
    ) -> List[Dict]:
        """
        Read raft log entries in range [start_index, end_index] (inclusive)

        Args:
            shard_path: Path to shard directory
            start_index: Starting index (0 = read all)
            end_index: Ending index (None = read to end)

        Returns: list of log entries
        """
        log_path = RaftLogManager.get_log_path(shard_path)

        if not os.path.exists(log_path):
            return []

        entries = []
        with open(log_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    entry = json.loads(line)
                    index = entry.get("index", 0)

                    # Filter by index range
                    if index < start_index:
                        continue
                    if end_index is not None and index > end_index:
                        break

                    entries.append(entry)
                except json.JSONDecodeError:
                    # Skip malformed entries
                    continue

        return entries

    @staticmethod
    def get_last_log_index(shard_path: str) -> int:
        """
        Get index of last log entry (including uncommitted/ghost entries)
        Returns: last index, or 0 if log is empty
        """
        log_path = RaftLogManager.get_log_path(shard_path)

        if not os.path.exists(log_path):
            return 0

        last_index = 0
        with open(log_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    entry = json.loads(line)
                    index = entry.get("index", 0)
                    last_index = max(last_index, index)
                except json.JSONDecodeError:
                    continue

        return last_index

    @staticmethod
    def get_log_length(shard_path: str) -> int:
        """
        Get number of entries in log (compatible with existing router.py)
        """
        log_path = RaftLogManager.get_log_path(shard_path)

        if not os.path.exists(log_path):
            return 0

        with open(log_path, "r") as f:
            return sum(1 for line in f if line.strip())

    @staticmethod
    def reconcile_log_with_leader(
        follower_path: str,
        leader_path: str,
        leader_last_index: int,
        leader_commit_index: int,
    ) -> int:
        """
        Raft log reconciliation: overwrite follower's log to match leader
        This overwrites any ghost entries on the follower.

        Flow:
        1. Read follower's log
        2. Read leader's log
        3. Find divergence point
        4. Truncate follower's log after divergence
        5. Copy leader's entries from divergence to last_index

        Args:
            follower_path: Path to follower shard
            leader_path: Path to leader shard
            leader_last_index: Leader's last log index
            leader_commit_index: Leader's commit index

        Returns: number of entries reconciled
        """
        # Read both logs
        follower_entries = RaftLogManager.read_log(follower_path)
        leader_entries = RaftLogManager.read_log(
            leader_path, end_index=leader_last_index
        )

        # Build maps for easy comparison
        follower_map = {e["index"]: e for e in follower_entries}
        leader_map = {e["index"]: e for e in leader_entries}

        # Find divergence point
        divergence_index = 0
        for i in range(1, min(len(follower_entries), len(leader_entries)) + 1):
            if i in follower_map and i in leader_map:
                if follower_map[i]["term"] == leader_map[i]["term"]:
                    divergence_index = i
                else:
                    break
            else:
                break

        # If logs match up to leader's last index, no reconciliation needed
        if divergence_index == leader_last_index:
            return 0

        # Truncate follower's log and copy leader's entries
        follower_log_path = RaftLogManager.get_log_path(follower_path)

        # Write new log (overwrite mode)
        with open(follower_log_path, "w") as f:
            # Write entries up to divergence point
            for i in range(1, divergence_index + 1):
                if i in follower_map:
                    f.write(json.dumps(follower_map[i]) + "\n")

            # Write leader's entries after divergence
            for i in range(divergence_index + 1, leader_last_index + 1):
                if i in leader_map:
                    f.write(json.dumps(leader_map[i]) + "\n")

        reconciled_count = leader_last_index - divergence_index
        return reconciled_count

    @staticmethod
    def copy_entries_to_follower(
        leader_path: str, follower_path: str, start_index: int, end_index: int
    ) -> int:
        """
        Copy log entries from leader to follower
        Used for recovery when follower is behind

        Returns: count of entries copied
        """
        # Read entries from leader
        entries = RaftLogManager.read_log(leader_path, start_index, end_index)

        if not entries:
            return 0

        # Append to follower's log
        RaftLogManager.ensure_log_exists(follower_path)
        follower_log_path = RaftLogManager.get_log_path(follower_path)

        with open(follower_log_path, "a") as f:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                for entry in entries:
                    f.write(json.dumps(entry) + "\n")
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except IOError:
                for entry in entries:
                    f.write(json.dumps(entry) + "\n")

        return len(entries)


# Convenience functions for backward compatibility
def append_raft_log(shard_path: str, term: int, sql: str, params: List) -> int:
    """Legacy compatibility function"""
    return RaftLogManager.append_entry(shard_path, term, "INSERT", sql, params)


def get_log_length(shard_path: str) -> int:
    """Legacy compatibility function"""
    return RaftLogManager.get_log_length(shard_path)
