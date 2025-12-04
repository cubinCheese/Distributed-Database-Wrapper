"""State Manager
Manages state.json files for all nodes in the distributed database.
Supports both synchronous (leader) and asynchronous (follower) updates.
"""

import json
import os
import threading
from typing import Dict, Optional
import fcntl


class StateManager:
    """Manages Raft state for shards"""

    @staticmethod
    def get_state_path(shard_path: str) -> str:
        """Get path to state.json for a shard"""
        return os.path.join(shard_path, "state.json")

    @staticmethod
    def initialize_state(
        shard_path: str,
        node_id: str,
        language: str,
        role: str = "follower",
        term: int = 0,
    ) -> None:
        """
        Initialize state.json for a shard

        Args:
            shard_path: Path to shard directory (e.g., "db/node_0/zh")
            node_id: Node identifier (e.g., "node_0")
            language: Language code (e.g., "zh")
            role: Initial role ("follower", "candidate", or "leader")
            term: Initial term (default 0)
        """
        os.makedirs(shard_path, exist_ok=True)

        state = {
            "node_id": node_id,
            "language": language,
            "role": role,
            "term": term,
            "voted_for": None,
            "commit_index": 0,
            "last_applied": 0,  # Track which entries have been applied to DB
            "current_leader": None,
            "voting_history": {},
            "last_heartbeat": None,
            "election_timeout": 10.0,
        }

        StateManager.save_state_sync(shard_path, state)

    @staticmethod
    def load_state(shard_path: str) -> Optional[Dict]:
        """
        Load state from state.json
        Returns None if file doesn't exist or is invalid
        """
        state_path = StateManager.get_state_path(shard_path)

        if not os.path.exists(state_path):
            return None

        try:
            with open(state_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None

    @staticmethod
    def save_state_sync(shard_path: str, state: Dict) -> None:
        """
        Synchronous state save (for leader)
        Uses file locking for safety
        """
        state_path = StateManager.get_state_path(shard_path)
        os.makedirs(os.path.dirname(state_path), exist_ok=True)

        # Write to temp file first, then atomic rename
        temp_path = state_path + ".tmp"

        with open(temp_path, "w") as f:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                json.dump(state, f, indent=2)
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except IOError:
                # If locking fails, write anyway
                json.dump(state, f, indent=2)

        # Atomic rename
        os.replace(temp_path, state_path)

    @staticmethod
    def save_state_async(shard_path: str, state: Dict) -> threading.Thread:
        """
        Asynchronous state save (for followers)
        Returns thread handle so caller can join if needed

        Note: Includes error handling for race conditions with cleanup operations
        """

        def _save():
            try:
                StateManager.save_state_sync(shard_path, state)
            except FileNotFoundError:
                # Silently ignore - file/directory may have been cleaned up by another thread
                # This is expected during rapid elections or system shutdown
                pass
            except Exception as e:
                # Log unexpected errors to stderr (not stdout to avoid cluttering output)
                import sys

                print(
                    f"Warning: Async state save failed for {shard_path}: {e}",
                    file=sys.stderr,
                )

        thread = threading.Thread(target=_save, daemon=True)
        thread.start()
        return thread

    @staticmethod
    def update_role(
        shard_path: str, role: str, sync: bool = True
    ) -> Optional[threading.Thread]:
        """
        Update node role

        Args:
            shard_path: Path to shard directory
            role: New role ("follower", "candidate", or "leader")
            sync: If True, update synchronously; if False, asynchronously

        Returns: Thread handle if async, None if sync
        """
        state = StateManager.load_state(shard_path)
        if state is None:
            return None

        state["role"] = role

        if sync:
            StateManager.save_state_sync(shard_path, state)
            return None
        else:
            return StateManager.save_state_async(shard_path, state)

    @staticmethod
    def update_leader(
        shard_path: str, leader_id: Optional[str], term: int, sync: bool = True
    ) -> Optional[threading.Thread]:
        """
        Update current leader and term

        Args:
            shard_path: Path to shard directory
            leader_id: Leader node ID (or None if no leader)
            term: New term
            sync: If True, update synchronously; if False, asynchronously

        Returns: Thread handle if async, None if sync
        """
        state = StateManager.load_state(shard_path)
        if state is None:
            return None

        state["current_leader"] = leader_id
        state["term"] = term

        if sync:
            StateManager.save_state_sync(shard_path, state)
            return None
        else:
            return StateManager.save_state_async(shard_path, state)

    @staticmethod
    def update_commit_index_sync(shard_path: str, index: int) -> None:
        """Synchronous commit_index update (leader)"""
        state = StateManager.load_state(shard_path)
        if state is None:
            return

        state["commit_index"] = index
        StateManager.save_state_sync(shard_path, state)

    @staticmethod
    def update_commit_index_async(shard_path: str, index: int) -> threading.Thread:
        """Asynchronous commit_index update (followers)"""
        state = StateManager.load_state(shard_path)
        if state is None:
            # Create dummy thread that does nothing
            t = threading.Thread(target=lambda: None)
            t.start()
            return t

        state["commit_index"] = index
        return StateManager.save_state_async(shard_path, state)

    @staticmethod
    def get_commit_index(shard_path: str) -> int:
        """Get current commit index"""
        state = StateManager.load_state(shard_path)
        return state.get("commit_index", 0) if state else 0

    @staticmethod
    def get_term(shard_path: str) -> int:
        """Get current term"""
        state = StateManager.load_state(shard_path)
        return state.get("term", 0) if state else 0

    @staticmethod
    def get_role(shard_path: str) -> str:
        """Get current role"""
        state = StateManager.load_state(shard_path)
        return state.get("role", "follower") if state else "follower"

    @staticmethod
    def get_current_leader(shard_path: str) -> Optional[str]:
        """Get current leader node ID"""
        state = StateManager.load_state(shard_path)
        return state.get("current_leader") if state else None

    @staticmethod
    def record_vote(
        shard_path: str, term: int, candidate: str, sync: bool = True
    ) -> Optional[threading.Thread]:
        """
        Record vote in voting_history for FlexiRaft Alg.2

        Args:
            shard_path: Path to shard directory
            term: Term for which vote was cast
            candidate: Candidate node ID
            sync: If True, update synchronously; if False, asynchronously

        Returns: Thread handle if async, None if sync
        """
        state = StateManager.load_state(shard_path)
        if state is None:
            return None

        state["voted_for"] = candidate

        # Update voting history (convert term to string for JSON compatibility)
        if "voting_history" not in state:
            state["voting_history"] = {}
        state["voting_history"][str(term)] = candidate

        if sync:
            StateManager.save_state_sync(shard_path, state)
            return None
        else:
            return StateManager.save_state_async(shard_path, state)

    @staticmethod
    def get_voting_history(shard_path: str) -> Dict[int, str]:
        """
        Get voting history for FlexiRaft Alg.2
        Returns: {term: candidate_id}
        """
        state = StateManager.load_state(shard_path)
        if state is None or "voting_history" not in state:
            return {}

        # Convert string keys back to integers
        history = state["voting_history"]
        return {int(k): v for k, v in history.items()}

    @staticmethod
    def increment_term(shard_path: str, sync: bool = True) -> int:
        """
        Increment term (used when starting election)
        Returns new term
        """
        state = StateManager.load_state(shard_path)
        if state is None:
            return 1

        new_term = state.get("term", 0) + 1
        state["term"] = new_term
        state["voted_for"] = None  # Clear vote for new term

        if sync:
            StateManager.save_state_sync(shard_path, state)
        else:
            StateManager.save_state_async(shard_path, state)

        return new_term

    @staticmethod
    def update_last_heartbeat(shard_path: str, timestamp: float) -> None:
        """Update last heartbeat timestamp (async)"""
        state = StateManager.load_state(shard_path)
        if state is None:
            return

        state["last_heartbeat"] = timestamp
        StateManager.save_state_async(shard_path, state)

    @staticmethod
    def get_last_heartbeat(shard_path: str) -> Optional[float]:
        """Get last heartbeat timestamp"""
        state = StateManager.load_state(shard_path)
        return state.get("last_heartbeat") if state else None

    @staticmethod
    def get_last_applied(shard_path: str) -> int:
        """Get last applied index"""
        state = StateManager.load_state(shard_path)
        return state.get("last_applied", 0) if state else 0

    @staticmethod
    def update_last_applied(
        shard_path: str, index: int, sync: bool = True
    ) -> Optional[threading.Thread]:
        """
        Update last_applied index

        Args:
            shard_path: Path to shard directory
            index: New last_applied value
            sync: If True, update synchronously; if False, asynchronously

        Returns: Thread handle if async, None if sync
        """
        state = StateManager.load_state(shard_path)
        if state is None:
            return None

        state["last_applied"] = index

        if sync:
            StateManager.save_state_sync(shard_path, state)
            return None
        else:
            return StateManager.save_state_async(shard_path, state)


# Convenience function for backward compatibility
def update_commit_index(shard_path: str, index: int) -> None:
    """Legacy compatibility function - defaults to sync"""
    StateManager.update_commit_index_sync(shard_path, index)
