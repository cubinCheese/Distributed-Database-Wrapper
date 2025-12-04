"""System Initialization
Initializes all components of the distributed database system.
Creates directory structure, databases, and state files for all 32 shards.
"""

import json
import os
import sys
from typing import Dict, List, Tuple
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.db_schema import DatabaseSchema
from db.state_manager import StateManager
from db.raft_log_manager import RaftLogManager


class SystemInitializer:
    """Initialize the distributed database system"""

    def __init__(self, config_path: str = "db/node_config.json"):
        """
        Initialize with configuration

        Args:
            config_path: Path to node configuration JSON file
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.base_path = os.path.dirname(config_path)

    def _load_config(self) -> Dict:
        """Load node configuration"""
        with open(self.config_path, "r") as f:
            return json.load(f)

    def ensure_directory_structure(self) -> None:
        """
        Create all 32 shard directories if they don't exist
        Based on config: node_0/zh, node_0/th, etc.
        """
        for node_id, languages in self.config["shards"].items():
            for language in languages:
                shard_path = os.path.join(self.base_path, node_id, language)
                os.makedirs(shard_path, exist_ok=True)

    def initialize_all_databases(self) -> Dict[str, List[str]]:
        """
        Initialize all 32 SQLite databases with schema

        Returns: {language: [node_paths]} mapping
        """
        initialized = {}

        for node_id, languages in self.config["shards"].items():
            for language in languages:
                shard_path = os.path.join(self.base_path, node_id, language)

                # Initialize database with schema
                DatabaseSchema.initialize_database(shard_path)

                # Track initialization
                if language not in initialized:
                    initialized[language] = []
                initialized[language].append(shard_path)

        return initialized

    def initialize_all_states(self) -> None:
        """
        Initialize all state.json files
        Sets all nodes to follower role with term 0
        """
        for node_id, languages in self.config["shards"].items():
            for language in languages:
                shard_path = os.path.join(self.base_path, node_id, language)

                # Initialize state
                StateManager.initialize_state(
                    shard_path=shard_path,
                    node_id=node_id,
                    language=language,
                    role="follower",
                    term=0,
                )

    def initialize_raft_logs(self) -> None:
        """
        Create empty raft_log.json files for all shards
        """
        for node_id, languages in self.config["shards"].items():
            for language in languages:
                shard_path = os.path.join(self.base_path, node_id, language)

                # Ensure log exists (creates empty file)
                RaftLogManager.ensure_log_exists(shard_path)

    def verify_system_ready(self) -> Tuple[bool, List[str]]:
        """
        Verify all components are ready

        Returns: (ready, list_of_missing_components)
        """
        missing = []

        for node_id, languages in self.config["shards"].items():
            for language in languages:
                shard_path = os.path.join(self.base_path, node_id, language)

                # Check directory exists
                if not os.path.exists(shard_path):
                    missing.append(f"{shard_path} (directory)")
                    continue

                # Check database exists
                db_path = DatabaseSchema.get_db_path(shard_path)
                if not os.path.exists(db_path):
                    missing.append(f"{db_path} (database)")

                # Check state file exists
                state_path = StateManager.get_state_path(shard_path)
                if not os.path.exists(state_path):
                    missing.append(f"{state_path} (state)")

                # Check raft log exists
                log_path = RaftLogManager.get_log_path(shard_path)
                if not os.path.exists(log_path):
                    missing.append(f"{log_path} (raft log)")

        return (len(missing) == 0, missing)

    def get_shard_count(self) -> int:
        """Get total number of shards"""
        count = 0
        for languages in self.config["shards"].values():
            count += len(languages)
        return count

    def get_language_replicas(self, language: str) -> List[str]:
        """
        Get list of node IDs that replicate a language

        Args:
            language: Language code

        Returns: List of node IDs
        """
        return self.config.get("replicas", {}).get(language, [])

    def get_shard_path(self, node_id: str, language: str) -> str:
        """
        Get path to a specific shard

        Args:
            node_id: Node identifier
            language: Language code

        Returns: Path to shard directory
        """
        return os.path.join(self.base_path, node_id, language)

    def initialize_all(self, verbose: bool = False) -> bool:
        """
        Complete system initialization

        Args:
            verbose: If True, print progress messages

        Returns: True if successful
        """
        try:
            if verbose:
                print("Creating directory structure...")
            self.ensure_directory_structure()

            if verbose:
                print("Initializing databases...")
            self.initialize_all_databases()

            if verbose:
                print("Initializing state files...")
            self.initialize_all_states()

            if verbose:
                print("Initializing raft logs...")
            self.initialize_raft_logs()

            if verbose:
                print("Verifying system...")
            ready, missing = self.verify_system_ready()

            if not ready:
                if verbose:
                    print(f"ERROR: Missing components:")
                    for item in missing:
                        print(f"  - {item}")
                return False

            if verbose:
                shard_count = self.get_shard_count()
                print(f"✓ System initialized successfully ({shard_count} shards)")

            return True

        except Exception as e:
            if verbose:
                print(f"ERROR during initialization: {e}")
            return False


# Convenience functions
def ensure_directory_structure(config_path: str = "db/node_config.json") -> None:
    """Convenience function for directory setup"""
    initializer = SystemInitializer(config_path)
    initializer.ensure_directory_structure()


def initialize_all_databases(
    config_path: str = "db/node_config.json",
) -> Dict[str, List[str]]:
    """Convenience function for database initialization"""
    initializer = SystemInitializer(config_path)
    return initializer.initialize_all_databases()


def initialize_all_states(config_path: str = "db/node_config.json") -> None:
    """Convenience function for state initialization"""
    initializer = SystemInitializer(config_path)
    initializer.initialize_all_states()


def initialize_raft_logs(config_path: str = "db/node_config.json") -> None:
    """Convenience function for raft log initialization"""
    initializer = SystemInitializer(config_path)
    initializer.initialize_raft_logs()


def verify_system_ready(
    config_path: str = "db/node_config.json",
) -> Tuple[bool, List[str]]:
    """Convenience function for system verification"""
    initializer = SystemInitializer(config_path)
    return initializer.verify_system_ready()
