"""Election Manager
Manages leader elections for all language groups with parallel execution.
Uses FlexiRaft for consensus, with random tie-breaking and retry logic.
"""

import random
import time
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple, Callable, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from wrapper.flexiraft import ElectionStatus, LeaderRef
from db.flexiraft_coordinator import FlexiRaftCoordinator
from db.state_manager import StateManager


class ElectionManager:
    """Manages parallel leader elections for all language groups"""

    def __init__(self, config_path: str = "db/node_config.json", shard_monitor=None):
        """
        Initialize election manager

        Args:
            config_path: Path to node configuration JSON
            shard_monitor: Optional ShardMonitor instance to check for crashed nodes
        """
        self.config_path = config_path
        self.coordinator = FlexiRaftCoordinator(
            config_path, shard_monitor=shard_monitor
        )
        self.executor = ThreadPoolExecutor(max_workers=8)

    def elect_leader_for_group(
        self,
        language: str,
        max_retries: int = -1,  # -1 = infinite
    ) -> Tuple[str, int]:
        """
        Elect leader for a single language group
        Uses FlexiRaft with retry on failure

        Args:
            language: Language code
            max_retries: Maximum retry attempts (-1 for infinite)

        Returns: (leader_node_id, term)
        """
        replicas = self.coordinator.get_replica_nodes(language)

        if not replicas:
            raise ValueError(f"No replicas found for language: {language}")

        attempt = 0
        backoff = 1.0  # Initial backoff in seconds

        while max_retries < 0 or attempt < max_retries:
            attempt += 1

            # Choose a random candidate for this attempt
            candidate_id = random.choice(replicas)

            # Get shard path and increment term
            shard_path = self.coordinator.get_shard_path(candidate_id, language)
            term = StateManager.increment_term(shard_path, sync=True)

            # Vote for self
            StateManager.record_vote(shard_path, term, candidate_id, sync=True)

            # Get last known leader (if any)
            current_leader_id = self.coordinator.get_leader_for_language(language)
            last_known_leader = None
            if current_leader_id:
                # Get term from leader's state
                leader_shard = self.coordinator.get_shard_path(
                    current_leader_id, language
                )
                leader_term = StateManager.get_term(leader_shard)
                last_known_leader = LeaderRef(
                    node_id=current_leader_id, group=language, term=leader_term
                )

            # Run FlexiRaft election
            result = self.coordinator.run_flexiraft_election(
                language=language,
                candidate_id=candidate_id,
                term=term,
                last_known_leader=last_known_leader,
            )

            if result.status == ElectionStatus.WON:
                # Election successful!
                # Update all replicas with new leader (force sync to prevent thread race conditions)
                self.coordinator.update_all_replicas_with_leader(
                    language=language,
                    leader_id=candidate_id,
                    term=term,
                    force_sync=True,
                )

                return (candidate_id, term)

            elif result.status == ElectionStatus.LOST:
                # Election lost, retry with backoff
                if attempt % 10 == 0:
                    print(
                        f"  Election attempt {attempt} for {language} failed, retrying..."
                    )
                time.sleep(backoff)
                backoff = min(backoff * 1.5, 5.0)  # Exponential backoff, max 5s

            else:  # UNDECIDED
                # Not enough votes yet, retry quickly
                time.sleep(0.5)

        # Max retries exceeded
        raise Exception(
            f"Failed to elect leader for {language} after {attempt} attempts"
        )

    def ensure_leader_for_language(self, language: str) -> Tuple[str, int]:
        """
        Ensure a leader exists for a language group.
        Triggers election only if no valid leader exists.

        Args:
            language: Language code

        Returns: (leader_id, term)
        """
        # Check if leader already exists
        current_leader = self.coordinator.get_leader_for_language(language)

        if current_leader:
            # Verify leader is valid (has state file)
            shard_path = self.coordinator.get_shard_path(current_leader, language)
            state = StateManager.load_state(shard_path)

            if state and state.get("role") == "leader":
                term = state.get("term", 0)
                return (current_leader, term)

        # No valid leader, trigger election
        print(f"  ⚡ Triggering on-demand election for {language}...")
        return self.elect_leader_for_group(language)

    def elect_all_leaders_parallel(self) -> Dict[str, Tuple[str, int]]:
        """
        Elect leaders for all 8 language groups in parallel

        Returns: {language: (leader_id, term)}
        """
        languages = self.coordinator.get_all_languages()
        results = {}

        # Submit all election tasks
        future_to_lang = {
            self.executor.submit(self.elect_leader_for_group, lang): lang
            for lang in languages
        }

        # Collect results as they complete
        for future in as_completed(future_to_lang):
            lang = future_to_lang[future]
            try:
                leader_id, term = future.result()
                results[lang] = (leader_id, term)
            except Exception as e:
                print(f"ERROR: Election failed for {lang}: {e}")
                # For now, continue without this language
                # In production, might want to retry

        return results

    def trigger_reelection(
        self, language: str, reason: str = "timeout"
    ) -> Tuple[str, int]:
        """
        Trigger reelection for a language group

        Args:
            language: Language code
            reason: Reason for reelection

        Returns: (new_leader_id, term)
        """
        print(f"Triggering reelection for {language} (reason: {reason})")
        return self.elect_leader_for_group(language)


# Convenience functions
def elect_all_leaders(
    config_path: str = "db/node_config.json",
) -> Dict[str, Tuple[str, int]]:
    """Convenience function for electing all leaders"""
    manager = ElectionManager(config_path)
    return manager.elect_all_leaders_parallel()


def elect_leader_for_language(
    language: str, config_path: str = "db/node_config.json"
) -> Tuple[str, int]:
    """Convenience function for electing single leader"""
    manager = ElectionManager(config_path)
    return manager.elect_leader_for_group(language)
