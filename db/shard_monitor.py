"""Shard Monitor
Monitors replica health and detects when shards have permanently died.
With 2/4 quorum, a shard dies when < 2 replicas are available.
"""

import json
import os
from typing import Dict, List, Set, Tuple
from db.flexiraft_coordinator import FlexiRaftCoordinator


class ShardMonitor:
    """Monitor replica health and detect shard death"""

    def __init__(self, config_path: str = "db/node_config.json"):
        """
        Initialize shard monitor

        Args:
            config_path: Path to node configuration
        """
        self.config_path = config_path
        self.coordinator = FlexiRaftCoordinator(config_path)
        self.crashed_nodes: Set[str] = set()  # Track which nodes are crashed

    def register_crash(self, node_id: str):
        """
        Register a node as crashed

        Args:
            node_id: Node ID that crashed
        """
        self.crashed_nodes.add(node_id)

    def register_recovery(self, node_id: str):
        """
        Register a node as recovered

        Args:
            node_id: Node ID that recovered
        """
        self.crashed_nodes.discard(node_id)

    def check_shard_health(self, language: str) -> Tuple[bool, int, List[str]]:
        """
        Check if a shard is healthy (can reach 2/4 quorum)

        Args:
            language: Language code

        Returns:
            (is_healthy, available_replicas, available_nodes)
            - is_healthy: True if >= 2 replicas available
            - available_replicas: Count of non-crashed replicas
            - available_nodes: List of available node IDs
        """
        replicas = self.coordinator.get_replica_nodes(language)
        available_nodes = [node for node in replicas if node not in self.crashed_nodes]
        available_count = len(available_nodes)

        # With 2/4 quorum, need at least 2 replicas
        is_healthy = available_count >= 2

        return is_healthy, available_count, available_nodes

    def check_all_shards(self) -> Dict[str, Tuple[bool, int, List[str]]]:
        """
        Check health of all shards

        Returns:
            Dict mapping language to (is_healthy, available_count, available_nodes)
        """
        results = {}
        languages = self.coordinator.get_all_languages()

        for lang in languages:
            results[lang] = self.check_shard_health(lang)

        return results

    def get_dead_shards(self) -> List[Tuple[str, int, List[str]]]:
        """
        Get list of dead shards (< 2 replicas available)

        Returns:
            List of (language, available_count, available_nodes) tuples
        """
        dead_shards = []
        all_health = self.check_all_shards()

        for lang, (is_healthy, count, nodes) in all_health.items():
            if not is_healthy:
                dead_shards.append((lang, count, nodes))

        return dead_shards

    def format_death_report(self, dead_shards: List[Tuple[str, int, List[str]]]) -> str:
        """
        Format shard death report for logging

        Args:
            dead_shards: List of dead shard tuples

        Returns:
            Formatted death report string
        """
        lines = []
        lines.append("=" * 70)
        lines.append("CRITICAL: SHARD DEATH DETECTED")
        lines.append("=" * 70)
        lines.append("")
        lines.append("The following shards have permanently failed due to")
        lines.append("insufficient replicas (need 2/4, but less than 2 available):")
        lines.append("")

        for lang, count, nodes in dead_shards:
            lines.append(f"  Language: {lang}")
            lines.append(f"    Available replicas: {count}/4")
            if nodes:
                lines.append(f"    Available nodes: {nodes}")
            else:
                lines.append(f"    Available nodes: NONE")

            # Identify which nodes are crashed
            all_replicas = set(self.coordinator.get_replica_nodes(lang))
            crashed = sorted(list(all_replicas - set(nodes)))
            lines.append(f"    Crashed nodes: {crashed}")
            lines.append("")

        lines.append("Crashed nodes across system:")
        lines.append(f"  {sorted(list(self.crashed_nodes))}")
        lines.append("")
        lines.append("=" * 70)
        lines.append("System cannot continue. Terminating gracefully.")
        lines.append("=" * 70)

        return "\n".join(lines)

    def get_crashed_nodes(self) -> List[str]:
        """Get list of currently crashed nodes"""
        return sorted(list(self.crashed_nodes))

    def is_node_crashed(self, node_id: str) -> bool:
        """
        Check if a specific node is currently crashed

        Args:
            node_id: Node ID to check

        Returns:
            True if node is crashed, False otherwise
        """
        return node_id in self.crashed_nodes

    def get_node_shards(self, node_id: str) -> List[str]:
        """
        Get list of shards managed by a specific node

        Args:
            node_id: Node ID

        Returns:
            List of language codes
        """
        # Load config to get shard assignments
        with open(self.config_path, "r") as f:
            config = json.load(f)

        return config.get("shards", {}).get(node_id, [])


# Convenience functions
def check_shard_health(
    language: str, config_path: str = "db/node_config.json"
) -> Tuple[bool, int, List[str]]:
    """Convenience function to check single shard health"""
    monitor = ShardMonitor(config_path)
    return monitor.check_shard_health(language)


def check_all_shards(
    config_path: str = "db/node_config.json",
) -> Dict[str, Tuple[bool, int, List[str]]]:
    """Convenience function to check all shard health"""
    monitor = ShardMonitor(config_path)
    return monitor.check_all_shards()
