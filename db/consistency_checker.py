"""Consistency Checker
Checks consistency of replicas across all language shards.
Generates detailed reports showing per-node counts and outliers.
"""

import json
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import Counter


class ConsistencyChecker:
    """Check and report on replica consistency across all shards"""

    def __init__(self, config_path: str = "db/node_config.json"):
        self.config_path = config_path
        self.config = self._load_config()
        self.base_path = os.path.dirname(config_path)

    def _load_config(self) -> Dict:
        """Load node configuration"""
        with open(self.config_path, "r") as f:
            return json.load(f)

    def count_entries_in_shard(self, node_id: str, language: str) -> int:
        """
        Count number of entries in a shard's database

        Args:
            node_id: Node ID (e.g., "node_0")
            language: Language code (e.g., "zh")

        Returns: Count of novels in database, or 0 if database doesn't exist
        """
        shard_path = os.path.join(self.base_path, node_id, language)
        db_path = os.path.join(shard_path, "novel.db")

        if not os.path.exists(db_path):
            return 0

        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM novels")
            count = cursor.fetchone()[0]
            conn.close()
            return count
        except Exception as e:
            # Database might not be initialized yet
            return 0

    def check_shard_consistency(self, language: str) -> Dict:
        """
        Check consistency for one language across all 4 replicas

        Args:
            language: Language code

        Returns:
            {
                "language": "zh",
                "replica_counts": {
                    "node_0": 25,
                    "node_1": 25,
                    "node_2": 24,
                    "node_7": 25
                },
                "consistent": False,
                "outliers": [{"node": "node_2", "count": 24, "diff": -1}],
                "expected_count": 25
            }
        """
        replicas = self.config.get("replicas", {}).get(language, [])

        # Query each replica's database
        replica_counts = {}
        for node_id in replicas:
            count = self.count_entries_in_shard(node_id, language)
            replica_counts[node_id] = count

        # Determine expected count (most common count)
        counts_list = list(replica_counts.values())
        if not counts_list:
            return {
                "language": language,
                "replica_counts": {},
                "consistent": True,
                "outliers": [],
                "expected_count": 0,
            }

        # Use Counter to find most common count
        count_freq = Counter(counts_list)
        expected_count = count_freq.most_common(1)[0][0]

        # Check if all counts match
        consistent = all(c == expected_count for c in counts_list)

        # Identify outliers
        outliers = []
        for node_id, count in replica_counts.items():
            if count != expected_count:
                diff = count - expected_count
                outliers.append({"node": node_id, "count": count, "diff": diff})

        return {
            "language": language,
            "replica_counts": replica_counts,
            "consistent": consistent,
            "outliers": outliers,
            "expected_count": expected_count,
        }

    def generate_consistency_report(self) -> Dict:
        """
        Generate complete consistency report for all 8 languages

        Returns:
            {
                "timestamp": "2025-12-04 15:30:42",
                "languages": {
                    "zh": {...},
                    "ja": {...},
                    ...
                },
                "summary": {
                    "total_shards": 8,
                    "fully_consistent": 7,
                    "partially_consistent": 1,
                    "total_entries": 799,
                    "expected_entries": 800
                },
                "inconsistent_shards": [
                    {"language": "ja", "details": "node_2 has 24 (expected 25, missing 1)"}
                ]
            }
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        languages = self.config.get("languages", [])

        # Check each language
        language_results = {}
        for lang in languages:
            language_results[lang] = self.check_shard_consistency(lang)

        # Calculate summary statistics
        total_shards = len(languages)
        fully_consistent = sum(
            1 for result in language_results.values() if result["consistent"]
        )
        partially_consistent = total_shards - fully_consistent

        # Calculate total entries
        total_entries = 0
        expected_entries = 0
        for result in language_results.values():
            # Use expected_count for this shard (most common count)
            expected_entries += result["expected_count"] * 4  # 4 replicas
            # Sum actual counts
            total_entries += sum(result["replica_counts"].values())

        # Identify inconsistent shards with details
        inconsistent_shards = []
        for lang, result in language_results.items():
            if not result["consistent"]:
                for outlier in result["outliers"]:
                    node = outlier["node"]
                    count = outlier["count"]
                    expected = result["expected_count"]
                    diff = outlier["diff"]
                    if diff < 0:
                        detail = f"{node} has {count} entries (expected {expected}, missing {abs(diff)})"
                    else:
                        detail = f"{node} has {count} entries (expected {expected}, excess {diff})"
                    inconsistent_shards.append({"language": lang, "details": detail})

        return {
            "timestamp": timestamp,
            "languages": language_results,
            "summary": {
                "total_shards": total_shards,
                "fully_consistent": fully_consistent,
                "partially_consistent": partially_consistent,
                "total_entries": total_entries,
                "expected_entries": expected_entries,
            },
            "inconsistent_shards": inconsistent_shards,
        }

    def format_consistency_report(
        self, report: Dict, section_header: str = None
    ) -> str:
        """
        Format report as human-readable string

        Args:
            report: Report dict from generate_consistency_report()
            section_header: Optional section header (e.g., "BEFORE RECOVERY")

        Returns: Formatted string ready for writing to file or printing
        """
        lines = []
        lines.append("=" * 70)
        if section_header:
            lines.append(f"CONSISTENCY CHECK REPORT - {section_header}")
        else:
            lines.append("CONSISTENCY CHECK REPORT")
        lines.append(f"Generated: {report['timestamp']}")
        lines.append("=" * 70)
        lines.append("")

        # Header
        lines.append(f"{'Language':<10} {'Replica Counts':<45} {'Status':<15}")
        lines.append("-" * 70)

        # Per-language details
        languages = sorted(report["languages"].keys())
        for lang in languages:
            result = report["languages"][lang]
            replica_counts = result["replica_counts"]
            consistent = result["consistent"]

            # Format replica counts
            counts_str = "  ".join(
                [f"{node}: {count}" for node, count in sorted(replica_counts.items())]
            )

            # Status
            total_replicas = len(replica_counts)
            consistent_count = (
                total_replicas
                if consistent
                else (total_replicas - len(result["outliers"]))
            )
            status = f"{consistent_count}/{total_replicas} consistent"

            lines.append(f"{lang:<10} {counts_str:<45} {status:<15}")

            # Show outliers if any
            if not consistent:
                outlier_details = []
                for outlier in result["outliers"]:
                    node = outlier["node"]
                    count = outlier["count"]
                    diff = outlier["diff"]
                    outlier_details.append(f"{node} ({count}, {diff:+d})")
                lines.append(f"{'':10} Outliers: {', '.join(outlier_details)}")

        # Summary section
        lines.append("")
        lines.append("Summary:")
        summary = report["summary"]
        lines.append(f"  Total shards: {summary['total_shards']}")
        lines.append(
            f"  Fully consistent: {summary['fully_consistent']}/{summary['total_shards']} "
            f"({summary['fully_consistent'] / summary['total_shards'] * 100:.1f}%)"
        )
        if summary["partially_consistent"] > 0:
            lines.append(
                f"  Partially consistent: {summary['partially_consistent']}/{summary['total_shards']} "
                f"({summary['partially_consistent'] / summary['total_shards'] * 100:.1f}%)"
            )

        # Total entries comparison
        if summary["expected_entries"] > 0:
            percentage = (summary["total_entries"] / summary["expected_entries"]) * 100
            lines.append(
                f"  Total entries: {summary['total_entries']}/{summary['expected_entries']} ({percentage:.1f}%)"
            )
        else:
            lines.append(f"  Total entries: {summary['total_entries']}")

        # Inconsistent shards details
        if report["inconsistent_shards"]:
            lines.append("")
            lines.append("Inconsistent Shards:")
            for item in report["inconsistent_shards"]:
                lines.append(f"  {item['language']}: {item['details']}")

        lines.append("=" * 70)

        return "\n".join(lines)

    def write_consistency_report(
        self,
        report: Dict,
        output_path: str = "report.log",
        mode: str = "w",
        section_header: str = None,
    ):
        """
        Write formatted report to file

        Args:
            report: Report dict from generate_consistency_report()
            output_path: Output file path
            mode: File mode ("w" for overwrite, "a" for append)
            section_header: Optional section header (e.g., "BEFORE RECOVERY")
        """
        formatted = self.format_consistency_report(report, section_header)
        with open(output_path, mode) as f:
            f.write(formatted)


# Convenience functions
def check_shard_consistency(
    language: str, config_path: str = "db/node_config.json"
) -> Dict:
    """Convenience function to check single shard consistency"""
    checker = ConsistencyChecker(config_path)
    return checker.check_shard_consistency(language)


def generate_consistency_report(config_path: str = "db/node_config.json") -> Dict:
    """Convenience function to generate full consistency report"""
    checker = ConsistencyChecker(config_path)
    return checker.generate_consistency_report()


def write_consistency_report(
    report: Dict,
    output_path: str = "report.log",
    mode: str = "w",
    section_header: str = None,
) -> None:
    """Convenience function to write report to file"""
    checker = ConsistencyChecker()
    checker.write_consistency_report(report, output_path, mode, section_header)


def write_test_header(
    output_path: str,
    test_name: str,
    test_params: Dict,
    mode: str = "w",
) -> None:
    """
    Write test metadata header to report file

    Args:
        output_path: Path to report file (e.g., "report.log")
        test_name: Name of the test (e.g., "CRASH RECOVERY TEST")
        test_params: Dict of test parameters
        mode: File mode ("w" for overwrite, "a" for append)
    """
    from datetime import datetime

    lines = []
    lines.append("=" * 70)
    lines.append(f"TEST RUN: {test_name}")
    lines.append("=" * 70)

    # Format timestamp: "December 4, 2025 at 9:30:45 PM"
    timestamp = datetime.now().strftime("%B %d, %Y at %I:%M:%S %p")
    lines.append(f"Timestamp: {timestamp}")

    # Format parameters (sorted alphabetically)
    lines.append("Parameters:")
    for key in sorted(test_params.keys()):
        lines.append(f"  {key}: {test_params[key]}")

    lines.append("")  # Empty line after header

    with open(output_path, mode) as f:
        f.write("\n".join(lines) + "\n")


def write_test_result_summary(
    output_path: str,
    report: Dict,
    duration: float,
    crash_events: list,
    election_failures: Dict[str, int] = None,
    mode: str = "a",
) -> None:
    """
    Write test result summary to report file

    Args:
        output_path: Path to report file
        report: Consistency report dict (after recovery)
        duration: Test duration in seconds
        crash_events: List of crash event dicts with node_id
        election_failures: Optional dict mapping shard -> failure count
        mode: File mode (default "a" for append)
    """
    lines = []
    lines.append("")  # Empty line before summary
    lines.append("=" * 70)
    lines.append("TEST RESULT SUMMARY")
    lines.append("=" * 70)

    # Determine test status
    fully_consistent = report["summary"]["fully_consistent"]
    total_shards = report["summary"]["total_shards"]

    if fully_consistent == total_shards:
        lines.append("Status: PASSED")
        lines.append(f"  All {total_shards} shards are fully consistent after recovery")
    else:
        lines.append("Status: PARTIAL")
        lines.append(f"  {fully_consistent}/{total_shards} shards fully consistent")

    lines.append("")
    lines.append("Crash Details:")

    # Extract node IDs from crash events
    crashed_nodes = []
    for event in crash_events:
        if isinstance(event, dict) and "node_id" in event:
            crashed_nodes.append(event["node_id"])
        elif isinstance(event, str):
            crashed_nodes.append(event)

    crashed_nodes = sorted(crashed_nodes)
    lines.append(f"  Crashed nodes: {crashed_nodes}")

    # Add duration and other details if available
    lines.append(f"  Test duration: {duration:.1f}s")

    # Add election failures if provided
    if election_failures and len(election_failures) > 0:
        lines.append("")
        lines.append("Election Failures Detected:")
        for shard in sorted(election_failures.keys()):
            count = election_failures[shard]
            lines.append(f"  {shard}: {count} failed election attempts")

    lines.append("=" * 70)

    with open(output_path, mode) as f:
        f.write("\n".join(lines) + "\n")
