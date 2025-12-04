"""Query Interface
Provides read/write interface with automatic leader routing.
Uses FlexiRaft coordinator for leader discovery and quorum writes.
"""

import sys
import os
from typing import Dict, List, Optional, Any, Tuple
from concurrent.futures import ThreadPoolExecutor
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.flexiraft_coordinator import FlexiRaftCoordinator
from db.state_manager import StateManager
from db.raft_log_manager import RaftLogManager
from db.db_schema import DatabaseSchema
from db.election_manager import ElectionManager


class QueryInterface:
    """High-level interface for querying and writing to the distributed database"""

    def __init__(self, config_path: str = "db/node_config.json"):
        """
        Initialize query interface

        Args:
            config_path: Path to node configuration
        """
        self.config_path = config_path
        self.coordinator = FlexiRaftCoordinator(config_path)
        self.election_manager = ElectionManager(config_path)

    def insert_novel(
        self, title: str, language: str, author: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Insert a novel into the distributed database

        Args:
            title: Novel title
            language: Language code (zh, ja, ko, ms, fil, id, km, th)
            author: Optional author name

        Returns: (success, message)
        """
        # Validate language
        if language not in self.coordinator.get_all_languages():
            return False, f"Invalid language: {language}"

        # Get leader for this language
        leader_id = self.coordinator.get_leader_for_language(language)
        if not leader_id:
            return False, f"No leader elected for language: {language}"

        # Prepare data
        data = {"title": title, "language": language}

        # Prepare SQL for raft log
        sql = "INSERT INTO novels (title, original_language) VALUES (?, ?)"
        params = [title, language]

        # Get leader path
        leader_path = self.coordinator.get_shard_path(leader_id, language)

        # Get leader's current term
        term = StateManager.get_term(leader_path)

        # Collect votes from replicas (including leader)
        replicas = self.coordinator.get_replica_nodes(language)
        votes = 0
        required_votes = 2  # 2/4 quorum

        responses = []
        new_index = None

        for replica_id in replicas:
            try:
                replica_path = self.coordinator.get_shard_path(replica_id, language)

                # Append to raft log
                index = RaftLogManager.append_entry(
                    replica_path, term, "INSERT", sql, params
                )
                if new_index is None:
                    new_index = index

                # If this is the leader, also write to database immediately
                if replica_id == leader_id:
                    db_path = DatabaseSchema.get_db_path(replica_path)
                    DatabaseSchema.insert_novel(db_path, title, language)

                votes += 1
                responses.append(f"{replica_id}: ✓")

            except Exception as e:
                responses.append(f"{replica_id}: ✗ ({str(e)[:30]})")

        # Check if quorum achieved
        if votes >= required_votes:
            # Update commit indices for all replicas that voted
            for replica_id in replicas:
                try:
                    replica_path = self.coordinator.get_shard_path(replica_id, language)
                    if replica_id == leader_id:
                        StateManager.update_commit_index_sync(replica_path, new_index)
                    else:
                        StateManager.update_commit_index_async(replica_path, new_index)
                except Exception:
                    pass

            return (
                True,
                f"Success: {votes}/{len(replicas)} votes. {', '.join(responses)}",
            )
        else:
            return (
                False,
                f"Quorum failed: {votes}/{len(replicas)} votes ({required_votes} required). {', '.join(responses)}",
            )

    def query_novels(
        self,
        language: Optional[str] = None,
        title: Optional[str] = None,
        author: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Query novels from the distributed database

        Args:
            language: Optional language filter
            title: Optional title filter (partial match)
            author: Optional author filter (partial match)
            limit: Maximum number of results

        Returns: List of novel records
        """
        results = []

        # Determine which languages to query
        if language:
            if language not in self.coordinator.get_all_languages():
                return []
            languages = [language]
        else:
            languages = self.coordinator.get_all_languages()

        # Query each language's leader
        for lang in languages:
            leader_id = self.coordinator.get_leader_for_language(lang)
            if not leader_id:
                continue

            leader_path = self.coordinator.get_shard_path(leader_id, lang)
            db_path = DatabaseSchema.get_db_path(leader_path)

            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()

                # Build query
                query = "SELECT title, original_language FROM novels WHERE 1=1"
                params = []

                if title:
                    query += " AND title LIKE ?"
                    params.append(f"%{title}%")

                query += f" LIMIT {limit}"

                cursor.execute(query, params)
                rows = cursor.fetchall()
                conn.close()

                # Convert to dicts
                for row in rows:
                    results.append({"title": row[0], "language": row[1]})

            except Exception as e:
                print(f"Query error for {lang}: {e}")
                continue

        return results[:limit]

    def count_novels(self, language: Optional[str] = None) -> Dict[str, int]:
        """
        Count novels per language

        Args:
            language: Optional language filter

        Returns: Dict of {language: count}
        """
        counts = {}

        # Determine which languages to query
        if language:
            if language not in self.coordinator.get_all_languages():
                return {}
            languages = [language]
        else:
            languages = self.coordinator.get_all_languages()

        # Count for each language
        for lang in languages:
            leader_id = self.coordinator.get_leader_for_language(lang)
            if not leader_id:
                counts[lang] = 0
                continue

            leader_path = self.coordinator.get_shard_path(leader_id, lang)
            db_path = DatabaseSchema.get_db_path(leader_path)

            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM novels")
                count = cursor.fetchone()[0]
                conn.close()
                counts[lang] = count
            except Exception as e:
                print(f"Count error for {lang}: {e}")
                counts[lang] = 0

        return counts

    def get_system_status(self) -> Dict[str, Any]:
        """
        Get overall system status

        Returns: Dict with system information
        """
        languages = self.coordinator.get_all_languages()
        status = {
            "languages": [],
            "total_novels": 0,
            "total_replicas": 0,
        }

        for lang in languages:
            leader_id = self.coordinator.get_leader_for_language(lang)
            replicas = self.coordinator.get_replica_nodes(lang)

            if leader_id:
                leader_path = self.coordinator.get_shard_path(leader_id, lang)
                term = StateManager.get_term(leader_path)
                commit_index = StateManager.get_commit_index(leader_path)
            else:
                term = 0
                commit_index = 0

            # Count novels
            count = self.count_novels(lang).get(lang, 0)

            status["languages"].append(
                {
                    "language": lang,
                    "leader": leader_id or "none",
                    "term": term,
                    "commit_index": commit_index,
                    "replica_count": len(replicas),
                    "novel_count": count,
                }
            )

            status["total_novels"] += count
            status["total_replicas"] += len(replicas)

        return status


# Convenience functions for command-line usage
def insert(title: str, language: str, author: Optional[str] = None) -> None:
    """Insert a novel"""
    qi = QueryInterface()
    success, message = qi.insert_novel(title, language, author)
    if success:
        print(f"✓ {message}")
    else:
        print(f"✗ {message}")


def query(
    language: Optional[str] = None,
    title: Optional[str] = None,
    author: Optional[str] = None,
    limit: int = 100,
) -> None:
    """Query novels"""
    qi = QueryInterface()
    results = qi.query_novels(language, title, author, limit)

    if not results:
        print("No results found")
        return

    print(f"\nFound {len(results)} novels:")
    print("-" * 80)
    for novel in results:
        print(f"  [{novel['language']}] {novel['title']}")


def count(language: Optional[str] = None) -> None:
    """Count novels"""
    qi = QueryInterface()
    counts = qi.count_novels(language)

    print("\nNovel counts by language:")
    print("-" * 40)
    for lang, count in sorted(counts.items()):
        print(f"  {lang}: {count:,}")
    print(f"\nTotal: {sum(counts.values()):,}")


def status() -> None:
    """Show system status"""
    qi = QueryInterface()
    status_data = qi.get_system_status()

    print("\n" + "=" * 80)
    print("DISTRIBUTED DATABASE STATUS")
    print("=" * 80)

    for lang_status in status_data["languages"]:
        print(f"\n{lang_status['language'].upper()}:")
        print(f"  Leader:        {lang_status['leader']} (term {lang_status['term']})")
        print(f"  Commit Index:  {lang_status['commit_index']}")
        print(f"  Replicas:      {lang_status['replica_count']}")
        print(f"  Novels:        {lang_status['novel_count']:,}")

    print("\n" + "=" * 80)
    print(f"Total Replicas: {status_data['total_replicas']}")
    print(f"Total Novels:   {status_data['total_novels']:,}")
    print("=" * 80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Query interface for distributed database"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Insert command
    insert_parser = subparsers.add_parser("insert", help="Insert a novel")
    insert_parser.add_argument("title", help="Novel title")
    insert_parser.add_argument("language", help="Language code")
    insert_parser.add_argument("--author", help="Author name")

    # Query command
    query_parser = subparsers.add_parser("query", help="Query novels")
    query_parser.add_argument("--language", help="Language filter")
    query_parser.add_argument("--title", help="Title filter (partial match)")
    query_parser.add_argument("--author", help="Author filter (partial match)")
    query_parser.add_argument("--limit", type=int, default=100, help="Result limit")

    # Count command
    count_parser = subparsers.add_parser("count", help="Count novels")
    count_parser.add_argument("--language", help="Language filter")

    # Status command
    status_parser = subparsers.add_parser("status", help="Show system status")

    args = parser.parse_args()

    if args.command == "insert":
        insert(args.title, args.language, args.author)
    elif args.command == "query":
        query(args.language, args.title, args.author, args.limit)
    elif args.command == "count":
        count(args.language)
    elif args.command == "status":
        status()
    else:
        parser.print_help()
