"""Database Schema Module
Manages SQLite database schema and operations for the distributed database.
Includes functions to apply raft log entries to databases.
"""

import sqlite3
import os
from typing import Dict, List, Tuple, Optional


class DatabaseSchema:
    """Manages SQLite schema and operations"""

    # Schema definition
    NOVELS_TABLE_SCHEMA = """
        CREATE TABLE IF NOT EXISTS novels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            original_language TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """

    # Index for faster queries by language
    NOVELS_INDEX_SCHEMA = """
        CREATE INDEX IF NOT EXISTS idx_language 
        ON novels(original_language)
    """

    @staticmethod
    def get_db_path(shard_path: str) -> str:
        """Get path to novel.db for a shard"""
        return os.path.join(shard_path, "novel.db")

    @staticmethod
    def initialize_database(shard_path: str) -> None:
        """
        Create database with schema if it doesn't exist

        Args:
            shard_path: Path to shard directory (e.g., "db/node_0/zh")
        """
        os.makedirs(shard_path, exist_ok=True)
        db_path = DatabaseSchema.get_db_path(shard_path)

        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            cursor.execute(DatabaseSchema.NOVELS_TABLE_SCHEMA)
            cursor.execute(DatabaseSchema.NOVELS_INDEX_SCHEMA)
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def insert_novel(db_path: str, title: str, language: str) -> int:
        """
        Insert a single novel record

        Args:
            db_path: Path to SQLite database file
            title: Novel title
            language: Original language code

        Returns: Row ID of inserted record
        """
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO novels (title, original_language) VALUES (?, ?)",
                (title, language),
            )
            conn.commit()
            return cursor.lastrowid if cursor.lastrowid else 0
        finally:
            conn.close()

    @staticmethod
    def bulk_insert_novels(db_path: str, novels: List[Tuple[str, str]]) -> int:
        """
        Efficient bulk insert of novels

        Args:
            db_path: Path to SQLite database file
            novels: List of (title, language) tuples

        Returns: Count of records inserted
        """
        if not novels:
            return 0

        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            cursor.executemany(
                "INSERT INTO novels (title, original_language) VALUES (?, ?)", novels
            )
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()

    @staticmethod
    def query_novels(
        db_path: str, language: Optional[str] = None, limit: Optional[int] = None
    ) -> List[Dict]:
        """
        Query novels from database

        Args:
            db_path: Path to SQLite database file
            language: Optional language filter
            limit: Optional result limit

        Returns: List of novel dictionaries
        """
        if not os.path.exists(db_path):
            return []

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Enable dict access

        try:
            cursor = conn.cursor()

            if language:
                query = "SELECT * FROM novels WHERE original_language = ?"
                params = (language,)
            else:
                query = "SELECT * FROM novels"
                params = ()

            if limit:
                query += f" LIMIT {limit}"

            cursor.execute(query, params)
            rows = cursor.fetchall()

            # Convert to list of dicts
            return [dict(row) for row in rows]
        finally:
            conn.close()

    @staticmethod
    def count_novels(db_path: str, language: Optional[str] = None) -> int:
        """
        Count novels in database

        Args:
            db_path: Path to SQLite database file
            language: Optional language filter

        Returns: Count of novels
        """
        if not os.path.exists(db_path):
            return 0

        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()

            if language:
                cursor.execute(
                    "SELECT COUNT(*) FROM novels WHERE original_language = ?",
                    (language,),
                )
            else:
                cursor.execute("SELECT COUNT(*) FROM novels")

            return cursor.fetchone()[0]
        finally:
            conn.close()

    @staticmethod
    def apply_raft_entry_to_db(db_path: str, entry: Dict) -> bool:
        """
        Apply a single raft log entry to SQLite database

        Entry format: {
            "term": int,
            "index": int,
            "operation": str,
            "sql": str,
            "params": list
        }

        Args:
            db_path: Path to SQLite database file
            entry: Raft log entry

        Returns: True if successful
        """
        if not os.path.exists(db_path):
            return False

        operation = entry.get("operation", "")
        sql = entry.get("sql", "")
        params = entry.get("params", [])

        if not sql:
            return False

        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()

            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error applying raft entry: {e}")
            return False
        finally:
            conn.close()

    @staticmethod
    def bulk_apply_raft_entries(db_path: str, entries: List[Dict]) -> int:
        """
        Apply multiple raft entries in a transaction (more efficient)

        Args:
            db_path: Path to SQLite database file
            entries: List of raft log entries

        Returns: Count of entries successfully applied
        """
        if not os.path.exists(db_path) or not entries:
            return 0

        conn = sqlite3.connect(db_path)
        applied_count = 0

        try:
            cursor = conn.cursor()

            for entry in entries:
                sql = entry.get("sql", "")
                params = entry.get("params", [])

                if not sql:
                    continue

                try:
                    if params:
                        cursor.execute(sql, params)
                    else:
                        cursor.execute(sql)
                    applied_count += 1
                except sqlite3.Error as e:
                    print(f"Error applying entry {entry.get('index', '?')}: {e}")
                    # Continue with other entries

            conn.commit()
        except Exception as e:
            print(f"Transaction error: {e}")
            conn.rollback()
        finally:
            conn.close()

        return applied_count

    @staticmethod
    def create_novels_from_data(
        db_path: str, novels_data: List[Dict]
    ) -> Tuple[int, List[str]]:
        """
        Insert novels from data dictionaries

        Args:
            db_path: Path to SQLite database file
            novels_data: List of novel dicts with 'title' and 'original_language'

        Returns: (count_inserted, list_of_sql_statements)
        """
        if not novels_data:
            return 0, []

        # Convert to tuples for bulk insert
        novels = [(n["title"], n["original_language"]) for n in novels_data]

        # Generate SQL statements for raft log
        sql_statements = []
        for title, lang in novels:
            sql = f"INSERT INTO novels (title, original_language) VALUES ('{title}', '{lang}')"
            sql_statements.append(sql)

        # Perform bulk insert
        count = DatabaseSchema.bulk_insert_novels(db_path, novels)

        return count, sql_statements

    @staticmethod
    def generate_insert_sql(title: str, language: str) -> Tuple[str, List]:
        """
        Generate SQL statement and params for inserting a novel
        Properly escapes parameters using placeholders

        Args:
            title: Novel title
            language: Language code

        Returns: (sql_statement, params_list)
        """
        sql = "INSERT INTO novels (title, original_language) VALUES (?, ?)"
        params = [title, language]
        return sql, params

    @staticmethod
    def clear_database(db_path: str) -> int:
        """
        Clear all novels from database (for testing/reset)

        Returns: Number of rows deleted
        """
        if not os.path.exists(db_path):
            return 0

        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM novels")
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()


# Convenience functions
def initialize_database(shard_path: str) -> None:
    """Legacy compatibility function"""
    DatabaseSchema.initialize_database(shard_path)


def query_novels(db_path: str, language: Optional[str] = None) -> List[Dict]:
    """Legacy compatibility function"""
    return DatabaseSchema.query_novels(db_path, language)
