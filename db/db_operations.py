"""Basic DB operations implementation.

Provides a thin adapter around the SimpleInMemoryDB (used for tests) and a
small SQLite-backed adapter (optional). Keep this file minimal and testable.
"""
from typing import Any, Dict, List
import sqlite3
from . import db_config
from wrapper.db_interface import SimpleInMemoryDB


class DBOperations:
    def __init__(self, use_sqlite: bool = False, sqlite_path: str = ":memory:"):
        self.use_sqlite = use_sqlite
        self.sqlite_path = sqlite_path
        self.mem = SimpleInMemoryDB()
        self.conn = None

    def connect(self):
        if self.use_sqlite:
            self.conn = sqlite3.connect(self.sqlite_path)
        else:
            self.mem.connect()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
        self.mem.close()

    # CRUD operations against the chosen backend
    def insert(self, table: str, document: Dict[str, Any]) -> Any:
        if self.use_sqlite and self.conn:
            # Very small helper: create a table with key and value columns for demo
            cur = self.conn.cursor()
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table} (k TEXT PRIMARY KEY, v TEXT)")
            cur.execute("REPLACE INTO %s (k, v) VALUES (?, ?)" % table, (document.get("k"), str(document.get("v"))))
            self.conn.commit()
            return True
        return self.mem.insert(table, document)

    def find(self, table: str, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        if self.use_sqlite and self.conn:
            cur = self.conn.cursor()
            sql = f"SELECT k, v FROM {table} WHERE k = ?"
            cur.execute(sql, (query.get("k"),))
            row = cur.fetchone()
            return [{"k": row[0], "v": row[1]}] if row else []
        return self.mem.find(table, query)

    def update(self, table: str, query: Dict[str, Any], update: Dict[str, Any]) -> int:
        if self.use_sqlite and self.conn:
            cur = self.conn.cursor()
            cur.execute(f"UPDATE {table} SET v = ? WHERE k = ?", (str(update.get("v")), query.get("k")))
            self.conn.commit()
            return cur.rowcount
        return self.mem.update(table, query, update)

    def delete(self, table: str, query: Dict[str, Any]) -> int:
        if self.use_sqlite and self.conn:
            cur = self.conn.cursor()
            cur.execute(f"DELETE FROM {table} WHERE k = ?", (query.get("k"),))
            self.conn.commit()
            return cur.rowcount
        return self.mem.delete(table, query)
