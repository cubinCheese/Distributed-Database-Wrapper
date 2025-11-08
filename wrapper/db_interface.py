"""Interface for interacting with the underlying database.

Provides a small adapter interface that concrete DB implementations (e.g.,
SQLite or MongoDB) can implement. The wrapper code should depend on this
interface so DB implementations are swappable for testing.
"""
from typing import Protocol, Any, Dict, Optional, List

class DBInterface(Protocol):
    def connect(self) -> None: ...
    def close(self) -> None: ...
    def insert(self, table: str, document: Dict[str, Any]) -> Any: ...
    def find(self, table: str, query: Dict[str, Any]) -> List[Dict[str, Any]]: ...
    def update(self, table: str, query: Dict[str, Any], update: Dict[str, Any]) -> int: ...
    def delete(self, table: str, query: Dict[str, Any]) -> int: ...


class SimpleInMemoryDB:
    """A tiny in-memory DB implementation for tests and local runs."""

    def __init__(self):
        self.store: Dict[str, List[Dict[str, Any]]] = {}

    def connect(self) -> None:
        # nothing to do for in-memory
        pass

    def close(self) -> None:
        self.store.clear()

    def insert(self, table: str, document: Dict[str, Any]) -> Any:
        self.store.setdefault(table, []).append(document.copy())
        return True

    def find(self, table: str, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        results = []
        for doc in self.store.get(table, []):
            if all(doc.get(k) == v for k, v in query.items()):
                results.append(doc.copy())
        return results

    def update(self, table: str, query: Dict[str, Any], update: Dict[str, Any]) -> int:
        matched = 0
        for doc in self.store.get(table, []):
            if all(doc.get(k) == v for k, v in query.items()):
                doc.update(update)
                matched += 1
        return matched

    def delete(self, table: str, query: Dict[str, Any]) -> int:
        before = len(self.store.get(table, []))
        self.store[table] = [d for d in self.store.get(table, []) if not all(d.get(k) == v for k, v in query.items())]
        return before - len(self.store.get(table, []))
