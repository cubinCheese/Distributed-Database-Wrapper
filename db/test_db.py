"""Unit tests for DB operations (very small smoke tests).

Run with: python -m unittest db.test_db
"""
import unittest
from db.db_operations import DBOperations


class TestDBOperations(unittest.TestCase):
    def test_in_memory_insert_find(self):
        db = DBOperations(use_sqlite=False)
        db.connect()
        db.insert("kv", {"k": "a", "v": "1"})
        res = db.find("kv", {"k": "a"})
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["v"], "1")
        db.close()

    def test_sqlite_insert_find(self):
        db = DBOperations(use_sqlite=True)
        db.connect()
        db.insert("kv", {"k": "x", "v": "10"})
        res = db.find("kv", {"k": "x"})
        self.assertEqual(len(res), 1)
        db.close()


if __name__ == "__main__":
    unittest.main()
