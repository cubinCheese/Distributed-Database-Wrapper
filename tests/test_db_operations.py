import unittest
from db.db_operations import DBOperations

class TestDBOps(unittest.TestCase):
    def test_mem_crud(self):
        db = DBOperations(use_sqlite=False)
        db.connect()
        db.insert("t", {"k":"1","v":"one"})
        self.assertEqual(db.find("t", {"k":"1"})[0]["v"], "one")
        db.update("t", {"k":"1"}, {"v":"uno"})
        self.assertEqual(db.find("t", {"k":"1"})[0]["v"], "uno")
        db.delete("t", {"k":"1"})
        self.assertEqual(db.find("t", {"k":"1"}), [])
        db.close()

if __name__ == "__main__":
    unittest.main()
