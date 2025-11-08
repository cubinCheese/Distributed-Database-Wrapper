import unittest
from wrapper.replication import Replicator

class TestReplication(unittest.TestCase):
    def test_replicate_empty(self):
        r = Replicator([])
        self.assertEqual(r.replicate({}), {})

    def test_replicate_payload(self):
        r = Replicator(["a"])
        res = r.replicate({"k": "v"})
        self.assertEqual(res, {"a": True})

if __name__ == "__main__":
    unittest.main()
