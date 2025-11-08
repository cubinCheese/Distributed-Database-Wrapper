"""Basic tests for the wrapper package.

Run with: python -m unittest discover -s tests
"""
import unittest
from wrapper.leader_election import LeaderElection
from wrapper.replication import Replicator
from wrapper.sharding import Sharder


class TestWrapper(unittest.TestCase):
    def test_leader_election(self):
        le = LeaderElection()
        leader = le.elect_leader(["n2", "n1"])
        self.assertEqual(leader, "n1")

    def test_replicator(self):
        r = Replicator(["r1", "r2"])
        out = r.replicate({"x": 1})
        self.assertTrue(all(out.values()))

    def test_sharder(self):
        s = Sharder(3)
        idx = s.shard_for_key("key1")
        self.assertIn(idx, [0,1,2])


if __name__ == "__main__":
    unittest.main()
