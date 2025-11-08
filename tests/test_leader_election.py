import unittest
from wrapper.leader_election import LeaderElection

class TestLeaderElection(unittest.TestCase):
    def test_empty(self):
        le = LeaderElection()
        self.assertIsNone(le.elect_leader([]))

    def test_order(self):
        le = LeaderElection()
        self.assertEqual(le.elect_leader(["b","a","c"]), "a")

if __name__ == "__main__":
    unittest.main()
