import unittest
from wrapper.flexiraft import FlexiRaftCluster, QuorumConfig


class TestFlexiRaft(unittest.TestCase):
    def setUp(self):
        self.members = ["A", "B", "C", "D", "E"]
        # Election quorum and commit quorum both include C to guarantee intersection in this test
        self.qc = QuorumConfig(election={"A", "C", "D"}, commit={"A", "C"})
        self.cluster = FlexiRaftCluster(self.members, self.qc)

    def test_election_succeeds_with_quorum(self):
        ok = self.cluster.elect_leader("A")
        self.assertTrue(ok)
        self.assertEqual(self.cluster.leader_id, "A")

    def test_commit_majority_in_commit_set(self):
        self.cluster.elect_leader("A")
        committed = self.cluster.leader_append_and_commit({"k": 1})
        self.assertTrue(committed)
        # committed entries reflect intersection across commit quorum members
        self.assertEqual(len(self.cluster.committed_entries()), 1)

    def test_reconfigure_quorums_valid(self):
        # Update to a different but intersecting quorum (still includes C)
        self.cluster.reconfigure_quorums({"B", "C", "E"}, {"C", "E"})
        ok = self.cluster.elect_leader("B")
        self.assertTrue(ok)
        self.assertEqual(self.cluster.leader_id, "B")
        committed = self.cluster.leader_append_and_commit("x")
        self.assertTrue(committed)

    def test_invalid_quorum_outside_members(self):
        with self.assertRaises(ValueError):
            self.cluster.reconfigure_quorums({"A", "Z"}, {"A", "C"})

    def test_commit_requires_quorum(self):
        self.cluster.elect_leader("A")
        # Make commit quorum size 3 to require more acks
        self.cluster.reconfigure_quorums({"A", "C", "D"}, {"A", "C", "D"})
        committed = self.cluster.leader_append_and_commit("e1")
        self.assertTrue(committed)  # leader + two followers reach majority of 3


if __name__ == "__main__":
    unittest.main()
