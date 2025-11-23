import unittest

from wrapper.flexiraft import (
    ReplicaSetTopology,
    QuorumSpecification,
    QuorumMode,
    StaticQuorumOption,
    GroupRequirement,
    RequestVoteResponse,
    VoterInfo,
    LeaderRef,
    Alg2Status,
    ElectionStatus,
    flexiraft_leader_election,
)


def rv(voter_id: str, group: str, term: int = 1, granted: bool = True) -> RequestVoteResponse:
    return RequestVoteResponse(voter=VoterInfo(voter_id, group), term=term, vote_granted=granted, voting_history={})


class TestFlexiRaftElection(unittest.TestCase):
    def setUp(self):
        # Two groups: G1 with 3 nodes, G2 with 2 nodes
        self.topo = ReplicaSetTopology(groups={"G1": {"A", "B", "C"}, "G2": {"D", "E"}})

    def test_static_quorum_satisfied(self):
        # Static quorum: majority in G1 is sufficient
        opt = StaticQuorumOption(requirements=(GroupRequirement(k_of_groups=1, groups=("G1",)),))
        spec = QuorumSpecification(mode=QuorumMode.STATIC, topology=self.topo, static_options=(opt,))

        responses = [rv("A", "G1", granted=True), rv("B", "G1", granted=True)]

        res = flexiraft_leader_election(
            current_term=1,
            responses=responses,
            quorum_spec=spec,
            last_known_leader=None,
            get_potential_next_leaders=lambda t, g: (Alg2Status.WAITING_FOR_MORE_VOTES, t, []),
        )

        self.assertEqual(res.status, ElectionStatus.WON)

    def test_dynamic_pessimistic_quorum(self):
        # Dynamic mode; require majority in every group (pessimistic quorum) -> all groups must have majorities
        spec = QuorumSpecification(mode=QuorumMode.DYNAMIC, topology=self.topo)

        # G1 majority: A,B ; G2 majority (size 2): D,E
        responses = [rv("A", "G1", granted=True), rv("B", "G1", granted=True), rv("D", "G2", granted=True), rv("E", "G2", granted=True)]

        res = flexiraft_leader_election(
            current_term=1,
            responses=responses,
            quorum_spec=spec,
            last_known_leader=None,
            get_potential_next_leaders=lambda t, g: (Alg2Status.WAITING_FOR_MORE_VOTES, t, []),
        )

        self.assertEqual(res.status, ElectionStatus.WON)

    def test_majority_in_last_known_leader_group(self):
        spec = QuorumSpecification(mode=QuorumMode.DYNAMIC, topology=self.topo)
        last = LeaderRef(node_id="X", group="G1", term=1)

        # current_term == last.term + 1 -> evaluate majority in last-known-leader group
        responses = [rv("A", "G1", granted=True), rv("B", "G1", granted=True)]

        res = flexiraft_leader_election(
            current_term=2,
            responses=responses,
            quorum_spec=spec,
            last_known_leader=last,
            get_potential_next_leaders=lambda t, g: (Alg2Status.WAITING_FOR_MORE_VOTES, t, []),
        )

        self.assertEqual(res.status, ElectionStatus.WON)

    def test_waiting_for_more_votes_from_alg2(self):
        spec = QuorumSpecification(mode=QuorumMode.DYNAMIC, topology=self.topo)
        last = LeaderRef(node_id="L", group="G1", term=5)

        # current_term not equal to last.term+1 so it will invoke Alg.2 path; stub returns WAITING_FOR_MORE_VOTES
        responses = [rv("A", "G1", granted=True)]

        def alg2_stub(term_it: int, groups: set):
            return (Alg2Status.WAITING_FOR_MORE_VOTES, term_it + 1, [])

        res = flexiraft_leader_election(
            current_term=10,
            responses=responses,
            quorum_spec=spec,
            last_known_leader=last,
            get_potential_next_leaders=alg2_stub,
        )

        self.assertEqual(res.status, ElectionStatus.UNDECIDED)


if __name__ == "__main__":
    unittest.main()
