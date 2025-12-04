import unittest

from wrapper.flexiraft import (
    ReplicaSetTopology,
    RequestVoteResponse,
    VoterInfo,
    LeaderRef,
    Alg2Status,
    ElectionStatus,
    QuorumSpecification,
    QuorumMode,
    StaticQuorumOption,
    GroupRequirement,
    flexiraft_leader_election,
)
from wrapper.flexiraft_helper import getPotentialNextLeaders


def rv(voter_id: str, group: str, granted: bool = True, history: dict | None = None) -> RequestVoteResponse:
    return RequestVoteResponse(voter=VoterInfo(voter_id, group), term=1, vote_granted=granted, voting_history=history or {})


class TestFlexiRaftIntegration(unittest.TestCase):
    def setUp(self):
        # Topology with two groups; G1 size 3, G2 size 2
        self.topo = ReplicaSetTopology(groups={"G1": {"A", "B", "C"}, "G2": {"D", "E"}})

    def test_integration_potential_next_leader_immediate_win(self):
        # Setup: last_known_leader exists at small term so Alg.1 will invoke Alg.2 path
        last = LeaderRef(node_id="L", group="G1", term=1)

        # Historical votes at term 10: candidate 'X' has a majority in G1
        h1 = {10: "X"}
        h2 = {10: "X"}
        # Current responses include granted votes in terminal group G1 so early-win check should succeed
        responses = [rv("r1", "G1", granted=True, history=h1), rv("r2", "G1", granted=True, history=h2), rv("r3", "G1", granted=False, history={}), rv("d", "G2", granted=True, history={}), rv("e", "G2", granted=True, history={})]

        alg2 = getPotentialNextLeaders(responses, self.topo)

        spec = QuorumSpecification(mode=QuorumMode.DYNAMIC, topology=self.topo)

        result = flexiraft_leader_election(
            current_term=5,
            responses=responses,
            quorum_spec=spec,
            last_known_leader=last,
            get_potential_next_leaders=alg2,
        )

        # Because Alg.2 will detect potential leaders for G1 at term 10 and
        # current responses have majority in G1, the early-win check should return WON.
        self.assertEqual(result.status, ElectionStatus.WON)

    def test_integration_waiting_path(self):
        # Setup where votes are still outstanding in a candidate group -> Alg.2 returns WAITING
        last = LeaderRef(node_id="L", group="G1", term=2)

        # G1: one granted, one responded false; remaining could supply majority -> alg2 should WAIT
        responses = [rv("r1", "G1", granted=True, history={}), rv("r2", "G1", granted=False, history={}), rv("d", "G2", granted=False, history={})]

        alg2 = getPotentialNextLeaders(responses, self.topo)
        spec = QuorumSpecification(mode=QuorumMode.DYNAMIC, topology=self.topo)

        result = flexiraft_leader_election(
            current_term=10,
            responses=responses,
            quorum_spec=spec,
            last_known_leader=last,
            get_potential_next_leaders=alg2,
        )

        # Because Alg.2 will indicate WAITING_FOR_MORE_VOTES, flexiraft should return UNDECIDED
        self.assertEqual(result.status, ElectionStatus.UNDECIDED)

    def test_integration_multi_term_progression(self):
        # Simulate Alg.2 detecting a potential leader in G1 at term 5, but current
        # responses do NOT provide majority in G1, and no further higher-term
        # histories exist => final outcome should be UNDECIDED (still possible).
        last = LeaderRef(node_id="L", group="G1", term=1)

        # Two voters have historical votes at term 5 for candidate 'X' (a majority in G1)
        h1 = {5: "X"}
        h2 = {5: "X"}
        # Current responses lack a current majority in G1 but not all voters have responded
        # (we intentionally omit one G1 voter to model a non-response)
        responses = [
            rv("r1", "G1", granted=True, history=h1),
            rv("r2", "G1", granted=False, history=h2),
            rv("d", "G2", granted=True, history={}),
        ]

        alg2 = getPotentialNextLeaders(responses, self.topo)
        spec = QuorumSpecification(mode=QuorumMode.DYNAMIC, topology=self.topo)

        result = flexiraft_leader_election(
            current_term=3,
            responses=responses,
            quorum_spec=spec,
            last_known_leader=last,
            get_potential_next_leaders=alg2,
        )

        # The algorithm will detect potential leaders at term 5, but since the
        # current replies don't have majority in G1 and there are no higher
        # terms to advance to, the outcome should be UNDECIDED (not enough info
        # to declare LOSS and still possible to reach majority).
        self.assertEqual(result.status, ElectionStatus.UNDECIDED)


if __name__ == "__main__":
    unittest.main()
