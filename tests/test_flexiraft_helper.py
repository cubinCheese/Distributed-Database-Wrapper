# tests/test_flexiraft_helper.py
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from Algorithm1 import (
    flexiraft_leader_election,
    QuorumMode, QuorumSpecification, ReplicaSetTopology,
    StaticQuorumOption, GroupRequirement,
    VoterInfo, RequestVoteResponse, LeaderRef,
    ElectionStatus, Alg2Status,
)

import unittest

from wrapper.flexiraft import (
    ReplicaSetTopology,
    RequestVoteResponse,
    VoterInfo,
    LeaderRef,
    Alg2Status,
)
from wrapper.flexiraft_helper import make_alg2_fn, getPotentialNextLeaders


def rv(voter_id: str, group: str, granted: bool = True, history: dict | None = None) -> RequestVoteResponse:
    return RequestVoteResponse(voter=VoterInfo(voter_id, group), term=1, vote_granted=granted, voting_history=history or {})


class TestFlexiRaftHelperAlg2(unittest.TestCase):
    def setUp(self):
        # Topology: G1 has A,B,C (size 3), G2 has D,E (size 2)
        self.topo = ReplicaSetTopology(groups={"G1": {"A", "B", "C"}, "G2": {"D", "E"}})

    def test_waiting_for_more_votes(self):
        # For G1 (size 3): need 2. Only 1 granted and 1 responded -> remaining could supply majority -> WAITING
        responses = [rv("A", "G1", granted=True), rv("B", "G1", granted=False)]
        alg2 = make_alg2_fn(responses, self.topo)
        status, next_term, leaders = alg2(1, {"G1"})
        self.assertEqual(status, Alg2Status.WAITING_FOR_MORE_VOTES)

    def test_all_intermediate_terms_defunct(self):
        # Provide enough current votes so no WAITING, but no historical votes -> defunct
        responses = [rv("A", "G1", granted=True), rv("B", "G1", granted=True), rv("D", "G2", granted=True), rv("E", "G2", granted=True)]
        alg2 = getPotentialNextLeaders(responses, self.topo)
        status, next_term, leaders = alg2(5, {"G1", "G2"})
        self.assertEqual(status, Alg2Status.ALL_INTERMEDIATE_TERMS_DEFUNCT)
        self.assertEqual(next_term, -1)
        self.assertEqual(leaders, [])

    def test_potential_next_leaders_detected(self):
        # Create historical voting history at term 10 where in G1 candidate 'A' got 2 votes (majority)
        history1 = {10: "A"}
        history2 = {10: "A"}
        history3 = {9: "B"}
        responses = [rv("X1", "G1", granted=True, history=history1), rv("X2", "G1", granted=True, history=history2), rv("X3", "G1", granted=False, history=history3)]
        alg2 = getPotentialNextLeaders(responses, self.topo)
        status, next_term, leaders = alg2(1, {"G1"})
        self.assertEqual(status, Alg2Status.POTENTIAL_NEXT_LEADERS_DETECTED)
        self.assertEqual(next_term, 10)
        # Leaders should contain a LeaderRef for candidate 'A' in group 'G1'
        self.assertTrue(any(isinstance(l, LeaderRef) and l.node_id == "A" and l.group == "G1" for l in leaders))


if __name__ == "__main__":
    unittest.main()
