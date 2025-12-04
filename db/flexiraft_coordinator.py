"""FlexiRaft Coordinator
Bridges the FlexiRaft consensus engine with the distributed database.
Builds topologies, simulates votes, and manages election results.
"""

import json
import os
import sys
from typing import Dict, List, Optional, Set

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from wrapper.flexiraft import (
    ReplicaSetTopology,
    QuorumSpecification,
    QuorumMode,
    RequestVoteResponse,
    VoterInfo,
    LeaderRef,
    ElectionResult,
    flexiraft_leader_election,
)
from wrapper.flexiraft_helper import getPotentialNextLeaders
from db.state_manager import StateManager


class FlexiRaftCoordinator:
    """Coordinates FlexiRaft operations for the distributed database"""

    def __init__(self, config_path: str = "db/node_config.json"):
        """
        Initialize coordinator with configuration

        Args:
            config_path: Path to node configuration JSON
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.base_path = os.path.dirname(config_path)
        self.topology = self.build_topology_from_config()
        self.quorum_spec = self.create_quorum_spec()

    def _load_config(self) -> Dict:
        """Load node configuration"""
        with open(self.config_path, "r") as f:
            return json.load(f)

    def build_topology_from_config(self) -> ReplicaSetTopology:
        """
        Build ReplicaSetTopology from node_config.json

        Returns: ReplicaSetTopology with all 8 language groups
        """
        # Build groups dictionary: {language: set(node_ids)}
        groups = {}

        for language, replicas in self.config.get("replicas", {}).items():
            groups[language] = set(replicas)

        return ReplicaSetTopology(groups=groups)

    def create_quorum_spec(self) -> QuorumSpecification:
        """
        Create QuorumSpecification with DYNAMIC mode
        This enables flexible quorum (2/4 requirement)

        Returns: QuorumSpecification for dynamic mode
        """
        return QuorumSpecification(mode=QuorumMode.DYNAMIC, topology=self.topology)

    def get_shard_path(self, node_id: str, language: str) -> str:
        """Get path to a specific shard"""
        return os.path.join(self.base_path, node_id, language)

    def simulate_vote_request(
        self, voter_node_id: str, language: str, candidate_id: str, term: int
    ) -> RequestVoteResponse:
        """
        Simulate vote request to a node by reading its state.json

        Args:
            voter_node_id: Node being asked to vote
            language: Language group
            candidate_id: Candidate requesting vote
            term: Election term

        Returns: RequestVoteResponse based on current state
        """
        shard_path = self.get_shard_path(voter_node_id, language)
        state = StateManager.load_state(shard_path)

        if state is None:
            # Node not available - vote not granted
            return RequestVoteResponse(
                voter=VoterInfo(voter_node_id, language),
                term=term,
                vote_granted=False,
                voting_history={},
            )

        current_term = state.get("term", 0)
        voted_for = state.get("voted_for")
        voting_history = StateManager.get_voting_history(shard_path)

        # Vote granting logic:
        # 1. If candidate term > current term, grant vote
        # 2. If candidate term == current term and haven't voted, grant vote
        # 3. If candidate term == current term and already voted for this candidate, grant vote
        # 4. Otherwise, don't grant vote

        vote_granted = False

        if term > current_term:
            vote_granted = True
        elif term == current_term:
            if voted_for is None or voted_for == candidate_id:
                vote_granted = True

        return RequestVoteResponse(
            voter=VoterInfo(voter_node_id, language),
            term=term,
            vote_granted=vote_granted,
            voting_history=voting_history,
        )

    def collect_votes(
        self, language: str, candidate_id: str, term: int
    ) -> List[RequestVoteResponse]:
        """
        Collect votes from all replicas in a language group

        Args:
            language: Language group
            candidate_id: Candidate node ID
            term: Election term

        Returns: List of RequestVoteResponse from all replicas
        """
        replicas = self.config.get("replicas", {}).get(language, [])
        responses = []

        for node_id in replicas:
            response = self.simulate_vote_request(node_id, language, candidate_id, term)
            responses.append(response)

        return responses

    def run_flexiraft_election(
        self,
        language: str,
        candidate_id: str,
        term: int,
        last_known_leader: Optional[LeaderRef] = None,
    ) -> ElectionResult:
        """
        Run FlexiRaft election for a language group

        Args:
            language: Language group
            candidate_id: Candidate node ID
            term: Election term
            last_known_leader: Previous leader (if any)

        Returns: ElectionResult (WON/LOST/UNDECIDED)
        """
        # Collect votes from all replicas
        responses = self.collect_votes(language, candidate_id, term)

        # Build single-group topology for this election
        # This is critical: FlexiRaft's pessimistic quorum requires majority in ALL groups
        # We only want to check majority in THIS language group
        replicas = set(self.get_replica_nodes(language))
        single_group_topology = ReplicaSetTopology(groups={language: replicas})
        single_group_quorum_spec = QuorumSpecification(
            mode=QuorumMode.DYNAMIC, topology=single_group_topology
        )

        # Build algorithm 2 function for this election
        alg2_fn = getPotentialNextLeaders(responses, single_group_topology)

        # Run FlexiRaft election algorithm
        result = flexiraft_leader_election(
            current_term=term,
            responses=responses,
            quorum_spec=single_group_quorum_spec,
            last_known_leader=last_known_leader,
            get_potential_next_leaders=alg2_fn,
        )

        return result

    def get_replica_nodes(self, language: str) -> List[str]:
        """Get list of node IDs that replicate a language"""
        return self.config.get("replicas", {}).get(language, [])

    def get_all_languages(self) -> List[str]:
        """Get list of all supported languages"""
        return self.config.get("languages", [])

    def get_leader_for_language(self, language: str) -> Optional[str]:
        """
        Get current leader node ID for a language group
        Reads from any replica's state file

        Returns: Leader node ID or None if no leader
        """
        replicas = self.get_replica_nodes(language)

        if not replicas:
            return None

        # Check first replica's state
        shard_path = self.get_shard_path(replicas[0], language)
        state = StateManager.load_state(shard_path)

        if state:
            return state.get("current_leader")

        return None

    def update_all_replicas_with_leader(
        self,
        language: str,
        leader_id: str,
        term: int,
        sync_leader: bool = True,
        force_sync: bool = False,
    ) -> None:
        """
        Update all replicas with new leader information

        Args:
            language: Language group
            leader_id: New leader node ID
            term: Election term
            sync_leader: If True, leader update is synchronous, followers async
            force_sync: If True, ALL updates are synchronous (prevents thread race conditions during elections)
        """
        replicas = self.get_replica_nodes(language)

        for node_id in replicas:
            shard_path = self.get_shard_path(node_id, language)

            # Update role
            if node_id == leader_id:
                # Leader: always synchronous update
                StateManager.update_role(shard_path, "leader", sync=True)
                StateManager.update_leader(shard_path, leader_id, term, sync=True)
            else:
                # Follower: synchronous if force_sync=True, else asynchronous
                use_sync = True if force_sync else False
                StateManager.update_role(shard_path, "follower", sync=use_sync)
                StateManager.update_leader(shard_path, leader_id, term, sync=use_sync)


# Convenience functions
def build_topology_from_config(
    config_path: str = "db/node_config.json",
) -> ReplicaSetTopology:
    """Convenience function to build topology"""
    coordinator = FlexiRaftCoordinator(config_path)
    return coordinator.topology


def create_quorum_spec(config_path: str = "db/node_config.json") -> QuorumSpecification:
    """Convenience function to create quorum spec"""
    coordinator = FlexiRaftCoordinator(config_path)
    return coordinator.quorum_spec
