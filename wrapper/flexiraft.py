"""FlexiRaft (Simplified) Implementation

This module provides a lightweight, testable simulation of key FlexiRaft ideas
(Algorithms 1 & 2 as described conceptually):

Algorithm 1 (Leader Election with Flexible Quorums):
- Each node may specify an election quorum set E (subset of cluster members).
- A candidate requests votes only from E. A node grants a vote if:
  * Candidate's term is >= current term.
  * Node not already voted in this term.
- Candidate becomes leader if votes >= election_threshold(E).
  We model election_threshold as majority: floor(|E|/2)+1.

Algorithm 2 (Log Entry Commit with Flexible Commit Quorum):
- Leader appends an entry and replicates to nodes in its commit quorum set C.
- Entry is considered committed when acks >= commit_threshold(C).
  We model commit_threshold as majority of C.

Simplifications / Assumptions:
- No network partitions; messages are instantaneous.
- Terms increment deterministically on election attempts.
- Log entries are simple integers or strings.
- Reconfiguration ensures E and C are subsets of current members.
- We do not model Raft timing, heartbeats, or follower log matching.

Future Extensions:
- Distinct vote weighting or dynamic quorum reshaping at runtime.
- Integration with real RPC mechanics and persistent logs.
- Joint consensus style transitions for safe membership change.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Any

@dataclass
class QuorumConfig:
    election: Set[str]
    commit: Set[str]

    def validate(self, members: Set[str]) -> None:
        if not self.election or not self.commit:
            raise ValueError("Quorum sets must be non-empty")
        if not self.election.issubset(members):
            raise ValueError("Election quorum must be subset of members")
        if not self.commit.issubset(members):
            raise ValueError("Commit quorum must be subset of members")

    @staticmethod
    def majority_threshold(quorum: Set[str]) -> int:
        return (len(quorum) // 2) + 1


# Individual FlexiRaft Node Class
@dataclass
class FlexiRaftNode:
    node_id: str
    term: int = 0
    voted_for: Optional[str] = None
    log: List[Any] = field(default_factory=list)

    def can_vote_for(self, candidate_id: str, candidate_term: int) -> bool:
        if candidate_term < self.term:
            return False
        if self.voted_for is not None and self.voted_for != candidate_id and candidate_term == self.term:
            return False
        return True

    def grant_vote(self, candidate_id: str, candidate_term: int) -> bool:
        if self.can_vote_for(candidate_id, candidate_term):
            if candidate_term > self.term:
                # update term if candidate higher
                self.term = candidate_term
                self.voted_for = candidate_id
            else:
                self.voted_for = candidate_id
            return True
        return False

    def append_entry(self, entry: Any) -> None:
        self.log.append(entry)

# FlexiRaft Node Cluster Class
class FlexiRaftCluster:
    def __init__(self, members: List[str], quorum_config: QuorumConfig):
        self.members: Dict[str, FlexiRaftNode] = {m: FlexiRaftNode(m) for m in members}
        self.quorum_config = quorum_config
        self.quorum_config.validate(set(members))
        self.leader_id: Optional[str] = None
        self.current_term: int = 0

    # Algorithm 1: leader election using election quorum set
    def elect_leader(self, candidate_id: str) -> bool:
        if candidate_id not in self.members:
            raise ValueError("Candidate must be a cluster member")
        self.current_term += 1
        candidate_term = self.current_term
        votes = 0
        threshold = QuorumConfig.majority_threshold(self.quorum_config.election)
        for nid in self.quorum_config.election:
            node = self.members[nid]
            if node.grant_vote(candidate_id, candidate_term):
                votes += 1
        if votes >= threshold:
            self.leader_id = candidate_id
            # Update leader's term
            self.members[candidate_id].term = candidate_term
            return True
        return False

    def reconfigure_quorums(self, new_election: Set[str], new_commit: Set[str]) -> None:
        cfg = QuorumConfig(new_election, new_commit)
        cfg.validate(set(self.members.keys()))
        self.quorum_config = cfg

    # Algorithm 2: commit log entry using commit quorum
    def leader_append_and_commit(self, entry: Any) -> bool:
        if not self.leader_id:
            raise RuntimeError("No leader elected")
        leader = self.members[self.leader_id]
        leader.append_entry(entry)
        acks = 1  # leader acknowledges itself
        threshold = QuorumConfig.majority_threshold(self.quorum_config.commit)
        # replicate to commit quorum (excluding leader if present already counted)
        for nid in self.quorum_config.commit:
            if nid == self.leader_id:
                continue
            follower = self.members[nid]
            follower.append_entry(entry)
            acks += 1
            if acks >= threshold:
                return True
        return acks >= threshold

    def committed_entries(self) -> List[Any]:
        if not self.leader_id:
            return []
        # In this simplified model, entries at indices that satisfy commit quorum replication
        # are those present on all nodes in commit quorum. Determine intersection length.
        quorum_logs = [self.members[n].log for n in self.quorum_config.commit]
        if not quorum_logs:
            return []
        # Find min length across quorum logs (since we replicate sequentially)
        min_len = min(len(l) for l in quorum_logs)
        return quorum_logs[0][:min_len]

    def node_logs(self) -> Dict[str, List[Any]]:
        return {nid: node.log[:] for nid, node in self.members.items()}


if __name__ == "__main__":
    # Simple demonstration
    members = ["n1", "n2", "n3", "n4"]
    qc = QuorumConfig(election={"n1", "n2", "n3"}, commit={"n1", "n2", "n3"})
    cluster = FlexiRaftCluster(members, qc)
    elected = cluster.elect_leader("n2")
    print("Leader elected?", elected, "Leader:", cluster.leader_id)
    if elected:
        for e in ["x", "y", "z"]:
            committed = cluster.leader_append_and_commit(e)
            print(f"Append {e} committed?", committed)
        print("Committed entries:", cluster.committed_entries())
        print("All logs:", cluster.node_logs())
