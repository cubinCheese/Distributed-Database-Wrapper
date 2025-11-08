"""Leader election primitives (Raft-like) - starter implementation.

This file contains a small, testable stub for leader election logic. It is
intentionally simple and synchronous for testing and demonstration.
"""
from typing import Optional

class LeaderElection:
    """Simple leader election stub.

    Contract:
    - elect_leader(nodes: list[str]) -> Optional[str]
      Choose a leader deterministically (e.g., lowest ID) for now.

    Error modes:
    - returns None if nodes is empty
    """

    def elect_leader(self, nodes: list[str]) -> Optional[str]:
        if not nodes:
            return None
        # Deterministic choice for demo/testing: smallest lexical id
        return sorted(nodes)[0]


if __name__ == "__main__":
    le = LeaderElection()
    print(le.elect_leader(["node3", "node1", "node2"]))
