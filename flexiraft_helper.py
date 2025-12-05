"""Helper implementing a practical Algorithm 2 (getPotentialNextLeaders) for FlexiRaft.

This module exposes `make_alg2_fn(responses, topo)` which returns a callable
matching the `Alg2Fn` signature used by `flexiraft.flexiraft_leader_election`.

The implementation is a simplified, testable version of the paper's Alg.2:
- Wait for more current votes if a majority in any candidate group is still possible
  but not yet satisfied (so responses could change outcome).
- Otherwise, scan historical vote records (each voter's `voting_history`) to
  find the smallest term > term_it that contains historical votes. If none,
  return ALL_INTERMEDIATE_TERMS_DEFUNCT.
- For that term, produce potential LeaderRef objects for groups that have
  a majority of historical votes in that term. If any are found, return
  POTENTIAL_NEXT_LEADERS_DETECTED with that term and the leader refs.

Notes/assumptions:
- `responses` is an iterable of `RequestVoteResponse` (from flexiraft).
- `topo` is a `ReplicaSetTopology` describing groups -> members mapping.
- `voting_history` in each response maps term->candidate_id (node id string).
- Candidate -> group mapping is inferred from `topo`.
"""

from __future__ import annotations
from collections import defaultdict
from typing import Iterable, Set, Tuple, List, Dict, Callable

from wrapper.flexiraft import (
    RequestVoteResponse,
    ReplicaSetTopology,
    LeaderRef,
    Alg2Status,
    majority_count,
)


Alg2Fn = Callable[[int, Set[str]], Tuple[Alg2Status, int, List[LeaderRef]]]


def _candidate_group_map(topo: ReplicaSetTopology) -> Dict[str, str]:
    """Return mapping candidate_id -> group for quick lookup."""
    m = {}
    for g, members in topo.groups.items():
        for node in members:
            m[node] = g
    return m


def make_alg2_fn(
    responses: Iterable[RequestVoteResponse], topo: ReplicaSetTopology
) -> Alg2Fn:
    """Return an Alg2Fn closure using the provided `responses` and `topo`.

    The returned function has signature (term_it: int, possible_leader_groups: Set[str]) ->
    (Alg2Status, next_term, List[LeaderRef]).
    """
    # Snapshot responses into a list to iterate multiple times
    resp_list = list(responses)
    candidate_to_group = _candidate_group_map(topo)

    # Build helper: count current responses per group (for WAITING logic)
    def current_group_counts() -> Dict[str, Tuple[int, int, int]]:
        # returns mapping group -> (granted, responded, size)
        counts: Dict[str, Tuple[int, int, int]] = {}
        for g in topo.all_groups:
            counts[g] = (0, 0, topo.group_size(g))
        for r in resp_list:
            g = r.voter.group
            granted, responded, size = counts[g]
            responded += 1
            if r.vote_granted:
                granted += 1
            counts[g] = (granted, responded, size)
        return counts

    # Build historical votes mapping: term -> group -> set(candidate_ids)
    term_group_candidates: Dict[int, Dict[str, Dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )
    for r in resp_list:
        # Each voter's voting_history maps term -> candidate_id
        for t, cand in r.voting_history.items():
            # Map candidate id to group if possible
            g = candidate_to_group.get(cand)
            if g is None:
                continue
            term_group_candidates[t][g][cand] += 1

    def GetPotentialNextLeaders(
        term_it: int, possible_leader_groups: Set[str]
    ) -> Tuple[Alg2Status, int, List[LeaderRef]]:
        # 1) WAITING: if any possible group still could reach a majority based on current responses
        counts = current_group_counts()
        for g in possible_leader_groups:
            granted, responded, size = counts[g]
            needed = majority_count(size, for_election=True)
            # If we have not yet reached majority but remaining non-respondents could still provide votes,
            # we should wait for more votes (no safe inference yet).
            if granted < needed and (granted + (size - responded)) >= needed:
                return Alg2Status.WAITING_FOR_MORE_VOTES, term_it, []

        # 2) Iterate increasing terms greater than term_it and try to find potential leaders.
        higher_terms = sorted(t for t in term_group_candidates.keys() if t > term_it)
        if not higher_terms:
            return Alg2Status.ALL_INTERMEDIATE_TERMS_DEFUNCT, -1, []

        for cand_term in higher_terms:
            leader_refs: List[LeaderRef] = []
            for g in possible_leader_groups:
                group_info = term_group_candidates[cand_term].get(g, {})
                if not group_info:
                    continue
                # find if any candidate id reached majority within this group
                size = topo.group_size(g)
                needed = majority_count(size, for_election=True)
                # group_info maps candidate_id -> votes (counts of voters who historically voted for them)
                for cand_id, vcount in group_info.items():
                    if vcount >= needed:
                        leader_refs.append(
                            LeaderRef(node_id=cand_id, group=g, term=cand_term)
                        )
                        break

            if leader_refs:
                return (
                    Alg2Status.POTENTIAL_NEXT_LEADERS_DETECTED,
                    cand_term,
                    leader_refs,
                )

        # If we examined all higher terms and found no leaders, treat as defunct.
        return Alg2Status.ALL_INTERMEDIATE_TERMS_DEFUNCT, -1, []

    return GetPotentialNextLeaders


# Backwards-compatible name expected by earlier code / paper pseudocode
def getPotentialNextLeaders(
    responses: Iterable[RequestVoteResponse], topo: ReplicaSetTopology
) -> Alg2Fn:
    """Alias for make_alg2_fn to match paper naming / earlier code.

    Returns a callable (term_it, possible_leader_groups) -> (status, next_term, leader_refs).
    """
    return make_alg2_fn(responses, topo)
