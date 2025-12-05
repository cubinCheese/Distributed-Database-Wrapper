# flexiraft_election.py
#
# MODIFICATION NOTE: majority_count() has been modified to support separate quorums:
# - Election quorum: 3/4 (true majority) - required for leader election
# - Write quorum: 2/4 - required for write operations
# This is a project-specific customization for the distributed database.
#
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum, auto
from math import floor
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

# ---------- Types & helpers ----------


def majority_count(n: int, for_election: bool = False) -> int:
    """
    Smallest integer strictly greater than n/2.

    Args:
        n: The number of replicas
        for_election: If True, returns true majority (3/4 for n=4).
                     If False, returns write quorum (2/4 for n=4).

    Special case: For n=4, returns 2 for write quorum, 3 for election quorum.
    """
    if for_election:
        return (n // 2) + 1  # True majority: 3/4 for n=4
    else:
        if n == 4:
            return 2  # Write quorum: 2/4 for n=4
        return (n // 2) + 1


@dataclass(frozen=True)
class VoterInfo:
    voter_id: str
    group: str


@dataclass
class RequestVoteResponse:
    """
    A single voter's response to RequestVote/PreVote.
    - vote_granted: whether this voter grants its vote to *this* candidate (current term).
    - voting_history: term -> candidate_id that this voter previously voted for (used by Alg.2).
    """

    voter: VoterInfo
    term: int
    vote_granted: bool
    voting_history: Dict[int, str] = field(default_factory=dict)


@dataclass(frozen=True)
class LeaderRef:
    node_id: str
    group: str
    term: int


class ElectionStatus(Enum):
    WON = auto()
    LOST = auto()
    UNDECIDED = auto()


@dataclass
class ElectionResult:
    status: ElectionStatus
    quorum_satisfied: bool
    quorum_still_possible: bool
    reason: str


# ---------- Quorum specifications ----------


class QuorumMode(Enum):
    STATIC = "static"
    DYNAMIC = "dynamic"


@dataclass(frozen=True)
class ReplicaSetTopology:
    """
    Topology describes groups and their member voters.
    Example: {"G1": {"n1","n2","n3"}, "G2": {"n4","n5"}}
    """

    groups: Dict[str, Set[str]]

    def group_size(self, g: str) -> int:
        return len(self.groups[g])

    @property
    def all_groups(self) -> Set[str]:
        return set(self.groups.keys())


@dataclass(frozen=True)
class GroupRequirement:
    # "majority in at least k of these groups"
    k_of_groups: int
    groups: Tuple[str, ...]  # tuple for hashability


@dataclass(frozen=True)
class StaticQuorumOption:
    """
    One option in DNF form. All requirements inside must be met (AND),
    and the overall static quorum is a disjunction of such options (OR).
    """

    requirements: Tuple[GroupRequirement, ...]


@dataclass(frozen=True)
class QuorumSpecification:
    mode: QuorumMode
    topology: ReplicaSetTopology
    # For STATIC mode only (DNF: OR over options; each option is AND of GroupRequirements)
    static_options: Tuple[StaticQuorumOption, ...] = tuple()

    def is_static(self) -> bool:
        return self.mode == QuorumMode.STATIC


# ---------- Vote counting primitives ----------


@dataclass
class CountView:
    granted: int
    responded: int
    size: int

    @property
    def needed(self) -> int:
        return majority_count(self.size, for_election=True)

    @property
    def satisfied(self) -> bool:
        return self.granted >= self.needed

    @property
    def still_possible(self) -> bool:
        # Can we still reach majority if all non-respondents voted yes?
        return self.granted + (self.size - self.responded) >= self.needed


def by_group_counts(
    responses: Iterable[RequestVoteResponse], topo: ReplicaSetTopology
) -> Dict[str, CountView]:
    per_group = {
        g: {"granted": 0, "responded": 0, "size": topo.group_size(g)}
        for g in topo.all_groups
    }
    for r in responses:
        g = r.voter.group
        if g not in per_group:
            # Ignore unknown groups (or raise)
            continue
        per_group[g]["responded"] += 1
        if r.vote_granted:
            per_group[g]["granted"] += 1
    return {g: CountView(**counts) for g, counts in per_group.items()}


# ---------- Static quorum evaluation (Alg.1 lines 1–4) ----------


def is_quorum_satisfied_static(
    responses: Iterable[RequestVoteResponse],
    spec: QuorumSpecification,
) -> Tuple[bool, bool]:
    """
    Evaluate whether a STATIC leader-election quorum is satisfied or still possible
    given partial responses. Implements helper is_quorum_satisfied(...) used in the paper.
    Each StaticQuorumOption is an AND of GroupRequirements; the static quorum is an OR of options.
    """
    assert spec.mode == QuorumMode.STATIC
    counts = by_group_counts(responses, spec.topology)

    def req_status(req: GroupRequirement) -> Tuple[bool, bool]:
        group_views = [counts[g] for g in req.groups]
        satisfied_groups = sum(1 for v in group_views if v.satisfied)
        possible_groups = sum(1 for v in group_views if v.still_possible)
        # Requirement met if a majority is ALREADY satisfied in >= k groups
        req_sat = satisfied_groups >= req.k_of_groups
        # Still possible if we could still hit >= k groups using remaining voters
        req_possible = possible_groups >= req.k_of_groups
        return req_sat, req_possible

    any_option_satisfied = False
    any_option_possible = False

    for opt in spec.static_options:
        # AND across requirements inside an option
        sat_all, poss_all = True, True
        for req in opt.requirements:
            rs, rp = req_status(req)
            sat_all = sat_all and rs
            poss_all = poss_all and rp
        any_option_satisfied = any_option_satisfied or sat_all
        any_option_possible = any_option_possible or poss_all

    return any_option_satisfied, any_option_possible


# ---------- Dynamic quorum helpers ----------


def pessimistic_quorum_status(
    responses: Iterable[RequestVoteResponse],
    topo: ReplicaSetTopology,
) -> Tuple[bool, bool]:
    """
    Majority within EVERY group (the 'pessimistic quorum'), as defined in §2.6 and §5.2.
    If satisfied, it necessarily satisfies any valid leader-election quorum.
    """
    counts = by_group_counts(responses, topo)
    sat_all = all(v.satisfied for v in counts.values())
    poss_all = all(v.still_possible for v in counts.values())
    return sat_all, poss_all


def majority_in_group_status(
    responses: Iterable[RequestVoteResponse],
    topo: ReplicaSetTopology,
    group: str,
) -> Tuple[bool, bool]:
    v = by_group_counts(responses, topo)[group]
    return v.satisfied, v.still_possible


# ---------- Algorithm 2 interface (to be implemented by your teammate) ----------


class Alg2Status(Enum):
    ALL_INTERMEDIATE_TERMS_DEFUNCT = auto()
    WAITING_FOR_MORE_VOTES = auto()
    POTENTIAL_NEXT_LEADERS_DETECTED = auto()


Alg2Fn = Callable[
    [int, Set[str]],  # term_it, possible_leader_groups
    Tuple[
        Alg2Status, int, List[LeaderRef]
    ],  # (status, next_term, potential_next_leaders)
]

# ---------- Algorithm 1: FlexiRaft Leader Election ----------


def flexiraft_leader_election(
    *,
    current_term: int,
    responses: Iterable[RequestVoteResponse],
    quorum_spec: QuorumSpecification,
    last_known_leader: Optional[
        LeaderRef
    ],  # may be None before first successful election
    get_potential_next_leaders: Alg2Fn,  # Algorithm 2
) -> ElectionResult:
    """
    Implements Algorithm 1 from the paper.
    Returns (status, quorum_satisfied, quorum_still_possible).
    """
    topo = quorum_spec.topology

    # --- Static mode: just evaluate the configured static quorum (Alg.1 lines 1–4) ---
    if quorum_spec.is_static():
        sat, poss = is_quorum_satisfied_static(responses, quorum_spec)
        return ElectionResult(
            status=(
                ElectionStatus.WON
                if sat
                else (ElectionStatus.UNDECIDED if poss else ElectionStatus.LOST)
            ),
            quorum_satisfied=sat,
            quorum_still_possible=poss,
            reason="static quorum evaluation",
        )

    # --- Dynamic mode path (Alg.1 lines 5–8) ---
    pess_sat, pess_poss = pessimistic_quorum_status(responses, topo)
    if pess_sat or last_known_leader is None:
        # If pessimistic quorum is already met OR we don't know the last leader, return now.
        # (Without LKL, the only way to win is the pessimistic quorum.)
        return ElectionResult(
            status=(
                ElectionStatus.WON
                if pess_sat
                else (ElectionStatus.UNDECIDED if pess_poss else ElectionStatus.LOST)
            ),
            quorum_satisfied=pess_sat,
            quorum_still_possible=pess_poss,
            reason=(
                "pessimistic quorum"
                if pess_sat
                else "awaiting pessimistic quorum or more votes"
            ),
        )

    # --- If current_term == last_known_leader.term + 1, try majority in LKL's group (Alg.1 lines 9–13) ---
    if current_term == last_known_leader.term + 1:
        sat, poss = majority_in_group_status(responses, topo, last_known_leader.group)
        return ElectionResult(
            status=(
                ElectionStatus.WON
                if sat
                else (ElectionStatus.UNDECIDED if poss else ElectionStatus.LOST)
            ),
            quorum_satisfied=sat,
            quorum_still_possible=poss,
            reason=f"majority in last-known-leader group {last_known_leader.group}",
        )

    # --- Otherwise, iteratively infer the highest-term leader using Alg.2 (Alg.1 lines 14–35) ---
    term_it = last_known_leader.term
    next_leader_groups: Set[str] = {last_known_leader.group}
    explored_leader_groups: Set[str] = {last_known_leader.group}
    terminal_groups: Set[str] = (
        set()
    )  # Filled by Alg.2 via side effects in the paper; we maintain it here.

    # We emulate Alg.2 side-effect by having Alg.2 return candidate leaders; we aggregate their groups
    while explored_leader_groups < topo.all_groups:
        status, next_term, potential_next_leaders = get_potential_next_leaders(
            term_it, next_leader_groups
        )

        if status == Alg2Status.ALL_INTERMEDIATE_TERMS_DEFUNCT:
            # At this point, candidate can win iff it has majority in each terminal group.
            if not terminal_groups:
                # If Alg.2 didn't build terminal_groups explicitly, interpret it as
                # "the union of all groups explored so far".
                terminal_groups = set(explored_leader_groups)

            qr = [majority_in_group_status(responses, topo, g) for g in terminal_groups]
            sat_all = all(s for (s, _) in qr)
            poss_all = all(p for (_, p) in qr)
            return ElectionResult(
                status=(
                    ElectionStatus.WON
                    if sat_all
                    else (ElectionStatus.UNDECIDED if poss_all else ElectionStatus.LOST)
                ),
                quorum_satisfied=sat_all,
                quorum_still_possible=poss_all,
                reason=f"majority required in terminal groups {sorted(terminal_groups)}",
            )

        if status == Alg2Status.WAITING_FOR_MORE_VOTES:
            # Not enough info yet; a majority in some candidate group hasn't responded.
            return ElectionResult(
                status=ElectionStatus.UNDECIDED,
                quorum_satisfied=False,
                quorum_still_possible=True,
                reason="waiting for more votes (Alg.2)",
            )

        if status == Alg2Status.POTENTIAL_NEXT_LEADERS_DETECTED:
            # Advance to the next term window and set of groups to explore (Alg.1 lines 29–31).
            term_it = next_term
            next_leader_groups = {lr.group for lr in potential_next_leaders}
            terminal_groups |= next_leader_groups
            explored_leader_groups |= next_leader_groups

            # Optional early win check: if we already have a majority in ALL terminal groups, win now.
            qr = [majority_in_group_status(responses, topo, g) for g in terminal_groups]
            if all(s for (s, _) in qr):
                return ElectionResult(
                    status=ElectionStatus.WON,
                    quorum_satisfied=True,
                    quorum_still_possible=True,
                    reason=f"majority already held in terminal groups {sorted(terminal_groups)}",
                )
            # Otherwise loop again until convergence to pessimistic quorum or timeout.

        # If the loop runs out of groups, we exit below.

    # If we exhaust the loop, the pessimistic quorum becomes necessary per the paper.
    return ElectionResult(
        status=ElectionStatus.UNDECIDED,
        quorum_satisfied=False,
        quorum_still_possible=True,
        reason="needs pessimistic quorum; continue collecting votes",
    )
