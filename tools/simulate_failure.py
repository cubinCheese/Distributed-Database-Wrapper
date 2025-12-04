#!/usr/bin/env python3
"""
Node Failure Simulation Tool
Simulates node failures by renaming state files and optionally databases.
Allows testing of recovery mechanisms and eventual consistency.
"""

import argparse
import os
import sys
from pathlib import Path


def fail_node(node_id: str, base_path: str = "db") -> None:
    """
    Simulate node failure by renaming state files to .failed
    This makes the node appear offline to the system.

    Args:
        node_id: Node identifier (e.g., "node_0")
        base_path: Base path to db directory
    """
    node_path = Path(base_path) / node_id

    if not node_path.exists():
        print(f"ERROR: Node {node_id} does not exist at {node_path}")
        return

    failed_count = 0
    for shard_dir in node_path.iterdir():
        if shard_dir.is_dir():
            state_file = shard_dir / "state.json"
            if state_file.exists():
                state_file.rename(shard_dir / "state.json.failed")
                failed_count += 1

    print(f"✓ Failed {failed_count} shards in {node_id}")
    print(f"  Node {node_id} is now simulating an offline state")
    print(f"  Recovery: python tools/simulate_failure.py recover {node_id}")


def recover_node(node_id: str, base_path: str = "db") -> None:
    """
    Simulate node recovery by renaming .failed files back to .json
    This makes the node come back online.

    Args:
        node_id: Node identifier (e.g., "node_0")
        base_path: Base path to db directory
    """
    node_path = Path(base_path) / node_id

    if not node_path.exists():
        print(f"ERROR: Node {node_id} does not exist at {node_path}")
        return

    recovered_count = 0
    for shard_dir in node_path.iterdir():
        if shard_dir.is_dir():
            failed_file = shard_dir / "state.json.failed"
            if failed_file.exists():
                failed_file.rename(shard_dir / "state.json")
                recovered_count += 1

    if recovered_count == 0:
        print(f"⚠ No failed shards found in {node_id}")
        print(f"  Node may already be online")
        return

    print(f"✓ Recovered {recovered_count} shards in {node_id}")
    print(f"  Node {node_id} is now back online")
    print(f"  Recovery daemon will automatically pull missing data from leaders")


def status_node(node_id: str, base_path: str = "db") -> None:
    """
    Show status of a node

    Args:
        node_id: Node identifier (e.g., "node_0")
        base_path: Base path to db directory
    """
    node_path = Path(base_path) / node_id

    if not node_path.exists():
        print(f"ERROR: Node {node_id} does not exist at {node_path}")
        return

    online_count = 0
    offline_count = 0
    shards = []

    for shard_dir in node_path.iterdir():
        if shard_dir.is_dir():
            state_file = shard_dir / "state.json"
            failed_file = shard_dir / "state.json.failed"

            if state_file.exists():
                online_count += 1
                shards.append((shard_dir.name, "ONLINE"))
            elif failed_file.exists():
                offline_count += 1
                shards.append((shard_dir.name, "OFFLINE"))

    print(f"\n=== Node {node_id} Status ===")
    print(f"Online shards:  {online_count}")
    print(f"Offline shards: {offline_count}")
    print(f"\nShard Details:")
    for shard_name, status in sorted(shards):
        icon = "✓" if status == "ONLINE" else "✗"
        print(f"  {icon} {shard_name:6s} : {status}")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Simulate node failures and recoveries for distributed database testing"
    )
    parser.add_argument(
        "action", choices=["fail", "recover", "status"], help="Action to perform"
    )
    parser.add_argument("node_id", help="Node ID (e.g., node_0, node_1, ...)")
    parser.add_argument("--base-path", default="db", help="Base path to db directory")

    args = parser.parse_args()

    if args.action == "fail":
        fail_node(args.node_id, args.base_path)
    elif args.action == "recover":
        recover_node(args.node_id, args.base_path)
    else:  # status
        status_node(args.node_id, args.base_path)
