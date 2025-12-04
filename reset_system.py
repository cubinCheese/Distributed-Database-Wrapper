#!/usr/bin/env python3
"""
Reset System Utility
Resets the entire distributed database to initial state.

This script:
1. Deletes all SQLite databases
2. Deletes all state.json files
3. Deletes all raft_log.json files
4. Optionally deletes generated CSV files
5. Leaves node_config.json and core code intact

Usage:
    python reset_system.py              # Interactive mode
    python reset_system.py --yes        # Auto-confirm
    python reset_system.py --full       # Also delete CSV files
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Tuple, Dict


def load_config(config_path: str = "db/node_config.json") -> dict:
    """Load node configuration"""
    with open(config_path, "r") as f:
        return json.load(f)


def find_all_shards(config: dict) -> List[Tuple[str, str]]:
    """
    Find all shard directories
    Returns: [(node_id, language), ...]
    """
    shards = []
    for node_id, languages in config["shards"].items():
        for language in languages:
            shards.append((node_id, language))
    return shards


def reset_shard(node_id: str, language: str, base_path: str = "db") -> Dict:
    """
    Reset a single shard to initial state
    Returns: {deleted_files: [...], errors: [...]}
    """
    shard_path = os.path.join(base_path, node_id, language)
    deleted = []
    errors = []

    if not os.path.exists(shard_path):
        return {
            "deleted_files": deleted,
            "errors": [f"Shard does not exist: {shard_path}"],
        }

    # Delete novel.db
    db_file = os.path.join(shard_path, "novel.db")
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
            deleted.append(db_file)
        except Exception as e:
            errors.append(f"Failed to delete {db_file}: {e}")

    # Delete state.json
    state_file = os.path.join(shard_path, "state.json")
    if os.path.exists(state_file):
        try:
            os.remove(state_file)
            deleted.append(state_file)
        except Exception as e:
            errors.append(f"Failed to delete {state_file}: {e}")

    # Delete raft_log.json
    raft_log_file = os.path.join(shard_path, "raft_log.json")
    if os.path.exists(raft_log_file):
        try:
            os.remove(raft_log_file)
            deleted.append(raft_log_file)
        except Exception as e:
            errors.append(f"Failed to delete {raft_log_file}: {e}")

    return {"deleted_files": deleted, "errors": errors}


def delete_csv_files(base_path: str = ".") -> List[str]:
    """Delete generated CSV files"""
    deleted = []
    csv_files = Path(base_path).glob("*.csv")

    for csv_file in csv_files:
        # Only delete if it looks like a generated file
        if "novel" in csv_file.name.lower() or "dataset" in csv_file.name.lower():
            try:
                csv_file.unlink()
                deleted.append(str(csv_file))
            except Exception as e:
                print(f"Warning: Could not delete {csv_file}: {e}")

    return deleted


def verify_reset(config: dict, base_path: str = "db") -> Tuple[bool, List[str]]:
    """
    Verify system is in initial state
    Returns: (is_clean, remaining_files)
    """
    remaining = []
    shards = find_all_shards(config)

    for node_id, language in shards:
        shard_path = os.path.join(base_path, node_id, language)

        for filename in ["novel.db", "state.json", "raft_log.json"]:
            filepath = os.path.join(shard_path, filename)
            if os.path.exists(filepath):
                remaining.append(filepath)

    return (len(remaining) == 0, remaining)


def main():
    parser = argparse.ArgumentParser(
        description="Reset distributed database to initial state"
    )
    parser.add_argument(
        "--yes", "-y", action="store_true", help="Auto-confirm without prompting"
    )
    parser.add_argument(
        "--full", action="store_true", help="Also delete generated CSV files"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="db/node_config.json",
        help="Path to node_config.json",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("DISTRIBUTED DATABASE - RESET UTILITY")
    print("=" * 60)

    # Load configuration
    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"ERROR: Could not load config: {e}")
        return 1

    # Find all shards
    shards = find_all_shards(config)
    print(f"\nFound {len(shards)} shards across {len(config['nodes'])} nodes")

    # Show what will be deleted
    print("\nThe following will be deleted:")
    print("  - All novel.db files (SQLite databases)")
    print("  - All state.json files (Raft state)")
    print("  - All raft_log.json files (operation logs)")
    if args.full:
        print("  - All generated CSV files")

    print("\nThe following will be PRESERVED:")
    print("  - db/node_config.json")
    print("  - All Python source code")
    print("  - Directory structure")

    # Confirm
    if not args.yes:
        response = input("\nProceed with reset? [y/N]: ")
        if response.lower() != "y":
            print("Reset cancelled.")
            return 0

    print("\nResetting system...")

    # Reset all shards
    total_deleted = 0
    total_errors = 0

    for node_id, language in shards:
        result = reset_shard(node_id, language)
        total_deleted += len(result["deleted_files"])
        total_errors += len(result["errors"])

        if result["errors"]:
            for error in result["errors"]:
                print(f"  ✗ {error}")

    print(f"\n✓ Deleted {total_deleted} shard files")

    # Delete CSV files if requested
    if args.full:
        csv_deleted = delete_csv_files()
        if csv_deleted:
            print(f"✓ Deleted {len(csv_deleted)} CSV files")
        else:
            print("✓ No CSV files to delete")

    # Verify
    is_clean, remaining = verify_reset(config)

    if is_clean:
        print("\n" + "=" * 60)
        print("RESET COMPLETE")
        print("=" * 60)
        print("System is now in initial state.")
        print("Run 'python main.py --generate-sample' to reinitialize.")
        print("=" * 60)
        return 0
    else:
        print(f"\n⚠ WARNING: {len(remaining)} files could not be deleted:")
        for f in remaining[:10]:  # Show first 10
            print(f"  - {f}")
        if len(remaining) > 10:
            print(f"  ... and {len(remaining) - 10} more")
        return 1


if __name__ == "__main__":
    sys.exit(main())
