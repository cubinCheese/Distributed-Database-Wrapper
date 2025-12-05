#!/usr/bin/env python3
"""
Test shard death by crashing 3 nodes that share a common shard
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from main import DistributedDatabaseOrchestrator
from db.csv_loader import CSVLoader

def test_shard_death():
    """Crash nodes 0, 1, 2 which all host 'zh' shard - should cause shard death"""
    
    print("=" * 70)
    print("SHARD DEATH TEST")
    print("=" * 70)
    print("\nTarget: Crash nodes 0, 1, 2 (all host 'zh' shard)")
    print("Expected: Shard death when attempting writes without quorum")
    
    orchestrator = DistributedDatabaseOrchestrator()
    
    # Initialize
    print("\n[1/5] Initializing system...")
    orchestrator.initializer.initialize_all(verbose=False)
    print("System initialized (32 shards)")
    
    # Start daemons
    print("\n[2/5] Starting daemons...")
    orchestrator.start_commit_daemons()
    print(f"Started {len(orchestrator.daemons)} commit daemons")
    
    # Elect leaders
    print("\n[3/5] Electing leaders...")
    leaders = orchestrator.election_manager.elect_all_leaders_parallel()
    print(f"Elected leaders for {len(leaders)} languages")
    print(f"  'zh' leader: {leaders.get('zh', 'NONE')}")
    
    # Crash the nodes BEFORE starting population
    print("\n[4/5] Crashing nodes 0, 1, 2...")
    for node_id in ["node_0", "node_1", "node_2"]:
        success, shards = orchestrator.pause_node(node_id)
        if success:
            print(f"  Crashed {node_id}: affects {', '.join(shards)}")
    
    print("\n'zh' replicas status:")
    print("  node_0: CRASHED")
    print("  node_1: CRASHED")
    print("  node_2: CRASHED")
    print("  node_7: RUNNING (only 1/4 alive - quorum = 2/4)")
    print("\nExpected: Next write to 'zh' shard will fail due to no quorum")
    
    # Try to populate
    print("\n[5/5] Attempting to populate (should detect shard death)...")
    csv_path = "test_shard_death.csv"
    CSVLoader.create_sample_csv(csv_path, 100)
    novels = CSVLoader.load_csv(csv_path)
    grouped = CSVLoader.group_by_language(novels)
    
    try:
        results = orchestrator.populate_all_parallel(grouped, leaders)
        print("\nPopulation completed (unexpected)")
        return False
    except SystemExit as e:
        print("\nShard death detected - system terminated gracefully")
        return True

if __name__ == "__main__":
    success = test_shard_death()
    sys.exit(0 if success else 1)
