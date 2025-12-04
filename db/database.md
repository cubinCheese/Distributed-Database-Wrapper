# DB blueprint

## Big Picture
Dataset is sharded by **Original Languages**:
1. Chinese (zh)
2. Japanese (ja)
3. Korean (ko)
4. Malaysian (ms)
5. Filipino (fil)
6. Indonesian (id)
7. Khmer (km)
8. Thai (th)

## Sharding implementaion
Sharding
- Main goal: to segment a dataset into multiple different physical nodes for modular access(typically beneficial for geographical access latency). This will interface with Raft’s replication factor to create a robust network of data shards.
- Sharding is primarily a routing algorithm, and the logic ties directly to database access pathfinding: dataset is partitioned into 8 shard groups, each with 4 replicas: 32 shards total in simulated nodes
  - each shard group has a leader (by Leader Election)
  - when write_entry() is called, a shard is queried
    - get_leader() looks for current leader of shard
    - raft_quorum() starts quorum algorithm via FlexiRaft
      - append_raft_log() preps the update in the log file of each affected shard. Successful updates will return as 1 vote.
      - Log commits are crucial for raft-based distributed systems, preventing data loss in premature failures.
    - update_commit_index() executes the update globally when quorum is reached
