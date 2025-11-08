"""Main runner for the distributed-db-wrapper demo.

This script runs a tiny simulation using the in-memory DB and wrapper stubs.
"""
from wrapper.leader_election import LeaderElection
from wrapper.replication import Replicator
from wrapper.sharding import Sharder
from wrapper.db_interface import SimpleInMemoryDB


def main():
    nodes = ["node1", "node2", "node3"]
    le = LeaderElection()
    leader = le.elect_leader(nodes)
    print("Elected leader:", leader)

    rep = Replicator(["replica1", "replica2"]) 
    print("Replication result:", rep.replicate({"hello":"world"}))

    s = Sharder(3)
    print("Shard for user:123 ->", s.shard_for_key("user:123"))

    db = SimpleInMemoryDB()
    db.connect()
    db.insert("kv", {"k":"a","v":"1"})
    print("DB find:", db.find("kv", {"k":"a"}))
    db.close()


if __name__ == "__main__":
    main()
