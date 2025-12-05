"""Sharding and load balancing utilities - starter implementation.

This module contains a simple consistent-hash-like mapper for mapping keys to
shards (by index) for demonstration and tests.
"""
from typing import List

class Sharder:
    """Map keys to shard indices using a simple hash mod strategy."""

    def __init__(self, num_shards: int = 1):
        if num_shards < 1:
            raise ValueError("num_shards must be >= 1")
        self.num_shards = num_shards

    def shard_for_key(self, key: str) -> int:
        return abs(hash(key)) % self.num_shards


if __name__ == "__main__":
    s = Sharder(4)
    print(s.shard_for_key("user:1234"))
