import unittest
from wrapper.sharding import Sharder

class TestSharding(unittest.TestCase):
    def test_shard_range(self):
        s = Sharder(5)
        for k in ["a","b","c"]:
            idx = s.shard_for_key(k)
            self.assertTrue(0 <= idx < 5)

if __name__ == "__main__":
    unittest.main()
