from pathfinder import Pathfinder
import os
import json


class Router:
    def __init__(self):
        self.client = Pathfinder()

    def append_raft_log(self, shard, sql, params):
        """Add SQL entry to raft log
        Nodes will self-update upon commit index verification
        """
        log = os.path.join(shard, "raft_log.json")

        entry = {"test": sql}

        with open(log, "a") as f:
            f.write(json.dumps(entry) + "\n")

        return self.get_log_length(shard)

    def get_log_length(self, shard):
        log_path = os.path.join(shard, "raft_log.json")
        with open(log_path, "r") as f:
            return sum(1 for line in f if line.strip())

    def raft_quorum(self, followers, language, sql, params):
        """Write SQL command into each follower's Raft log.
        Each successful write equates to a successful vote.

        :return boolean: majority gained or not
        """
        votes = 1
        majority = ((len(followers) + 1) // 2) + 1

        # for each follower, try to add SQL query to log. Each successfull write = 1 vote
        for n in followers:
            try:
                shard_path = self.client.convert_to_path(n, language)
                self.append_raft_log(shard_path, sql, params)
                votes += 1

                print(f"{n}/{language} write pass ({votes})")

            except Exception as e:
                print(f"{n}/{language} failed: {e}")

        if votes >= majority:
            return True
        else:
            raise Exception("consensus failed, not enough votes(failed mid write)")
        return False

    def update_commit_index(self, shard, index):
        """Once quorum is completed, this index is a receipt number to propagate confirmation to all replicas
        Update state.json of all replica shards
        It is up to the node's process to check receipt to execute updates
        """
        state_path = os.path.join(shard, "state.json")
        with open(state_path, "r") as f:
            state = json.load(f)

        state["commit_index"] = index

        with open(state_path, "w") as f:
            json.dump(state, f, indent=2)

        print(f"[COMMIT] updated {shard} index to {index}")

    def write_entry(self, title, language):
        """Received request for new entry;
        Find proper leader and followers -> generate SQL command -> add intent to raft log -> start quorum
        If quorum passed -> propagate results
        """

        # get leader
        try:
            leader_path, leader_id = self.client.get_leader(language)
        except Exception as e:
            print(f"Route error: {e}")
            return False

        # PREPARE SQL COMMAND (PLACEHOLDER)
        sql = ""
        params = (title, language)

        # [LEADER] LOG INTENT TO RAFT
        new_index = self.append_raft_log(leader_path, sql, params)

        replicas = self.client.get_nodes(language)
        followers = [n for n in replicas if n != leader_id]

        try:
            # [FOLLOWERS] RAFT QUORUM BY LOGGING INTENT -> possible concurrency later
            quorum_success = self.raft_quorum(followers, language, sql, params)
        except Exception as e:
            return f"Write failed: {e}"

        # if quorum succeeds -> signal to commit changes
        if quorum_success:
            print("consensus reached, committing update")

            # leader commit
            self.update_commit_index(leader_path, new_index)

            # followers commit
            for follower in followers:
                shard_path = self.client.convert_to_path(follower, language)
                try:
                    self.update_commit_index(shard_path, new_index)
                except Exception:
                    pass

            print("Distributed logs updated")
