import os
import json
import sqlite3
import threading
import time

try:
    with open("node_config.json", "r") as f:
        config = json.load(f)
except json.JSONDecodeError:
    exit(1)

ALL_NODES = [n for n in config["nodes"]]
ALL_SHARDS = config["shards"]


class Daemon:
    def __init__(self, node_id):
        self._node_id = node_id
        self.running = True
        self.threads = []

    def execute_log_entry(self, shard_path, entry):
        db_path = os.path.join(shard_path, "novel.db")

        if "command" not in entry or "sql" not in entry["command"]:
            return

        sql = entry["command"]["sql"]
        params = tuple(entry["command"]["params"])

        conn = sqlite3.connect(db_path)

        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS novels (Title, Original Language)")

        try:
            cursor.execute(sql, params)
            conn.commit()
        except Exception as e:
            print(f"SQL Error: {e}")
        finally:
            conn.close()

    def check_shard(self, shard_name):
        shard_path = os.path.join(self._node_id, shard_name)
        log_path = os.path.join(shard_path, "raft_log.json")

        if not os.path.exists(log_path):
            return

        log_entries = []
        try:
            with open(log_path, "r") as f:
                for line in f:
                    if line.strip():
                        log_entries.append(json.loads(line))
        except (json.JSONDecodeError, FileNotFoundError):
            return

        applied_index_path = os.path.join(shard_path, "last_applied.txt")
        last_applied = 0
        if os.path.exists(applied_index_path):
            with open(applied_index_path, "r") as f:
                try:
                    last_applied = int(f.read().strip())
                except ValueError:
                    last_applied = 0
        state_path = os.path.join(shard_path, "state.json")
        commit_index = 0

        if os.path.exists(state_path):
            try:
                with open(state_path, "r") as f:
                    state = json.load(f)
                    commit_index = state.get("commit_index", 0)
            except Exception:
                pass

        safety_boundary = min(len(log_entries), commit_index)

        if safety_boundary > last_applied:
            for i in range(last_applied, safety_boundary):
                entry = log_entries[i]
                self.execute_log_entry(shard_path, entry)

                with open(applied_index_path, "w") as f:
                    f.write(str(i + 1))

    def start_shard_worker(self, shard_name):
        while self.running:
            try:
                self.check_shard(shard_name)
            except Exception as e:
                print(f"crash in {shard_name}: {e}")

            time.sleep(2)

    def start(self):
        SHARDS = ALL_SHARDS.get(self._node_id, [])

        for shard in SHARDS:
            t = threading.Thread(target=self.start_shard_worker, args=(shard,))
            t.daemon = True
            t.start()
            self.threads.append(t)
        return self.threads

    def stop(self):
        print(f"[{self._node_id}] begin graceful shutdown...")
        self.running = False
        for t in self.threads:
            t.join()

        print(f"[{self._node_id}] all threads joined")


def spin_cluster():
    running_nodes = []
    for node_id in ALL_NODES:
        node = Daemon(node_id)
        node.start()
        running_nodes.append(node)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Simulation interrupted: shutting down")
        for n in running_nodes:
            n.stop()
