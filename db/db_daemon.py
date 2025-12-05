import os
import json
import sqlite3
import threading
import time

try:
    config_path = os.path.join(os.path.dirname(__file__), "node_config.json")
    with open(config_path, "r") as f:
        config = json.load(f)
except json.JSONDecodeError:
    exit(1)

ALL_NODES = config["nodes"]
ALL_SHARDS = config["shards"]


class Daemon:
    def __init__(self, node_id):
        self._node_id = node_id
        self.running = True
        self.threads = []
        # Set base path for db directory
        self.base_path = os.path.join(os.path.dirname(__file__), node_id)
        # Pause mechanism for crash simulation
        self.paused = False
        self.pause_lock = threading.Lock()

    def execute_log_entry(self, shard_path, entry):
        db_path = os.path.join(shard_path, "novel.db")

        # FIX: Use direct entry structure (not nested in "command")
        sql = entry.get("sql", "")
        params = tuple(entry.get("params", []))

        if not sql:
            return

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # FIX: Use proper schema matching db_schema.py
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS novels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                original_language TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        try:
            cursor.execute(sql, params)
            conn.commit()
        except Exception as e:
            print(f"SQL Error in {shard_path}: {e}")
        finally:
            conn.close()

    def check_shard(self, shard_name):
        shard_path = os.path.join(self.base_path, shard_name)
        state_path = os.path.join(shard_path, "state.json")
        log_path = os.path.join(shard_path, "raft_log.json")

        if not os.path.exists(log_path) or not os.path.exists(state_path):
            return

        # Load state
        try:
            with open(state_path, "r") as f:
                state = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return

        commit_index = state.get("commit_index", 0)
        last_applied = state.get("last_applied", 0)

        # ADD HEARTBEAT: If this shard is a leader, update heartbeat
        if state.get("role") == "leader":
            state["last_heartbeat"] = time.time()
            try:
                with open(state_path, "w") as f:
                    json.dump(state, f, indent=2)
            except Exception:
                pass  # Ignore heartbeat write failures

        # No new entries to apply
        if commit_index <= last_applied:
            return

        # Read log entries
        log_entries = []
        try:
            with open(log_path, "r") as f:
                for line in f:
                    if line.strip():
                        log_entries.append(json.loads(line))
        except (json.JSONDecodeError, FileNotFoundError):
            return

        # Apply entries from last_applied+1 to commit_index
        for entry in log_entries:
            entry_index = entry.get("index", 0)

            if last_applied < entry_index <= commit_index:
                self.execute_log_entry(shard_path, entry)
                last_applied = entry_index

                # Update last_applied in state
                state["last_applied"] = last_applied
                try:
                    with open(state_path, "w") as f:
                        json.dump(state, f, indent=2)
                except Exception as e:
                    print(f"Error updating last_applied in {shard_path}: {e}")

    def start_shard_worker(self, shard_name):
        while self.running:
            # Check if paused (simulating crash)
            with self.pause_lock:
                if self.paused:
                    time.sleep(0.5)
                    continue

            try:
                self.check_shard(shard_name)
            except Exception as e:
                print(f"Daemon crash in {shard_name}: {e}")

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

    def pause(self):
        """Pause all daemon workers (simulates node crash)"""
        with self.pause_lock:
            self.paused = True

    def resume(self):
        """Resume all daemon workers (simulates node recovery)"""
        with self.pause_lock:
            self.paused = False

    def is_paused(self) -> bool:
        """Check if daemon is currently paused"""
        with self.pause_lock:
            return self.paused

    def get_shards(self):
        """Get list of shards managed by this daemon"""
        return ALL_SHARDS.get(self._node_id, [])


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
