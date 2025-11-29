import json
import os


class Pathfinder:
    def __init__(self, config_path="node_config.json"):
        with open(config_path, "r") as f:
            self.config = json.load(f)

    def get_nodes(self, language):
        """Get list of (replicated) nodes for that language

        :param language: language of novel
        :return: list of nodes
        """
        return self.config["replicas"].get(language, [])

    def convert_to_path(self, node_id, language):
        """Convert parameters to relative folder path

        :param node_id: directory name of node
        :param language: language of novel
        :return: relative path of shard in string
        """
        return os.path.join(node_id, language)

    def get_leader(self, language):
        """Get leader node path by looking at state.config to see current leader.
        If no current leader (mid-election) for some reason, use find_leader to crawl through nodes

        :param language: language of novel
        :return: relative leader shard directory path
        """
        replica_list = self.get_nodes(language)

        try:
            node_path = self.convert_to_path(replica_list[0], language)
            with open(os.path.join(node_path, "state.json")) as f:
                state = json.load(f)
                if state.get("role") == "leader":
                    return node_path, replica_list[0]

                curr_leader = state.get("current_leader")
                if curr_leader and curr_leader in replica_list:
                    return self.convert_to_path(curr_leader, language), curr_leader
        except (FileNotFoundError, json.JSONDecodeError):
            pass

        return self.find_leader(replica_list, language)

    def find_leader(self, replica_list, language):
        """Crawl through list to find leader node (slow)

        :param replica_list: list of nodes that hold requested content
        :param language: language of novel
        :return: relative leader shard directory path
        """
        for node in replica_list:
            node_path = self.convert_to_path(node, language)
            state_path = os.path.join(node_path, "state.json")

            try:
                with open(state_path, "r") as f:
                    state = json.load(f)
                    if state.get("role") == "leader":
                        return node_path, node
            except FileNotFoundError:
                continue
