import sqlite3, json, os, threading

config = json.load(open("node_config.json"))
NODES = {n["name"]: n["path"] for n in config["nodes"]}  # node path shortcut
SHARDS = {s["name"]: s for s in config["shards"]}  # leader lookup shortcut
LANGS = set(config["languages"])


def get_novel(language, novel_id):
    """Verify query and return results.

    :param language: language of novel
    :param novel_id: id of novel
    """
    assert language in LANGS
    shard = get_leader(language)
    conn = sqlite3.connect(shard)  #

def insert_novel():
    

def get_leader(language):
    """Get leader shard of language. For now it only returns one shard, since leader election not implemented.

    :param language: find leader shard of language; always valid(pre-checked)
    :return: relative shard directory path
    """
    return SHARDS[language]
