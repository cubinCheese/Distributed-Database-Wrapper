# distributed-db-wrapper

A small scaffold for a distributed database wrapper. This includes stubs for:
- Leader election (Raft/FlexiRaft placeholder)
- Replication (QuORAM-like placeholder)
- Sharding
- DB adapter layer (in-memory + SQLite demo)

Getting started
1. Create a virtualenv and install requirements:

   python -m venv .venv
   .venv\Scripts\Activate.ps1; pip install -r requirements.txt

2. Run the demo:

   python run.py

3. Run tests:

   python -m unittest discover -s tests

Notes
- This repository contains minimal, well-documented stubs so you can extend
  with networking, persistent DBs, and real consensus algorithms later.
