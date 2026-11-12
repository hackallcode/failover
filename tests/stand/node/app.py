"""A writer: numbered rows into SQLite, each also logged with the time, so runs can be checked for overlap and loss."""
import os
import sqlite3
import time
import uuid

db = sqlite3.connect("/data/app.db", isolation_level=None)
db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA busy_timeout=5000")
db.execute("CREATE TABLE IF NOT EXISTS log(seq INTEGER PRIMARY KEY, node TEXT, run TEXT, ts REAL)")
node, run = os.environ["NODE"], uuid.uuid4().hex[:8]
seq = db.execute("SELECT coalesce(max(seq), 0) FROM log").fetchone()[0]
with open("/data/writes.log", "a", buffering=1) as out:
    while True:
        seq += 1
        ts = time.time()
        db.execute("INSERT INTO log VALUES (?, ?, ?, ?)", (seq, node, run, ts))
        out.write(f"{ts:.3f} {seq} {node} {run}\n")
        time.sleep(float(os.environ.get("WRITE_EVERY", "0.1")))
