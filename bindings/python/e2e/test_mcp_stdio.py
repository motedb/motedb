#!/usr/bin/env python3
"""MCP stdio end-to-end round-trip (round-28).

Drives `python -m motedb.mcp` exactly like a real MCP client over stdio
and verifies the tool surface against a fresh engine build:
  * initialize handshake
  * list_tables returns REAL table rows (round-22: the streaming
    dispatcher used to discard SHOW TABLES rows for a Definition message)
  * query truncates at 200 rows WITH a marker (round-20: vector_search
    was unbounded; every tool now uses _rows_payload)
  * vector_search returns exact nearest neighbors as dict rows
  * describe returns schema columns (round-22 DESCRIBE row-discard fix)
  * the engine-level read-only gate rejects writes
"""

import json
import os
import subprocess
import sys
import tempfile

FAIL = False


def check(name, ok, detail=""):
    global FAIL
    print(f"{'ok  ' if ok else 'FAIL'} {name}{(' — ' + detail) if detail and not ok else ''}")
    if not ok:
        FAIL = True


def main():
    import motedb  # noqa: F401 — ensures the venv has the built extension

    d = tempfile.mkdtemp()
    path = os.path.join(d, "agent.mote")
    db = motedb.Database(path)
    db.execute("CREATE TABLE robots (id INT PRIMARY KEY, name TEXT, emb VECTOR(3))")
    for i in range(250):
        db.execute(
            "INSERT INTO robots VALUES (?, ?, ?)", [i, f"bot-{i}", [float(i), 1.0, 0.5]]
        )
    db.execute("CREATE TABLE metrics (id INT PRIMARY KEY, v INT)")
    db.execute("INSERT INTO metrics VALUES (1, 10)")
    db.checkpoint()
    db.close()

    proc = subprocess.Popen(
        [sys.executable, "-m", "motedb.mcp", path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )

    def rpc(method, params, rid):
        msg = {"jsonrpc": "2.0", "method": method, "id": rid, "params": params}
        proc.stdin.write(json.dumps(msg) + "\n")
        proc.stdin.flush()
        return json.loads(proc.stdout.readline())

    r = rpc("initialize", {"protocolVersion": "2024-11-05"}, 1)
    check("initialize handshake", r["result"]["serverInfo"]["name"] == "motedb")

    r = rpc("tools/call", {"name": "list_tables", "arguments": {}}, 2)
    text = r["result"]["content"][0]["text"]
    check("list_tables returns rows", '"robots"' in text and '"metrics"' in text)

    r = rpc("tools/call", {"name": "query", "arguments": {"sql": "SELECT id FROM robots"}}, 3)
    text = r["result"]["content"][0]["text"]
    check("query truncates at 200 with marker", "truncated to first 200" in text)

    r = rpc(
        "tools/call",
        {
            "name": "vector_search",
            "arguments": {"table": "robots", "column": "emb", "vector": [0.0, 1.0, 0.5], "k": 3},
        },
        4,
    )
    rows = json.loads(r["result"]["content"][0]["text"])
    ids = sorted(row["id"] for row in rows)
    check("vector_search exact top-3", ids == [0, 1, 2], f"got {ids}")

    r = rpc("tools/call", {"name": "describe", "arguments": {"table": "metrics"}}, 5)
    text = r["result"]["content"][0]["text"]
    check("describe returns schema", "Field" in text and "v" in text)

    r = rpc("tools/call", {"name": "query", "arguments": {"sql": "INSERT INTO metrics VALUES (2, 20)"}}, 6)
    check("write rejected (read-only gate)", r["result"].get("isError") is True)

    proc.stdin.close()
    proc.wait(timeout=10)
    print("MCP E2E: ALL OK" if not FAIL else "MCP E2E: FAILURES")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
