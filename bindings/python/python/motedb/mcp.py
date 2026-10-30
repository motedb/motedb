"""MCP server for MoteDB — expose a database to AI agents over stdio.

Run (Claude Desktop / any MCP client config):

    {"mcpServers": {"motedb": {
        "command": "python", "args": ["-m", "motedb.mcp", "/path/to/db.mote"]
    }}}

The database is opened with the ENGINE-LEVEL read-only gate
(``Database(path, read_only=True)``): every write statement is rejected by
the engine, multiple servers/clients may share one database, and the handle
never mutates state — the safe form for agent access.

Tools: list_tables, describe, query, vector_search, episodes, doctor.
Protocol: Model Context Protocol over stdio (JSON-RPC 2.0), no SDK
dependency — a minimal, dependency-free implementation.
"""

from __future__ import annotations

import json
import sys

_TOOLS = [
    {
        "name": "list_tables",
        "description": "List all tables in the database with row counts.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "describe",
        "description": "Show one table's schema (column names, types, indexes).",
        "inputSchema": {
            "type": "object",
            "properties": {"table": {"type": "string"}},
            "required": ["table"],
        },
    },
    {
        "name": "query",
        "description": (
            "Run a SELECT statement (read-only enforced by the engine). "
            "Use ? placeholders with `params`. Returns up to 200 rows as JSON."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string"},
                "params": {"type": "array"},
            },
            "required": ["sql"],
        },
    },
    {
        "name": "vector_search",
        "description": "Top-k nearest rows by vector distance: ORDER BY col <-> vec LIMIT k.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table": {"type": "string"},
                "column": {"type": "string"},
                "vector": {"type": "array", "items": {"type": "number"}},
                "k": {"type": "integer", "default": 10},
                "select": {"type": "string", "description": "projection, default *"},
            },
            "required": ["table", "column", "vector"],
        },
    },
    {
        "name": "episodes",
        "description": "List episodes of an EPISODE table (key, span, row count).",
        "inputSchema": {
            "type": "object",
            "properties": {"table": {"type": "string"}},
            "required": ["table"],
        },
    },
    {
        "name": "doctor",
        "description": "Run the engine self-check (layout, memory budget, index coverage).",
        "inputSchema": {"type": "object", "properties": {}},
    },
]

_MAX_ROWS = 200


def _text(content: str) -> dict:
    return {"content": [{"type": "text", "text": content}]}


def _rows_payload(rows: list) -> str:
    """Cap tool output at _MAX_ROWS and SAY so — a silently-truncated (or
    unbounded multi-MB) result misleads the agent either way."""
    if len(rows) > _MAX_ROWS:
        return json.dumps(rows[:_MAX_ROWS], ensure_ascii=False, default=str) + (
            f'\n_note: truncated to first {_MAX_ROWS} of {len(rows)} rows_'
        )
    return json.dumps(rows, ensure_ascii=False, default=str)


def _call(db, name: str, args: dict) -> dict:
    if name == "list_tables":
        rows = db.execute("SHOW TABLES")
        return _text(json.dumps(rows, ensure_ascii=False, default=str))
    if name == "describe":
        # 🔴 round-28: this used `cols, _ = fetch_arrays(...)` — it kept the
        # OUTPUT HEADER names (["Field","Type","Nullable","Position"], the
        # same for every table) and discarded the actual schema rows. The
        # row API returns real per-column records.
        table = args["table"].replace('"', "")
        rows = db.execute(f"DESCRIBE {table}")
        return _text(_rows_payload(rows))
    if name == "query":
        sql = args["sql"]
        rows = db.execute(sql, params=args.get("params"))
        return _text(_rows_payload(rows))
    if name == "vector_search":
        table = args["table"].replace('"', "")
        column = args["column"].replace('"', "")
        vec = json.dumps([float(x) for x in args["vector"]])
        k = int(args.get("k", 10))
        select = args.get("select", "*")
        rows = db.execute(
            f"SELECT {select} FROM {table} ORDER BY {column} <-> ? LIMIT {k}",
            params=[json.loads(vec)],
        )
        return _text(_rows_payload(rows))
    if name == "episodes":
        table = args["table"].replace('"', "")
        rows = db.execute(f"SELECT * FROM EPISODES({table})")
        return _text(_rows_payload(rows))
    if name == "doctor":
        return _text(db.doctor())
    raise ValueError(f"unknown tool: {name}")


def serve(db_path: str) -> None:
    import motedb

    # Engine-level read-only: writes are rejected by the engine itself, not
    # by string sniffing here.
    db = motedb.Database(db_path, read_only=True)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue
        method = req.get("method", "")
        req_id = req.get("id")
        if method == "initialize":
            resp = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": req.get("params", {}).get(
                        "protocolVersion", "2024-11-05"
                    ),
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "motedb", "version": motedb.__version__},
                },
            }
        elif method == "notifications/initialized":
            continue  # notification — no response
        elif method == "tools/list":
            resp = {"jsonrpc": "2.0", "id": req_id, "result": {"tools": _TOOLS}}
        elif method == "tools/call":
            try:
                result = _call(db, req["params"]["name"], req["params"].get("arguments", {}))
                resp = {"jsonrpc": "2.0", "id": req_id, "result": result}
            except Exception as e:  # tool errors are results, not protocol errors
                resp = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {"content": [{"type": "text", "text": f"error: {e}"}], "isError": True},
                }
        elif method == "ping":
            resp = {"jsonrpc": "2.0", "id": req_id, "result": {}}
        else:
            if req_id is not None:
                resp = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32601, "message": f"method not found: {method}"},
                }
            else:
                continue
        sys.stdout.write(json.dumps(resp, ensure_ascii=False) + "\n")
        sys.stdout.flush()


def main() -> None:
    if len(sys.argv) != 2:
        print("usage: python -m motedb.mcp <db-path>", file=sys.stderr)
        sys.exit(2)
    serve(sys.argv[1])


if __name__ == "__main__":
    main()
