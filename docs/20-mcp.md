# MCP Server (AI agents)

`python -m motedb.mcp <db-path>` — expose a MoteDB database to MCP clients
(Claude Desktop, Cursor, any Model Context Protocol host) over stdio.

```json
{"mcpServers": {"motedb": {
    "command": "python",
    "args": ["-m", "motedb.mcp", "/data/robot.mote"]
}}}
```

## Safety model

The server opens the database with the **engine-level read-only gate**
(`Database(path, read_only=True)`): every write statement is rejected by the
engine itself — not by string sniffing in the server — and multiple readers
can share one database file. Point it at an exported snapshot container
(`EXPORT DB TO 'fleet.mote'`) for a fully immutable view.

## Tools

| Tool | What it does |
|---|---|
| `list_tables` | tables + row counts |
| `describe(table)` | schema of one table |
| `query(sql, params?)` | SELECT statements (writes rejected by the engine), ≤200 rows |
| `vector_search(table, column, vector, k)` | `ORDER BY col <-> vec LIMIT k` |
| `episodes(table)` | episode list of an EPISODE table |
| `doctor` | engine self-check report |

Example: *"find episodes where the note mentions grasp failure"* →

```
query: SELECT session, t0, t1 FROM EPISODES(teleop)
       WHERE MATCH(note, 'grasp failure')
```

Zero dependencies beyond `motedb` itself (JSON-RPC over stdio implemented
in ~150 lines). See `bindings/python/python/motedb/mcp.py`.
