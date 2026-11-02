# motedb (Node.js)

AI-native embedded multimodal database — Node.js bindings (napi-rs v3).

## 安装 Install

```bash
npm install motedb
```

## 快速开始 Quick start

```js
const Database = require('motedb');
// or: import { Database } from 'motedb';

const db = new Database('/data/app.mote');
db.execute('CREATE TABLE robots (id INT PRIMARY KEY, name TEXT, emb VECTOR(3), data BLOB)');
db.execute('INSERT INTO robots VALUES (?, ?, ?, ?)',
  [1, 'alpha', [0.1, 0.9, 0.2], Buffer.from([0xde, 0xad])]);

const rows = db.query('SELECT name FROM robots ORDER BY emb <-> [0.1, 0.9, 0.2] LIMIT 1');
// [{ name: 'alpha' }]
db.close();
```

Bulk load, hybrid search and transactions (same surface as the Python binding):

```js
// One batch, one WAL fsync — a template statement run once per parameter row.
db.executeMany('INSERT INTO robots VALUES (?, ?, ?, ?)', [
  [2, 'beta',  [0.3, 0.1, 0.8], null],
  [3, 'gamma', [0.9, 0.2, 0.1], null],
]);
// Columnar form: columns are matched by name; an omitted column is NULL.
db.insertArrays('robots', { id: [4, 5], name: ['delta', 'eps'], emb: [[0, 1, 0], [1, 0, 0]] });

// BM25 text hits + vector KNN fused by Reciprocal Rank Fusion.
db.execute('CREATE TEXT INDEX robots_name ON robots (name)');
db.execute('CREATE VECTOR INDEX robots_emb ON robots (emb)');
const hits = db.hybridSearch('robots_name', 'alpha beta', 'robots_emb', [0.1, 0.9, 0.2], 5);
// [{ id, name, emb, data, __rrf__, __bm25__, __distance__ }, ...]  (best first)

const tx = db.begin();
db.execute("UPDATE robots SET name = 'alpha-2' WHERE id = 1");
db.commit(tx);          // or db.rollback(tx); no argument = the latest begin()
```

## Value mapping

| SQL type | JS param | JS result |
|---|---|---|
| INTEGER / FLOAT | `number` | `number` |
| TEXT | `string` | `string` |
| BOOLEAN | `boolean` | `boolean` |
| NULL | `null` | `null` |
| VECTOR(n) | `number[]` | `number[]` |
| BLOB | `Buffer` / `Uint8Array` | base64 `string` |
| TIMESTAMP | µs `number` | µs `number` |

## Build (development)

```bash
cd bindings/node
cargo build --release
# Node only dlopen()s files named *.node — copy the cdylib under the napi
# layout name and the loader in index.js picks it up (dev layout):
cp target/release/libmotedb_node.dylib motedb.darwin-arm64.node   # .so on Linux, .dll on Windows
node your-test.js
# …or point the loader at any *.node file explicitly:
MOTEDB_NODE_LIB=/abs/path/to/motedb.node node your-test.js
```

Prebuilt binaries ship as per-platform optional dependencies, built by
`.github/workflows/node-publish.yml`: macOS arm64/x64, Linux x64/arm64
(glibc). Windows x64 and musl (Alpine) have NO prebuilt package yet — on
those platforms `npm install motedb` succeeds but the loader requires a
source build (see above). 🔴 B-01 lesson: the win32 platform package is only
declared as an optionalDependency once it actually exists on the registry —
a declared-but-unpublished package is an E404 that fails `npm install`
outright. If `npm install` ran with `--no-optional`, or you are on an unlisted
platform, build from source as above. Troubleshooting:
`docs/18-install-troubleshooting.md`.
