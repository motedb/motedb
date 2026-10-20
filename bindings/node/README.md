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
(glibc), Windows x64. musl (Alpine) packages are wired up in the workflow but
not published yet — the loader already detects musl and will pick
`motedb-linux-<arch>-musl` once it exists; until then build from source on
Alpine. If `npm install` ran with `--no-optional`, or you are on an unlisted
platform, build from source as above. Troubleshooting:
`docs/18-install-troubleshooting.md`.
