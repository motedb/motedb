# motedb (Node.js)

AI-native embedded multimodal database — Node.js bindings (napi-rs v3).

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
# dev loader picks up target/release/libmotedb_node.dylib (or .so/.dll):
NODE_LIB=target/release/libmotedb_node.dylib node your-test.js
```

Multi-platform prebuilt binaries ship via napi-rs CLI in CI (`motedb.darwin-arm64.node` etc.).
