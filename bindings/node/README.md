# @motedb/motedb (Node.js)

AI-native embedded multimodal database — Node.js bindings (napi-rs v3).

## 安装 Install

发布在 GitHub Packages（npm registry），一次配置后正常 `npm i`：

```bash
# 项目里一次性配置（写入 .npmrc）
npm config set @motedb:registry https://npm.pkg.github.com
npm install @motedb/motedb
```

公共包**无需任何登录/token** 即可安装。

## 快速开始 Quick start

```js
const Database = require('@motedb/motedb');
// or: import { Database } from '@motedb/motedb';

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
