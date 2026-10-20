// CI / release smoke for the Node binding: create → DDL → INSERT with vector
// and BLOB params → KNN → checkpoint → reopen → close.
//
// MOTEDB_SMOKE_PKG selects what to require — './index.js' (default: the
// in-tree build, used by node-publish.yml) or 'motedb' (the published
// package, used by scripts/release_smoke.sh after copying this file next to
// a fresh `npm install motedb`).
'use strict';
const Database = require(process.env.MOTEDB_SMOKE_PKG || './index.js');
const os = require('node:os');
const path = require('node:path');
const fs = require('node:fs');

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'motedb-smoke-'));
try {
  const db = new Database(path.join(dir, 't.mote'));
  db.execute('CREATE TABLE t (id INT PRIMARY KEY, emb VECTOR(2), data BLOB)');
  for (let i = 0; i < 10; i++) {
    db.execute('INSERT INTO t VALUES (?, ?, ?)', [i, [i, -i], Buffer.from([1, 2, i])]);
  }
  const nn = db.query('SELECT id FROM t ORDER BY emb <-> [5, -5] LIMIT 1');
  if (nn[0].id !== 5) throw new Error('knn ' + JSON.stringify(nn));
  db.checkpoint();
  const again = new Database(path.join(dir, 't.mote'));
  if (again.query('SELECT COUNT(*) AS n FROM t')[0].n !== 10) throw new Error('reopen');
  again.close();
  console.log(`node smoke OK (${process.platform}-${process.arch}, node ${process.version})`);
} finally {
  fs.rmSync(dir, { recursive: true, force: true });
}
