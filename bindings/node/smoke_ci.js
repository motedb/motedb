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
  // 🔑 P0 regression: JS numbers are doubles; an integer-first napi probe
  // used to truncate them (9.9 → 9). Whole numbers map to INTEGER, the
  // rest stay FLOAT; BigInt converts losslessly.
  db.execute('CREATE TABLE nums (id INT PRIMARY KEY, v DOUBLE)');
  for (const [id, param] of [[1, 9.9], [2, -2.75], [3, 1e-7], [4, 2.0], [5, 123n]]) {
    db.execute('INSERT INTO nums VALUES (?, ?)', [id, param]);
  }
  const expect = { 1: 9.9, 2: -2.75, 3: 1e-7, 4: 2, 5: 123 };
  for (const row of db.query('SELECT id, v FROM nums')) {
    if (row.v !== expect[row.id]) throw new Error(`param roundtrip id=${row.id}: ${row.v} != ${expect[row.id]}`);
  }
  // SHOW INDEXES (engine grammar) and the snapshot roundtrip with a
  // NON-.mote destination directory (path-resolution regression: the
  // sibling-.mote rule used to send open() at a nonexistent path).
  db.execute('CREATE INDEX nums_v ON nums (v) USING COLUMN');
  if (db.query('SHOW INDEXES FROM nums').length !== 1) throw new Error('SHOW INDEXES');
  const snap = path.join(dir, 'snap.mote');
  db.exportSnapshot(snap);
  const dest = path.join(dir, 'unpacked');
  require('node:fs').mkdirSync(dest);
  const db3 = Database.openSnapshot(snap, dest);
  if (db3.query('SELECT COUNT(*) AS n FROM nums')[0].n !== 5) throw new Error('snapshot roundtrip');
  db3.close();

  db.checkpoint();
  const again = new Database(path.join(dir, 't.mote'));
  if (again.query('SELECT COUNT(*) AS n FROM t')[0].n !== 10) throw new Error('reopen');
  again.close();
  console.log(`node smoke OK (${process.platform}-${process.arch}, node ${process.version})`);
} finally {
  fs.rmSync(dir, { recursive: true, force: true });
}
