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

  // 🔑 B-05 parity with the Python binding: executeMany / insertArrays /
  // hybridSearch / begin-commit-rollback.
  db.execute('CREATE TABLE ev (id INT PRIMARY KEY, emb VECTOR(2), tag TEXT)');
  const em = db.executeMany('INSERT INTO ev VALUES (?, ?, ?)', [[1, [1, 1], 'a'], [2, [2, 2], 'b']]);
  if (em.affectedRows !== 2) throw new Error('executeMany ' + JSON.stringify(em));
  // Columns in non-schema order are placed by name, not by position.
  const ia = db.insertArrays('ev', { tag: ['c', 'd'], id: [3, 4], emb: [[3, 3], [4, 4]] });
  if (ia !== 2) throw new Error('insertArrays ' + ia);
  const evRows = db.query('SELECT id, tag FROM ev ORDER BY id');
  const evWant = [{ id: 1, tag: 'a' }, { id: 2, tag: 'b' }, { id: 3, tag: 'c' }, { id: 4, tag: 'd' }];
  if (JSON.stringify(evRows) !== JSON.stringify(evWant)) throw new Error('ev rows ' + JSON.stringify(evRows));
  let threw = false;
  try { db.insertArrays('ev', { id: [5], nope: [1] }); } catch (e) { threw = /unknown column/.test(String(e)); }
  if (!threw) throw new Error('insertArrays must reject unknown columns');
  threw = false;
  try { db.insertArrays('ev', { id: [5, 6], tag: ['x'] }); } catch (e) { threw = /expected 2/.test(String(e)); }
  if (!threw) throw new Error('insertArrays must reject ragged columns');

  db.execute('CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, emb VECTOR(4))');
  const bodies = ['rust database engine', 'rust rust rust embedded', 'database of databases', 'engine room',
    'rust database', 'vector search index', 'nothing relevant at all', 'engine engine engine'];
  db.executeMany('INSERT INTO docs VALUES (?, ?, ?)', bodies.map((b, i) => [i, b, [i * 0.25, i * 0.25, i * 0.25, i * 0.25]]));
  db.execute('CREATE TEXT INDEX docs_body ON docs (body)');
  db.execute('CREATE VECTOR INDEX docs_emb ON docs (emb)');
  const hy = db.hybridSearch('docs_body', 'rust database', 'docs_emb', [1, 1, 1, 1], 5);
  if (hy.length !== 5 || typeof hy[0].__rrf__ !== 'number' || !('__bm25__' in hy[0]) || !('__distance__' in hy[0]) || typeof hy[0].body !== 'string') {
    throw new Error('hybridSearch ' + JSON.stringify(hy));
  }
  for (let i = 1; i < hy.length; i++) if (hy[i].__rrf__ > hy[i - 1].__rrf__) throw new Error('hybridSearch order');

  // Transactions: rollback discards, commit keeps; the no-argument form acts
  // on the latest begin().
  const tx = db.begin();
  db.execute("INSERT INTO ev VALUES (9, [9, 9], 'z')");
  db.rollback(tx);
  if (db.query('SELECT COUNT(*) AS n FROM ev')[0].n !== 4) throw new Error('rollback');
  db.begin();
  db.execute("INSERT INTO ev VALUES (9, [9, 9], 'z')");
  db.commit();
  if (db.query('SELECT COUNT(*) AS n FROM ev')[0].n !== 5) throw new Error('commit');
  threw = false;
  try { db.commit(); } catch (e) { threw = /no transaction/.test(String(e)); }
  if (!threw) throw new Error('commit without begin must error');

  db.checkpoint();
  const again = new Database(path.join(dir, 't.mote'));
  if (again.query('SELECT COUNT(*) AS n FROM t')[0].n !== 10) throw new Error('reopen');
  again.close();
  console.log(`node smoke OK (${process.platform}-${process.arch}, node ${process.version})`);
} finally {
  fs.rmSync(dir, { recursive: true, force: true });
}
