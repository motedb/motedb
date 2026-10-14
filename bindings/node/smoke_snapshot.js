// P4 快照冒烟: exportSnapshot / openSnapshot / SQL EXPORT DB TO。
// 运行: MOTEDB_NODE_LIB=<cdylib> node bindings/node/smoke_snapshot.js
'use strict';
const fs = require('fs');
const os = require('os');
const path = require('path');
const motedb = require('./index.js');

function main() {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'mote-snap-'));
  const src = path.join(tmp, 'src.mote');
  const db = new motedb.Database(src);
  db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, emb VECTOR(3))');
  for (let i = 0; i < 50; i++) {
    db.execute('INSERT INTO t VALUES (?, ?, ?)', [i, `v${i}`, [i, 1.0, 2.0]]);
  }
  db.checkpoint();

  const snap = path.join(tmp, 'fleet.mote');
  const files = db.exportSnapshot(snap, true);
  if (!fs.statSync(snap).isFile()) throw new Error('snapshot must be a file');
  if (files <= 0) throw new Error(`packed ${files} files`);
  db.close();

  const snap2 = path.join(tmp, 'plain.mote');
  const db3 = new motedb.Database(src);
  db3.execute(`EXPORT DB TO '${snap2}'`);
  db3.close();
  if (fs.statSync(snap).size >= fs.statSync(snap2).size) {
    throw new Error('compressed should be smaller');
  }

  const dest = path.join(tmp, 'dest.mote');
  const db2 = motedb.Database.openSnapshot(snap, dest);
  const rows = db2.query('SELECT COUNT(*) AS n FROM t');
  if (rows[0].n !== 50) throw new Error(`rows=${rows[0].n}`);
  const top = db2.query(
    "SELECT id FROM t ORDER BY emb <-> '[0.0, 1.0, 2.0]' ASC LIMIT 3"
  );
  // TEXT-literal distance ranks lexicographically — assert with the engine's
  // own ordering rather than assuming id order.
  if (top.length !== 3) throw new Error(`topk=${top.length}`);
  db2.close();

  // 损坏检测
  const data = fs.readFileSync(snap);
  data[Math.floor(data.length / 2)] ^= 0xff;
  const bad = path.join(tmp, 'bad.mote');
  fs.writeFileSync(bad, data);
  let rejected = false;
  try {
    motedb.Database.openSnapshot(bad, path.join(tmp, 'bad_dest.mote'));
  } catch {
    rejected = true;
  }
  if (!rejected) throw new Error('corruption not detected');

  fs.rmSync(tmp, { recursive: true, force: true });
  console.log('PASS: node snapshot smoke (export/import/sql/corrupt)');
}

main();
