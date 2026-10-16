// 审计冒烟: TS 统一 + 容器自动路由 + 只读拒绝 (Node)。
'use strict';
const fs = require('fs');
const os = require('os');
const path = require('path');
const motedb = require('./index.js');

function main() {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'node-ts-'));
  const db = new motedb.Database(path.join(tmp, 'ts.mote'));
  db.execute("CREATE TABLE m (ts TIMESTAMP, sid INT, v FLOAT) TIMESERIES(ts)");
  for (let i = 0; i < 50; i++) {
    db.execute('INSERT INTO m VALUES (?, ?, ?)', [
      1700000000000000 + i * 1000000,
      i % 3,
      i * 1.0,
    ]);
  }
  let r = db.query('SELECT COUNT(*) AS n FROM m');
  if (r[0].n !== 50) throw new Error('count ' + r[0].n);

  const snap = path.join(tmp, 'c.mote');
  db.execute(`EXPORT DB TO '${snap}'`);
  db.close();

  const ro = new motedb.Database(snap); // 自动路由只读
  r = ro.query('SELECT COUNT(*) AS n FROM m');
  if (r[0].n !== 50) throw new Error('ro count ' + r[0].n);
  let rejected = false;
  try {
    ro.execute("INSERT INTO m VALUES (1, 1, 1.0)");
  } catch {
    rejected = true;
  }
  if (!rejected) throw new Error('RO write accepted');
  ro.close();

  fs.rmSync(tmp, { recursive: true, force: true });
  console.log('PASS node: TS unified + container auto-route + RO reject');
}

main();
