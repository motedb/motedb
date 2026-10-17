// MoteDB Node.js bindings — loads the platform napi binary.
// Lookup order: MOTEDB_NODE_LIB env → motedb.<platform>-<arch>.node
// (napi-rs layout) → any *.node/*.dylib beside this file (dev layout).
'use strict';
const { existsSync, readdirSync } = require('node:fs');
const path = require('node:path');

function loadNative() {
  if (process.env.MOTEDB_NODE_LIB) {
    return require(process.env.MOTEDB_NODE_LIB);
  }
  const plat = `${process.platform}-${process.arch}`;
  const local = path.join(__dirname, `motedb.${plat}.node`);
  if (existsSync(local)) return require(local);
  // napi optionalDependencies layout: motedb-<platform>-<arch>[-gnu]
  // packages installed alongside this one.
  const libc = process.platform === 'linux' ? '-gnu' : '';
  const suffix = `${process.platform}-${process.arch}${libc}`;
  try {
    return require(`@motedb/motedb-${suffix}`);
  } catch {
    // fall through to dev layouts
  }
  const candidates = readdirSync(__dirname).filter(
    (f) => f.endsWith('.node') || f.endsWith('.dylib')
  );
  if (candidates.length > 0) {
    return require(path.join(__dirname, candidates[0]));
  }
  throw new Error(
    `motedb native library not found for ${plat} — install a motedb prebuilt package or build: cd bindings/node && cargo build --release`
  );
}

const native = loadNative();
module.exports = native.Database;
module.exports.Database = native.Database;
