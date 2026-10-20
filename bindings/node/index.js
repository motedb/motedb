// MoteDB Node.js bindings — loads the platform napi binary.
// Lookup order: MOTEDB_NODE_LIB env → motedb.<platform>-<arch>[-libc].node
// beside this file (napi-rs layout) → motedb-<platform>-<arch>[-libc]
// platform package (optionalDependencies) → any *.node beside this file
// (dev layout).
'use strict';
const { existsSync, readdirSync, readFileSync } = require('node:fs');
const path = require('node:path');

// Linux ships two incompatible libc ABIs; the platform package name carries
// which one the binary was built against. Same heuristics as napi-rs's own
// generated loader: the musl ldd banner first (cheap), then the process
// report, which only has glibcVersionRuntime on glibc systems.
function isMusl() {
  if (process.platform !== 'linux') return false;
  try {
    if (readFileSync('/usr/bin/ldd', 'utf8').includes('musl')) return true;
  } catch {
    // no ldd — fall through to the report
  }
  try {
    const report =
      typeof process.report?.getReport === 'function' ? process.report.getReport() : null;
    if (report?.header) return !report.header.glibcVersionRuntime;
  } catch {
    // report unavailable — assume glibc
  }
  return false;
}

function platformSuffix() {
  const { platform, arch } = process;
  if (platform === 'linux') return `${platform}-${arch}-${isMusl() ? 'musl' : 'gnu'}`;
  if (platform === 'win32') return `${platform}-${arch}-msvc`;
  return `${platform}-${arch}`;
}

function loadNative() {
  if (process.env.MOTEDB_NODE_LIB) {
    return require(process.env.MOTEDB_NODE_LIB);
  }
  const suffix = platformSuffix();
  const local = path.join(__dirname, `motedb.${suffix}.node`);
  if (existsSync(local)) return require(local);
  try {
    return require(`motedb-${suffix}`);
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
    `motedb native library not found for ${suffix} — install a motedb prebuilt package (npm ls motedb-${suffix}) or build from source: cd bindings/node && cargo build --release. See docs/18-install-troubleshooting.md.`
  );
}

const native = loadNative();
module.exports = native.Database;
module.exports.Database = native.Database;
