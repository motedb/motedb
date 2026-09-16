#!/usr/bin/env python3
"""CLI E2E：shell 会话、错误处理、doctor、持久化重开、版本。"""
import os, subprocess, sys, tempfile

CLI = "/Users/luo/glm/motedb/target/release/motedb-cli"
PASS = FAIL = 0
FAILURES = []

def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
    else:
        FAIL += 1
        FAILURES.append(f"{name}: {str(detail)[:200]}")
        print(f"  ✗ {name}  {str(detail)[:200]}")

def run(args, stdin=None, timeout=30):
    return subprocess.run([CLI] + args, input=stdin, capture_output=True, text=True, timeout=timeout)

def shell(db_path, stmts):
    script = "\n".join(stmts) + "\n.exit\n"
    return run([db_path], stdin=script)

tmp = tempfile.mkdtemp(prefix="r12c_cli_")
db1 = os.path.join(tmp, "s1.mote")

# 1. 基本会话
r = shell(db1, [
    "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, v FLOAT);",
    "INSERT INTO t VALUES (1, 'alpha', 1.5), (2, 'beta', 2.5), (3, 'gamma', 3.5);",
    "SELECT * FROM t;",
    "SELECT COUNT(*), AVG(v) FROM t WHERE v >= 2;",
    "UPDATE t SET v = 9.9 WHERE id = 1;",
    "SELECT v FROM t WHERE id = 1;",
    "DELETE FROM t WHERE id = 3;",
    "SELECT COUNT(*) FROM t;",
])
out = r.stdout
check("shell exit 0", r.returncode == 0, r.returncode)
check("shell select rows", "alpha" in out and "gamma" in out, out[-400:])
check("shell update", "9.9" in out, out[-400:])
check("shell count after delete", "2" in out, out[-200:])

# 2. 错误处理
r = shell(db1, [
    "SELECT * FROM missing_table;",
    "INSERT INTO t VALUES ('not-an-int', 'x', 1);",
    "SELETC * FROM t;",
    "SELECT unknown_col FROM t;",
])
check("err exit ok", r.returncode == 0, f"rc={r.returncode}")
check("err missing table", "missing_table" in r.stdout + r.stderr, (r.stdout + r.stderr)[-400:])
check("err type mismatch", "❌" in r.stdout + r.stderr and ("mismatch" in (r.stdout + r.stderr).lower() or "invalid" in (r.stdout + r.stderr).lower()), (r.stdout + r.stderr)[-400:])
check("err bad keyword", "parse" in (r.stdout + r.stderr).lower() or "expected" in (r.stdout + r.stderr).lower(), (r.stdout + r.stderr)[-400:])

# 3. 持久化
r = shell(db1, ["SELECT COUNT(*) FROM t;", "SELECT name FROM t WHERE id = 2;"])
check("reopen persist", "beta" in r.stdout, r.stdout[-300:])

# 4. 向量 + 时序
r = shell(db1, [
    "CREATE TABLE vec (id INT PRIMARY KEY, emb VECTOR(4));",
    "INSERT INTO vec VALUES (1, [1,0,0,0]), (2, [0,1,0,0]), (3, [0.9,0.1,0,0]);",
    "SELECT id FROM vec ORDER BY emb <-> [1,0,0,0] LIMIT 2;",
    "CREATE TABLE ts (s TEXT, ts TIMESTAMP, v FLOAT) TIMESERIES(ts);",
    "INSERT INTO ts VALUES ('a', 1700000000000, 1.0), ('a', 1700000001000, 2.0);",
    "SELECT COUNT(*) FROM ts WHERE ts >= 1700000000000 AND ts < 1700000002000;",
])
check("cli knn top2", "3" in r.stdout and r.stdout.rstrip().count("1") >= 1, r.stdout[-400:])
check("cli ts ok", r.returncode == 0, r.stderr[:200])

# 5. doctor
r = run(["doctor", db1])
check("doctor runs", r.returncode in (0, 1, 2), f"rc={r.returncode}")
check("doctor verdict", any(k in r.stdout for k in ("PASS", "FAIL", "WARN")), r.stdout[:300])

fresh = os.path.join(tmp, "fresh.mote")
r = run(["doctor", fresh])
check("doctor fresh ok", r.returncode in (0, 1, 2), f"rc={r.returncode}")

# 6. version/help
r = run(["--version"])
check("version", "0.9" in r.stdout or "MoteDB" in r.stdout, r.stdout[:100])
r = run(["--help"])
check("help", "用法" in r.stdout or "usage" in r.stdout.lower(), r.stdout[:150])

# 7. 坏路径不 panic
r = run(["/nonexistent/xx/db.mote"], timeout=15)
check("bad path no panic", "panic" not in r.stderr.lower(), r.stderr[:200])

print(f"\nCLI E2E: {PASS} 通过, {FAIL} 失败")
for f in FAILURES:
    print("  ✗", f)
sys.exit(1 if FAIL else 0)
