import os, sys, tempfile, shutil, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import motedb

tmp = tempfile.mkdtemp()
try:
    # invalid durability name
    try:
        motedb.Database(os.path.join(tmp, "a.mote"), durability="bogus")
        raise SystemExit("FAIL: bogus durability accepted")
    except ValueError as e:
        assert "unknown durability" in str(e), e
    # periodic_ms without durability
    try:
        motedb.Database(os.path.join(tmp, "b.mote"), periodic_ms=50)
        raise SystemExit("FAIL: periodic_ms without durability accepted")
    except ValueError as e:
        assert "periodic_ms requires" in str(e), e
    # periodic knob: autocommit writes land and survive reopen
    path = os.path.join(tmp, "c.mote")
    db = motedb.Database(path, durability="periodic", periodic_ms=100)
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    t0 = time.perf_counter()
    for i in range(500):
        db.execute("INSERT INTO t VALUES (?, ?)", params=[i, i])
    dt = time.perf_counter() - t0
    db.checkpoint(); db.close()
    db = motedb.Database(path, durability="periodic")
    n = db.query("SELECT COUNT(*) FROM t")[1][0][0]
    assert n == 500, f"periodic lost rows: {n}"
    print(f"periodic(100) autocommit: {500/dt:.0f} rows/s, 500/500 survive reopen — OK")
    # synchronous knob still correct (fewer rows in same time, just verify function)
    db.close()
    db = motedb.Database(os.path.join(tmp, "d.mote"), durability="synchronous")
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    db.execute("INSERT INTO t VALUES (1, 1)")
    db.close()
    db = motedb.Database(os.path.join(tmp, "d.mote"), durability="synchronous")
    assert db.query("SELECT v FROM t WHERE id = 1")[1][0][0] == 1
    print("synchronous knob roundtrip — OK")
    db.close()
    print("DURABILITY KWARG E2E PASS")
finally:
    shutil.rmtree(tmp, ignore_errors=True)
