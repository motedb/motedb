import motedb, tempfile, os

d = tempfile.mkdtemp()
db = motedb.Database(os.path.join(d, "store.mote"), preset="edge")
print("version:", motedb.__version__)

db.execute("CREATE TABLE IF NOT EXISTS robots (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, score FLOAT, emb VECTOR(3))")
n = db.execute("INSERT INTO robots (name, score, emb) VALUES (?, ?, ?)", params=["alpha", 0.5, [1.0, 2.0, 3.0]])
print("insert affected:", n)

rows = db.execute("SELECT id, name, score FROM robots WHERE name = ?", params=["alpha"])
print("select:", rows)
assert rows[0]["name"] == "alpha"

cols, tuples = db.query("SELECT id, name FROM robots")
print("query:", cols, tuples)

# Vector ANN
db.execute("INSERT INTO robots (name, emb) VALUES (?, ?)", params=["beta", [0.9, 0.1, 0.0]])
# Param-vector ANN (used to misorder — expression ORDER BY keys were skipped).
nn_param = db.execute("SELECT name FROM robots ORDER BY emb <-> ? LIMIT 1", params=[[1.0, 2.0, 3.0]])
print("ANN (param vector):", nn_param)
assert nn_param[0]["name"] == "alpha"
nn = db.execute("SELECT name FROM robots ORDER BY emb <-> [1.0, 2.0, 3.0] LIMIT 1")
print("ANN:", nn)
assert nn[0]["name"] == "alpha"
# Filtered ANN: WHERE applies before top-k.
db.execute("INSERT INTO robots (name, emb) VALUES (?, ?)", params=["gamma", [1.0, 2.0, 2.9]])
nnf = db.execute(
    "SELECT name FROM robots WHERE name != 'alpha' ORDER BY emb <-> ? LIMIT 2",
    params=[[1.0, 2.0, 3.0]],
)
print("filtered ANN:", nnf)
assert [r["name"] for r in nnf] == ["gamma", "beta"]

# Transactions
tx = db.begin()
db.execute("UPDATE robots SET score = 1.0 WHERE name = 'alpha'")
db.rollback(tx)
s = db.execute("SELECT score FROM robots WHERE name = 'alpha'")
print("after rollback score:", s[0]["score"])

# Aggregates
cnt = db.execute("SELECT COUNT(*) AS n FROM robots")[0]["n"]
print("count:", cnt)
assert cnt == 3

# executemany batch insert
db.execute("CREATE TABLE sensors (id INT PRIMARY KEY, temp FLOAT)")
n = db.executemany(
    "INSERT INTO sensors (id, temp) VALUES (?, ?)",
    [[i, 20.0 + i * 0.5] for i in range(1, 101)],
)
print("executemany:", n)
assert n == 100
mx = db.execute("SELECT MAX(temp) AS m FROM sensors")[0]["m"]
assert mx == 70.0, mx

db.checkpoint()
db.close()
print("SMOKE OK")
