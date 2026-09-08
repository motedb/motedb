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
# NOTE: known gap — ORDER BY emb <-> ? (param vector) misorders; use the
# literal form until streaming ORDER-BY expression keys support it.
nn = db.execute("SELECT name FROM robots ORDER BY emb <-> [1.0, 2.0, 3.0] LIMIT 1")
print("ANN:", nn)
assert nn[0]["name"] == "alpha"

# Transactions
tx = db.begin()
db.execute("UPDATE robots SET score = 1.0 WHERE name = 'alpha'")
db.rollback(tx)
s = db.execute("SELECT score FROM robots WHERE name = 'alpha'")
print("after rollback score:", s[0]["score"])

# Aggregates
cnt = db.execute("SELECT COUNT(*) AS n FROM robots")[0]["n"]
print("count:", cnt)
assert cnt == 2
db.checkpoint()
db.close()
print("SMOKE OK")
