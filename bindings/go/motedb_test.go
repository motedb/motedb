package motedb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func openTemp(t *testing.T) (*Database, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "t.mote"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db, dir
}

func TestCRUDAndTypes(t *testing.T) {
	db, _ := openTemp(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score FLOAT, ok BOOLEAN, emb VECTOR(3), data BLOB)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 50; i++ {
		if _, err := db.Exec(
			"INSERT INTO t VALUES (?, ?, ?, ?, ?, ?)",
			i, fmt.Sprintf("row %d", i), float64(i)*1.5, i%2 == 0, []float64{float64(i), 0.5, 0.0}, []byte{0xde, 0xad, byte(i)},
		); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	res, err := db.Query("SELECT id, data FROM t WHERE id = 1")
	if err != nil {
		t.Fatalf("query blob: %v", err)
	}
	data, ok := res[0]["data"].([]byte)
	if !ok || !bytes.Equal(data, []byte{0xde, 0xad, 1}) {
		t.Fatalf("blob round-trip: %#v", res[0]["data"])
	}

	nn, err := db.Query("SELECT id FROM t ORDER BY emb <-> [10.0, 0.5, 0.0] LIMIT 1")
	if err != nil {
		t.Fatalf("knn: %v", err)
	}
	if nn[0]["id"].(float64) != 10 {
		t.Fatalf("knn id = %v, want 10", nn[0]["id"])
	}

	qr, err := db.Query("SELECT COUNT(*) AS n FROM t WHERE name LIKE 'row 1%'")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if qr[0]["n"] != float64(11) { // row 1, 10..19
		t.Fatalf("count = %v, want 11", qr[0]["n"])
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
}

func TestReopenDurability(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "d.mote")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := db.Exec("INSERT INTO t VALUES (?, ?)", i, "v"); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	qr, err := db2.Query("SELECT COUNT(*) AS n FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if qr[0]["n"] != float64(20) {
		t.Fatalf("reopen count = %v, want 20", qr[0]["n"])
	}
}

func TestEpisodeAndAsofAlignment(t *testing.T) {
	db, _ := openTemp(t)
	defer db.Close()
	if _, err := db.Exec(
		"CREATE EPISODE TABLE teleop (session TEXT, ts TIMESTAMP, joint VECTOR(2)) EPISODE BY (session, ts) GAP 1000000",
	); err != nil {
		t.Fatalf("create episode: %v", err)
	}
	t0 := int64(1_700_000_000_000_000)
	for k := int64(0); k < 4; k++ {
		if _, err := db.Exec(
			"INSERT INTO teleop VALUES (?, ?, ?)", "A", t0+k*1_000_000, []float64{float64(k), 0.5},
		); err != nil {
			t.Fatal(err)
		}
	}
	// gap → second episode
	if _, err := db.Exec(
		"INSERT INTO teleop VALUES (?, ?, ?)", "A", t0+8_000_000, []float64{10, 0.5},
	); err != nil {
		t.Fatal(err)
	}
	res, err := db.Query("SELECT session, n_rows FROM EPISODES(teleop)")
	if err != nil {
		t.Fatalf("episodes: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("episodes = %d, want 2", len(res))
	}

	// ASOF: frame at +2.5s aligns to the joint at +2s (plain-table shape,
	// same as the Rust integration test).
	if _, err := db.Exec(
		"CREATE TABLE joints (session TEXT, ts TIMESTAMP, joint VECTOR(2))",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO joints VALUES (?, ?, ?)", "A", t0+2_000_000, []float64{2, 0.5},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE frames (fname TEXT, session TEXT, ts TIMESTAMP)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO frames VALUES (?, ?, ?)", "f1", "A", t0+2_500_000,
	); err != nil {
		t.Fatal(err)
	}
	al, err := db.Query(
		"SELECT f.fname AS fname, j.joint AS joint FROM frames f ASOF JOIN " +
			"joints j ON f.session = j.session AND j.ts <= f.ts",
	)
	if err != nil {
		t.Fatalf("asof: %v", err)
	}
	if len(al) != 1 {
		t.Fatalf("asof rows = %d, want 1", len(al))
	}
}

func TestBackupSnapshot(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "b.mote"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := db.Exec("INSERT INTO t VALUES (?, ?)", i, "v"); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.BackupTo(filepath.Join(dir, "snap.mote")); err != nil {
		t.Fatalf("backup: %v", err)
	}
	// mutate after snapshot
	if _, err := db.Exec("INSERT INTO t VALUES (99, 'later')"); err != nil {
		t.Fatal(err)
	}
	snap, err := Open(filepath.Join(dir, "snap.mote"))
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	qr, err := snap.Query("SELECT COUNT(*) AS n FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if qr[0]["n"] != float64(10) {
		t.Fatalf("snapshot rows = %v, want 10", qr[0]["n"])
	}
	_ = os.Stdout
}

func TestExportImportSnapshot(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "src.mote"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE t (id INT PRIMARY KEY, v TEXT, emb VECTOR(3))"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		if _, err := db.Exec("INSERT INTO t VALUES (?, ?, ?)", i, fmt.Sprintf("v%d", i),
			[]float64{float64(i), 1, 2}); err != nil {
			t.Fatal(err)
		}
	}

	snap := filepath.Join(dir, "fleet.mote")
	n, err := db.ExportSnapshot(snap, true)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if n <= 0 {
		t.Fatalf("export packed %d files, want > 0", n)
	}
	st, err := os.Stat(snap)
	if err != nil || st.IsDir() {
		t.Fatalf("snapshot must be a single file: %v %v", st, err)
	}

	dest := filepath.Join(dir, "dest.mote")
	if err := ImportSnapshot(snap, dest); err != nil {
		t.Fatalf("import: %v", err)
	}
	imported, err := Open(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer imported.Close()
	qr, err := imported.Query("SELECT COUNT(*) AS n FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if qr[0]["n"] != float64(40) {
		t.Fatalf("imported rows = %v, want 40", qr[0]["n"])
	}
}

func TestTimeSeriesUnifiedAndContainerReadonly(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "ts.mote"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE m (ts TIMESTAMP, sid INT, v FLOAT) TIMESERIES(ts)"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if _, err := db.Exec(
			"INSERT INTO m VALUES (?, ?, ?)",
			1700000000000000+int64(i)*1000000, i%3, float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	qr, err := db.Query("SELECT COUNT(*) AS n FROM m")
	if err != nil {
		t.Fatal(err)
	}
	if qr[0]["n"] != float64(50) {
		t.Fatalf("count=%v", qr[0]["n"])
	}

	snap := filepath.Join(dir, "c.mote")
	if _, err := db.Exec(fmt.Sprintf("EXPORT DB TO '%s'", snap)); err != nil {
		t.Fatal(err)
	}
	ro, err := Open(snap) // 自动路由只读 (FFI → api::open)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	qr, err = ro.Query("SELECT COUNT(*) AS n FROM m")
	if err != nil {
		t.Fatal(err)
	}
	if qr[0]["n"] != float64(50) {
		t.Fatalf("ro count=%v", qr[0]["n"])
	}
	if _, err := ro.Exec("INSERT INTO m VALUES (1, 1, 1.0)"); err == nil {
		t.Fatal("read-only write must be rejected")
	}
}
