# motedb (Go)

AI-native embedded multimodal database — Go bindings via the C ABI
(`src/ffi.rs`).

## Build

Requires the MoteDB static library (built at the repo root):

```bash
cargo build --release          # → target/release/libmotedb.a
cd bindings/go && go test ./...
```

The cgo directives default to repo-relative paths; override with
`CGO_CFLAGS` / `CGO_LDFLAGS` for prebuilt libraries elsewhere.

## Usage

```go
import motedb "github.com/motedb/motedb/bindings/go"

db, err := motedb.Open("/data/robot.mote") // creates when missing
_, err = db.Exec("INSERT INTO frames VALUES (?, ?, ?)",
    "f1", time.Now(), []byte{0xde, 0xad})          // []byte → BLOB
rows, err := db.Query(
    "SELECT name FROM robots ORDER BY emb <-> ? LIMIT 1",
    []float64{0.1, 0.9, 0.2})                       // []float64 → VECTOR
db.Checkpoint()
db.BackupTo("/data/snap.mote")
db.Close()
```

## Value mapping

| SQL type | Go param | Go result |
|---|---|---|
| INTEGER / FLOAT | `int` / `int64` / `float64` | `float64` |
| TEXT | `string` | `string` |
| BOOLEAN | `bool` | `bool` |
| NULL | `nil` | `nil` |
| VECTOR(n) | `[]float64` | `[]any` (numbers) |
| BLOB | `[]byte` | `[]byte` (base64 over the JSON boundary) |
| TIMESTAMP | `time.Time` | `int64` µs-since-epoch |
