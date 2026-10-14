// Package motedb — Go bindings for MoteDB, the AI-native embedded
// multimodal database (via the C ABI in src/ffi.rs).
//
// Build: requires the MoteDB static library (cargo build --release at the
// repo root produces target/release/libmotedb.a). The cgo directives below
// default to the repo-relative path; override with CGO_CFLAGS/CGO_LDFLAGS
// for prebuilt libraries elsewhere.
//
// Value mapping (Go ⇄ engine):
//
//	int/int64      ↔ INTEGER          (exact)
//	float64        ↔ FLOAT
//	string         ↔ TEXT
//	bool           ↔ BOOLEAN
//	nil            ↔ NULL
//	[]float64      ↔ VECTOR(n)
//	[]byte         ↔ BLOB (base64 over the JSON boundary)
//	time.Time      ↔ TIMESTAMP (µs)
//
// Results: BLOB columns come back as {"$blob": "<base64>"} decoded into
// []byte; TIMESTAMP columns as int64 µs.
package motedb

/*
#cgo CFLAGS: -I${SRCDIR}
#cgo darwin,amd64 LDFLAGS: -L${SRCDIR}/../../target/release -lmotedb -Wl,-undefined,dynamic_lookup
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/../../target/release -lmotedb -Wl,-undefined,dynamic_lookup
#cgo linux LDFLAGS: -L${SRCDIR}/../../target/release -lmotedb -lpthread -lm -ldl
#include <stdlib.h>
#include "motedb.h"
*/
import "C"

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
	"unsafe"
)

// Database is an open handle to an embedded MoteDB database.
type Database struct {
	handle *C.DbHandle
}

// resultJSON mirrors the FFI result envelope.
// lastError pulls the FFI error slot (empty when none).
func lastError() string {
	c := C.motedb_last_error()
	defer C.motedb_db_free_string(c)
	return C.GoString(c)
}

// Open opens the database at path, creating it when missing.
func Open(path string) (*Database, error) {
	cp := C.CString(path)
	defer C.free(unsafe.Pointer(cp))
	h := C.motedb_db_open(cp)
	if h == nil {
		return nil, fmt.Errorf("motedb: open %q: %s", path, lastError())
	}
	return &Database{handle: h}, nil
}

// Close flushes pending writes and releases the handle.
func (db *Database) Close() error {
	if db == nil || db.handle == nil {
		return nil
	}
	C.motedb_db_close(db.handle)
	db.handle = nil
	return nil
}

// Checkpoint flushes pending writes and truncates the WAL.
func (db *Database) Checkpoint() error {
	if C.motedb_db_checkpoint(db.handle) != 0 {
		return fmt.Errorf("motedb: checkpoint: %s", lastError())
	}
	return nil
}

// BackupTo takes an online snapshot into dest (must be outside the DB dir).
func (db *Database) BackupTo(dest string) error {
	cd := C.CString(dest)
	defer C.free(unsafe.Pointer(cd))
	if C.motedb_db_backup(db.handle, cd) != 0 {
		return fmt.Errorf("motedb: backup: %s", lastError())
	}
	return nil
}

// ExportSnapshot packs the whole database into a single MOTESNAP file
// (P4 transport form — one file to copy/rsync/checksum). compressed
// selects per-file zstd framing.
func (db *Database) ExportSnapshot(dest string, compressed bool) (int32, error) {
	cd := C.CString(dest)
	defer C.free(unsafe.Pointer(cd))
	ci := C.int(0)
	if compressed {
		ci = 1
	}
	n := C.motedb_db_export(db.handle, cd, ci)
	if n < 0 {
		return 0, fmt.Errorf("motedb: export: %s", lastError())
	}
	return int32(n), nil
}

// ImportSnapshot unpacks a MOTESNAP file into a fresh directory.
// Open the directory afterwards with Open(destDir).
func ImportSnapshot(src, destDir string) error {
	cs := C.CString(src)
	defer C.free(unsafe.Pointer(cs))
	cd := C.CString(destDir)
	defer C.free(unsafe.Pointer(cd))
	if C.motedb_db_import(cs, cd) < 0 {
		return fmt.Errorf("motedb: import: %s", lastError())
	}
	return nil
}

// blobParam marks a []byte parameter as a BLOB over the JSON boundary.
type blobParam struct {
	Blob string `json:"$blob"`
}

// Result is a unified statement outcome.
type Result struct {
	Columns      []string
	Rows         [][]any
	AffectedRows int64
}

// blobJSON is how BLOB params serialize.
func blobJSON(b []byte) string {
	return `{"$blob": "` + base64.StdEncoding.EncodeToString(b) + `"}`
}

// Exec runs a statement with optional ?-parameters. SELECT results carry
// Columns/Rows; mutations carry AffectedRows.
//
// Parameter mapping: int/int64/float64 → numeric · string → TEXT ·
// bool → BOOLEAN · nil → NULL · []float64 → VECTOR · []byte → BLOB ·
// time.Time → TIMESTAMP (µs).
func (db *Database) Exec(sql string, args ...any) (*Result, error) {
	params := make([]json.RawMessage, 0, len(args))
	for i, a := range args {
		switch v := a.(type) {
		case nil:
			params = append(params, json.RawMessage("null"))
		case int:
			params = append(params, json.RawMessage(fmt.Sprintf("%d", v)))
		case int64:
			params = append(params, json.RawMessage(fmt.Sprintf("%d", v)))
		case float64:
			b, _ := json.Marshal(v)
			params = append(params, b)
		case string:
			b, _ := json.Marshal(v)
			params = append(params, b)
		case bool:
			params = append(params, json.RawMessage(fmt.Sprintf("%v", v)))
		case []float64:
			b, _ := json.Marshal(v)
			params = append(params, b)
		case []byte:
			params = append(params, json.RawMessage(blobJSON(v)))
		default:
			if tm, ok := a.(time.Time); ok {
				params = append(params, json.RawMessage(fmt.Sprintf("%d", tm.UnixMicro())))
				continue
			}
			return nil, fmt.Errorf("motedb: unsupported param %d (%T)", i, a)
		}
	}

	var paramsPtr *C.char
	var paramsJSON []byte
	if len(params) > 0 {
		paramsJSON = []byte("[" + joinRaw(params) + "]")
		paramsPtr = (*C.char)(unsafe.Pointer(&paramsJSON[0]))
	}

	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	out := C.motedb_db_exec(db.handle, csql, paramsPtr)
	if out == nil {
		return nil, fmt.Errorf("motedb: exec: %s", lastError())
	}
	defer C.motedb_db_free_string(out)

	var env map[string]any
	if err := json.Unmarshal([]byte(C.GoString(out)), &env); err != nil {
		return nil, fmt.Errorf("motedb: result decode: %w", err)
	}
	res := &Result{}
	if cols, ok := env["columns"].([]any); ok {
		for _, c := range cols {
			if s, ok := c.(string); ok {
				res.Columns = append(res.Columns, s)
			}
		}
	}
	if rows, ok := env["rows"].([]any); ok {
		for _, r := range rows {
			if list, ok := r.([]any); ok {
				res.Rows = append(res.Rows, decodeRow(list))
			}
		}
	}
	if n, ok := env["affectedRows"].(float64); ok {
		res.AffectedRows = int64(n)
	}
	return res, nil
}

// Query runs a SELECT and returns row objects (column → value).
func (db *Database) Query(sql string, args ...any) ([]map[string]any, error) {
	res, err := db.Exec(sql, args...)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(res.Rows))
	for _, row := range res.Rows {
		m := make(map[string]any, len(res.Columns))
		for i, v := range row {
			if i < len(res.Columns) {
				m[res.Columns[i]] = v
			}
		}
		out = append(out, m)
	}
	return out, nil
}

// decodeRow normalizes JSON-decoded values: blobs arrive as
// {"$blob": base64} and become []byte; numbers stay float64 (µs timestamps
// below 2^53 are exact in f64).
func decodeRow(list []any) []any {
	out := make([]any, len(list))
	for i, v := range list {
		if m, ok := v.(map[string]any); ok {
			if b64, ok := m["$blob"].(string); ok {
				if raw, err := base64.StdEncoding.DecodeString(b64); err == nil {
					out[i] = raw
					continue
				}
			}
		}
		out[i] = v
	}
	return out
}

func joinRaw(items []json.RawMessage) string {
	out := ""
	for i, it := range items {
		if i > 0 {
			out += ","
		}
		out += string(it)
	}
	return out
}
