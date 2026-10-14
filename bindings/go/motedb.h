/* MoteDB C ABI (v0.15) — see src/ffi.rs for semantics. */
#ifndef MOTEDB_H
#define MOTEDB_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DbHandle DbHandle;

DbHandle *motedb_db_open(const char *path);
void motedb_db_close(DbHandle *handle);
int motedb_db_checkpoint(DbHandle *handle);
int motedb_db_backup(DbHandle *handle, const char *dest);
/* JSON in / JSON out; NULL on error (see motedb_last_error). */
char *motedb_db_exec(DbHandle *handle, const char *sql, const char *params_json);
const char *motedb_last_error(void);
void motedb_db_free_string(char *s);

#ifdef __cplusplus
}
#endif
#endif

/* P4: single-file MOTESNAP snapshot transport. */
int motedb_db_export(DbHandle *handle, const char *dest, int compressed);
int motedb_db_import(const char *src, const char *dest);
