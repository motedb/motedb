/**
 * MoteDB — AI-native embedded multimodal database (Node.js bindings).
 *
 * Value mapping:
 * - VECTOR columns ↔ number[] (params and results)
 * - BLOB columns ↔ base64 string on results; Buffer params on inserts
 * - TIMESTAMP columns → µs-since-epoch number (exact in f64 < 2^53)
 * - JSON numbers are doubles: integral params become INTEGER, others FLOAT
 */
export declare class Database {
  /** Create-or-open an embedded database at `path`. */
  constructor(path: string);
  /** Run a mutation/DDL statement (params: ?-placeholders). Returns { affectedRows }. */
  execute(sql: string, params?: ParamValue[]): { affectedRows: number };
  /** Run a SELECT; rows come back as objects (column → value). */
  query(sql: string, params?: ParamValue[]): Record<string, unknown>[];
  /**
   * Run one INSERT/UPDATE/DELETE template once per parameter row as a single
   * batch (one WAL fsync). `params` is an array of per-row parameter arrays.
   */
  executeMany(sql: string, params: ParamValue[][]): { affectedRows: number };
  /**
   * Columnar bulk insert:
   *   db.insertArrays("ev", { id: [1, 2], emb: [[0.1, 0.2], [0.3, 0.4]] })
   * Columns are placed by schema position; omitted columns are NULL (an
   * auto-increment PK can be left out); all given columns must be equal
   * length. Returns the row count.
   */
  insertArrays(table: string, columns: Record<string, ParamValue[]>): number;
  /**
   * Hybrid search: BM25 text hits + vector KNN hits fused by Reciprocal Rank
   * Fusion. Rows also carry __rrf__ (higher = better), __bm25__ and
   * __distance__ (null when the respective list missed the doc).
   */
  hybridSearch(
    textIndex: string,
    textQuery: string,
    vectorIndex: string,
    queryVector: number[],
    k?: number,
    rrfK?: number,
    fetchMult?: number
  ): Record<string, unknown>[];
  /** Begin a transaction; returns its id (commit/rollback take it, or act on the latest). */
  begin(): number;
  /** Commit a transaction begun with begin() (no argument = the latest). */
  commit(tx?: number): void;
  /** Roll back a transaction begun with begin() (no argument = the latest). */
  rollback(tx?: number): void;
  /** Flush pending writes + truncate the WAL. */
  checkpoint(): void;
  /** Online snapshot into a new directory (must be outside the DB dir). */
  backupTo(dest: string): void;
  /** Pack the whole database into a single MOTESNAP file. */
  exportSnapshot(dest: string, compressed?: boolean): number;
  /** Static: unpack a MOTESNAP into a fresh directory and open it. */
  static openSnapshot(src: string, destDir: string): Database;
  /**
   * Static: open a MOTESNAP container read-only (extracted to a per-container
   * cache; all writes rejected). The plain constructor auto-routes container
   * files to this mode.
   */
  static openReadonly(container: string): Database;
  /** Close the database (flush + clean-close marker). */
  close(): void;
  /** Self-check report: [{name, status, detail}]. */
  doctor(): { name: string; status: string; detail: string }[];
}

export type ParamValue =
  | number
  | string
  | boolean
  | null
  | number[]
  | Uint8Array;
