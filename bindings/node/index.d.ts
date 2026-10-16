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
