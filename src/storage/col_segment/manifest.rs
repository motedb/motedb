//! Append-only manifest log. Records segment lifecycle (add / compaction / gc /
//! drop) for every columnar_ms table in ONE file.
//!
//! 🔑 P2 global layout (v2): a single `columnar_ms/MANIFEST` covers all
//! tables — each record carries the owning `table_id`, eliminating the
//! per-table manifest file and its resident fd. Crash-safe: segment FILES
//! are fsync'd before their record is appended; record appends are
//! write+flush only (page cache), so a lost manifest tail leaves complete
//! orphan files that recovery adopts (see `recover_from_disk`).
//!
//! Distinct from `storage::manifest` (the LSM manifest).
//!
//! Two construction modes share one code path:
//! - global: `columnar_ms/MANIFEST`, records tagged with real table ids.
//! - local (unit tests): `columnar_ms/<table>/MANIFEST`, all records use
//!   `LOCAL_TABLE_ID` (0) — replay/scavenge is id-driven and works unchanged.

use crate::Result;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAGIC: &[u8; 4] = b"MOTS";
/// v2: records carry table_id; adds DropTable.
const VERSION: u16 = 2;
/// Table id used by the local (per-table test) manifest mode.
pub const LOCAL_TABLE_ID: u32 = 0;

#[derive(Debug, Clone, Default)]
pub struct TableManifestState {
    /// Segment ids currently active (queryable).
    pub active_segments: Vec<u64>,
    /// Segment files safe to delete (superseded + manifest-recorded).
    pub obsolete_files: Vec<u64>,
    /// Every segment id mentioned by any record (active, superseded or GC'd).
    /// Recovery uses this to distinguish orphan files (id never recorded —
    /// written after the last durable manifest byte → adopt) from retired
    /// files (id recorded then superseded/GC'd → delete).
    pub ever_seen: Vec<u64>,
}

impl TableManifestState {
    fn note_add(&mut self, id: u64) {
        self.active_segments.push(id);
        self.ever_seen.push(id);
    }
    fn note_compaction(&mut self, new_id: u64, olds: &[u64]) {
        self.active_segments.retain(|x| !olds.contains(x));
        self.active_segments.push(new_id);
        self.obsolete_files.extend(olds.iter().copied());
        self.ever_seen.push(new_id);
        self.ever_seen.extend(olds.iter().copied());
    }
    fn note_gc(&mut self, gced: &[u64]) {
        self.obsolete_files.retain(|x| !gced.contains(x));
    }
    /// Compaction-written history marker: ids are known (ever_seen) but not
    /// active — a leftover file for one of them must be RETRIED for deletion,
    /// never adopted as an orphan.
    fn note_retired(&mut self, ids: &[u64]) {
        self.ever_seen.extend(ids.iter().copied());
        self.obsolete_files.extend(ids.iter().copied());
    }
}

#[derive(Debug, Clone, Default)]
pub struct ManifestState {
    /// Per-table state (v2). Tables absent from the map have no records.
    pub tables: HashMap<u32, TableManifestState>,
    /// Tables that saw a DropTable record — their leftover segment files on
    /// disk are NOT orphans and must not be adopted.
    pub dropped: HashSet<u32>,
}

impl ManifestState {
    pub fn table(&self, tid: u32) -> TableManifestState {
        self.tables.get(&tid).cloned().unwrap_or_default()
    }
}

enum Record {
    AddSegment {
        tid: u32,
        id: u64,
    },
    Compaction {
        tid: u32,
        new_id: u64,
        old_ids: Vec<u64>,
    },
    GcCompleted {
        tid: u32,
        ids: Vec<u64>,
    },
    DropTable {
        tid: u32,
    },
    /// ids that are known-history (ever_seen) but not active — emitted only
    /// by manifest compaction to preserve "never adopt these files" semantics.
    Retired {
        tid: u32,
        ids: Vec<u64>,
    },
}

impl Record {
    fn type_byte(&self) -> u8 {
        match self {
            Record::AddSegment { .. } => 1,
            Record::Compaction { .. } => 2,
            Record::GcCompleted { .. } => 3,
            Record::DropTable { .. } => 4,
            Record::Retired { .. } => 5,
        }
    }
    fn encode(&self) -> Vec<u8> {
        let mut v = Vec::new();
        v.push(self.type_byte());
        match self {
            Record::AddSegment { tid, id } => {
                v.extend_from_slice(&tid.to_le_bytes());
                v.extend_from_slice(&id.to_le_bytes());
            }
            Record::Compaction {
                tid,
                new_id,
                old_ids,
            } => {
                v.extend_from_slice(&tid.to_le_bytes());
                v.extend_from_slice(&new_id.to_le_bytes());
                v.extend_from_slice(&(old_ids.len() as u32).to_le_bytes());
                for id in old_ids {
                    v.extend_from_slice(&id.to_le_bytes());
                }
            }
            Record::GcCompleted { tid, ids } => {
                v.extend_from_slice(&tid.to_le_bytes());
                v.extend_from_slice(&(ids.len() as u32).to_le_bytes());
                for id in ids {
                    v.extend_from_slice(&id.to_le_bytes());
                }
            }
            Record::DropTable { tid } => {
                v.extend_from_slice(&tid.to_le_bytes());
            }
            Record::Retired { tid, ids } => {
                v.extend_from_slice(&tid.to_le_bytes());
                v.extend_from_slice(&(ids.len() as u32).to_le_bytes());
                for id in ids {
                    v.extend_from_slice(&id.to_le_bytes());
                }
            }
        }
        v
    }
}

/// Decode a v2 record at `pos` (after the type byte). Returns None on a
/// truncated/corrupt tail — replay stops there (same contract as v1).
fn decode_v2(data: &[u8], pos: &mut usize, t: u8) -> Option<Record> {
    fn take_u32(data: &[u8], pos: &mut usize) -> Option<u32> {
        if *pos + 4 > data.len() {
            return None;
        }
        let v = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
        *pos += 4;
        Some(v)
    }
    fn take_u64(data: &[u8], pos: &mut usize) -> Option<u64> {
        if *pos + 8 > data.len() {
            return None;
        }
        let v = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
        *pos += 8;
        Some(v)
    }
    fn take_ids(data: &[u8], pos: &mut usize) -> Option<Vec<u64>> {
        let n = take_u32(data, pos)? as usize;
        let mut ids = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            ids.push(take_u64(data, pos)?);
        }
        Some(ids)
    }
    match t {
        1 => {
            let tid = take_u32(data, pos)?;
            let id = take_u64(data, pos)?;
            Some(Record::AddSegment { tid, id })
        }
        2 => {
            let tid = take_u32(data, pos)?;
            let new_id = take_u64(data, pos)?;
            let old_ids = take_ids(data, pos)?;
            Some(Record::Compaction {
                tid,
                new_id,
                old_ids,
            })
        }
        3 => {
            let tid = take_u32(data, pos)?;
            let ids = take_ids(data, pos)?;
            Some(Record::GcCompleted { tid, ids })
        }
        4 => {
            let tid = take_u32(data, pos)?;
            Some(Record::DropTable { tid })
        }
        5 => {
            let tid = take_u32(data, pos)?;
            let ids = take_ids(data, pos)?;
            Some(Record::Retired { tid, ids })
        }
        _ => None,
    }
}

/// Compact the manifest when its file exceeds max(64 KiB, 4 × live-state
/// bytes). The floor keeps tiny databases from rewriting on every record;
/// the factor bounds steady-state size to a small multiple of the minimum
/// encoding (one AddSegment per active segment + DropTable history).
const MANIFEST_MIN_COMPACTION_BYTES: u64 = 64 * 1024;
const MANIFEST_COMPACTION_FACTOR: u64 = 4;

pub struct Manifest {
    path: PathBuf,
    writer: BufWriter<File>,
    /// 🔑 F1: incrementally-maintained decoded state. Built once at open
    /// (single file parse); every append folds its record in. Store creation
    /// reads its table slice from memory instead of re-parsing the file —
    /// opening an N-table database parses the manifest ONCE, not N times.
    state: ManifestState,
    /// Current file length in bytes (header + records); drives compaction.
    file_bytes: u64,
    /// 🔑 F2: records appended since the last durable `sync()`. `sync()` on
    /// a clean manifest skips the fsync entirely — a checkpoint that flushed
    /// nothing (or whose records were already synced) paid one fsync per
    /// table for nothing.
    dirty: bool,
    /// 🔑 F1b: per-table segment directories (registered by ColSegmentStore
    /// at creation). Compaction stats retired ids' files here: an id whose
    /// file is GONE can be forgotten entirely — segment ids are strictly
    /// monotonic per table, so the name can never be reused; the record's
    /// only job was "don't adopt / retry-delete this file". Tables without a
    /// registered dir keep their full history (conservative).
    table_dirs: HashMap<u32, PathBuf>,
}

impl Manifest {
    /// Create/open the GLOBAL manifest at `<base_dir>/columnar_ms/MANIFEST`.
    pub fn open_or_create_global(base_dir: &Path) -> Result<Self> {
        let path = base_dir.join("columnar_ms").join("MANIFEST");
        if path.exists() {
            Self::open(&path)
        } else {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            Self::create(&path)
        }
    }

    /// Create a LOCAL (per-table, test-only) manifest at `dir/MANIFEST`.
    pub fn open_or_create_local(dir: &Path) -> Result<Self> {
        let path = dir.join("MANIFEST");
        if path.exists() {
            Self::open(&path)
        } else {
            Self::create(&path)
        }
    }

    fn create(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.write_all(MAGIC)?;
        file.write_all(&VERSION.to_le_bytes())?;
        file.write_all(&0u32.to_le_bytes())?; // record_count placeholder
        file.sync_all()?;
        crate::fsync_dir(path);
        Ok(Self {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            state: ManifestState::default(),
            file_bytes: 10, // magic + version + count placeholder
            table_dirs: HashMap::new(),
            dirty: false,
        })
    }

    /// Open an existing manifest for appending. Seeks to end so that append()
    /// writes after the existing records (NOT at offset 0, which would
    /// overwrite the MAGIC header — the v0.5.0 WAL-recovery-gap bug).
    fn open(path: &Path) -> Result<Self> {
        // Parse ONCE here — every later read (store creation, compaction)
        // works from the in-memory state.
        let state = Self::parse_file(path);
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        // Position the write cursor at end-of-file so append() extends the log
        // instead of clobbering the header.
        use std::io::Seek;
        let file_bytes = file.seek(std::io::SeekFrom::End(0))?;
        Ok(Self {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            state,
            file_bytes,
            table_dirs: HashMap::new(),
            dirty: false,
        })
    }

    fn append(&mut self, rec: Record) -> Result<()> {
        // write+flush only (page cache) — NO fsync. Per-record fsync cost
        // ~3.8ms and the query-path flush/compaction appends 2-4 records per
        // statement under sustained writes (filter 4ms → 25ms). Durability is
        // provided at sync points: segment FILES are fsync'd before their
        // manifest record is appended, so a lost manifest tail leaves complete
        // orphan files that recovery adopts (see recover_from_disk). Kill -9
        // always sees page-cache data; only power loss relies on adoption.
        let bytes = rec.encode();
        self.writer.write_all(&bytes)?;
        self.writer.flush()?;
        apply_record(&mut self.state, &rec);
        self.file_bytes += bytes.len() as u64;
        self.dirty = true;
        self.maybe_compact()?;
        Ok(())
    }

    /// In-memory table slice for `recover_from_disk` (no file I/O).
    pub fn table_snapshot(&self, tid: u32) -> TableManifestState {
        self.state.table(tid)
    }

    /// Register a table's segment directory (called by ColSegmentStore at
    /// creation) so compaction can prune retired history whose files are
    /// already gone.
    pub fn register_table_dir(&mut self, tid: u32, dir: PathBuf) {
        self.table_dirs.insert(tid, dir);
    }

    /// Whether a DropTable record covers `tid` (no file I/O).
    pub fn is_dropped(&self, tid: u32) -> bool {
        self.state.dropped.contains(&tid)
    }

    /// Rewrite the manifest as its minimal encoding when the append log has
    /// grown far beyond the live state. Crash-safe: minimal file is written
    /// to a sibling temp, fsync'd, renamed — a crash mid-compaction leaves
    /// the old log authoritative. Preserves the recovery contract:
    /// - active segments → AddSegment records;
    /// - ever_seen-but-inactive ids → Retired records (leftover files must
    ///   be retried for deletion, NEVER adopted as orphans);
    /// - DropTable history → DropTable records (5 B each).
    fn maybe_compact(&mut self) -> Result<()> {
        let live = self.live_bytes_estimate();
        let threshold = MANIFEST_COMPACTION_FACTOR * live;
        let threshold = threshold.max(MANIFEST_MIN_COMPACTION_BYTES);
        if self.file_bytes <= threshold {
            return Ok(());
        }
        let tmp_path = self.path.with_file_name(".MANIFEST.cmp.tmp");
        let mut minimal = Manifest::create(&tmp_path)?;
        for (tid, ts) in &self.state.tables {
            for id in &ts.active_segments {
                minimal.append(Record::AddSegment { tid: *tid, id: *id })?;
            }
            // ever_seen is an append-only multiset (an id can arrive via
            // AddSegment AND Compaction) — dedupe, then prune by file
            // existence: ids whose segment file is already deleted are
            // FORGOTTEN (monotonic ids never reuse the name), ids whose file
            // lingers (crash between GcCompleted and delete) are kept so
            // recovery retries deletion instead of adopting the file.
            let active: std::collections::HashSet<u64> =
                ts.active_segments.iter().copied().collect();
            let retired: Vec<u64> = {
                let mut seen: std::collections::HashSet<u64> =
                    ts.ever_seen.iter().copied().collect();
                seen.retain(|id| !active.contains(id));
                let mut v: Vec<u64> = match self.table_dirs.get(tid) {
                    Some(dir) => seen
                        .into_iter()
                        .filter(|id| dir.join(format!("{id:010}.sst")).exists())
                        .collect(),
                    None => seen.into_iter().collect(),
                };
                v.sort_unstable();
                v
            };
            if !retired.is_empty() {
                minimal.append(Record::Retired {
                    tid: *tid,
                    ids: retired,
                })?;
            }
        }
        for tid in &self.state.dropped {
            minimal.append(Record::DropTable { tid: *tid })?;
        }
        minimal.sync()?;
        drop(minimal);
        std::fs::rename(&tmp_path, &self.path)?;
        crate::fsync_dir(&self.path);
        // Reopen the (now minimal) file for appending, and rebuild the
        // in-memory state FROM it so memory and file agree exactly (the
        // minimal encoding folds GC'd history into Retired — semantically
        // equivalent, but we keep one source of truth).
        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        use std::io::Seek;
        let file_bytes = file.seek(std::io::SeekFrom::End(0))?;
        self.writer = BufWriter::new(file);
        self.file_bytes = file_bytes;
        self.dirty = false; // minimal.sync() above made it durable
        self.state = Self::parse_file(&self.path);
        Ok(())
    }

    /// Minimal-encoding size estimate for the live state (UNIQUE ids —
    /// ever_seen is an append-only multiset and would over-count 2-3×).
    fn live_bytes_estimate(&self) -> u64 {
        let mut total = 10u64; // header
        for (tid, ts) in &self.state.tables {
            let active: std::collections::HashSet<u64> =
                ts.active_segments.iter().copied().collect();
            let seen: std::collections::HashSet<u64> = ts.ever_seen.iter().copied().collect();
            total += (active.len() as u64) * (1 + 4 + 8);
            let retired = match self.table_dirs.get(tid) {
                Some(dir) => seen
                    .iter()
                    .filter(|id| {
                        !active.contains(*id) && dir.join(format!("{:010}.sst", id)).exists()
                    })
                    .count(),
                None => seen.len().saturating_sub(active.len()),
            };
            if retired > 0 {
                total += 1 + 4 + 4 + (retired as u64) * 8;
            }
        }
        total += (self.state.dropped.len() as u64) * (1 + 4);
        total
    }

    /// fsync the manifest (durability point). Callers then may physically
    /// delete retired segment files. 🔑 F2: no-op when nothing was appended
    /// since the last sync — there is nothing to make durable.
    pub fn sync(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        self.dirty = false;
        Ok(())
    }

    pub fn add_segment(&mut self, tid: u32, id: u64) -> Result<()> {
        self.append(Record::AddSegment { tid, id })
    }

    pub fn record_compaction(&mut self, tid: u32, new_id: u64, old_ids: &[u64]) -> Result<()> {
        self.append(Record::Compaction {
            tid,
            new_id,
            old_ids: old_ids.to_vec(),
        })
    }

    pub fn record_gc(&mut self, tid: u32, ids: &[u64]) -> Result<()> {
        self.append(Record::GcCompleted {
            tid,
            ids: ids.to_vec(),
        })
    }

    pub fn record_drop_table(&mut self, tid: u32) -> Result<()> {
        self.append(Record::DropTable { tid })
    }

    /// Replay all records to reconstruct state. Used at recovery.
    /// Truncated/corrupt tails stop the replay (records before the tail
    /// remain valid — appends are write-ordered).
    ///
    /// v1 (per-table) files are decoded with the legacy record shapes and
    /// all records land under [`LOCAL_TABLE_ID`] — the migration path then
    /// re-tags them with real registry ids.
    pub fn replay(&self) -> ManifestState {
        Self::parse_file(&self.path)
    }

    /// One-shot file parse (v1 legacy shapes or v2).
    fn parse_file(path: &Path) -> ManifestState {
        let mut data = Vec::new();
        if let Ok(mut f) = File::open(path) {
            let _ = f.read_to_end(&mut data);
        }
        if data.len() < 10 || &data[..4] != MAGIC {
            return ManifestState::default();
        }
        let version = u16::from_le_bytes(data[4..6].try_into().unwrap());
        let mut pos = 10; // skip magic(4) + version(2) + count(4)
        let mut state = ManifestState::default();
        while pos < data.len() {
            if version < 2 {
                // Legacy v1: [tag][payload] without table_id.
                let tag = data[pos - 1];
                let Some(rec) = decode_v1(&data, &mut pos, tag) else {
                    break;
                };
                match rec {
                    LegacyRecord::AddSegment(id) => {
                        state.tables.entry(LOCAL_TABLE_ID).or_default().note_add(id);
                    }
                    LegacyRecord::Compaction { new_id, old_ids } => {
                        state
                            .tables
                            .entry(LOCAL_TABLE_ID)
                            .or_default()
                            .note_compaction(new_id, &old_ids);
                    }
                    LegacyRecord::GcCompleted(ids) => {
                        state
                            .tables
                            .entry(LOCAL_TABLE_ID)
                            .or_default()
                            .note_gc(&ids);
                    }
                }
                continue;
            }
            let t = data[pos];
            pos += 1;
            let Some(rec) = decode_v2(&data, &mut pos, t) else {
                break;
            };
            apply_record(&mut state, &rec);
        }
        state
    }
}

/// Fold one v2 record into a state — shared by replay (file parse) and
/// `Manifest::append` (incremental in-memory maintenance).
fn apply_record(state: &mut ManifestState, rec: &Record) {
    match rec {
        Record::AddSegment { tid, id } => {
            state.tables.entry(*tid).or_default().note_add(*id);
        }
        Record::Compaction {
            tid,
            new_id,
            old_ids,
        } => {
            state
                .tables
                .entry(*tid)
                .or_default()
                .note_compaction(*new_id, old_ids);
        }
        Record::GcCompleted { tid, ids } => {
            state.tables.entry(*tid).or_default().note_gc(ids);
        }
        Record::DropTable { tid } => {
            state.tables.remove(tid);
            state.dropped.insert(*tid);
        }
        Record::Retired { tid, ids } => {
            state.tables.entry(*tid).or_default().note_retired(ids);
        }
    }
}

/// v1 record shapes (pre-P2 per-table manifests).
enum LegacyRecord {
    AddSegment(u64),
    Compaction { new_id: u64, old_ids: Vec<u64> },
    GcCompleted(Vec<u64>),
}

fn decode_v1(data: &[u8], pos: &mut usize, t: u8) -> Option<LegacyRecord> {
    fn take_u64(data: &[u8], pos: &mut usize) -> Option<u64> {
        if *pos + 8 > data.len() {
            return None;
        }
        let v = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
        *pos += 8;
        Some(v)
    }
    fn take_ids_u16(data: &[u8], pos: &mut usize) -> Option<Vec<u64>> {
        if *pos + 2 > data.len() {
            return None;
        }
        let n = u16::from_le_bytes(data[*pos..*pos + 2].try_into().unwrap()) as usize;
        *pos += 2;
        let mut ids = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            ids.push(take_u64(data, pos)?);
        }
        Some(ids)
    }
    match t {
        1 => Some(LegacyRecord::AddSegment(take_u64(data, pos)?)),
        2 => {
            let new_id = take_u64(data, pos)?;
            let old_ids = take_ids_u16(data, pos)?;
            Some(LegacyRecord::Compaction { new_id, old_ids })
        }
        3 => Some(LegacyRecord::GcCompleted(take_ids_u16(data, pos)?)),
        _ => None,
    }
}

/// Shared manifest handle held by ColSegmentStore (one global instance across
/// all tables in a database) — replaces the per-table manifest file + fd.
pub type SharedManifest = Arc<parking_lot::Mutex<Manifest>>;

/// 🔑 P2 migration: fold legacy per-table manifests (`columnar_ms/<t>/MANIFEST`,
/// v0.14 layout) into the single global manifest. `name_to_id` maps table
/// directory names to registry table ids (the legacy layout carries no ids).
///
/// Crash-safe ordering:
/// 1. Global missing + legacy files present → replay each legacy file, write
///    the global (temp + fsync + rename), THEN delete the legacy files.
///    A crash before the rename leaves everything legacy (retry re-does it);
///    a crash after the rename but before deletes leaves the global in place
///    plus stale legacy files (the cleanup pass removes them — the global
///    wins).
/// 2. Global present → just clean up any leftover legacy manifests.
///
/// Directories whose name is absent from `name_to_id` belong to tables that
/// were already DROPped — their files are deleted outright.
///
/// Returns the opened-or-created global manifest handle.
pub fn migrate_to_global(
    base_dir: &Path,
    name_to_id: &HashMap<String, u32>,
) -> Result<SharedManifest> {
    let global_path = base_dir.join("columnar_ms").join("MANIFEST");
    let ms_dir = base_dir.join("columnar_ms");

    let legacy_exist = !global_path.exists()
        && std::fs::read_dir(&ms_dir)
            .map(|entries| {
                entries
                    .flatten()
                    .any(|e| e.path().is_dir() && e.path().join("MANIFEST").is_file())
            })
            .unwrap_or(false);

    if legacy_exist {
        // Aggregate: collapse each legacy table's history to one AddSegment
        // per active id, tagged with the REAL registry id. Dropped/absent
        // tables are skipped (their directories removed below).
        let mut adds: Vec<(u32, u64)> = Vec::new();
        let mut known_dirs: Vec<PathBuf> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&ms_dir) {
            for e in entries.flatten() {
                let dir = e.path();
                if !dir.is_dir() {
                    continue;
                }
                // Registry-absent tables (dropped pre-upgrade) are stale
                // data, not orphans — remove the whole directory. Checked
                // BEFORE the MANIFEST probe: a dropped table may leave a
                // bare directory with segment files and no manifest.
                let Some(name) = dir.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if !name_to_id.contains_key(name) {
                    let _ = std::fs::remove_dir_all(&dir);
                    continue;
                }
                let tid = name_to_id[name];
                let mp = dir.join("MANIFEST");
                if !mp.is_file() {
                    continue;
                }
                known_dirs.push(dir);
                if let Ok(m) = Manifest::open(&mp) {
                    // v1 per-table manifest: all records replay under
                    // LOCAL_TABLE_ID regardless of the real tid.
                    for id in m.replay().table(LOCAL_TABLE_ID).active_segments {
                        adds.push((tid, id));
                    }
                }
            }
        }
        let mut state = ManifestState::default();
        for (tid, id) in adds {
            state.tables.entry(tid).or_default().note_add(id);
        }
        write_global_from_state(base_dir, &state)?;
        // Global is authoritative — remove legacy manifests.
        for dir in known_dirs {
            let _ = std::fs::remove_file(dir.join("MANIFEST"));
        }
    }

    // Cleanup pass: a previous migration may have crashed between the global
    // rename and the legacy deletes — the global is authoritative either way.
    if global_path.exists() {
        if let Ok(entries) = std::fs::read_dir(&ms_dir) {
            for e in entries.flatten() {
                let p = e.path().join("MANIFEST");
                if p.is_file() {
                    let _ = std::fs::remove_file(p);
                }
            }
        }
    }

    Ok(Arc::new(parking_lot::Mutex::new(
        Manifest::open_or_create_global(base_dir)?,
    )))
}

/// Write a fresh global manifest holding exactly `state` (used by migration).
fn write_global_from_state(base_dir: &Path, state: &ManifestState) -> Result<()> {
    let ms_dir = base_dir.join("columnar_ms");
    std::fs::create_dir_all(&ms_dir)?;
    let final_path = ms_dir.join("MANIFEST");
    let tmp_path = ms_dir.join(".MANIFEST.mig.tmp");
    {
        let mut m = Manifest::create(&tmp_path)?;
        for (tid, ts) in &state.tables {
            for id in &ts.active_segments {
                // Re-emit as AddSegment; compaction/GC history collapses —
                // replay of a single AddSegment per active id is equivalent.
                m.add_segment(*tid, *id)?;
            }
        }
        for tid in &state.dropped {
            m.record_drop_table(*tid)?;
        }
        m.sync()?;
    }
    std::fs::rename(&tmp_path, &final_path)?;
    crate::fsync_dir(&final_path);
    Ok(())
}

#[cfg(test)]
mod f1_tests {
    use super::*;

    #[test]
    fn f1_compaction_triggers_and_binds_size() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = Manifest::open_or_create_local(dir.path()).unwrap();
        // Register a (file-less) table dir: every retired id's file is absent
        // → compaction must FORGET them, not carry them as Retired records.
        m.register_table_dir(7, dir.path().join("7"));
        // Steady-state churn: each cycle retires the previous segment
        // (flush → compaction → gc), active stays 1, history accumulates —
        // the shape a long-running ingest database produces.
        m.add_segment(7, 1).unwrap();
        let mut old = 1u64;
        for new in 2..=6000u64 {
            m.add_segment(7, new).unwrap();
            m.record_compaction(7, new, &[old]).unwrap();
            m.record_gc(7, &[old]).unwrap();
            old = new;
        }
        let len = std::fs::metadata(dir.path().join("MANIFEST"))
            .unwrap()
            .len();
        eprintln!("manifest after 6000 churn cycles: {len}B");
        assert!(
            len < 70 * 1024,
            "manifest {len}B must stay bounded (≈ constant under churn)"
        );
        // Active state survived the compaction swap; pruned (file-less)
        // history is intentionally forgotten.
        let ts = m.table_snapshot(7);
        assert!(ts.active_segments.contains(&6000), "active id kept");
        assert!(!ts.ever_seen.contains(&100), "file-less retired id pruned");
        // A fresh parse of the compacted file agrees.
        let m2 = Manifest::open_or_create_local(dir.path()).unwrap();
        let ts2 = m2.table_snapshot(7);
        assert!(ts2.active_segments.contains(&6000));
        assert!(!ts2.ever_seen.contains(&100));
    }
}
