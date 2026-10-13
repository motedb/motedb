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
}

impl Record {
    fn type_byte(&self) -> u8 {
        match self {
            Record::AddSegment { .. } => 1,
            Record::Compaction { .. } => 2,
            Record::GcCompleted { .. } => 3,
            Record::DropTable { .. } => 4,
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
        _ => None,
    }
}

pub struct Manifest {
    path: PathBuf,
    writer: BufWriter<File>,
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
        })
    }

    /// Open an existing manifest for appending. Seeks to end so that append()
    /// writes after the existing records (NOT at offset 0, which would
    /// overwrite the MAGIC header — the v0.5.0 WAL-recovery-gap bug).
    fn open(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        // Position the write cursor at end-of-file so append() extends the log
        // instead of clobbering the header.
        use std::io::Seek;
        let _ = file.seek(std::io::SeekFrom::End(0));
        Ok(Self {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
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
        Ok(())
    }

    /// fsync the manifest (durability point). Callers then may physically
    /// delete retired segment files.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
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
        let mut data = Vec::new();
        if let Ok(mut f) = File::open(&self.path) {
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
            match rec {
                Record::AddSegment { tid, id } => {
                    state.tables.entry(tid).or_default().note_add(id);
                }
                Record::Compaction {
                    tid,
                    new_id,
                    old_ids,
                } => {
                    state
                        .tables
                        .entry(tid)
                        .or_default()
                        .note_compaction(new_id, &old_ids);
                }
                Record::GcCompleted { tid, ids } => {
                    state.tables.entry(tid).or_default().note_gc(&ids);
                }
                Record::DropTable { tid } => {
                    state.tables.remove(&tid);
                    state.dropped.insert(tid);
                }
            }
        }
        state
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
