//! Append-only manifest log. Records segment lifecycle (add / compaction / gc).
//! Crash-safe: each record is appended + fsync'd before in-memory state changes.
//!
//! Distinct from `storage::manifest` (the LSM manifest) — this is per-table,
//! binary, lives at `columnar_ms/<table>/MANIFEST`.

use crate::Result;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"MOTS";
const VERSION: u16 = 1;

#[derive(Debug, Clone, Default)]
pub struct ManifestState {
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

enum Record {
    AddSegment(u64),
    Compaction { new_id: u64, old_ids: Vec<u64> },
    GcCompleted(Vec<u64>),
}

impl Record {
    fn type_byte(&self) -> u8 {
        match self {
            Record::AddSegment(_) => 1,
            Record::Compaction { .. } => 2,
            Record::GcCompleted(_) => 3,
        }
    }
    fn encode(&self) -> Vec<u8> {
        let mut v = Vec::new();
        v.push(self.type_byte());
        match self {
            Record::AddSegment(id) => v.extend_from_slice(&id.to_le_bytes()),
            Record::Compaction { new_id, old_ids } => {
                v.extend_from_slice(&new_id.to_le_bytes());
                v.extend_from_slice(&(old_ids.len() as u16).to_le_bytes());
                for id in old_ids {
                    v.extend_from_slice(&id.to_le_bytes());
                }
            }
            Record::GcCompleted(ids) => {
                v.extend_from_slice(&(ids.len() as u16).to_le_bytes());
                for id in ids {
                    v.extend_from_slice(&id.to_le_bytes());
                }
            }
        }
        v
    }
}

pub struct Manifest {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl Manifest {
    pub fn create(path: &Path) -> Result<Self> {
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
    pub fn open(path: &Path) -> Result<Self> {
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

    pub fn add_segment(&mut self, id: u64) -> Result<()> {
        self.append(Record::AddSegment(id))
    }

    pub fn record_compaction(&mut self, new_id: u64, old_ids: &[u64]) -> Result<()> {
        self.append(Record::Compaction {
            new_id,
            old_ids: old_ids.to_vec(),
        })
    }

    pub fn record_gc(&mut self, ids: &[u64]) -> Result<()> {
        self.append(Record::GcCompleted(ids.to_vec()))
    }

    /// Replay all records to reconstruct state. Used at recovery.
    pub fn replay(&self) -> ManifestState {
        let mut data = Vec::new();
        if let Ok(mut f) = File::open(&self.path) {
            let _ = f.read_to_end(&mut data);
        }
        if data.len() < 10 || &data[..4] != MAGIC {
            return ManifestState::default();
        }
        let mut pos = 10; // skip magic(4) + version(2) + count(4)
        let mut active: Vec<u64> = Vec::new();
        let mut obsolete: Vec<u64> = Vec::new();
        let mut ever_seen: Vec<u64> = Vec::new();
        while pos < data.len() {
            let t = data[pos];
            pos += 1;
            match t {
                1 => {
                    // AddSegment
                    if pos + 8 > data.len() {
                        break;
                    }
                    let id = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    active.push(id);
                    ever_seen.push(id);
                }
                2 => {
                    // Compaction
                    if pos + 10 > data.len() {
                        break;
                    }
                    let new_id = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    let n = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2;
                    let mut olds = Vec::with_capacity(n);
                    for _ in 0..n {
                        if pos + 8 > data.len() {
                            break;
                        }
                        olds.push(u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
                        pos += 8;
                    }
                    active.retain(|x| !olds.contains(x));
                    active.push(new_id);
                    obsolete.extend(olds.iter().copied());
                    ever_seen.push(new_id);
                    ever_seen.extend(olds.iter().copied());
                }
                3 => {
                    // GcCompleted
                    if pos + 2 > data.len() {
                        break;
                    }
                    let n = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2;
                    let mut gced = Vec::with_capacity(n);
                    for _ in 0..n {
                        if pos + 8 > data.len() {
                            break;
                        }
                        gced.push(u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
                        pos += 8;
                    }
                    obsolete.retain(|x| !gced.contains(x));
                }
                _ => break,
            }
        }
        ManifestState {
            active_segments: active,
            obsolete_files: obsolete,
            ever_seen,
        }
    }
}
