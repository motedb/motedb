//! Unified metadata container (`meta.bin`) — P3 file-layout convergence.
//!
//! Folds the three root-level metadata files of the v0.14 layout into one
//! sectioned file with per-section CRC32:
//!
//! ```text
//! b"META1" | version u16 | flags u8 | section_count u16
//! section: kind u8 | len u32 | crc32 u32 | payload[len]
//!   kind 1 = catalog    (bincode TableRegistry::RegistryMetadata — bytes unchanged)
//!   kind 2 = index_meta (bincode Vec<IndexMetadata>          — bytes unchanged)
//!   kind 3 = lsn        (u64 LE)
//! flags bit0 = clean_close (replaces the `.clean_close` marker file)
//! ```
//!
//! Write discipline: ANY section update rewrites the whole file via
//! temp + fsync + rename + dir-fsync (the file is KB-scale). All writes go
//! through one Mutex, so DDL (registry persist) and checkpoint (lsn /
//! clean-close stamp) serialize here instead of racing separate files.
//!
//! Migration: when `meta.bin` is absent but the legacy files exist, their
//! bytes are adopted as the initial sections (payload formats are
//! unchanged) and the legacy files are removed after the first durable
//! write. Legacy order safety: the absence of a legacy file simply means an
//! empty section — identical to what the old readers did.

use crate::{Result, StorageError};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub const META_MAGIC: &[u8; 5] = b"META1";
pub const META_VERSION: u16 = 1;

pub const FLAG_CLEAN_CLOSE: u8 = 0b0000_0001;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetaSection {
    Catalog = 1,
    IndexMeta = 2,
    Lsn = 3,
}

struct MetaInner {
    flags: u8,
    /// Latest durable payload per section (what a rewrite would persist).
    sections: HashMap<u8, Vec<u8>>,
}

pub struct MetaStore {
    path: PathBuf,
    inner: Mutex<MetaInner>,
}

impl MetaStore {
    /// Open `db_path/meta.bin`, migrating the legacy trio
    /// (`catalog.bin` / `index_metadata.bin` / `lsn_counter`) when present.
    ///
    /// Migration is lazy-durable: legacy bytes become the in-memory sections
    /// immediately (readers get identical data), and the legacy files are
    /// removed on the FIRST `flush()` — before that, a crash leaves the old
    /// layout untouched and migration simply re-runs. Until that first
    /// write, sections are served from memory so behavior is unchanged.
    pub fn open_or_migrate(db_path: &Path) -> Result<Self> {
        let path = db_path.join("meta.bin");
        let mut flags = 0u8;
        let mut sections: HashMap<u8, Vec<u8>> = HashMap::new();

        if path.exists() {
            let data = std::fs::read(&path)?;
            if !(data.len() >= 10 && &data[..5] == META_MAGIC) {
                // Not our magic: the authoritative container is unreadable.
                return Err(StorageError::InvalidData(
                    "meta.bin: unrecognized content — metadata corruption detected".to_string(),
                ));
            }
            let version = u16::from_le_bytes(data[5..7].try_into().unwrap());
            if version != META_VERSION {
                return Err(StorageError::InvalidData(format!(
                    "meta.bin: unsupported version {version} (expected {META_VERSION}) — \
                     the database was written by a newer build"
                )));
            }
            flags = data[7];
            let count = u16::from_le_bytes(data[8..10].try_into().unwrap()) as usize;
            let mut pos = 10usize;
            for _ in 0..count {
                if pos + 9 > data.len() {
                    break; // truncated tail — keep what decoded
                }
                let kind = data[pos];
                let len = u32::from_le_bytes(data[pos + 1..pos + 5].try_into().unwrap()) as usize;
                let crc = u32::from_le_bytes(data[pos + 5..pos + 9].try_into().unwrap());
                pos += 9;
                if pos + len > data.len() {
                    break;
                }
                let payload = data[pos..pos + len].to_vec();
                pos += len;
                if crc32fast::hash(&payload) != crc {
                    if kind == MetaSection::Catalog as u8 {
                        // 🔒 Fail fast: a corrupt catalog means the table
                        // directory is unusable. Opening an EMPTY registry
                        // here would let new writes silently shadow whatever
                        // backup the user could have recovered from. (The
                        // legacy standalone catalog.bin errored the same way
                        // — test_corrupt_catalog_clear_error.)
                        return Err(StorageError::InvalidData(
                            "meta.bin: catalog section failed CRC32 — \
                             catalog corruption detected"
                                .to_string(),
                        ));
                    }
                    // Other sections (lsn, index metadata) tolerate loss:
                    // readers fall back to empty/zero — the same semantics
                    // as the legacy missing-file case.
                    continue;
                }
                sections.insert(kind, payload);
            }
        } else {
            // Adopt legacy files (payload bytes are format-identical).
            if let Ok(d) = std::fs::read(db_path.join("catalog.bin")) {
                sections.insert(MetaSection::Catalog as u8, d);
            }
            if let Ok(d) = std::fs::read(db_path.join("index_metadata.bin")) {
                sections.insert(MetaSection::IndexMeta as u8, d);
            }
            if let Ok(d) = std::fs::read(db_path.join("lsn_counter")) {
                if d.len() >= 8 {
                    sections.insert(MetaSection::Lsn as u8, d);
                }
            }
            // Legacy marker files are NOT adopted here: the open path
            // consumes `.clean_close` before this runs (crash-evidence gate),
            // so a fresh flags=0 (crash-verify) is the safe default.
        }

        let store = Self {
            path,
            inner: Mutex::new(MetaInner { flags, sections }),
        };
        // Make the merged state durable right away when migrating (idempotent
        // for the already-migrated case: rewrite only if the file is missing).
        if !store.path.exists() {
            store.flush()?;
        }
        Ok(store)
    }

    /// Payload bytes for a section (None = never persisted).
    pub fn load_section(&self, section: MetaSection) -> Option<Vec<u8>> {
        self.inner.lock().sections.get(&(section as u8)).cloned()
    }

    /// Update one section's payload and durably rewrite the file.
    pub fn update_section(&self, section: MetaSection, payload: Vec<u8>) -> Result<()> {
        {
            let mut inner = self.inner.lock();
            inner.sections.insert(section as u8, payload);
        }
        self.flush()
    }

    /// Read the clean-close flag (does not consume it).
    pub fn clean_close(&self) -> bool {
        self.inner.lock().flags & FLAG_CLEAN_CLOSE != 0
    }

    /// Set the clean-close flag and durably rewrite the file.
    pub fn set_clean_close(&self, v: bool) -> Result<()> {
        {
            let mut inner = self.inner.lock();
            if v {
                inner.flags |= FLAG_CLEAN_CLOSE;
            } else {
                inner.flags &= !FLAG_CLEAN_CLOSE;
            }
        }
        self.flush()
    }

    /// Rewrite the whole file atomically from the in-memory state.
    fn flush(&self) -> Result<()> {
        let inner = self.inner.lock();
        let mut buf =
            Vec::with_capacity(64 + inner.sections.values().map(|s| s.len() + 9).sum::<usize>());
        buf.extend_from_slice(META_MAGIC);
        buf.extend_from_slice(&META_VERSION.to_le_bytes());
        buf.extend_from_slice(&inner.flags.to_le_bytes());
        buf.extend_from_slice(&(inner.sections.len() as u16).to_le_bytes());
        // Deterministic order (by kind) for reproducible files.
        let mut kinds: Vec<&u8> = inner.sections.keys().collect();
        kinds.sort();
        for &k in kinds {
            let payload = &inner.sections[&k];
            buf.push(k);
            buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            buf.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
            buf.extend_from_slice(payload);
        }
        let tmp = self.path.with_extension("bin.tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            use std::io::Write;
            f.write_all(&buf)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        crate::fsync_dir(&self.path);
        // Migration cleanup: once the merged file is durable, the legacy
        // trio is redundant (removal is best-effort; leftovers are inert).
        if let Some(parent) = self.path.parent() {
            let _ = std::fs::remove_file(parent.join("catalog.bin"));
            let _ = std::fs::remove_file(parent.join("index_metadata.bin"));
            let _ = std::fs::remove_file(parent.join("lsn_counter"));
        }
        Ok(())
    }
}

impl std::fmt::Debug for MetaStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock();
        f.debug_struct("MetaStore")
            .field("path", &self.path)
            .field("flags", &inner.flags)
            .field(
                "sections",
                &inner
                    .sections
                    .keys()
                    .map(|k| format!("kind{}", k))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// Convenience: parse a section payload as a little-endian u64 (the LSN
/// section's format). Returns 0 for missing/short payloads — identical to
/// the legacy `load_lsn_counter` fallback.
pub fn payload_as_u64(payload: Option<Vec<u8>>) -> u64 {
    payload
        .and_then(|d| d.try_into().ok().map(u64::from_le_bytes))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_store_roundtrip_and_migration() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path();

        // Legacy layout present.
        std::fs::write(db.join("catalog.bin"), b"CATALOG-PAYLOAD").unwrap();
        std::fs::write(db.join("index_metadata.bin"), b"IDX-PAYLOAD").unwrap();
        std::fs::write(db.join("lsn_counter"), 1234u64.to_le_bytes()).unwrap();

        let ms = MetaStore::open_or_migrate(db).unwrap();
        assert_eq!(
            ms.load_section(MetaSection::Catalog).as_deref(),
            Some(&b"CATALOG-PAYLOAD"[..])
        );
        assert_eq!(payload_as_u64(ms.load_section(MetaSection::Lsn)), 1234);
        assert!(!ms.clean_close());

        // Update persists; legacy files removed after the durable write.
        ms.update_section(MetaSection::Lsn, 5678u64.to_le_bytes().to_vec())
            .unwrap();
        assert!(!db.join("catalog.bin").exists());
        assert!(!db.join("lsn_counter").exists());

        // Reopen: everything loads from meta.bin.
        let ms2 = MetaStore::open_or_migrate(db).unwrap();
        assert_eq!(
            ms2.load_section(MetaSection::Catalog).as_deref(),
            Some(&b"CATALOG-PAYLOAD"[..])
        );
        assert_eq!(payload_as_u64(ms2.load_section(MetaSection::Lsn)), 5678);

        // Clean-close flag round-trips through reopen.
        ms2.set_clean_close(true).unwrap();
        let ms3 = MetaStore::open_or_migrate(db).unwrap();
        assert!(ms3.clean_close());
        ms3.set_clean_close(false).unwrap();
        let ms4 = MetaStore::open_or_migrate(db).unwrap();
        assert!(!ms4.clean_close());
    }

    #[test]
    fn meta_store_corrupt_catalog_fails_fast() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path();
        let ms = MetaStore::open_or_migrate(db).unwrap();
        ms.update_section(MetaSection::Catalog, b"good".to_vec())
            .unwrap();
        // Flip a payload byte on disk.
        let p = db.join("meta.bin");
        let mut data = std::fs::read(&p).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        std::fs::write(&p, &data).unwrap();

        // 🔒 Catalog corruption must FAIL FAST (never an empty registry).
        let err = MetaStore::open_or_migrate(db).expect_err("corrupt catalog must error");
        assert!(
            err.to_string().to_lowercase().contains("catalog"),
            "clear catalog error, got: {err}"
        );
    }

    #[test]
    fn meta_store_corrupt_lsn_section_tolerated() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path();
        let ms = MetaStore::open_or_migrate(db).unwrap();
        ms.update_section(MetaSection::Lsn, 99u64.to_le_bytes().to_vec())
            .unwrap();
        let p = db.join("meta.bin");
        let mut data = std::fs::read(&p).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        std::fs::write(&p, &data).unwrap();

        // LSN loss is tolerable: falls back to 0 (safe direction).
        let ms2 = MetaStore::open_or_migrate(db).unwrap();
        assert_eq!(payload_as_u64(ms2.load_section(MetaSection::Lsn)), 0);
    }
}
