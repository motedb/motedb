//! Single-file database snapshot container (`MOTESNAP`) — P4 file-layout
//! convergence: the "transport form" of a database directory.
//!
//! One `.mote`-style archive holds every durable file of the database
//! (columnar segments + global manifest, indexes incl. FTS directories,
//! meta.bin, legacy-engine data) so a robot-fleet database can be copied,
//! rsync'd, checksum'd and shipped as ONE file. The runtime layout stays a
//! directory (see docs/design/2026-10-file-layout.md).
//!
//! ## Format (v1)
//! ```text
//! header:  b"MOTESNAP" | version u16 | flags u16 (bit0=zstd) | entry_count u32
//! entry*:  name_len u16 | name bytes (UTF-8, '/'-separated) | logical_size u64
//!          | stored_size u64 | crc32(logical) u32 | stored bytes
//! footer:  b"MOTESNAP"
//! ```
//! Entries stream sequentially — no up-front TOC, no seeking, memory-bounded
//! (1 MiB copy buffer). With the zstd flag each entry's bytes are an
//! independent zstd frame (level 3). `crc32` always covers the LOGICAL
//! (uncompressed) bytes, so corruption is detected identically in both
//! modes.
//!
//! ## Exclusions (live-state, not data)
//! `.lock`, `wal/`, `*.tmp`, `.clean_close`, legacy `fts_indexes.fresh`.
//! The importer opens with the standard crash-verify path (meta.bin's
//! clean-close flag is 0 on a live database), which is exactly the right
//! amount of paranoia for a copy that was mid-flight.
//!
//! ## Durability contract
//! The exporter (see `MoteDB::export_snapshot_to`) quiesces and checkpoints
//! BEFORE packing — everything the archive contains is already durable in
//! segments + meta.bin. The archive itself is written via temp + fsync +
//! rename so a crash never leaves a half snapshot under the final name.

use crate::{Result, StorageError};
use std::io::{Read, Write};
use std::path::Path;

const SNAP_MAGIC: &[u8; 8] = b"MOTESNAP";
const SNAP_VERSION: u16 = 1;
const FLAG_ZSTD: u16 = 0b0000_0000_0000_0001;
const ZSTD_LEVEL: i32 = 3;
const COPY_BUF: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SnapshotStats {
    pub entries: u32,
    pub logical_bytes: u64,
    pub stored_bytes: u64,
}

/// Files/directories that never belong in a snapshot (live-state artifacts).
/// True when `path` is an existing FILE carrying the MOTESNAP magic —
/// used by `Database::open` to auto-route container opens to read-only mode.
pub fn is_motesnap_container(path: &Path) -> bool {
    use std::io::Read;
    if !path.is_file() {
        return false;
    }
    let Ok(mut f) = std::fs::File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 8];
    matches!(f.read_exact(&mut magic), Ok(())) && &magic == SNAP_MAGIC
}

fn excluded(rel: &str) -> bool {
    if rel == ".lock" || rel == ".clean_close" || rel == "fts_indexes.fresh" {
        return true;
    }
    if rel == "wal" || rel.starts_with("wal/") {
        return true;
    }
    if rel.ends_with(".tmp") {
        return true;
    }
    false
}

fn collect_files(dir: &Path, rel: &str, out: &mut Vec<(String, u64)>) -> Result<()> {
    let rd = std::fs::read_dir(dir).map_err(StorageError::Io)?;
    for e in rd.flatten() {
        let p = e.path();
        let name = e.file_name();
        let Some(name) = name.to_str() else { continue };
        let child_rel = if rel.is_empty() {
            name.to_string()
        } else {
            format!("{rel}/{name}")
        };
        if excluded(&child_rel) {
            continue;
        }
        if p.is_dir() {
            collect_files(&p, &child_rel, out)?;
        } else {
            let len = e.metadata().map(|m| m.len()).unwrap_or(0);
            out.push((child_rel, len));
        }
    }
    Ok(())
}

/// Pack `src_dir` into a single snapshot file at `dest` (atomic publish).
///
/// When `compressed`, each file is spooled through a temp file as an
/// independent zstd frame (stored_size must precede the bytes, so the frame
/// is materialized before the header is written).
pub fn pack_directory(src_dir: &Path, dest: &Path, compressed: bool) -> Result<SnapshotStats> {
    if let Some(parent) = dest.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let tmp_dest = dest.with_extension("motesnap.tmp");
    let mut files = Vec::new();
    collect_files(src_dir, "", &mut files)?;
    files.sort();

    let stats = {
        let mut out =
            std::io::BufWriter::with_capacity(COPY_BUF, std::fs::File::create(&tmp_dest)?);
        out.write_all(SNAP_MAGIC)?;
        out.write_all(&SNAP_VERSION.to_le_bytes())?;
        out.write_all(&(if compressed { FLAG_ZSTD } else { 0 }).to_le_bytes())?;
        out.write_all(&(files.len() as u32).to_le_bytes())?;

        let mut written: u64 = 16; // header
        let mut logical_bytes: u64 = 0;
        let spool = dest.with_extension("spool.tmp");
        for (rel, _len) in &files {
            let src_path = src_dir.join(rel);
            let (stored_len, crc) = if compressed {
                // Spool one zstd frame: sizes must precede the bytes.
                let logical = std::fs::File::open(&src_path)?;
                let mut enc = zstd::stream::write::Encoder::new(
                    std::io::BufWriter::new(std::fs::File::create(&spool)?),
                    ZSTD_LEVEL,
                )?;
                let crc = copy_and_hash(&mut { logical }, &mut enc)?;
                let mut spooled = enc.finish()?; // flush frame, hand back the BufWriter
                spooled.flush()?;
                let len = spooled.get_ref().metadata()?.len();
                spooled.get_ref().sync_all()?;
                (len, crc)
            } else {
                let mut f = std::fs::File::open(&src_path)?;
                let mut hasher = crc32fast::Hasher::new();
                let mut total = 0u64;
                let mut buf = vec![0u8; COPY_BUF];
                loop {
                    let n = f.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                    total += n as u64;
                }
                (total, hasher.finalize())
            };
            let logical_len = if compressed {
                // Recompute cheaply? No — logical size came from collect; the
                // file is quiesced (exporter holds the write stripes), so
                // metadata len == bytes hashed.
                std::fs::metadata(&src_path)?.len()
            } else {
                stored_len
            };

            let name_bytes = rel.as_bytes();
            if name_bytes.len() > u16::MAX as usize {
                return Err(StorageError::InvalidData(format!(
                    "snapshot entry name too long: {rel}"
                )));
            }
            let entry_header_len = 2 + name_bytes.len() as u64 + 8 + 8 + 4;
            out.write_all(&(name_bytes.len() as u16).to_le_bytes())?;
            out.write_all(name_bytes)?;
            out.write_all(&logical_len.to_le_bytes())?;
            out.write_all(&stored_len.to_le_bytes())?;
            out.write_all(&crc.to_le_bytes())?;
            if compressed {
                let mut sp = std::fs::File::open(&spool)?;
                written += std::io::copy(&mut sp, &mut out)?;
                let _ = std::fs::remove_file(&spool);
            } else {
                let mut f = std::fs::File::open(&src_path)?;
                written += std::io::copy(&mut f, &mut out)?;
            }
            written += entry_header_len;
            logical_bytes += logical_len;
        }
        out.write_all(SNAP_MAGIC)?; // footer
        written += SNAP_MAGIC.len() as u64;
        out.flush()?;
        use std::io::Write;
        let f = out
            .into_inner()
            .map_err(|e| StorageError::Io(e.into_error()))?;
        f.sync_all()?;
        SnapshotStats {
            entries: files.len() as u32,
            logical_bytes,
            stored_bytes: written,
        }
    };

    std::fs::rename(&tmp_dest, dest)?;
    crate::fsync_dir(dest);
    Ok(stats)
}

fn copy_and_hash<R: Read, W: Write>(src: &mut R, dst: &mut W) -> Result<u32> {
    let mut hasher = crc32fast::Hasher::new();
    let mut buf = vec![0u8; COPY_BUF];
    loop {
        let n = src.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        dst.write_all(&buf[..n])?;
    }
    Ok(hasher.finalize())
}

/// Unpack a snapshot into `dest_dir` (which must not exist yet).
///
/// Every entry's CRC32 is verified against its logical bytes; any mismatch
/// or truncation aborts the import with the destination only partially
/// written (the caller should remove it — importing again is idempotent
/// from the archive side).
pub fn unpack_snapshot(src: &Path, dest_dir: &Path) -> Result<SnapshotStats> {
    if dest_dir.exists() {
        return Err(StorageError::InvalidData(format!(
            "import destination already exists: {}",
            dest_dir.display()
        )));
    }
    std::fs::create_dir_all(dest_dir)?;
    let mut f = std::fs::File::open(src)?;
    let mut header = [0u8; 16];
    read_exact_or_eof(&mut f, &mut header)?;
    if &header[..8] != SNAP_MAGIC {
        return Err(StorageError::InvalidData(
            "not a MOTESNAP snapshot (bad magic)".into(),
        ));
    }
    let version = u16::from_le_bytes(header[8..10].try_into().unwrap());
    if version != SNAP_VERSION {
        return Err(StorageError::InvalidData(format!(
            "unsupported snapshot version {version} (expected {SNAP_VERSION})"
        )));
    }
    let flags = u16::from_le_bytes(header[10..12].try_into().unwrap());
    let compressed = flags & FLAG_ZSTD != 0;
    let count = u32::from_le_bytes(header[12..16].try_into().unwrap());

    let mut logical_bytes = 0u64;
    let mut stored_bytes = 16u64;
    for _ in 0..count {
        let mut nlen = [0u8; 2];
        read_exact_or_eof(&mut f, &mut nlen)?;
        let mut name = vec![0u8; u16::from_le_bytes(nlen) as usize];
        read_exact_or_eof(&mut f, &mut name)?;
        let name = String::from_utf8(name)
            .map_err(|_| StorageError::InvalidData("snapshot entry name is not UTF-8".into()))?;
        // Path traversal guard: entries are relative, no `..` components.
        if name.starts_with('/') || name.split('/').any(|c| c == "..") {
            return Err(StorageError::InvalidData(format!(
                "unsafe snapshot entry name: {name}"
            )));
        }
        let mut nums = [0u8; 20];
        read_exact_or_eof(&mut f, &mut nums)?;
        let logical_len = u64::from_le_bytes(nums[0..8].try_into().unwrap());
        let stored_len = u64::from_le_bytes(nums[8..16].try_into().unwrap());
        let expect_crc = u32::from_le_bytes(nums[16..20].try_into().unwrap());

        let out_path = dest_dir.join(&name);
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut out =
            std::io::BufWriter::with_capacity(COPY_BUF, std::fs::File::create(&out_path)?);
        let got_crc = if compressed {
            let mut take = (&mut f).take(stored_len);
            let mut dec = zstd::stream::read::Decoder::new(&mut take)?.single_frame();
            copy_and_hash(&mut dec, &mut out)?
        } else {
            let mut take = (&mut f).take(stored_len);
            copy_and_hash(&mut take, &mut out)?
        };
        out.flush()?;
        let logical_actual = std::fs::metadata(&out_path)?.len();
        stored_bytes += 9 + name.len() as u64 + 20 + stored_len;
        if got_crc != expect_crc || logical_actual != logical_len {
            return Err(StorageError::InvalidData(format!(
                "snapshot entry '{name}' failed verification (crc {got_crc:#x} vs {expect_crc:#x}, \
                 size {logical_actual} vs {logical_len})"
            )));
        }
        logical_bytes += logical_len;
    }
    let mut footer = [0u8; 8];
    read_exact_or_eof(&mut f, &mut footer)?;
    if &footer != SNAP_MAGIC {
        return Err(StorageError::InvalidData(
            "snapshot truncated (missing footer)".into(),
        ));
    }

    Ok(SnapshotStats {
        entries: count,
        logical_bytes,
        stored_bytes,
    })
}

fn read_exact_or_eof(f: &mut std::fs::File, buf: &mut [u8]) -> Result<()> {
    use std::io::Read;
    let mut off = 0;
    while off < buf.len() {
        let n = f.read(&mut buf[off..])?;
        if n == 0 {
            return Err(StorageError::InvalidData(
                "snapshot truncated (unexpected EOF)".into(),
            ));
        }
        off += n;
    }
    Ok(())
}

use std::io::Take;

/// Marker to keep the Take import local to the decode path.
#[allow(dead_code)]
fn _take_type_check(_: &mut Take<&mut std::fs::File>) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snap_roundtrip_plain_and_compressed() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(src.join("columnar_ms/t")).unwrap();
        std::fs::write(src.join("meta.bin"), b"meta-bytes-000000000").unwrap();
        std::fs::write(src.join("columnar_ms/t/1.sst"), vec![7u8; 100_000]).unwrap();
        // Excluded artifacts must not appear in the archive.
        std::fs::write(src.join(".lock"), b"").unwrap();
        std::fs::create_dir_all(src.join("wal")).unwrap();
        std::fs::write(src.join("wal/partition_0.wal"), b"w").unwrap();
        std::fs::write(src.join(".writebuf.tmp"), b"t").unwrap();

        for compressed in [false, true] {
            let snap = dir.path().join(format!("s{}.mote", compressed as u8));
            let stats = pack_directory(&src, &snap, compressed).unwrap();
            assert_eq!(stats.entries, 2, "only data files packed");
            // Compressible payload must actually shrink.
            if compressed {
                assert!(stats.stored_bytes < stats.logical_bytes);
            }

            let out = dir.path().join(format!("out{}", compressed as u8));
            let rs = unpack_snapshot(&snap, &out).unwrap();
            assert_eq!(rs.entries, 2);
            assert_eq!(rs.logical_bytes, stats.logical_bytes);
            assert_eq!(
                std::fs::read(out.join("meta.bin")).unwrap(),
                b"meta-bytes-000000000"
            );
            assert_eq!(
                std::fs::read(out.join("columnar_ms/t/1.sst")).unwrap(),
                vec![7u8; 100_000]
            );
            assert!(!out.join(".lock").exists());
            assert!(!out.join("wal").exists());
        }
    }

    #[test]
    fn snap_corruption_detected() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("meta.bin"), b"payload-here").unwrap();
        let snap = dir.path().join("s.mote");
        pack_directory(&src, &snap, false).unwrap();

        // Flip one payload byte.
        let mut data = std::fs::read(&snap).unwrap();
        let last = data.len() - 1 - SNAP_MAGIC.len(); // inside the last entry's data
        data[last] ^= 0xFF;
        std::fs::write(&snap, &data).unwrap();

        let out = dir.path().join("out");
        let err = unpack_snapshot(&snap, &out).expect_err("must fail");
        assert!(
            err.to_string().to_lowercase().contains("verification"),
            "crc error, got: {err}"
        );
    }
}
