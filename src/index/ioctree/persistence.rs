//! Persistence for i-Octree: save/load with v2 disk-first format and v1 migration
//!
//! v2 format: tree structure (inner nodes + leaf handles) → bincode
//!            leaf data → separate LeafStore file
//!
//! v1 migration: on load, converts old Vec<IndexedPoint3D> format to v2

use super::{IOctreeConfig, IOctreeIndex};
use super::leaf_store::LeafStore;
use crate::types::BoundingBox3D;
use crate::{Result, StorageError};
use std::io::{BufReader, BufWriter, Read, Write};

const MAGIC: u32 = 0x10C7_10EE;
const VERSION_V1: u32 = 1;
const VERSION_V2: u32 = 2;

fn io_err(e: std::io::Error) -> StorageError {
    StorageError::Io(e)
}

/// Save an i-Octree index to disk (v2 format)
pub fn save(tree: &IOctreeIndex, path: &std::path::Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = std::fs::File::create(path).map_err(io_err)?;
    let mut writer = BufWriter::new(file);

    // Header
    writer.write_all(&MAGIC.to_le_bytes()).map_err(io_err)?;
    writer.write_all(&VERSION_V2.to_le_bytes()).map_err(io_err)?;
    writer.write_all(&(tree.size as u64).to_le_bytes()).map_err(io_err)?;

    // Config
    let config_bytes = bincode::serialize(&tree.config)
        .map_err(|e| StorageError::InvalidData(format!("Serialize config: {}", e)))?;
    writer.write_all(&(config_bytes.len() as u32).to_le_bytes()).map_err(io_err)?;
    writer.write_all(&config_bytes).map_err(io_err)?;

    // World bounds
    let bounds = &tree.world_bounds;
    for val in &[bounds.min_x, bounds.min_y, bounds.min_z, bounds.max_x, bounds.max_y, bounds.max_z] {
        writer.write_all(&val.to_le_bytes()).map_err(io_err)?;
    }

    // Name
    let name_bytes = tree.name.as_bytes();
    writer.write_all(&(name_bytes.len() as u32).to_le_bytes()).map_err(io_err)?;
    writer.write_all(name_bytes).map_err(io_err)?;

    // Tree structure (Octant with leaf handles)
    let tree_bytes = bincode::serialize(&tree.root)
        .map_err(|e| StorageError::InvalidData(format!("Serialize tree: {}", e)))?;
    writer.write_all(&tree_bytes).map_err(io_err)?;

    // CRC32 footer
    let crc = crc32fast::hash(&tree_bytes);
    writer.write_all(&crc.to_le_bytes()).map_err(io_err)?;

    writer.flush().map_err(io_err)?;
    // fsync for crash safety — without this, a power failure after save()
    // could leave a truncated/corrupt index file
    writer.get_ref().sync_all().map_err(io_err)?;
    Ok(())
}

/// Load an i-Octree index from disk (supports v1 and v2)
pub fn load(path: &std::path::Path, _config: IOctreeConfig, _name: String) -> Result<IOctreeIndex> {
    let file = std::fs::File::open(path)
        .map_err(|e| StorageError::InvalidData(format!("Open {}: {}", path.display(), e)))?;
    let mut reader = BufReader::new(file);

    let mut buf4 = [0u8; 4];

    // Magic
    reader.read_exact(&mut buf4).map_err(io_err)?;
    let magic = u32::from_le_bytes(buf4);
    if magic != MAGIC {
        return Err(StorageError::InvalidData(format!("Invalid i-Octree file: bad magic {:x}", magic)));
    }

    // Version
    reader.read_exact(&mut buf4).map_err(io_err)?;
    let version = u32::from_le_bytes(buf4);

    match version {
        VERSION_V1 => load_v1(&mut reader, path),
        VERSION_V2 => load_v2(&mut reader, path),
        _ => Err(StorageError::InvalidData(format!("Unsupported i-Octree version {}", version))),
    }
}

/// Load v2 format
fn load_v2(reader: &mut BufReader<std::fs::File>, path: &std::path::Path) -> Result<IOctreeIndex> {
    let mut buf4 = [0u8; 4];
    let mut buf8 = [0u8; 8];

    reader.read_exact(&mut buf8).map_err(io_err)?;
    let size = u64::from_le_bytes(buf8) as usize;

    // Config
    reader.read_exact(&mut buf4).map_err(io_err)?;
    let config_len = u32::from_le_bytes(buf4) as usize;
    let mut config_buf = vec![0u8; config_len];
    reader.read_exact(&mut config_buf).map_err(io_err)?;
    let config: IOctreeConfig = bincode::deserialize(&config_buf)
        .map_err(|e| StorageError::InvalidData(format!("Deserialize config: {}", e)))?;

    // World bounds
    let read_f64 = |reader: &mut BufReader<std::fs::File>| -> Result<f64> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf).map_err(io_err)?;
        Ok(f64::from_le_bytes(buf))
    };
    let world_bounds = BoundingBox3D::new(
        read_f64(reader)?, read_f64(reader)?, read_f64(reader)?,
        read_f64(reader)?, read_f64(reader)?, read_f64(reader)?,
    );

    // Name
    reader.read_exact(&mut buf4).map_err(io_err)?;
    let name_len = u32::from_le_bytes(buf4) as usize;
    let mut name_buf = vec![0u8; name_len];
    reader.read_exact(&mut name_buf).map_err(io_err)?;
    let name = String::from_utf8(name_buf)
        .map_err(|e| StorageError::InvalidData(format!("Invalid name: {}", e)))?;

    // Tree bytes + CRC
    let mut tree_buf = Vec::new();
    reader.read_to_end(&mut tree_buf).map_err(io_err)?;
    if tree_buf.len() < 4 {
        return Err(StorageError::InvalidData("Truncated i-Octree file".into()));
    }
    let crc_bytes = tree_buf.split_off(tree_buf.len() - 4);
    let stored_crc = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
    let computed_crc = crc32fast::hash(&tree_buf);
    if stored_crc != computed_crc {
        return Err(StorageError::InvalidData(
            format!("CRC mismatch: stored={:x}, computed={:x}", stored_crc, computed_crc),
        ));
    }

    let root = bincode::deserialize(&tree_buf)
        .map_err(|e| StorageError::InvalidData(format!("Deserialize tree: {}", e)))?;

    // Set up work_dir for LeafStore/WAL (parent of the ioctree.bin file)
    let work_dir = config.data_dir.as_ref()
        .map(|p| {
            if p.extension().map(|e| e == "bin").unwrap_or(false) {
                p.parent().unwrap_or(p).to_path_buf()
            } else {
                p.clone()
            }
        })
        .unwrap_or_else(|| path.parent().unwrap_or(std::path::Path::new(".")).to_path_buf());

    let leaf_store = LeafStore::open(&work_dir, config.cache_capacity())?;

    Ok(IOctreeIndex {
        root,
        config,
        size,
        world_bounds,
        name,
        leaf_store,
    })
}

/// Load v1 format and migrate to v2
fn load_v1(reader: &mut BufReader<std::fs::File>, path: &std::path::Path) -> Result<IOctreeIndex> {
    let mut buf4 = [0u8; 4];
    let mut buf8 = [0u8; 8];

    reader.read_exact(&mut buf8).map_err(io_err)?;
    let size = u64::from_le_bytes(buf8) as usize;

    // Config
    reader.read_exact(&mut buf4).map_err(io_err)?;
    let config_len = u32::from_le_bytes(buf4) as usize;
    let mut config_buf = vec![0u8; config_len];
    reader.read_exact(&mut config_buf).map_err(io_err)?;
    let config: IOctreeConfig = bincode::deserialize(&config_buf)
        .map_err(|e| StorageError::InvalidData(format!("Deserialize config: {}", e)))?;

    // World bounds
    let read_f64 = |reader: &mut BufReader<std::fs::File>| -> Result<f64> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf).map_err(io_err)?;
        Ok(f64::from_le_bytes(buf))
    };
    let world_bounds = BoundingBox3D::new(
        read_f64(reader)?, read_f64(reader)?, read_f64(reader)?,
        read_f64(reader)?, read_f64(reader)?, read_f64(reader)?,
    );

    // Name
    reader.read_exact(&mut buf4).map_err(io_err)?;
    let name_len = u32::from_le_bytes(buf4) as usize;
    let mut name_buf = vec![0u8; name_len];
    reader.read_exact(&mut name_buf).map_err(io_err)?;
    let name = String::from_utf8(name_buf)
        .map_err(|e| StorageError::InvalidData(format!("Invalid name: {}", e)))?;

    // v1 tree bytes (old format with Vec<IndexedPoint3D>)
    let mut tree_buf = Vec::new();
    reader.read_to_end(&mut tree_buf).map_err(io_err)?;
    if tree_buf.len() < 4 {
        return Err(StorageError::InvalidData("Truncated i-Octree file".into()));
    }
    let crc_bytes = tree_buf.split_off(tree_buf.len() - 4);
    let stored_crc = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
    let computed_crc = crc32fast::hash(&tree_buf);
    if stored_crc != computed_crc {
        return Err(StorageError::InvalidData(
            format!("CRC mismatch: stored={:x}, computed={:x}", stored_crc, computed_crc),
        ));
    }

    // Set up data_dir for v2
    let data_dir = config.data_dir.clone().unwrap_or_else(|| {
        path.parent().unwrap_or(std::path::Path::new(".")).to_path_buf()
    });

    let leaf_store = LeafStore::open(&data_dir, config.cache_capacity())?;

    // Migrate v1 tree to v2: extract Vec<IndexedPoint3D> → LeafStore leaf_ids
    let legacy_root: LegacyOctant = bincode::deserialize(&tree_buf)
        .map_err(|e| StorageError::InvalidData(format!("Deserialize v1 tree: {}", e)))?;
    let root = migrate_octant(legacy_root, &leaf_store)?;

    Ok(IOctreeIndex {
        root,
        config,
        size,
        world_bounds,
        name,
        leaf_store,
    })
}

// === v1 legacy types for migration ===

use super::node::IndexedPoint3D;

/// v1 Octant format (with inline Vec<IndexedPoint3D>)
#[derive(serde::Deserialize)]
enum LegacyOctant {
    Inner {
        center: [f64; 3],
        extent: f64,
        children: Box<[Option<Box<LegacyOctant>>; 8]>,
        size: usize,
    },
    Leaf {
        center: [f64; 3],
        extent: f64,
        points: Vec<IndexedPoint3D>,
    },
}

fn migrate_octant(legacy: LegacyOctant, store: &LeafStore) -> Result<super::node::Octant> {
    match legacy {
        LegacyOctant::Inner { center, extent, children, size } => {
            let new_children: Box<[Option<Box<super::node::Octant>>; 8]> = {
                let mut result: Vec<Option<Box<super::node::Octant>>> = Vec::with_capacity(8);
                for opt in children.into_iter() {
                    match opt {
                        Some(c) => result.push(Some(Box::new(migrate_octant(*c, store)?))),
                        None => result.push(None),
                    }
                }
                result.into_boxed_slice().try_into().unwrap()
            };
            Ok(super::node::Octant::Inner { center, extent, children: new_children, size })
        }
        LegacyOctant::Leaf { center, extent, points } => {
            let point_count = points.len() as u32;
            let leaf_id = store.create_leaf(points)
                .map_err(|e| StorageError::Index(format!("Failed to create leaf during migration: {}", e)))?;
            Ok(super::node::Octant::Leaf { center, extent, leaf_id, point_count })
        }
    }
}
