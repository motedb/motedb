//! B+Tree Integrity Tests — scale, crash safety, correctness under load
//!
//! Run: cargo test --release --test test_btree_integrity -- --nocapture --test-threads=1

use motedb::index::btree_generic::{GenericBTree, GenericBTreeConfig};
use tempfile::TempDir;

/// 100K insert→flush→verify cycle — catches split bugs, superblock overflow
#[test]
#[ignore = "slow in debug, run with --ignored"]
fn test_btree_insert_flush_verify_100k() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test_100k.bt");
    let mut btree = GenericBTree::<u64>::with_config(path, GenericBTreeConfig::default()).unwrap();

    let n = 100_000u64;
    for i in 0..n {
        if i % 10000 == 0 {
            eprintln!("  insert {}...", i);
        }
        btree.insert(i, i.to_le_bytes().to_vec()).unwrap();
    }

    eprintln!("  flushing...");
    btree.flush().unwrap();

    eprintln!("  verifying {} entries...", n);
    for i in 0..n {
        let val = btree.get(&i).unwrap();
        assert!(val.is_some(), "key {} not found after flush", i);
        assert_eq!(
            val.unwrap(),
            i.to_le_bytes().to_vec(),
            "wrong value for key {}",
            i
        );
    }
    eprintln!("  OK: all {} entries verified", n);
}

/// interleaved insert+get: queries should see already-inserted keys
#[test]
#[ignore = "slow in debug, run with --ignored"]
fn test_btree_insert_and_query_interleaved() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test_interleaved.bt");
    let mut btree = GenericBTree::<u64>::with_config(path, GenericBTreeConfig::default()).unwrap();

    let batch = 500;
    for round in 0..20 {
        let base = round * batch;
        for i in 0..batch {
            btree
                .insert((base + i) as u64, vec![(i % 256) as u8])
                .unwrap();
        }
        // verify all previously inserted keys
        for i in 0..=(base + batch - 1) {
            let val = btree.get(&(i as u64)).unwrap();
            assert!(val.is_some(), "key {} lost at round {}", i, round);
        }
    }
    eprintln!("  OK: interleaved insert+get passed");
}

/// Delete then re-insert: deleted keys should vanish, re-inserted should reappear
#[test]
#[ignore = "slow in debug, run with --ignored"]
fn test_btree_delete_and_reinsert() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test_delete.bt");
    let mut btree = GenericBTree::<u64>::with_config(path, GenericBTreeConfig::default()).unwrap();

    // Insert 1000 keys
    for i in 0..1000u64 {
        btree.insert(i, i.to_le_bytes().to_vec()).unwrap();
    }

    // Delete even keys
    for i in (0..1000u64).step_by(2) {
        let old = btree.delete(&i).unwrap();
        assert!(old.is_some(), "delete {} should return old value", i);
    }

    // Verify odd keys exist, even keys gone
    for i in 0..1000u64 {
        if i % 2 == 0 {
            assert!(
                btree.get(&i).unwrap().is_none(),
                "even key {} should be deleted",
                i
            );
        } else {
            assert!(
                btree.get(&i).unwrap().is_some(),
                "odd key {} should exist",
                i
            );
        }
    }

    // Re-insert even keys with new value
    for i in (0..1000u64).step_by(2) {
        let old = btree.insert(i, (i * 10).to_le_bytes().to_vec()).unwrap();
        assert!(
            old.is_none(),
            "re-insert should not return old value for deleted key {}",
            i
        );
    }

    // All keys exist again
    for i in 0..1000u64 {
        assert!(
            btree.get(&i).unwrap().is_some(),
            "key {} should exist after re-insert",
            i
        );
    }

    // Flush and re-verify
    btree.flush().unwrap();
    for i in 0..1000u64 {
        assert!(
            btree.get(&i).unwrap().is_some(),
            "key {} should exist after flush",
            i
        );
    }
    eprintln!("  OK: delete+reinsert verified");
}

/// Range queries across split boundaries
#[test]
#[ignore = "slow in debug, run with --ignored"]
fn test_btree_range_query() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test_range.bt");
    let mut btree = GenericBTree::<u64>::with_config(path, GenericBTreeConfig::default()).unwrap();

    let n = 5000u64;
    for i in 0..n {
        btree.insert(i, vec![(i % 256) as u8]).unwrap();
    }

    // Full range
    let all = btree.range(&0, &(n - 1)).unwrap();
    assert_eq!(all.len(), n as usize);

    // Sub-range
    let sub = btree.range(&1000, &1999).unwrap();
    assert_eq!(sub.len(), 1000);
    for (i, (key, _)) in sub.iter().enumerate() {
        assert_eq!(*key, 1000 + i as u64);
    }

    // Range with limit
    let limited = btree.range_with_limit(&0, &(n - 1), 50).unwrap();
    assert_eq!(limited.len(), 50);

    // Empty range
    let empty = btree.range(&(n + 1), &(n + 100)).unwrap();
    assert!(empty.is_empty());

    eprintln!("  OK: range queries verified");
}

/// Two B+Tree instances on different files — no cross-contamination
#[test]
#[ignore = "slow in debug, run with --ignored"]
fn test_two_independent_trees() {
    let dir = TempDir::new().unwrap();
    let path_a = dir.path().join("tree_a.bt");
    let path_b = dir.path().join("tree_b.bt");
    let mut a = GenericBTree::<u64>::with_config(path_a, GenericBTreeConfig::default()).unwrap();
    let mut b = GenericBTree::<u64>::with_config(path_b, GenericBTreeConfig::default()).unwrap();

    for i in 0..1000u64 {
        a.insert(i, vec![1u8]).unwrap();
        b.insert(i + 10000, vec![2u8]).unwrap();
    }

    a.flush().unwrap();
    b.flush().unwrap();

    // A should only have keys 0..999
    assert!(a.get(&0).unwrap().is_some());
    assert!(a.get(&999).unwrap().is_some());
    assert!(a.get(&10000).unwrap().is_none());

    // B should only have keys 10000..10999
    assert!(b.get(&10000).unwrap().is_some());
    assert!(b.get(&10999).unwrap().is_some());
    assert!(b.get(&0).unwrap().is_none());

    eprintln!("  OK: independent trees verified");
}
