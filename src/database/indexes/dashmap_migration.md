# DashMap 迁移指南 - 索引访问模式

## API 对照表

### 读取操作
```rust
// ❌ 旧：RwLock<HashMap>
let indexes = self.vector_indexes.read();
if let Some(index) = indexes.get(name) {
    // use index
}

// ✅ 新：DashMap
if let Some(index_ref) = self.vector_indexes.get(name) {
    let index = index_ref.value();
    // use index
}
```

### 写入操作
```rust
// ❌ 旧：RwLock<HashMap>
self.vector_indexes.write().insert(name.to_string(), Arc::new(RwLock::new(index)));

// ✅ 新：DashMap
self.vector_indexes.insert(name.to_string(), Arc::new(RwLock::new(index)));
```

### 检查键是否存在
```rust
// ❌ 旧：RwLock<HashMap>
if self.column_indexes.read().contains_key(&index_name) { ... }

// ✅ 新：DashMap
if self.column_indexes.contains_key(&index_name) { ... }
```

### 遍历所有键值
```rust
// ❌ 旧：RwLock<HashMap>
let indexes = self.vector_indexes.read();
for (name, index) in indexes.iter() {
    // process
}

// ✅ 新：DashMap
for entry in self.vector_indexes.iter() {
    let name = entry.key();
    let index = entry.value();
    // process
}
```

### 收集所有值
```rust
// ❌ 旧：RwLock<HashMap>
let indexes_to_flush: Vec<_> = {
    let indexes = self.vector_indexes.read();
    indexes.values().cloned().collect()
};

// ✅ 新：DashMap
let indexes_to_flush: Vec<_> = self.vector_indexes.iter()
    .map(|entry| entry.value().clone())
    .collect();
```

## 关键优化点

1. **无锁读取** - DashMap 使用分片锁，读取性能提升 3-10x
2. **细粒度锁** - 不同键的操作可以并发执行
3. **简化 API** - 不需要显式获取 read/write lock
