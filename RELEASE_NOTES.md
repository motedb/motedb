# MoteDB v0.10.0

正确性与 SQL 兼容性专注版：两轮差分 fuzz campaign（SQLite 作为 oracle 的三路
对拍 + 变异复跑 + 重开一致性）挖出并修复 17 个正确性/语义问题，4 个常驻
回归 harness 入库。性能经 compete_bench 对照复核无回退（JOIN 18.9→16.7ms、
GROUP BY 2.1→1.57ms、knn 7.9→6.5ms @100K×384 基准）。

## 正确性修复（差分对拍挖出）

- SQL 三值逻辑 ×5：JOIN/WHERE 快过滤 `<>`+NULL 误纳 NULL 行；行内解释器
  AND/OR 二值化（`NOT (NULL比较 OR …)` 全行通过）；聚合谓词 NULL 全序误用
  （`grp > NULL` 匹配全部行）
- 排序 ×8：JOIN ORDER BY 未投影列裸名跨列误匹配（三处）；join LIMIT 提前
  终止先于排序（LIMIT 边界取错行）；LATEST BY+ORDER BY 索引错位 + 输出乱序；
  GEOMETRY `loc <-> ST_POINT(...)` 静默返回插入序；ST_POINT 未实现
  求值；L2Distance 不认 Spatial 操作数
- 快照一致性 ×2：`IN (SELECT …)` 直读列存段绕过墓碑/写集（DELETE/UPDATE
  后返回已删行）；子查询 WHERE 编译失败静默丢过滤
- 其他 ×2：ROUND 改为对 f64 二进制真值精确十进制 half-away 舍入（对齐
  SQLite/MySQL）；非 AUTO_INCREMENT PK 无列索引时点查询硬报错 → 回落全扫

## 新能力

- `GROUP BY` 表达式（`id % 5`、`CASE WHEN …`）与 SELECT 别名
- `INSTR(haystack, needle)`：1-based、未命中 0、NULL→NULL
- `ST_POINT(x, y[, z])` 通用求值 + 几何 `loc <-> ST_POINT(...)` 距离排序
- JOIN `ORDER BY` 未投影列多键排序（combined 全列行上解析）

## 行为变更（升级注意）

- `CONCAT` 跳过 NULL 参数（对齐 SQLite concat()/PostgreSQL；`||` 保持 NULL
  传播；全 NULL → 空串）。此前任一 NULL 参数得 NULL。

## 质量基建

- `bindings/python/tests/`：test_fuzz_differential.py（SQLite oracle 三路
  对拍 + 变异复跑 + 跨相位自洽）、test_fuzz_bigtable.py（多段列存 + 重开
  一致性，有界查询）、test_feature_selfcheck.py（KNN/LATEST BY/BM25/空间/
  事务暴力自洽）、E2E ×2 —— 全部常驻回归门槛
- `tests/test_round13_bug_hunt.rs` 13 例 Rust 回归
- 全量 cargo test 229 个测试二进制 EXIT=0

## 已知限制

- `BM25_SCORE()` 需先 `CREATE TEXT INDEX`（无索引时 MATCH 过滤可用、分数
  为 NULL）
- TIMESERIES 行不可变：UPDATE 报错并提示 DELETE(时间范围)+重插
- 多列 IN `(a, b) IN ((…))` 不支持（明确 parse error，非静默错误）
- Python 绑定 params 必须为 list（宽松计数是文档化 footgun）
