# MoteDB 版本政策

本文件规定 MoteDB 的语义化版本(semver)政策与**存储格式兼容性政策** —— 对
嵌入式数据库,后者直接决定用户能否升级,是 1.0 承诺的核心。

## 1. semver(代码 API)

- **0.x(当前)**:minor(`0.15 → 0.16`)允许破坏性 API 变更;patch 只修
  bug。Rust crate / Python wheel / npm 三端版本号保持一致。
- **1.x**:minor 允许新增功能与不改变既有签名语义的重构;任何破坏性变更
  (移除/重命名公开项、改变可观察行为语义)只在 **2.0**。
- 公开面定义:
  - Rust:`motedb` crate 的公开项(re-exports + `pub` 模块)。发布前用
    `cargo public-api` 生成清单对比(`docs/api/`),非新增即需说明。
  - Python:`motedb` 包的类与方法(`docs/14-api-reference.md` 冻结清单)。
  - Node:`motedb` npm 包的导出对象与 TS 类型。
- SQL 语法面按"新增不破坏"处理:新增语法/函数不升 minor 不算破坏;改变
  既有语句的返回语义算破坏,只在 2.0。

## 2. 存储格式版本政策

**承诺:1.x 内,任何 1.x 版本打开由任何更早 1.x/0.15+ 版本写入的数据库
目录/快照,不做无损迁移即可读。**

- 写入格式版本随版本演进时,**读侧必须保留对旧格式的解码**;
  段(segment)不可变设计使多数优化以"新段生效"落地,旧段保持旧格式
  可读 —— 这是不升 2.0 的前提。
- 破坏性格式变更(需要重写/无法解码旧段)只能出现在 **2.0**,并附带
  `motedb-migrate` 工具。
- 格式版本标记:
  - columnar MANIFEST:内嵌 `VERSION`(当前 2;读侧拒绝更高版本并明示
    "database written by a newer MoteDB");
  - `.mote` 快照容器:头部魔数 + 版本,同上策略;
  - WAL 帧格式:字段仅追加,不重排。
- 0.12 → 0.15 的历史布局在打开时自动迁移(前向迁移在 1.x 内继续保留;
  迁移在打开时一次完成,不动原始文件直到 checkpoint)。

## 3. 发布流程

1. tag `vX.Y.Z` 触发:crates.io 发布 → PyPI(wheel ×8 平台,可信发布)→
   npm(root + 平台包,幂等可重试)→ release-smoke(8 平台安装冒烟)。
2. 任何端发布失败不阻塞其余端;失败自动开 issue,恢复 = 重跑同一
   workflow(幂等)。
3. RELEASE_NOTES.md 每版更新;格式变更必须在此声明并更新本文件第 2 节。

## 4. 弃用政策

1.x 内弃用的 API:标注 `#[deprecated]`(Rust)/ 文档标注(Python/Node),
至少保留 2 个 minor 版本后移除;移除即 2.0。
