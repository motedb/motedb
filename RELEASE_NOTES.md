# MoteDB v0.12.1

工程整洁 patch：全目标（lib + tests + examples）clippy/rustc 告警清零，
废弃代码删除，无行为变更。发布通道（crates.io + PyPI）自 v0.12.0 起
恢复正常。

## 清理

- **clippy --all-targets 清零**：0.12.0 时 lib 已为 0，本版把
  tests/examples 的 ~95 条风格项全部清掉——`match` 单模式改 `if let`/
  `let`、循环索引改迭代器、doc 注释列表缩进修正、`filter_map` 化、
  `&mut Vec` 收窄为 `&mut [_]`、deprecated API 替换（`TempDir::
  into_path` → `keep()`）等
- **废弃代码删除**：两处未用的 `rows` 测试 helper、一处未用
  `tokenize`、`collect_files` 的死参数 `depth`
- 引擎代码路径零触碰（lib 内唯一改动是测试 helper 的初始化语法）

## 已知限制

同 v0.12.0（见 CHANGELOG [0.12.0] Known Limitations 一节）。
