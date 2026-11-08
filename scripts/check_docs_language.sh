#!/usr/bin/env bash
# 🔖 文档语言门禁: 用户可见文档统一英文，防中英混杂回潮（v0.16 曾统一过
# 一次，0.17/0.18 期间又漂回——此脚本把漂移变成 CI 红灯）。
#
# 规则: 指定文档的**散文部分**不得含 CJK 字符; 代码块（``` 围栏内）豁免
# —— 中文测试数据（如 jieba 分词示例）是合法内容。
#
# 豁免文件（允许中文）:
#   - CHANGELOG.md            中文工作日志（0.19 起新条目转英文，历史保留）
#   - docs/design/            内部设计稿
#   - docs/optimization*      内部工作文档
#   - docs/benchmark_report_* 内部基准报告（不入包）
#   - docs/rustcc-*           中文渠道宣告稿
#
# 用法: scripts/check_docs_language.sh   （CI lint job 调用）

set -euo pipefail
cd "$(dirname "$0")/.."

python3 - <<'EOF'
import re
import sys
from pathlib import Path

TARGETS = [
    "README.md", "CAPABILITIES.md", "ROADMAP.md", "VERSIONING.md",
    "CONTRIBUTING.md", "SECURITY.md", "CODE_OF_CONDUCT.md",
    "GOVERNANCE.md", "BENCHMARK.md",
]
TARGETS += sorted(str(p) for p in Path("docs").glob("*.md"))

EXEMPT = re.compile(
    r"^(CHANGELOG\.md"
    r"|docs/design/.*"
    r"|docs/optimization.*"
    r"|docs/benchmark_report_.*"
    r"|docs/rustcc-.*"
    r")$"
)

cjk = re.compile(r"[\u4e00-\u9fff]")
fail = False
for f in TARGETS:
    if EXEMPT.match(f):
        continue
    inside_fence = False
    for n, line in enumerate(Path(f).read_text(encoding="utf-8").split("\n"), 1):
        if line.lstrip().startswith("```"):
            inside_fence = not inside_fence
            continue
        if inside_fence:
            continue  # 代码块内的中文数据（jieba 示例等）合法
        if cjk.search(line):
            print(f"check_docs_language: {f}:{n} 散文含 CJK 字符（代码块内中文数据不受限）", file=sys.stderr)
            print(f"  >> {line.strip()[:120]}", file=sys.stderr)
            fail = True

if fail:
    print("check_docs_language: FAILED（用户文档须英文; 内部/中文渠道豁免表见脚本头）", file=sys.stderr)
    sys.exit(1)
print("check_docs_language: ok — 用户文档散文层无 CJK")
EOF
