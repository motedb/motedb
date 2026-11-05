#!/usr/bin/env python3
"""🔖 0.18 发版清单: 从 CHANGELOG.md 提取指定版本的段落。

用法: extract_changelog.py <version>   # 如 0.18.0 或 v0.18.0
输出: 该版本 `## [X.Y.Z]` 小节的正文（不含下一个 ## 段），stdout。
GitHub Release 工作流（.github/workflows/release.yml）用 body_path 填充。
"""
import re
import sys


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: extract_changelog.py <version>", file=sys.stderr)
        return 2
    ver = sys.argv[1].lstrip("v")
    text = open("CHANGELOG.md", encoding="utf-8").read()
    # `## [0.18.0] — 开发中` / `## [0.17.0] - 2026-…` 均可。
    m = re.search(
        r"^## \[" + re.escape(ver) + r"\][^\n]*\n(.*?)(?=^## \[|\Z)",
        text,
        re.S | re.M,
    )
    if not m:
        print(f"extract_changelog: CHANGELOG.md 无 [{ver}] 段落", file=sys.stderr)
        return 1
    print(m.group(1).strip())
    return 0


if __name__ == "__main__":
    sys.exit(main())
