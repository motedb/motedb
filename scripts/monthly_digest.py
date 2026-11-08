#!/usr/bin/env python3
"""🔖 社区物料: 从 CHANGELOG.md 生成 "This Month in MoteDB" 月度摘要。

用法: monthly_digest.py [版本号]        # 缺省 = 最高版本段
输出: markdown 正文（stdout）——头部/链接由发布渠道自加。

设计: 只做"结构化收拢"不做改写——所有事实句来自 CHANGELOG 本身，
标题行原文保留; 渠道语气调整由人工完成（诚实性门槛）。
"""
import re
import sys

CHANGELOG = "CHANGELOG.md"
REPO = "https://github.com/motedb/motedb"


def sections(text: str, ver: str):
    m = re.search(
        r"^## \[" + re.escape(ver) + r"\][^\n]*\n(.*?)(?=^## \[|\Z)", text, re.S | re.M
    )
    if not m:
        return None
    body = m.group(1)
    return re.findall(r"^### (.+)$", body, re.M), body


def main() -> int:
    text = open(CHANGELOG, encoding="utf-8").read()
    vers = re.findall(r"^## \[([0-9][^\]]*)\]", text, re.M)
    if not vers:
        print("no versions in CHANGELOG", file=sys.stderr)
        return 1
    ver = sys.argv[1] if len(sys.argv) > 1 else vers[0]
    got = sections(text, ver)
    if not got:
        print(f"no section for {ver}", file=sys.stderr)
        return 1
    heads, _body = got
    print(f"This Month in MoteDB — v{ver}")
    print(f"\n高亮点（来自 CHANGELOG {ver} 段，全文见 {REPO}/releases）:\n")
    for h in heads:
        print(f"- {h}")
    print(
        f"\n- 仓库: {REPO}\n- 文档: https://motedb.github.io\n"
        "- 安装: `cargo add motedb` · `pip install motedb-python` · `npm install motedb`"
        "\n- good first issue: "
        f"{REPO}/issues?q=label%3A%22good+first+issue%22"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
