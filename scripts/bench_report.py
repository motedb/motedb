#!/usr/bin/env python3
"""Render benchmark_results/*.jsonl into a publishable markdown report.

Usage: python3 scripts/bench_report.py <results.jsonl> <out.md>

The report carries the methodology header verbatim — data without method is
not publishable (docs/design/2026-10-industry-parity-plan.md §7).
"""
import json
import sys
from datetime import date

HEADER = """# MoteDB 向量检索行业对照报告

> 生成日期: {today} · harness: `bindings/python/bench/compete_vec_engines.py`
> (`make bench-industry` 一键复现; 规模档由 N/DIM 环境变量切换)

## 方法学

- **语料**: 聚簇合成向量(确定性 seed — 见 `prof_ann.build_corpus`; ANN 基准必须
  用聚簇分布, 均匀随机是最坏情形), 数量/维度见表中 engine_mode。
- **ground truth**: numpy 暴力 top-k, 同一查询集(语料内采样, seed=42, q=200,
  k=10)。recall@10 = 命中交集 / (q×k)。
- **各引擎取官方默认索引参数 + 1-2 个调优档, 全部列出** — 包括对 MoteDB 不利
  的行, 不挑好话。延迟为单查询串行(python 侧计时, 含反序列化)。
- **公平性声明**: 各引擎为不同设计点(内存图索引/磁盘图索引/列存扩展/精扫);
  本报告目的是给出同一负载下的可复现横截面, 不是"胜者通吃"。
- 环境: 见报告尾部 env 行。任何人对结果有异议 → PR 重跑, 方法即仲裁。
"""


def main():
    src, dst = sys.argv[1], sys.argv[2]
    rows = [json.loads(l) for l in open(src) if l.strip()]

    skipped = [r for r in rows if "skipped" in r]
    ok = [r for r in rows if "skipped" not in r]

    lines = [HEADER.format(today=date.today().isoformat())]
    lines.append("## 结果\n")
    lines.append("| engine_mode | recall@10 | p50_ms | p95_ms | p99_ms | build_s | ingest kv/s | disk_mb | rss_mb |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for r in sorted(ok, key=lambda r: r["engine_mode"]):
        lines.append(
            "| {engine_mode} | {recall} | {p50} | {p95} | {p99} | {build} | {ingest} | {disk} | {rss} |".format(
                engine_mode=r["engine_mode"],
                recall=r.get("recall@10"),
                p50=r.get("p50_ms"), p95=r.get("p95_ms"), p99=r.get("p99_ms"),
                build=r.get("build_s") if r.get("build_s") is not None else "—",
                ingest=r.get("ingest_kvecs_s") or "—",
                disk=r.get("disk_mb") or "—", rss=r.get("rss_mb"),
            ))
    if skipped:
        lines.append("\n**跳过的引擎**:\n")
        for r in skipped:
            lines.append(f"- `{r['engine_mode']}`: {r['skipped']}")
    lines.append(f"\nenv: {sys.executable}, {date.today()}")
    open(dst, "w").write("\n".join(lines) + "\n")
    print(f"wrote {dst} ({len(ok)} rows, {len(skipped)} skipped)")


if __name__ == "__main__":
    main()
