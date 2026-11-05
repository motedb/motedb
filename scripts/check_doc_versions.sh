#!/usr/bin/env bash
# 🔖 0.18 发版清单: 文档版本单源校验。
#
# 单一来源 = Cargo.toml [package] version（发版时随版本号 bump 一并更新，
# 本脚本在 CI 强制文档跟随——README / docs 安装段此前各写各的 0.15/0.16
# 漂移了两个版本）。
#
# 校验规则: README.md 与 docs/*.md 中出现的 `motedb = "<ver>"` 与
# `version = "<ver>"`（Cargo 依赖拼写，形如 0.17）必须等于 Cargo.toml 的
# major.minor。历史文档（RELEASE-v0.2.0.md、CHANGELOG、design/ 归档）豁免。
#
# 用法: scripts/check_doc_versions.sh   （CI lint job 调用）

set -euo pipefail
cd "$(dirname "$0")/.."

CRATE_VER="$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -1)"
DOC_VER="$(echo "${CRATE_VER}" | cut -d. -f1-2)"

if [ -z "${CRATE_VER}" ]; then
  echo "check_doc_versions: 无法从 Cargo.toml 解析 version" >&2
  exit 2
fi

# 允许显式豁免（极少用; 目前无豁免文件）。
EXEMPT=(RELEASE-v0.2.0.md CHANGELOG.md)

fail=0
for f in README.md docs/*.md; do
  base="$(basename "${f}")"
  skip=0
  for e in "${EXEMPT[@]}"; do
    [ "${base}" = "${e}" ] && skip=1
  done
  [ "${f}" = "${f%%docs/design*}" ] || skip=1  # design/ 归档不校验
  [ "${skip}" = "0" ] || continue
  # 两种 Cargo 依赖拼写中的 motedb 版本字面量。
  while IFS=: read -r line match; do
    [ -n "${match}" ] || continue
    echo "check_doc_versions: ${f}:${line} 版本 ${match} ≠ Cargo.toml ${DOC_VER}" >&2
    fail=1
  done < <(grep -nE 'motedb = "[0-9.]+"|version = "[0-9.]+"' "${f}" \
    | awk -F: -v want="${DOC_VER}" '{
        line=$1; rest=$0;
        while (match(rest, /("[0-9]+.[0-9]+(.[0-9]+)?")/)) {
          v=substr(rest, RSTART+1, RLENGTH-2);
          if (v != want) { print line ":" v; }
          rest=substr(rest, RSTART+RLENGTH);
        }
      }')
done

if [ "${fail}" = "1" ]; then
  echo "check_doc_versions: FAILED（文档版本与 Cargo.toml 漂移; 发版时请同步 bump）" >&2
  exit 1
fi
echo "check_doc_versions: ok — 文档版本 = Cargo.toml ${CRATE_VER}"
