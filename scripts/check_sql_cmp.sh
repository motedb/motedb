#!/usr/bin/env bash
# A1 §A1.4 CI 门禁（棘轮模式）— src/sql 内裸值比较语义只减不增。
#
# 背景: 0.17 的“同一谓词不同路径不同结果”类缺陷全部源于 src/sql 里
# 散落的裸 ==/!=/partial_cmp 值比较。0.18 A1 把语义统一到
# src/sql/semantics.rs（L2），本脚本防止回潮。
#
# 规则:
#   1. 排除 semantics.rs（L2 本体）与文件尾 #[cfg(test)] 模块;
#   2. 行内 `cmp-gate-allow` 标记豁免（排序键包装/字符串列名比较等
#      非值语义位点，标记时须写明理由）;
#   3. 违规数不得超过 scripts/check_sql_cmp.ceiling 里的上限
#      （棘轮: 清偿后手动调低，只减不增）。
set -euo pipefail
cd "$(dirname "$0")/.."

CEILING_FILE=scripts/check_sql_cmp.ceiling
CEILING=$(cat "$CEILING_FILE" 2>/dev/null || echo 0)
PATTERN='(partial_cmp\(|[a-z_)] == &[a-z_]|[a-z_)\]] != &[a-z_]|lv == rv|v == lit|value == |\.eq\(&)'

COUNT=0
for f in $(find src/sql -name '*.rs' ! -name 'semantics.rs'); do
    # 本仓库 cfg(test) 模块均为文件尾部的 `mod tests` — 剥离后再扫
    STRIPPED=$(awk '/^#\[cfg\(test\)\]/{exit} {print}' "$f")
    while IFS=: read -r _ line_text; do
        if echo "$line_text" | grep -q 'cmp-gate-allow'; then continue; fi
        COUNT=$((COUNT + 1))
    done < <(echo "$STRIPPED" | grep -nE "$PATTERN" || true)
done

if [ "$COUNT" -gt "$CEILING" ]; then
    echo "check_sql_cmp: FAIL — 违规 ${COUNT} > 上限 ${CEILING}" >&2
    echo "新增的裸值比较请改走 sql::semantics（eval_cmp/equal/compare/coerce_pair）;" >&2
    echo "确属非值语义（排序键/字符串匹配）的位点加行内 'cmp-gate-allow' 注释豁免。" >&2
    for f in $(find src/sql -name '*.rs' ! -name 'semantics.rs'); do
        STRIPPED=$(awk '/^#\[cfg\(test\)\]/{exit} {print}' "$f")
        echo "$STRIPPED" | grep -nE "$PATTERN" \
            | sed "s|^|VIOLATION $f:|" | grep -v 'cmp-gate-allow' >&2 || true
    done
    exit 1
fi
echo "check_sql_cmp: ok — 违规 ${COUNT} / 上限 ${CEILING}（清偿后请调低上限）"
