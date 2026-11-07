#!/usr/bin/env bash
# 🔍 issue #3 挂死看门狗: 后台跑测试命令，监控日志停增; 停超阈值即抓现场
# （最后 Running 行 = 挂死二进制名 + 日志尾 + 进程树 + 各线程内核等待点，
# 有 gdb 则附全线程栈），然后击杀并以 199 退出——把"90 分钟超时无输出"
# 变成"几分钟内拿到命名了挂死二进制的诊断"。
#
# 用法: ci_watchdog.sh <logfile> <stall_seconds> -- <command...>
# 退出码: 命令原样透传; 挂死击杀 = 199。
set -uo pipefail

LOG="${1:?logfile}"; STALL="${2:?stall_seconds}"; shift 2
[ "${1:-}" = "--" ] && shift

: >"$LOG"
"$@" >"$LOG" 2>&1 &
PID=$!

last_size=0
last_change=$(date +%s)

while kill -0 "$PID" 2>/dev/null; do
  sleep 15
  size=$(stat -c %s "$LOG" 2>/dev/null || stat -f %z "$LOG" 2>/dev/null || echo 0)
  if [ "$size" != "$last_size" ]; then
    last_size=$size; last_change=$(date +%s); continue
  fi
  stalled=$(( $(date +%s) - last_change ))
  if [ "$stalled" -lt "$STALL" ]; then continue; fi

  echo "===== ci_watchdog: output stalled ${stalled}s — hang diagnosis ====="
  echo "----- last 'Running' lines (hung binary = last one) -----"
  grep -E "Running (tests|unittests|benches)" "$LOG" | tail -3
  echo "----- tail of log -----"
  tail -15 "$LOG"
  echo "----- process tree -----"
  ps -ef | grep -E "cargo|deps/" | grep -v grep | head -20

  have_gdb=0; command -v gdb >/dev/null 2>&1 && have_gdb=1
  for pid in $(pgrep -f "deps/" 2>/dev/null; pgrep -P "$PID" 2>/dev/null); do
    [ -d "/proc/$pid" ] || continue
    cmd=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | head -c 160)
    echo "===== pid $pid: $cmd"
    if [ "$have_gdb" = "1" ]; then
      timeout 60 gdb -batch -ex "thread apply all bt" -p "$pid" 2>&1 \
        | grep -vE "^\[|Downloading|Reading symbols" | head -100
    else
      # 无符号环境兜底: 每线程内核等待点（锁名/等待原因常可见）
      for t in /proc/$pid/task/*; do
        name=$(cat "$t/comm" 2>/dev/null)
        wchan=$(cat "$t/wchan" 2>/dev/null)
        echo "  tid $(basename "$t") $name wchan=$wchan"
      done
    fi
  done | sort -u

  echo "===== killing hung suite ====="
  pkill -9 -P "$PID" 2>/dev/null
  kill -9 "$PID" 2>/dev/null
  exit 199
done

wait "$PID"; exit $?
