#!/bin/bash
# FM ETL 每日增量跑数脚本（云端 cron 调用）
#
# 执行流程：
#   1. git pull --ff-only origin main   # 拉最新代码（遇冲突不合并，直接告警退出）
#   2. 激活 venv
#   3. python -m fm_etl_v3.executor 昨天 昨天
#   4. 日志追加到 /opt/fm/logs/etl-YYYY-MM.log
#
# 由 /etc/crontab 或 root crontab 每日 02:00 调用：
#   0 2 * * * /opt/fm/etl/daily_run.sh >> /opt/fm/logs/cron.log 2>&1

set -euo pipefail

REPO_DIR="/opt/fm/etl/cuihua-data"
LOG_DIR="/opt/fm/logs"
LOG_FILE="${LOG_DIR}/etl-$(date +%Y-%m).log"

mkdir -p "${LOG_DIR}"

echo ""
echo "========================================================================"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] FM ETL daily_run.sh start"
echo "========================================================================"

cd "${REPO_DIR}"

echo "[$(date '+%H:%M:%S')] git pull --ff-only origin main"
if ! git pull --ff-only origin main 2>&1; then
    echo "[ERROR] git pull failed (possibly conflict); aborting ETL" >&2
    exit 1
fi

# shellcheck source=/dev/null
source "${REPO_DIR}/.venv/bin/activate"

YESTERDAY="$(date -d 'yesterday' +%Y-%m-%d 2>/dev/null || date -v-1d +%Y-%m-%d)"
echo "[$(date '+%H:%M:%S')] running ETL for ${YESTERDAY}"

python -m fm_etl_v3.executor "${YESTERDAY}" "${YESTERDAY}" 2>&1 | tee -a "${LOG_FILE}"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] FM ETL daily_run.sh done"
