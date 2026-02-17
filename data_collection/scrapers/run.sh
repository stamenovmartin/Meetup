#!/usr/bin/env bash
# Run multiple scrapers sequentially with logs, retries, and optional timeout.
# Usage:
#   chmod +x run_scrapers.sh
#   ./run_scrapers.sh
#
# Optional env vars:
#   RETRIES=2 SLEEP_BETWEEN_RETRIES=5 TIMEOUT_SECS=0 ./run_scrapers.sh

set -Eeuo pipefail

# =========================
# CONFIG (can be overridden via env)
# =========================
RETRIES="${RETRIES:-2}"                # additional tries after the first (total runs = 1 + RETRIES)
SLEEP_BETWEEN_RETRIES="${SLEEP_BETWEEN_RETRIES:-5}"  # seconds between retries
TIMEOUT_SECS="${TIMEOUT_SECS:-0}"     # 0 = no timeout

# Your scrapers (run in this exact order)
SCRAPERS=(
  "python it_events_scraper.py"
  "python all_events_scraper.py"
  "python karti_com_mk_scraper.py"
  "python cineplexx_scraper.py"
)

# ВАЖНО: После scraping, автоматски внеси во базата!
POST_SCRAPING_TASKS=(
  "cd ../.. && python ingest_all_csvs.py"
)

# =========================
# LOGGING
# =========================
TS="$(date +'%Y-%m-%d_%H-%M-%S')"
LOG_DIR="./logs/${TS}"
mkdir -p "$LOG_DIR"

# =========================
# HELPERS
# =========================
have_timeout() { command -v timeout >/dev/null 2>&1; }

run_cmd() {
  local cmd="$1"
  local name="$2"
  local attempt="$3"
  local log_file="${LOG_DIR}/${name}.log"

  echo "[$(date +'%F %T')] ▶️  ${name} (attempt ${attempt})" | tee -a "$log_file"

  local wrapped_cmd=("$SHELL" -lc "$cmd")
  if [[ "$TIMEOUT_SECS" -gt 0 ]] && have_timeout; then
    wrapped_cmd=(timeout --preserve-status --kill-after=10s "${TIMEOUT_SECS}" "${wrapped_cmd[@]}")
  fi

  local start end
  start=$(date +%s)

  # run and tee output to its own log file
  if "${wrapped_cmd[@]}" 2>&1 | tee -a "$log_file"; then
    end=$(date +%s)
    echo "[$(date +'%F %T')] ✅  ${name} finished in $((end-start))s" | tee -a "$log_file"
    return 0
  else
    end=$(date +%s)
    echo "[$(date +'%F %T')] ❌  ${name} failed in $((end-start))s" | tee -a "$log_file"
    return 1
  fi
}

# Sanitize a human-readable name from the command (for filenames)
sanitize_name() {
  local s="$1"
  s="${s// /_}"
  s="${s//\//_}"
  echo "$s"
}

# =========================
# MAIN
# =========================
echo "Logs: $LOG_DIR"
overall_status=0
fail_list=()

for cmd in "${SCRAPERS[@]}"; do
  # trim leading/trailing spaces
  cmd="${cmd#"${cmd%%[![:space:]]*}"}"
  cmd="${cmd%"${cmd##*[![:space:]]}"}"

  name="$(sanitize_name "$cmd")"
  attempts=$((RETRIES + 1))
  success=0

  for ((i=1; i<=attempts; i++)); do
    if run_cmd "$cmd" "$name" "$i"; then
      success=1
      break
    fi
    if (( i < attempts )); then
      echo "↻ Will retry ${name} after ${SLEEP_BETWEEN_RETRIES}s..."
      sleep "${SLEEP_BETWEEN_RETRIES}"
    fi
  done

  if (( success == 0 )); then
    overall_status=1
    fail_list+=("$cmd")
  fi
done

echo
echo "================ SUMMARY ================"
if (( overall_status == 0 )); then
  echo "🎉 All scrapers finished successfully."
else
  echo "⚠️  Some scrapers failed:"
  for f in "${fail_list[@]}"; do
    echo "   - $f"
  done
  echo "Check logs in: $LOG_DIR"
fi

# Run post-scraping tasks (e.g., database ingest)
if (( overall_status == 0 )); then
  echo
  echo "================ POST-SCRAPING TASKS ================"
  for cmd in "${POST_SCRAPING_TASKS[@]}"; do
    cmd="${cmd#"${cmd%%[![:space:]]*}"}"
    cmd="${cmd%"${cmd##*[![:space:]]}"}"
    name="$(sanitize_name "$cmd")"

    echo "[$(date +'%F %T')] 🔄 Running: $cmd"
    if run_cmd "$cmd" "$name" "1"; then
      echo "✅ Post-scraping task succeeded: $cmd"
    else
      echo "❌ Post-scraping task failed: $cmd"
      overall_status=1
    fi
  done
fi

exit "$overall_status"
