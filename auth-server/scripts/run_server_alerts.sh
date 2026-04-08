#!/usr/bin/env bash
set -euo pipefail

AUTH_SERVER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ALERT_ENV_FILE:-${AUTH_SERVER_DIR}/.env}"
STATE_DIR="${ALERT_STATE_DIR:-${AUTH_SERVER_DIR}/monitoring-state}"

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-}"
ALERT_DISK_USED_PERCENT="${ALERT_DISK_USED_PERCENT:-90}"
ALERT_DISK_MOUNTS="${ALERT_DISK_MOUNTS:-/}"
ALERT_UNITS="${ALERT_UNITS:-update-duels.timer cleanup-chrome-tmp.timer}"
ALERT_UPDATE_DUELS_LOG="${ALERT_UPDATE_DUELS_LOG:-/var/log/carcassonne/update-duels.log}"
ALERT_LOG_PATTERN="${ALERT_LOG_PATTERN:-tab crashed}"
ALERT_LOG_PERSISTENCE_MINUTES="${ALERT_LOG_PERSISTENCE_MINUTES:-10}"
ALERT_LOG_MIN_MATCHES="${ALERT_LOG_MIN_MATCHES:-2}"

mkdir -p "${STATE_DIR}"

send_telegram() {
  local text="$1"

  if [[ -z "${TELEGRAM_BOT_TOKEN}" || -z "${TELEGRAM_CHAT_ID}" ]]; then
    echo "Telegram alerts are not configured; skipping alert: ${text}" >&2
    return 0
  fi

  curl -fsS --retry 3 --max-time 20 \
    -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
    --data-urlencode "chat_id=${TELEGRAM_CHAT_ID}" \
    --data-urlencode "text=${text}" \
    --data-urlencode "disable_web_page_preview=true" \
    >/dev/null
}

state_path() {
  local key="$1"
  echo "${STATE_DIR}/${key}"
}

load_state() {
  local key="$1"
  local path
  path="$(state_path "${key}")"
  if [[ -f "${path}" ]]; then
    cat "${path}"
  fi
}

save_state() {
  local key="$1"
  local value="$2"
  printf '%s' "${value}" >"$(state_path "${key}")"
}

normalize_key() {
  echo "$1" | tr '/ .:-' '_'
}

check_disk() {
  local mount used key previous
  for mount in ${ALERT_DISK_MOUNTS}; do
    used="$(df -P "${mount}" | awk 'NR==2 {gsub("%","",$5); print $5}')"
    key="disk_$(normalize_key "${mount}")"
    previous="$(load_state "${key}")"

    if [[ -n "${used}" && "${used}" -ge "${ALERT_DISK_USED_PERCENT}" ]]; then
      if [[ "${previous}" != "alert" ]]; then
        send_telegram "ALERT: disk usage on ${mount} is ${used}% (threshold ${ALERT_DISK_USED_PERCENT}%)."
        save_state "${key}" "alert"
      fi
    else
      if [[ "${previous}" == "alert" ]]; then
        send_telegram "RECOVERY: disk usage on ${mount} is back to ${used}%."
      fi
      save_state "${key}" "ok"
    fi
  done
}

check_units() {
  local unit kind key previous status
  for unit in ${ALERT_UNITS}; do
    kind="${unit##*.}"
    key="unit_$(normalize_key "${unit}")"
    previous="$(load_state "${key}")"

    if [[ "${kind}" == "timer" ]]; then
      status="$(systemctl is-active "${unit}" 2>/dev/null || true)"
      if [[ "${status}" != "active" ]]; then
        if [[ "${previous}" != "alert" ]]; then
          send_telegram "ALERT: ${unit} is ${status:-unknown}, expected active."
          save_state "${key}" "alert"
        fi
      else
        if [[ "${previous}" == "alert" ]]; then
          send_telegram "RECOVERY: ${unit} is active again."
        fi
        save_state "${key}" "ok"
      fi
      continue
    fi

    status="$(systemctl is-failed "${unit}" 2>/dev/null || true)"
    if [[ "${status}" == "failed" ]]; then
      if [[ "${previous}" != "alert" ]]; then
        send_telegram "ALERT: ${unit} entered failed state."
        save_state "${key}" "alert"
      fi
    else
      if [[ "${previous}" == "alert" ]]; then
        send_telegram "RECOVERY: ${unit} is no longer failed."
      fi
      save_state "${key}" "ok"
    fi
  done
}

check_update_duels_log() {
  local inode size previous_inode previous_size chunk match_count last_match
  local now persistence_seconds first_seen last_seen total_matches alert_sent
  local age_since_first age_since_last

  if [[ ! -f "${ALERT_UPDATE_DUELS_LOG}" ]]; then
    return 0
  fi

  now="$(date +%s)"
  persistence_seconds="$((ALERT_LOG_PERSISTENCE_MINUTES * 60))"
  inode="$(stat -c '%i' "${ALERT_UPDATE_DUELS_LOG}")"
  size="$(stat -c '%s' "${ALERT_UPDATE_DUELS_LOG}")"
  previous_inode="$(load_state "update_duels_log_inode")"
  previous_size="$(load_state "update_duels_log_size")"
  first_seen="$(load_state "update_duels_log_incident_first_seen")"
  last_seen="$(load_state "update_duels_log_incident_last_seen")"
  total_matches="$(load_state "update_duels_log_incident_total_matches")"
  alert_sent="$(load_state "update_duels_log_incident_alert_sent")"

  total_matches="${total_matches:-0}"
  alert_sent="${alert_sent:-0}"

  if [[ -z "${previous_size}" || -z "${previous_inode}" || "${previous_inode}" != "${inode}" || "${size}" -lt "${previous_size}" ]]; then
    previous_size=0
  fi

  if [[ "${size}" -gt "${previous_size}" ]]; then
    chunk="$(tail -c +"$((previous_size + 1))" "${ALERT_UPDATE_DUELS_LOG}")"
    match_count="$(printf '%s' "${chunk}" | grep -F -c "${ALERT_LOG_PATTERN}" || true)"
    if [[ "${match_count}" -gt 0 ]]; then
      last_match="$(printf '%s' "${chunk}" | grep -F "${ALERT_LOG_PATTERN}" | tail -n 1)"
      if [[ -z "${first_seen}" ]]; then
        first_seen="${now}"
      fi
      last_seen="${now}"
      total_matches="$((total_matches + match_count))"
      save_state "update_duels_log_last_match" "${last_match}"
    fi
  fi

  save_state "update_duels_log_inode" "${inode}"
  save_state "update_duels_log_size" "${size}"

  if [[ -n "${first_seen}" && -n "${last_seen}" ]]; then
    age_since_first="$((now - first_seen))"
    age_since_last="$((now - last_seen))"

    if [[ "${alert_sent}" != "1" \
      && "${total_matches}" -ge "${ALERT_LOG_MIN_MATCHES}" \
      && "${age_since_first}" -ge "${persistence_seconds}" \
      && "${age_since_last}" -lt "${persistence_seconds}" ]]; then
      last_match="$(load_state "update_duels_log_last_match")"
      send_telegram \
        "ALERT: '${ALERT_LOG_PATTERN}' persists for at least ${ALERT_LOG_PERSISTENCE_MINUTES} minutes in update-duels.log (${total_matches} matches). Last match: ${last_match}"
      alert_sent="1"
      save_state "update_duels_log_incident_alert_sent" "${alert_sent}"
    fi

    if [[ "${alert_sent}" == "1" && "${age_since_last}" -ge "${persistence_seconds}" ]]; then
      send_telegram \
        "RECOVERY: '${ALERT_LOG_PATTERN}' has not appeared in update-duels.log for ${ALERT_LOG_PERSISTENCE_MINUTES} minutes."
      first_seen=""
      last_seen=""
      total_matches=0
      alert_sent=0
      save_state "update_duels_log_incident_first_seen" ""
      save_state "update_duels_log_incident_last_seen" ""
      save_state "update_duels_log_incident_total_matches" "0"
      save_state "update_duels_log_incident_alert_sent" "0"
      save_state "update_duels_log_last_match" ""
      return 0
    fi
  fi

  if [[ -n "${first_seen}" ]]; then
    save_state "update_duels_log_incident_first_seen" "${first_seen}"
  fi
  if [[ -n "${last_seen}" ]]; then
    save_state "update_duels_log_incident_last_seen" "${last_seen}"
  fi
  save_state "update_duels_log_incident_total_matches" "${total_matches}"
  save_state "update_duels_log_incident_alert_sent" "${alert_sent}"
}

check_disk
check_units
check_update_duels_log
