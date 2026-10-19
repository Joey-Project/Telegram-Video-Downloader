#!/usr/bin/env bash
set -euo pipefail

DEFAULT_LABEL="io.github.telegram-local-downloader.bot"
SERVICE_STOP_TIMEOUT_SECONDS=10
SERVICE_STOP_POLL_SECONDS=0.1

usage() {
  cat <<'EOF'
Usage:
  scripts/launch_agent.sh plan [--label LABEL] [--config PATH] [--binary INPUT]
  scripts/launch_agent.sh install|deploy [--label LABEL] [--binary INPUT] [--no-build]
  scripts/launch_agent.sh rollback --backup PATH [--label LABEL] [--original-entrypoint]
  scripts/launch_agent.sh uninstall|restart|status|logs [--label LABEL]

install/deploy build in isolated staging directories, sign a COPY, then atomically
install ~/.local/lib/telegram-video-downloader/bin/telegram-video-downloader.
--binary is an input artifact; use --no-build with an existing release artifact.
Existing config, WorkingDirectory, environment and log paths remain unchanged.
plan is read-only. rollback restores executable/plist and retains live state.
--original-entrypoint explicitly restores the exact old plist, but only while
its old executable still exists and matches the saved trusted binary.

Environment overrides:
  BOT_LABEL       LaunchAgent label. Defaults to io.github.telegram-local-downloader.bot.
  BOT_CONFIG      Initial config path; an existing service config path is preserved.
  BOT_BINARY      Input release artifact; never the direct LaunchAgent target.
  BOT_LOG_DIR     Initial log directory; existing service log paths are preserved.
  BOT_DOMAIN      launchd domain. Defaults to gui/$(id -u); bypasses legacy migration when set.
  BOT_DOTNET_ROOT Optional .NET runtime root for an initial installation.
  BOT_SKIP_BUILD  Set to 1 to skip the isolated release build.
EOF
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_dir="$(cd -- "${script_dir}/.." && pwd -P)"
action="${1:-}"
if [[ -z "${action}" ]]; then
  usage
  exit 2
fi
shift

label="${BOT_LABEL:-${DEFAULT_LABEL}}"
config_path="${BOT_CONFIG:-}"
binary_path="${BOT_BINARY:-}"
log_dir="${BOT_LOG_DIR:-}"
default_domain="gui/$(id -u)"
legacy_domain="user/$(id -u)"
domain="${BOT_DOMAIN:-${default_domain}}"
use_default_domain=0
if [[ -z "${BOT_DOMAIN:-}" ]]; then
  use_default_domain=1
fi
dotnet_root="${BOT_DOTNET_ROOT:-}"
skip_build="${BOT_SKIP_BUILD:-0}"
backup_path=""
original_entrypoint=0
ready_timeout="180"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --label|--config|--binary|--backup|--ready-timeout)
      [[ $# -ge 2 ]] || die "$1 requires a value"
      case "$1" in
        --label) label="$2" ;;
        --config) config_path="$2" ;;
        --binary) binary_path="$2" ;;
        --backup) backup_path="$2" ;;
        --ready-timeout) ready_timeout="$2" ;;
      esac
      shift 2
      ;;
    --no-build)
      skip_build="1"
      shift
      ;;
    --original-entrypoint)
      original_entrypoint=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *) die "unknown argument: $1" ;;
  esac
done

plist_path() {
  printf '%s/Library/LaunchAgents/%s.plist\n' "${HOME}" "${label}"
}

service_name_for_domain() {
  printf '%s/%s\n' "$1" "${label}"
}

service_name() {
  service_name_for_domain "${domain}"
}

legacy_service_name() {
  service_name_for_domain "${legacy_domain}"
}

service_exists() {
  local target="$1"
  local output
  if output="$(launchctl print "${target}" 2>&1)"; then
    return 0
  fi
  # Only explicit launchd absent-service/domain diagnostics mean absence.
  if [[ "${output}" == *"Could not find service"* || "${output}" == *"Could not find domain"* ]]; then
    return 1
  fi
  printf '%s\n' "${output}" >&2
  return 2
}

wait_for_service_absence() {
  local target="$1"
  local deadline=$((SECONDS + SERVICE_STOP_TIMEOUT_SECONDS))
  local status
  while :; do
    if service_exists "${target}"; then
      if (( SECONDS >= deadline )); then
        die "LaunchAgent remained registered after bootout: ${target}"
      fi
      sleep "${SERVICE_STOP_POLL_SECONDS}"
    else
      status=$?
      [[ "${status}" -eq 1 ]] && return 0
      return "${status}"
    fi
  done
}

stop_service_if_present() {
  local target="$1"
  local status
  if service_exists "${target}"; then
    if launchctl bootout "${target}"; then :; else
      status=$?
      return "${status}"
    fi
    wait_for_service_absence "${target}"
  else
    status=$?
    [[ "${status}" -eq 1 ]] && return 0
    return "${status}"
  fi
}

cleanup_legacy_service() {
  [[ "${use_default_domain}" == "1" ]] || return 0
  stop_service_if_present "$(legacy_service_name)"
}

active_or_default_service_name() {
  local status
  if service_exists "$(service_name)"; then
    service_name
    return
  else
    status=$?
    [[ "${status}" -eq 1 ]] || return "${status}"
  fi
  if [[ "${use_default_domain}" == "1" ]]; then
    if service_exists "$(legacy_service_name)"; then
      legacy_service_name
      return
    else
      status=$?
      [[ "${status}" -eq 1 ]] || return "${status}"
    fi
  fi
  service_name
}

production_action() {
  local args=("${script_dir}/macos_service.py" "${action}" --repo "${repo_dir}"
    --label "${label}" --domain "${domain}" --ready-timeout "${ready_timeout}")
  [[ "${use_default_domain}" != "1" ]] || args+=(--legacy-domain "${legacy_domain}")
  [[ -z "${config_path}" ]] || args+=(--config "${config_path}")
  [[ -z "${binary_path}" ]] || args+=(--binary "${binary_path}")
  [[ -z "${log_dir}" ]] || args+=(--log-dir "${log_dir}")
  [[ -z "${dotnet_root}" ]] || args+=(--dotnet-root "${dotnet_root}")
  [[ -z "${backup_path}" ]] || args+=(--backup "${backup_path}")
  [[ "${skip_build}" != "1" ]] || args+=(--no-build)
  [[ "${original_entrypoint}" != "1" ]] || args+=(--original-entrypoint)
  python3 "${args[@]}"
}

case "${action}" in
  plan|install|deploy|rollback) production_action ;;
  uninstall)
    stop_service_if_present "$(service_name)"
    cleanup_legacy_service
    rm -f "$(plist_path)"
    ;;
  restart)
    active_service="$(active_or_default_service_name)"
    launchctl kickstart -k "${active_service}"
    ;;
  status)
    active_service="$(active_or_default_service_name)"
    launchctl print "${active_service}"
    ;;
  logs)
    # Resolve existing plist paths without printing any other plist/config values.
    python3 - "$(plist_path)" <<'PY'
import os
import plistlib
import sys
with open(sys.argv[1], "rb") as stream:
    plist = plistlib.load(stream)
paths = [plist[key] for key in ("StandardOutPath", "StandardErrorPath") if key in plist]
if not paths:
    sys.exit("error: LaunchAgent has no configured log paths")
os.execvp("tail", ["tail", "-f", *paths])
PY
    ;;
  -h|--help) usage ;;
  *) usage >&2; exit 2 ;;
esac
