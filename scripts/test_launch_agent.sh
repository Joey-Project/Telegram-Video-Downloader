#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/telegram-launch-agent-test.XXXXXX")"
trap 'rm -rf "${test_root}"' EXIT

fake_bin="${test_root}/bin"
fake_log="${test_root}/launchctl.log"
helper_log="${test_root}/helper.log"
fake_state="${test_root}/launchctl-state"
test_home="${test_root}/home"
label="io.github.telegram-local-downloader.migration-probe"
gui_service="gui/$(id -u)/${label}"
legacy_service="user/$(id -u)/${label}"
custom_service="custom/123/${label}"
mkdir -p "${fake_bin}" "${fake_state}" "${test_home}"
touch "${test_root}/config.toml"

cat > "${test_root}/stub" <<'EOF'
#!/usr/bin/env bash
sleep 600
EOF
chmod 700 "${test_root}/stub"

cat > "${fake_bin}/launchctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >> "${FAKE_LAUNCHCTL_LOG}"

state_file() {
  local target="$1"
  target="${target//\//_}"
  printf '%s/%s\n' "${FAKE_LAUNCHCTL_STATE}" "${target}"
}

missing_service() {
  printf 'Bad request.\nCould not find service "%s"\n' "$1" >&2
  exit 113
}

missing_domain() {
  printf 'Could not find domain for "%s"\n' "$1" >&2
  exit 3
}

target_state=""
case "$1" in
  print)
    target_state="$(state_file "$2")"
    if [[ "${FAKE_LAUNCHCTL_MODE:-normal}" == "gui-query-fails-code-one" && "$2" == gui/* ]]; then
      printf '%s\n' "simulated GUI query failure" >&2
      exit 1
    fi
    if [[ "${FAKE_LAUNCHCTL_MODE:-normal}" == "gui-domain-missing" && "$2" == gui/* ]]; then
      missing_domain "$2"
    fi
    if [[ "${FAKE_LAUNCHCTL_MODE:-normal}" == "legacy-query-fails" && "$2" == user/* ]]; then
      printf '%s\n' "simulated legacy query failure" >&2
      exit 42
    fi
    if [[ -e "${target_state}.delay" ]]; then
      remaining="$(<"${target_state}.delay")"
      if (( remaining > 0 )); then
        printf '%s\n' "$((remaining - 1))" > "${target_state}.delay"
      else
        rm -f "${target_state}" "${target_state}.delay"
      fi
    fi
    [[ -e "${target_state}" ]] || missing_service "$2"
    printf '%s\n' "$2"
    ;;
  bootout)
    target_state="$(state_file "$2")"
    if [[ "${FAKE_LAUNCHCTL_MODE:-normal}" == "legacy-bootout-fails" && "$2" == user/* ]]; then
      printf '%s\n' "simulated legacy bootout failure" >&2
      exit 42
    fi
    [[ -e "${target_state}" ]] || missing_service "$2"
    if [[ "${FAKE_LAUNCHCTL_MODE:-normal}" == "delayed-bootout" ]]; then
      printf '%s\n' 2 > "${target_state}.delay"
    else
      rm -f "${target_state}"
    fi
    ;;
  bootstrap)
    touch "$(state_file "$2/${FAKE_LAUNCHCTL_LABEL}")"
    ;;
  kickstart)
    target_state="$(state_file "$3")"
    [[ -e "${target_state}" ]] || missing_service "$3"
    ;;
  *)
    printf '%s\n' "unexpected launchctl action: $1" >&2
    exit 64
    ;;
esac
EOF
chmod 700 "${fake_bin}/launchctl"

cat > "${fake_bin}/python3" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "${FAKE_HELPER_LOG}"
exit "${FAKE_HELPER_EXIT:-0}"
EOF
chmod 700 "${fake_bin}/python3"

state_file() {
  local target="$1"
  target="${target//\//_}"
  printf '%s/%s\n' "${fake_state}" "${target}"
}

reset_state() {
  rm -f "${fake_state}"/* "${fake_state}"/*.delay
  local target
  for target in "$@"; do
    touch "$(state_file "${target}")"
  done
}

run_agent() {
  HOME="${test_home}" \
    PATH="${fake_bin}:${PATH}" \
    FAKE_LAUNCHCTL_LOG="${fake_log}" \
    FAKE_LAUNCHCTL_STATE="${fake_state}" \
    FAKE_LAUNCHCTL_LABEL="${label}" \
    FAKE_LAUNCHCTL_MODE="${FAKE_LAUNCHCTL_MODE:-normal}" \
    FAKE_HELPER_LOG="${helper_log}" \
    FAKE_HELPER_EXIT="${FAKE_HELPER_EXIT:-0}" \
    BOT_LABEL="${label}" \
    BOT_CONFIG="${test_root}/config.toml" \
    BOT_BINARY="${test_root}/stub" \
    BOT_LOG_DIR="${test_root}/logs" \
    bash "${repo_dir}/scripts/launch_agent.sh" "$@"
}

expect_log() {
  grep -Fqx -- "$1" "${fake_log}"
}

expect_no_log() {
  if grep -Fqx -- "$1" "${fake_log}"; then
    printf 'unexpected launchctl invocation: %s\n' "$1" >&2
    exit 1
  fi
}

empty_log() {
  : > "${fake_log}"
}

reset_state "${legacy_service}"
empty_log
run_agent install --no-build
grep -Fqx -- "${repo_dir}/scripts/macos_service.py" "${helper_log}"
grep -Fqx -- "install" "${helper_log}"
grep -Fqx -- "--no-build" "${helper_log}"
grep -Fqx -- "--legacy-domain" "${helper_log}"
grep -Fqx -- "user/$(id -u)" "${helper_log}"
[[ ! -s "${fake_log}" ]]
[[ -e "$(state_file "${legacy_service}")" ]]

run_agent deploy --binary "${test_root}/input artifact" --ready-timeout 321
grep -Fqx -- "deploy" "${helper_log}"
grep -Fqx -- "${test_root}/input artifact" "${helper_log}"
grep -Fqx -- "321" "${helper_log}"
[[ ! -s "${fake_log}" ]]

run_agent rollback --backup "${test_root}/retained backup" --original-entrypoint
grep -Fqx -- "rollback" "${helper_log}"
grep -Fqx -- "${test_root}/retained backup" "${helper_log}"
grep -Fqx -- "--original-entrypoint" "${helper_log}"
[[ ! -s "${fake_log}" ]]

if FAKE_HELPER_EXIT=37 run_agent install --no-build >/dev/null 2>&1; then
  printf '%s\n' "install ignored a production helper failure" >&2
  exit 1
fi

reset_state "${legacy_service}"
empty_log
run_agent status >/dev/null
expect_log "print ${gui_service}"
expect_log "print ${legacy_service}"

reset_state "${legacy_service}"
empty_log
run_agent restart
expect_log "kickstart -k ${legacy_service}"

reset_state "${legacy_service}"
empty_log
FAKE_LAUNCHCTL_MODE=gui-domain-missing run_agent status >/dev/null
expect_log "print ${gui_service}"
expect_log "print ${legacy_service}"

reset_state "${legacy_service}"
empty_log
if FAKE_LAUNCHCTL_MODE=gui-query-fails-code-one run_agent status >/dev/null 2>&1; then
  printf '%s\n' "status ignored a GUI service query failure with exit code 1" >&2
  exit 1
fi
expect_log "print ${gui_service}"
expect_no_log "print ${legacy_service}"

reset_state "${gui_service}" "${legacy_service}"
empty_log
mkdir -p "${test_home}/Library/LaunchAgents"
touch "${test_home}/Library/LaunchAgents/${label}.plist"
run_agent uninstall
expect_log "bootout ${gui_service}"
expect_log "bootout ${legacy_service}"
[[ ! -e "$(state_file "${gui_service}")" ]]
[[ ! -e "$(state_file "${legacy_service}")" ]]
if [[ -e "${test_home}/Library/LaunchAgents/${label}.plist" ]]; then
  printf '%s\n' "uninstall retained the generated plist" >&2
  exit 1
fi

reset_state
empty_log
BOT_DOMAIN="custom/123" run_agent install --no-build
grep -Fqx -- "custom/123" "${helper_log}"
if grep -Fqx -- "--legacy-domain" "${helper_log}"; then
  printf '%s\n' "explicit BOT_DOMAIN unexpectedly requested legacy-domain migration" >&2
  exit 1
fi
[[ ! -s "${fake_log}" ]]

empty_log
reset_state "${custom_service}"
BOT_DOMAIN="custom/123" run_agent uninstall
expect_log "bootout ${custom_service}"
if grep -Fq "${legacy_service}" "${fake_log}"; then
  printf '%s\n' "explicit BOT_DOMAIN unexpectedly cleaned the legacy domain" >&2
  exit 1
fi

reset_state "${gui_service}" "${legacy_service}"
empty_log
FAKE_LAUNCHCTL_MODE=delayed-bootout run_agent uninstall
[[ ! -e "$(state_file "${gui_service}")" ]]
[[ ! -e "$(state_file "${legacy_service}")" ]]

reset_state "${legacy_service}"
empty_log
FAKE_LAUNCHCTL_MODE=gui-domain-missing run_agent uninstall
expect_log "print ${gui_service}"
expect_log "bootout ${legacy_service}"
[[ ! -e "$(state_file "${legacy_service}")" ]]

printf '%s\n' "launch-agent integration test passed"
