#!/usr/bin/env python3
"""Install and run the isolated, one-shot Apple Music authorization test binary."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import plistlib
import pwd
import re
import selectors
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path

PRODUCTION_BINARY = Path(".local/lib/telegram-video-downloader/bin/telegram-video-downloader")
TEST_BINARY = Path(
    ".local/lib/telegram-video-downloader-test/bin/telegram-video-downloader-test"
)
TEST_IDENTIFIER = "io.github.telegram-local-downloader.bot.test"
TEST_LABEL = "io.github.telegram-local-downloader.bot.test.music"
TRUSTED_FINGERPRINT = "C24FE0C91539BD95BA4081CD944A8E17053FB304"
ENTITLEMENTS = Path(__file__).resolve().parents[1] / "macos/music-automation.entitlements.plist"
LOG_LIMIT = 64 * 1024
REQUEST_LIMIT = 64 * 1024
COMMAND_TIMEOUT = 15
OUTPUT_LIMIT = 128 * 1024
CONVERTER_INSTALLER = Path(__file__).with_name("install_flac_converter.py")
CONVERTER_IDENTIFIER = "io.github.telegram-local-downloader.bot.converter"
CONVERTER_INSTALL_TIMEOUT = 60


class RuntimeErrorSafe(Exception):
    """A safe diagnostic that never includes external command output."""


def _run(argv: list[str], *, timeout: int | None = COMMAND_TIMEOUT, check: bool = True):
    process = None
    selector = None
    try:
        process = subprocess.Popen(
            [str(item) for item in argv], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        assert process.stdout is not None and process.stderr is not None
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ, "stdout")
        selector.register(process.stderr, selectors.EVENT_READ, "stderr")
        captured = {"stdout": bytearray(), "stderr": bytearray()}
        deadline = time.monotonic() + timeout if timeout is not None else None
        while selector.get_map():
            remaining = deadline - time.monotonic() if deadline is not None else 0.25
            if remaining <= 0:
                raise RuntimeErrorSafe(f"{Path(argv[0]).name} could not complete")
            for key, _ in selector.select(min(remaining, 0.25)):
                chunk = os.read(key.fileobj.fileno(), 16 * 1024)
                if not chunk:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                output = captured[key.data]
                if len(output) + len(chunk) > OUTPUT_LIMIT:
                    raise RuntimeErrorSafe(f"{Path(argv[0]).name} output exceeded its limit")
                output.extend(chunk)
        return_code = (
            process.wait()
            if deadline is None
            else process.wait(timeout=max(0.1, deadline - time.monotonic()))
        )
        selector.close()
        result = subprocess.CompletedProcess(
            [str(item) for item in argv],
            return_code,
            captured["stdout"].decode("utf-8", errors="replace"),
            captured["stderr"].decode("utf-8", errors="replace"),
        )
    except RuntimeErrorSafe:
        if selector is not None:
            selector.close()
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        if process is not None:
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()
        raise
    except (OSError, subprocess.TimeoutExpired):
        if selector is not None:
            selector.close()
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        if process is not None:
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()
        raise RuntimeErrorSafe(f"{Path(argv[0]).name} could not complete") from None
    if check and result.returncode:
        raise RuntimeErrorSafe(f"{Path(argv[0]).name} failed (exit {result.returncode})")
    return result


def _home_path(relative: Path) -> Path:
    return Path.home() / relative


def _real_home() -> Path:
    """Use the account database home, not an overridable HOME environment value."""
    try:
        home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    except (KeyError, OSError):
        raise RuntimeErrorSafe("current account home directory could not be determined") from None
    if not home.is_absolute():
        raise RuntimeErrorSafe("current account home directory is not absolute")
    return home


def _validate_import_request(path: Path) -> dict:
    """Validate the private, numbered JSON request before passing it to the test CLI."""
    home = _real_home()
    request_path = Path(path)
    requests_dir = (
        home / ".local/lib/telegram-video-downloader-test/music-import/requests"
    )
    if not request_path.is_absolute() or request_path.parent != requests_dir:
        raise RuntimeErrorSafe("Music import request must be in the fixed requests directory")
    sequence = re.fullmatch(r"([0-9]+)\.json", request_path.name)
    if (
        sequence is None
        or len(sequence.group(1)) > 20
        or not 0 < int(sequence.group(1)) <= (1 << 63) - 1
    ):
        raise RuntimeErrorSafe("Music import request filename must be a positive sequence")
    _no_symlinks(request_path, home)

    for directory in (requests_dir.parent, requests_dir):
        try:
            metadata = directory.lstat()
        except OSError:
            raise RuntimeErrorSafe("Music import request directories are unavailable") from None
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise RuntimeErrorSafe("Music import request directories must be private and user-owned")

    try:
        metadata = request_path.lstat()
    except OSError:
        raise RuntimeErrorSafe("Music import request file is unavailable") from None
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_size > REQUEST_LIMIT
    ):
        raise RuntimeErrorSafe("Music import request file has invalid metadata")
    try:
        descriptor = os.open(
            request_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
        )
        with os.fdopen(descriptor, "rb") as stream:
            opened = os.fstat(stream.fileno())
            if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
                raise RuntimeErrorSafe("Music import request changed while opening")
            raw = stream.read(REQUEST_LIMIT + 1)
    except RuntimeErrorSafe:
        raise
    except OSError:
        raise RuntimeErrorSafe("Music import request file could not be read safely") from None
    if len(raw) > REQUEST_LIMIT:
        raise RuntimeErrorSafe("Music import request file exceeds 64 KiB")
    try:
        request = json.loads(raw)
    except (ValueError, UnicodeError):
        raise RuntimeErrorSafe("Music import request is not valid JSON") from None
    if not isinstance(request, dict) or set(request) != {
        "schema", "source_path", "expected_sha256", "expected_size"
    }:
        raise RuntimeErrorSafe("Music import request has an invalid schema")
    if type(request["schema"]) is not int or request["schema"] != 1:
        raise RuntimeErrorSafe("Music import request schema must be 1")
    source_path = request["source_path"]
    if (
        not isinstance(source_path, str)
        or not source_path
        or "\0" in source_path
        or not Path(source_path).is_absolute()
    ):
        raise RuntimeErrorSafe("Music import source path must be an absolute path")
    digest = request["expected_sha256"]
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-fA-F]{64}", digest) is None:
        raise RuntimeErrorSafe("Music import request has an invalid SHA-256")
    size = request["expected_size"]
    if type(size) is not int or size <= 0 or size > (1 << 63) - 1:
        raise RuntimeErrorSafe("Music import request has an invalid expected size")
    return request


def _no_symlinks(path: Path, root: Path | None = None) -> None:
    parts = [path, *path.parents]
    if root is not None:
        resolved_root = root.absolute()
        parts = parts[: parts.index(resolved_root) + 1] if resolved_root in parts else [path]
    for part in parts:
        if part.is_symlink():
            raise RuntimeErrorSafe(f"symbolic link is unsupported: {part}")


def _embedded_info_plist(binary: Path) -> dict:
    """Read the Mach-O __info_plist section via otool's bounded hex dump."""
    result = _run(["/usr/bin/otool", "-s", "__TEXT", "__info_plist", binary])
    payload = _otool_dump_bytes(result.stdout)
    try:
        info = plistlib.loads(bytes(payload).rstrip(b"\0"))
    except (ValueError, plistlib.InvalidFileException):
        raise RuntimeErrorSafe("test binary has no readable embedded Info.plist") from None
    if not isinstance(info, dict):
        raise RuntimeErrorSafe("test binary embedded Info.plist is invalid")
    return info


def _otool_dump_bytes(output: str) -> bytes:
    """Decode otool's 32-bit little-endian words in memory byte order."""
    payload = bytearray()
    for line in output.splitlines()[2:]:
        match = re.match(r"^\s*[0-9a-fA-F]+\s+((?:[0-9a-fA-F]{8}\s*)+)", line)
        if not match:
            continue
        for word in match.group(1).split():
            payload.extend(bytes.fromhex(word)[::-1])
    return bytes(payload)


def _validate_binary(binary: Path) -> dict:
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeErrorSafe("staged test binary is missing or not executable")
    production = _home_path(PRODUCTION_BINARY)
    try:
        if binary.resolve() == production.resolve() or (
            production.exists() and os.path.samefile(binary, production)
        ):
            raise RuntimeErrorSafe("production binary cannot be used as the test artifact")
    except OSError:
        raise RuntimeErrorSafe("binary identity could not be checked") from None
    info = _embedded_info_plist(binary)
    if info.get("CFBundleIdentifier") != TEST_IDENTIFIER:
        raise RuntimeErrorSafe("staged binary has the wrong test bundle identifier")
    if info.get("CFBundleExecutable") != TEST_BINARY.name:
        raise RuntimeErrorSafe("staged binary has the wrong test executable name")
    usage = info.get("NSAppleEventsUsageDescription")
    if not isinstance(usage, str) or not usage.strip():
        raise RuntimeErrorSafe("staged test binary lacks its Apple Events usage description")
    return info


def _designated_requirement(binary: Path) -> str:
    result = _run(["/usr/bin/codesign", "-d", "-r-", binary])
    output = result.stderr + result.stdout
    match = re.search(r"designated => (.+)", output)
    return match.group(1).strip() if match else ""


def _require_test_requirement(binary: Path) -> None:
    expected = (
        f'identifier "{TEST_IDENTIFIER}" and certificate root = H"{TRUSTED_FINGERPRINT.lower()}"'
    )
    _run(
        [
            "/usr/bin/codesign",
            "--verify",
            "--strict",
            "--test-requirement",
            "=" + expected,
            binary,
        ]
    )
    if _designated_requirement(binary) != expected:
        raise RuntimeErrorSafe("test binary does not have the required designated requirement")


def _active_test_process() -> bool:
    expected = str(_home_path(TEST_BINARY))
    anchored_path = "^" + re.escape(expected) + r"([[:space:]]|$)"
    result = _run(["/usr/bin/pgrep", "-f", anchored_path], check=False)
    if result.returncode == 1 and not result.stdout.strip():
        return False
    if result.returncode:
        raise RuntimeErrorSafe("pgrep could not determine whether the test binary is running")
    pids = result.stdout.splitlines()
    if len(pids) > 100 or any(not pid.isdecimal() for pid in pids):
        raise RuntimeErrorSafe("pgrep returned an invalid test process list")
    return bool(pids)


def _converter_destination() -> Path:
    return _home_path(
        Path(".local/lib/telegram-video-downloader-test/libexec/flac-alac-converter")
    )


def _converter_installer_args(source: Path, *, apply: bool) -> list[str]:
    arguments = [
        sys.executable,
        str(CONVERTER_INSTALLER),
        "--profile",
        "test",
        "--source",
        str(source),
        "--identity",
        TRUSTED_FINGERPRINT,
    ]
    if apply:
        arguments.append("--apply")
    return arguments


def plan(binary: Path | None = None, converter_binary: Path | None = None) -> dict:
    """Read-only artifact and destination preflight."""
    selected = Path(binary) if binary else _home_path(TEST_BINARY)
    result = {
        "action": "plan",
        "binary": str(selected),
        "destination": str(_home_path(TEST_BINARY)),
        "identifier": TEST_IDENTIFIER,
        "converter_identifier": CONVERTER_IDENTIFIER,
        "converter_destination": str(_converter_destination()),
        "label": TEST_LABEL,
        "valid": False,
    }
    if binary is not None:
        _validate_binary(selected)
    if converter_binary is not None:
        _run(
            _converter_installer_args(converter_binary, apply=False),
            timeout=CONVERTER_INSTALL_TIMEOUT,
        )
        result["converter_source"] = str(converter_binary)
    installed = _home_path(TEST_BINARY)
    if installed.exists():
        _no_symlinks(installed, Path.home())
        _require_test_requirement(installed)
        if _active_test_process():
            raise RuntimeErrorSafe("test binary is currently running")
        result["installed"] = True
    else:
        result["installed"] = False
    result["valid"] = True
    return result


def install(binary: Path, converter_binary: Path | None = None) -> dict:
    """Sign and atomically install only a caller-provided staged test artifact."""
    source = Path(binary).resolve(strict=True)
    _validate_binary(source)
    target = _home_path(TEST_BINARY)
    _no_symlinks(target, Path.home())
    if target.exists():
        _require_test_requirement(target)
        if _active_test_process():
            raise RuntimeErrorSafe("test binary is currently running")
    if converter_binary is not None:
        _run(
            _converter_installer_args(converter_binary, apply=False),
            timeout=CONVERTER_INSTALL_TIMEOUT,
        )
    if not ENTITLEMENTS.is_file():
        raise RuntimeErrorSafe("tracked Music automation entitlements file is missing")
    identities = _run(["/usr/bin/security", "find-identity", "-v", "-p", "codesigning"])
    if TRUSTED_FINGERPRINT not in identities.stdout:
        raise RuntimeErrorSafe("the required trusted signing identity is unavailable")

    target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
    rollback_dir = target.parent.parent / "rollback"
    rollback_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if target.exists():
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup = rollback_dir / f"{target.name}.{stamp}.backup"
        shutil.copy2(target, backup)
        backup.chmod(0o700)
    with tempfile.TemporaryDirectory(prefix="music-test-install-", dir=target.parent) as tmp:
        staged = Path(tmp) / target.name
        shutil.copy2(source, staged)
        staged.chmod(0o755)
        _validate_binary(staged)
        _run(
            [
                "/usr/bin/codesign",
                "--force",
                "--options",
                "runtime",
                "--identifier",
                TEST_IDENTIFIER,
                "--entitlements",
                ENTITLEMENTS,
                "--sign",
                TRUSTED_FINGERPRINT,
                staged,
            ],
            timeout=None,
        )
        _require_test_requirement(staged)
        if converter_binary is not None:
            _run(
                _converter_installer_args(converter_binary, apply=True),
                timeout=CONVERTER_INSTALL_TIMEOUT,
            )
        os.replace(staged, target)
    target.chmod(0o755)
    result = {"action": "install", "binary": str(target), "installed": True}
    if converter_binary is not None:
        result["converter"] = str(_converter_destination())
    return result


def _launchd_domain() -> str:
    return f"gui/{os.getuid()}"


def _launchd_status() -> dict:
    result = _run(
        ["/bin/launchctl", "print", f"{_launchd_domain()}/{TEST_LABEL}"], check=False
    )
    if result.returncode:
        diagnostic = result.stderr + result.stdout
        if "Could not find service" not in diagnostic and "Could not find domain" not in diagnostic:
            raise RuntimeErrorSafe("launchctl could not determine Music test job state")
        return {"loaded": False, "pid": None, "last_exit_code": None}
    output = result.stdout
    pid = re.search(r"\bpid = (\d+)", output)
    exit_code = re.search(r"\blast exit code = (-?\d+)", output)
    return {
        "loaded": True,
        "pid": int(pid.group(1)) if pid else None,
        "last_exit_code": int(exit_code.group(1)) if exit_code else None,
    }


def _private_paths(*, create: bool = True) -> tuple[Path, Path, Path]:
    state = _home_path(Path(".local/lib/telegram-video-downloader-test/music-check"))
    if create:
        state.mkdir(mode=0o700, parents=True, exist_ok=True)
    elif not state.exists():
        return state, state / "stdout.log", state / "agent.plist"
    if state.is_symlink() or not state.is_dir() or state.stat().st_uid != os.geteuid():
        raise RuntimeErrorSafe("Music test state directory has invalid ownership or type")
    if create:
        state.chmod(0o700)
    elif stat.S_IMODE(state.stat().st_mode) != 0o700:
        raise RuntimeErrorSafe("Music test state directory must have mode 0700")
    stdout_path, plist_path = state / "stdout.log", state / "agent.plist"
    for path in (stdout_path, plist_path):
        if path.is_symlink():
            raise RuntimeErrorSafe("Music test state file cannot be a symbolic link")
        if path.exists():
            metadata = path.stat()
            if (
                not path.is_file()
                or metadata.st_uid != os.geteuid()
                or metadata.st_nlink != 1
                or stat.S_IMODE(metadata.st_mode) != 0o600
            ):
                raise RuntimeErrorSafe("Music test state file has invalid metadata")
    return state, stdout_path, plist_path


def launch_action(action: str, request_path: Path | None = None) -> dict:
    """Load one one-shot GUI agent; launchd prompts in the logged-in user's session."""
    import_actions = {"import", "import-status"}
    if action not in {"check", "authorize", "quit", *import_actions}:
        raise RuntimeErrorSafe("unsupported Music authorization action")
    if action in import_actions:
        if request_path is None:
            raise RuntimeErrorSafe(f"{action} requires --request PATH")
        _validate_import_request(request_path)
    elif request_path is not None:
        raise RuntimeErrorSafe("--request is only valid for import actions")
    binary = _home_path(TEST_BINARY)
    _require_test_requirement(binary)
    state, stdout_path, plist_path = _private_paths()
    status = _launchd_status()
    if not status["loaded"] and _active_test_process():
        return {"action": action, "state": "pending", "reason": "active test process"}
    if status["loaded"]:
        if status["pid"] is not None or status["last_exit_code"] is None:
            return {"action": action, "state": "pending", **status}
        _run(["/bin/launchctl", "bootout", f"{_launchd_domain()}/{TEST_LABEL}"])
        # A completed prior run is no longer active; make the next receipt unambiguous.
        stdout_path.write_bytes(b"")
        stdout_path.chmod(0o600)
    native_flag = {
        "check": "--music-check",
        "authorize": "--music-authorize",
        "quit": "--music-quit",
        "import": "--music-import",
        "import-status": "--music-import-status",
    }[action]
    arguments = [str(binary), native_flag]
    if request_path is not None:
        arguments.append(str(request_path))
    plist = {
        "Label": TEST_LABEL,
        "ProgramArguments": arguments,
        "RunAtLoad": True,
        "KeepAlive": False,
        "StandardOutPath": str(stdout_path),
        "StandardErrorPath": str(stdout_path),
        "ProcessType": "Background",
    }
    encoded = plistlib.dumps(plist, fmt=plistlib.FMT_XML, sort_keys=True)
    temporary = plist_path.with_suffix(".plist.tmp")
    temporary.write_bytes(encoded)
    temporary.chmod(0o600)
    os.replace(temporary, plist_path)
    if not stdout_path.exists():
        stdout_path.touch(mode=0o600)
    stdout_path.chmod(0o600)
    _run(["/bin/launchctl", "bootstrap", _launchd_domain(), plist_path])
    return {"action": action, "state": "launched", "label": TEST_LABEL}


def status() -> dict:
    _, stdout_path, _ = _private_paths(create=False)
    result = {"action": "status", **_launchd_status()}
    if stdout_path.is_file():
        with stdout_path.open("rb") as stream:
            stream.seek(max(0, stdout_path.stat().st_size - LOG_LIMIT))
            data = stream.read(LOG_LIMIT)
        try:
            result["record"] = json.loads(data.decode("utf-8").strip().splitlines()[-1])
        except (ValueError, UnicodeError, IndexError):
            result["record"] = None
            completed = result["pid"] is None and (
                not result["loaded"] or result["last_exit_code"] is not None
            )
            if completed and data.strip():
                diagnostic = data[-2048:].decode("utf-8", errors="replace").strip()
                if diagnostic:
                    result["diagnostic"] = diagnostic
    return result


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "action",
        choices=(
            "plan", "install", "check", "authorize", "quit", "status", "import", "import-status"
        ),
    )
    result.add_argument("--binary", type=Path, help="staged test artifact for plan/install")
    result.add_argument(
        "--converter-binary",
        type=Path,
        help="staged native FLAC converter to install explicitly for plan/install",
    )
    result.add_argument("--request", type=Path, help="private numbered Music import request")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        request_actions = {"import", "import-status"}
        if args.action in request_actions and args.request is None:
            raise RuntimeErrorSafe(f"{args.action} requires --request PATH")
        if args.action not in request_actions and args.request is not None:
            raise RuntimeErrorSafe("--request is only valid for import actions")
        if args.converter_binary is not None and args.action not in {"plan", "install"}:
            raise RuntimeErrorSafe("--converter-binary is only valid for plan or install")
        if args.action == "plan":
            result = plan(args.binary, args.converter_binary)
        elif args.action == "install":
            if args.binary is None:
                raise RuntimeErrorSafe("install requires --binary STAGED_TEST_ARTIFACT")
            result = install(args.binary, args.converter_binary)
        elif args.action in {"check", "authorize", "quit", *request_actions}:
            result = launch_action(args.action, args.request)
        else:
            result = status()
    except RuntimeErrorSafe as error:
        print(json.dumps({"error": str(error)}, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
