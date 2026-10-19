#!/usr/bin/env python3
"""Explicit signed production install. Existing config, cwd, logs and state stay put."""

from __future__ import annotations

import argparse
import copy
import datetime
import fcntl
import hashlib
import json
import os
import plistlib
import re
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
import tomllib
from contextlib import contextmanager, nullcontext, suppress
from pathlib import Path

IDENTIFIER = "io.github.telegram-local-downloader.bot"
EXECUTABLE = "telegram-video-downloader"
LAUNCHD_CLOUD_SHARED_SECRET = "TELEGRAM_VIDEO_DOWNLOADER_CLOUD_SHARED_SECRET"
STOP_TIMEOUT = 15
SNAPSHOT_SECONDS = 120
SNAPSHOT_BYTES = 256 * 1024 * 1024
# launchd.plist(5), execvp(3), and the Darwin SDK's paths.h define this lookup.
LAUNCHD_STDPATH = "/usr/bin:/bin:/usr/sbin:/sbin"
AUTH_REMOVE_NAME = re.compile(r"\.telegram-video-downloader-remove-[0-9]+-[0-9a-f]{16}")


class InstallError(Exception):
    """A credential-free deployment diagnostic."""


def run(argv, *, check=True, capture=True, **kwargs):
    result = subprocess.run(
        [str(value) for value in argv],
        capture_output=capture,
        text=True,
        timeout=kwargs.pop("timeout", 60),
        **kwargs,
    )
    if check and result.returncode:
        # External diagnostics can contain configuration values.
        raise InstallError(f"{Path(str(argv[0])).name} failed (exit {result.returncode})")
    return result


def absolute(path, base):
    value = str(path)
    for marker in ("~", "$HOME", "${HOME}"):
        if value == marker:
            value = str(Path.home())
            break
        prefix = marker + "/"
        if value.startswith(prefix):
            value = str(Path.home() / value[len(prefix) :])
            break
    if "$" in value:
        raise InstallError("unsupported variable in a configured path")
    if "\0" in value:
        raise InstallError("invalid configured path")
    expanded = Path(value)
    absolute_base = Path(base) if Path(base).is_absolute() else Path.cwd() / base
    return expanded if expanded.is_absolute() else absolute_base / expanded


def config_path(value, config_parent, runtime_home):
    """Match Rust's config expansion without changing filesystem '..' semantics."""
    value = str(value)
    if "\0" in value:
        raise InstallError("invalid configured path")
    expanded = Path(value)
    if runtime_home is not None:
        for marker in ("~", "$HOME", "${HOME}"):
            if value == marker:
                expanded = runtime_home
                break
            prefix = marker + "/"
            if value.startswith(prefix):
                expanded = runtime_home / value[len(prefix) :]
                break
    return expanded if expanded.is_absolute() else Path(config_parent) / expanded


def plist_path(value, working_directory):
    """LaunchAgent paths are literal strings; launchd does not expand HOME."""
    if not isinstance(value, str) or not value or "\0" in value:
        raise InstallError("LaunchAgent contains an unsupported path")
    path = Path(value)
    return path if path.is_absolute() else Path(working_directory) / path


def resolve_launchd_executable(argument, working_directory, captured=None):
    """Match launchd's standard-path lookup and execvp's slash exception."""
    if not isinstance(argument, str) or not argument or "\0" in argument:
        raise InstallError("LaunchAgent contains an unsupported executable argument")
    if "/" in argument:
        executable = plist_path(argument, working_directory)
    elif captured is not None:
        # Default rollback remains usable after the original executable was removed.
        executable = Path(captured)
        candidates = {Path(directory) / argument for directory in LAUNCHD_STDPATH.split(":")}
        if executable not in candidates:
            raise InstallError("saved executable does not match launchd's standard search path")
    else:
        found = shutil.which(argument, path=LAUNCHD_STDPATH)
        if found is None:
            raise InstallError(
                "LaunchAgent executable was not found in launchd's standard search path"
            )
        executable = Path(found)
    if captured is not None and executable != Path(captured):
        raise InstallError("saved executable does not match the LaunchAgent argument")
    return executable


def no_symlinks(path):
    """Protect path identity: never copy or replace through symbolic links."""
    for entry in [Path(path), *Path(path).parents]:
        if entry.is_symlink():
            raise InstallError(f"symbolic link is unsupported: {entry}")


def validate_stderr_path(path):
    """Require observable stderr without opening special files or changing logs."""
    try:
        path = Path(path).resolve(strict=False)
    except (OSError, RuntimeError):
        raise InstallError("StandardErrorPath target cannot be resolved") from None
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        parent = path.parent
        while True:
            try:
                metadata = parent.stat()
                break
            except FileNotFoundError:
                parent = parent.parent
            except OSError:
                raise InstallError("StandardErrorPath parent cannot be inspected") from None
        if not stat.S_ISDIR(metadata.st_mode) or not os.access(parent, os.W_OK | os.X_OK):
            raise InstallError(
                "StandardErrorPath requires a writable and searchable directory"
            ) from None
        return
    except OSError:
        raise InstallError("StandardErrorPath cannot be inspected") from None
    if not stat.S_ISREG(metadata.st_mode):
        raise InstallError("StandardErrorPath must be a regular file to observe readiness")
    if not os.access(path, os.R_OK | os.W_OK):
        raise InstallError("StandardErrorPath must be readable and writable to observe readiness")


def traversal_error(error):
    raise InstallError("runtime state directory could not be completely enumerated") from error


def strict_walk(path):
    return os.walk(path, followlinks=False, onerror=traversal_error)


def auth_recovery_entry(path):
    """Recognize private safe_fs removal journals without scanning unrelated trees."""
    path = Path(path)
    tombstone = path.name.endswith(".cleanup.json")
    name = path.name.removesuffix(".cleanup.json") if tombstone else path.name
    if not AUTH_REMOVE_NAME.fullmatch(name):
        return False
    no_symlinks(path)
    metadata = path.lstat()
    if not tombstone and (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise InstallError("managed auth recovery directory has invalid ownership or permissions")
    manifest_path = path if tombstone else path / "manifest.json"
    no_symlinks(manifest_path)
    descriptor = os.open(manifest_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        with os.fdopen(descriptor, "rb") as stream:
            manifest_metadata = os.fstat(stream.fileno())
            if (
                not stat.S_ISREG(manifest_metadata.st_mode)
                or manifest_metadata.st_uid != os.geteuid()
                or stat.S_IMODE(manifest_metadata.st_mode) != 0o600
                or manifest_metadata.st_nlink != 1
                or manifest_metadata.st_size > 1024
            ):
                raise InstallError("managed auth recovery manifest has invalid metadata")
            manifest = json.loads(stream.read(1025))
    except (ValueError, UnicodeError):
        raise InstallError("managed auth recovery manifest is invalid") from None
    parent = path.parent.stat()
    if (
        not isinstance(manifest, dict)
        or type(manifest.get("version")) is not int
        or manifest["version"] not in {1, 2, 3}
        or manifest.get("quarantine_name") != name
        or manifest.get("parent_device") != parent.st_dev
        or manifest.get("parent_inode") != parent.st_ino
        or type(manifest.get("entry_is_directory")) is not bool
        or any(type(manifest.get(key)) is not int for key in ("entry_device", "entry_inode"))
    ):
        raise InstallError("managed auth recovery manifest does not describe its directory")
    if tombstone and manifest["version"] < 2:
        raise InstallError("managed auth cleanup tombstone has an unsupported version")
    if manifest["version"] >= 2:
        expected = (manifest.get("quarantine_device"), manifest.get("quarantine_inode"))
        if any(type(value) is not int for value in expected) or (
            not tombstone and expected != (metadata.st_dev, metadata.st_ino)
        ):
            raise InstallError(
                "managed auth recovery directory identity does not match its manifest"
            )
    if manifest["version"] == 3:
        original = manifest.get("original_name_hex")
        if not isinstance(original, str) or not re.fullmatch(r"(?:[0-9a-f]{2})+", original):
            raise InstallError("managed auth recovery manifest has an invalid original name")
        original = bytes.fromhex(original)
        if b"/" in original or b"\0" in original or original in {b".", b".."}:
            raise InstallError("managed auth recovery manifest has an invalid original name")
    if not tombstone:
        entry = path / "entry"
        no_symlinks(entry)
        try:
            entry_metadata = entry.stat()
        except FileNotFoundError:
            entry_metadata = None
        if entry_metadata and (
            (entry_metadata.st_dev, entry_metadata.st_ino)
            != (manifest["entry_device"], manifest["entry_inode"])
            or stat.S_ISDIR(entry_metadata.st_mode) != manifest["entry_is_directory"]
        ):
            raise InstallError("quarantined auth entry identity does not match its manifest")
    return True


def file_binding(configured, capture=False):
    """Bind a read-only alias to stable file identity and bytes, allowing symlinks."""
    configured = Path(configured)
    try:
        physical = configured.resolve(strict=True)
        preliminary = physical.stat()
        if not stat.S_ISREG(preliminary.st_mode) or preliminary.st_size > SNAPSHOT_BYTES:
            raise InstallError("read-only service input is not a supported regular file")
        descriptor = os.open(physical, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            metadata = os.fstat(stream.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > SNAPSHOT_BYTES:
                raise InstallError("read-only service input is not a supported regular file")
            digest, chunks, total = hashlib.sha256(), [], 0
            while chunk := stream.read(65536):
                total += len(chunk)
                if total > SNAPSHOT_BYTES:
                    raise InstallError("read-only service input exceeded its size budget")
                digest.update(chunk)
                if capture:
                    chunks.append(chunk)
            stream.seek(0)
            second, second_total = hashlib.sha256(), 0
            while chunk := stream.read(65536):
                second_total += len(chunk)
                if second_total > SNAPSHOT_BYTES:
                    raise InstallError("read-only service input exceeded its size budget")
                second.update(chunk)
            if digest.hexdigest() != second.hexdigest():
                raise InstallError("read-only service input changed while being read")
        current = physical.stat()
        if configured.resolve(strict=True) != physical or (current.st_dev, current.st_ino) != (
            metadata.st_dev,
            metadata.st_ino,
        ):
            raise InstallError("read-only service input binding changed while being read")
    except (OSError, RuntimeError):
        raise InstallError(f"read-only service input cannot be bound: {configured}") from None
    return {
        "configured": str(configured),
        "physical": str(physical),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "sha256": digest.hexdigest(),
    }, b"".join(chunks) if capture else None


def directory_binding(configured):
    try:
        physical = Path(configured).resolve(strict=True)
        metadata = physical.stat()
    except (OSError, RuntimeError):
        raise InstallError(f"working directory cannot be bound: {configured}") from None
    if not stat.S_ISDIR(metadata.st_mode):
        raise InstallError("working directory must resolve to a directory")
    return {
        "configured": str(configured),
        "physical": str(physical),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
    }


def detect_dotnet_root(explicit=None, home=None):
    def valid(candidate):
        return (candidate / "shared/Microsoft.NETCore.App").is_dir() or (
            candidate / "host/fxr"
        ).is_dir()

    if explicit:
        candidate = absolute(explicit, Path.cwd())
        if not valid(candidate):
            raise InstallError("BOT_DOTNET_ROOT does not identify a .NET runtime root")
        return candidate.resolve(strict=True)
    if shutil.which("dotnet"):
        try:
            result = run(["dotnet", "--list-runtimes"], check=False, timeout=10)
            for line in result.stdout.splitlines():
                match = re.search(r"\[(.+)/shared/[^\]]+\]", line)
                if match and valid(Path(match.group(1))):
                    return Path(match.group(1)).resolve(strict=True)
        except (OSError, subprocess.TimeoutExpired):
            pass
    for candidate in [
        Path(home or Path.home()) / ".dotnet",
        Path("/usr/local/share/dotnet"),
        Path("/opt/homebrew/share/dotnet"),
    ]:
        if valid(candidate):
            return candidate.resolve(strict=True)
    return None


def check_tree(path):
    no_symlinks(path)
    try:
        metadata = Path(path).lstat()
    except FileNotFoundError:
        return
    except OSError:
        raise InstallError(f"runtime state entry cannot be inspected: {path}") from None
    if not (stat.S_ISREG(metadata.st_mode) or stat.S_ISDIR(metadata.st_mode)):
        raise InstallError(f"unsupported filesystem entry: {path}")
    required_access = os.R_OK | os.X_OK if stat.S_ISDIR(metadata.st_mode) else os.R_OK
    if not os.access(path, required_access):
        raise InstallError(f"runtime state entry is not readable: {path}")
    if stat.S_ISDIR(metadata.st_mode):
        for root, dirs, files in strict_walk(path):
            for name in dirs + files:
                child = Path(root) / name
                mode = child.lstat().st_mode
                if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                    raise InstallError(f"unsupported state entry: {child}")
                required_access = os.R_OK | os.X_OK if stat.S_ISDIR(mode) else os.R_OK
                if not os.access(child, required_access):
                    raise InstallError(f"runtime state entry is not readable: {child}")


def preserve_copy(source, destination):
    """Retain ownership, mode, ACLs and xattrs; copied locks are evidence only."""
    source, destination = Path(source), Path(destination)
    check_tree(source)
    no_symlinks(destination)
    if destination.exists():
        raise InstallError(f"backup destination already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    run(["ditto", "--rsrc", "--extattr", "--acl", source, destination])
    entries = [source]
    if source.is_dir():
        entries.extend(
            Path(root) / name for root, dirs, files in strict_walk(source) for name in dirs + files
        )
    for old in entries:
        new = destination / old.relative_to(source) if source.is_dir() else destination
        a, b = old.stat(), new.stat()
        if (a.st_uid, a.st_gid, stat.S_IMODE(a.st_mode)) != (
            b.st_uid,
            b.st_gid,
            stat.S_IMODE(b.st_mode),
        ):
            raise InstallError(f"backup ownership or mode was not preserved: {old}")
        if old.is_file():
            with old.open("rb") as first, new.open("rb") as second:
                if (
                    hashlib.file_digest(first, "sha256").digest()
                    != hashlib.file_digest(second, "sha256").digest()
                ):
                    raise InstallError(f"backup content verification failed: {old}")


def stage_artifact(source, destination, trusted_binary):
    """Normalize only the new executable, without changing its build artifact.

    A temporary build may inherit group wheel from /private/tmp. Deployment uses
    the trusted installed executable's ownership, or this user for first install.
    State and rollback backups continue to use strict preserve_copy instead.
    """
    source, destination, trusted_binary = map(Path, (source, destination, trusted_binary))
    check_tree(source)
    no_symlinks(destination)
    if not source.is_file() or not os.access(source, os.X_OK):
        raise InstallError(f"input artifact is not executable: {source}")
    if destination.exists():
        raise InstallError(f"staging destination already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    run(["ditto", "--rsrc", "--extattr", "--acl", source, destination])
    if trusted_binary.exists():
        trusted = trusted_binary.stat()
        owner, group = trusted.st_uid, trusted.st_gid
    else:
        owner, group = os.geteuid(), os.getegid()
    staged = destination.stat()
    if (staged.st_uid, staged.st_gid) != (owner, group):
        os.chown(destination, owner, group)
    os.chmod(destination, 0o755)
    staged = destination.stat()
    if (staged.st_uid, staged.st_gid, stat.S_IMODE(staged.st_mode)) != (owner, group, 0o755):
        raise InstallError("staged executable ownership or mode could not be normalized")
    with source.open("rb") as first, destination.open("rb") as second:
        if (
            hashlib.file_digest(first, "sha256").digest()
            != hashlib.file_digest(second, "sha256").digest()
        ):
            raise InstallError("staged executable content differs from the input artifact")


def atomic_bytes(path, data, mode=0o600):
    path = Path(path)
    no_symlinks(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def atomic_install(source, destination):
    """Copy the signed artifact to a sibling, then replace in one rename."""
    destination = Path(destination)
    no_symlinks(destination)
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
    fd, temporary = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    os.close(fd)
    os.unlink(temporary)
    try:
        preserve_copy(source, temporary)
        with open(temporary, "rb") as stream:
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def service_info(target):
    result = run(["launchctl", "print", target], check=False)
    if result.returncode:
        diagnostic = result.stdout + result.stderr
        if "Could not find service" in diagnostic or "Could not find domain" in diagnostic:
            return None
        raise InstallError(f"cannot query LaunchAgent: {target}")
    match = re.search(r"^\s*pid = (\d+)\s*$", result.stdout, re.MULTILINE)
    return {"target": target, "pid": int(match.group(1)) if match else None}


def pid_exists(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def stop_services(targets):
    initial = [info for target in targets if (info := service_info(target))]
    for info in initial:
        run(["launchctl", "bootout", info["target"]])
    deadline = time.monotonic() + STOP_TIMEOUT
    while True:
        remaining = any(service_info(info["target"]) is not None for info in initial)
        remaining |= any(info["pid"] and pid_exists(info["pid"]) for info in initial)
        if not remaining:
            return initial
        if time.monotonic() >= deadline:
            raise InstallError("LaunchAgent registration or its old PID remained after bootout")
        time.sleep(0.1)


def process_inventory():
    """Read this user's executable paths without truncated names or arguments."""
    result = run(["/bin/ps", "-axww", "-o", "pid=,ppid=,uid=,comm="], check=False)
    if result.returncode:
        raise InstallError("cannot inspect downloader processes")
    inventory = {}
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        fields = line.strip().split(None, 3)
        if len(fields) != 4 or not all(re.fullmatch(r"[0-9]+", value) for value in fields[:3]):
            raise InstallError("unexpected downloader process inventory")
        pid, parent, uid = (int(value) for value in fields[:3])
        if uid != os.geteuid():
            continue
        executable = Path(fields[3])
        if pid in inventory:
            raise InstallError("duplicate PID in downloader process inventory")
        inventory[pid] = {"parent": parent, "uid": uid, "executable": executable}
    return inventory


def bot_processes(include_workers=False, *, expected_executables=()):
    inventory = process_inventory()
    expected_paths = {Path(path) for path in expected_executables}
    try:
        expected_paths |= {path.resolve() for path in expected_paths}
    except OSError:
        raise InstallError("cannot resolve the expected downloader executable") from None
    expected_names = {path.name for path in expected_paths}

    def matches(executable):
        if executable.name == EXECUTABLE or executable in expected_paths:
            return True
        if executable.name not in expected_names:
            return False
        try:
            return executable.resolve() in expected_paths
        except OSError:
            raise InstallError("cannot resolve a downloader process executable") from None

    downloaders = {
        pid for pid, process in inventory.items() if pid > 0 and matches(process["executable"])
    }
    if include_workers:
        return sorted(downloaders)
    roots = []
    for pid in downloaders:
        ancestor, visited = inventory[pid]["parent"], {pid}
        while ancestor > 0 and ancestor in inventory:
            if ancestor in visited:
                raise InstallError("cyclic downloader ancestry in process inventory")
            if ancestor in downloaders:
                break
            visited.add(ancestor)
            ancestor = inventory[ancestor]["parent"]
        else:
            roots.append(pid)
    return sorted(roots)


def wait_for_no_bot_processes(*, expected_executables=()):
    """Do not replace or restart until old service and worker images have exited."""
    deadline = time.monotonic() + STOP_TIMEOUT
    while True:
        remaining = bot_processes(include_workers=True, expected_executables=expected_executables)
        if not remaining:
            return
        if time.monotonic() >= deadline:
            raise InstallError(
                f"downloader executable PIDs remained after service stop: {remaining}"
            )
        time.sleep(0.1)


def signature(binary):
    run(["codesign", "--verify", "--strict", binary])
    result = run(["codesign", "-dr", "-", binary])
    requirement = re.search(r"designated => (.+)", result.stdout + result.stderr)
    if not requirement:
        raise InstallError("existing executable has no designated requirement")
    requirement = requirement.group(1).strip()
    fingerprint = re.search(r'certificate (?:root|leaf|0) = H"([a-fA-F0-9]{40})"', requirement)
    if f'identifier "{IDENTIFIER}"' not in requirement or not fingerprint:
        raise InstallError("existing executable lacks the required certificate and identifier")
    identity = fingerprint.group(1).upper()
    identities = run(["security", "find-identity", "-v", "-p", "codesigning"])
    if not re.search(rf"\b{identity}\b", identities.stdout, re.IGNORECASE):
        raise InstallError(f"original signing identity is unavailable: {identity}")
    return requirement, identity


def initial_identity():
    identities = run(["security", "find-identity", "-v", "-p", "codesigning"]).stdout
    matches = re.findall(
        r'\b([A-Fa-f0-9]{40})\s+"Telegram Video Downloader Local Signing"\s*$',
        identities,
        re.MULTILINE,
    )
    if len(matches) != 1:
        raise InstallError("exact original signing identity is unavailable or ambiguous")
    identity = matches[0].upper()
    return f'identifier "{IDENTIFIER}" and certificate root = H"{identity.lower()}"', identity


@contextmanager
def deployment_lock():
    path = Path.home() / ".local/lib/telegram-video-downloader/deployment.lock"
    no_symlinks(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        owner = os.fstat(fd)
        if (
            owner.st_uid != os.geteuid()
            or owner.st_nlink != 1
            or not stat.S_ISREG(owner.st_mode)
            or stat.S_IMODE(owner.st_mode) != 0o600
        ):
            raise InstallError(
                "deployment lock must be private, owned by this user, and have one link"
            )
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise InstallError(
                "another production install or rollback is already running"
            ) from None
        yield
    finally:
        os.close(fd)


def build_release(repo, backup):
    """Supervise the whole Cargo process group with finite output and wall time."""
    build, cache = backup / "build", backup / "cargo-build"
    environment = {
        **os.environ,
        "CARGO_TARGET_DIR": str(build),
        "CARGO_BUILD_BUILD_DIR": str(cache),
    }
    command = [
        "cargo",
        "build",
        "--release",
        "--locked",
        "--manifest-path",
        str(repo / "Cargo.toml"),
        "--target-dir",
        str(build),
    ]
    log = backup / "build.log"
    with log.open("xb") as output:
        os.chmod(log, 0o600)
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=environment,
            start_new_session=True,
        )
        deadline = time.monotonic() + 1800
        written = 0
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                while selector.get_map():
                    if time.monotonic() >= deadline:
                        raise InstallError("release build exceeded its 30-minute budget")
                    for key, _ in selector.select(timeout=0.1):
                        chunk = os.read(key.fileobj.fileno(), 65536)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        available = 8 * 1024 * 1024 - written
                        output.write(chunk[:available])
                        written += min(len(chunk), available)
                        if len(chunk) > available:
                            raise InstallError("release build exceeded its 8-MiB diagnostic budget")
            process.wait(timeout=max(1, deadline - time.monotonic()))
            if process.returncode:
                raise InstallError(
                    f"cargo failed (exit {process.returncode}); private diagnostic: {log}"
                )
        except BaseException:
            # Cargo may already have exited while a descendant still owns stdout.
            # The new session belongs exclusively to this build invocation.
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
            try:
                with suppress(subprocess.TimeoutExpired):
                    process.wait(timeout=5)
            finally:
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
            raise
        finally:
            process.stdout.close()
    return build / "release" / EXECUTABLE


def file_manifest(root, budget=SNAPSHOT_BYTES, deadline=None):
    files = [Path(root)]
    if Path(root).is_dir():
        files = [
            Path(directory) / name for directory, _, names in strict_walk(root) for name in names
        ]
    entries = []
    total = 0
    for path in sorted(files):
        metadata = path.stat()
        total += metadata.st_size
        if total > budget or (deadline and time.monotonic() >= deadline):
            raise InstallError("state snapshot exceeded its size or time budget")
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        entries.append(
            {
                "path": str(path),
                "sha256": digest,
                "size": metadata.st_size,
                "uid": metadata.st_uid,
                "gid": metadata.st_gid,
                "mode": stat.S_IMODE(metadata.st_mode),
                "device": metadata.st_dev,
                "inode": metadata.st_ino,
                "links": metadata.st_nlink,
            }
        )
    return entries


def media_binding(configured):
    """Bind an allowed media alias to its physical directory's object identity.

    Child-entry churn and File Provider metadata changes do not change the root
    object. Only its resolved path and device/inode participate in revalidation.
    """
    try:
        physical = configured.resolve(strict=False)
        try:
            metadata = physical.stat()
        except FileNotFoundError:
            metadata = None
    except (OSError, RuntimeError):
        raise InstallError(f"cannot resolve media root: {configured}") from None
    if metadata and not stat.S_ISDIR(metadata.st_mode):
        raise InstallError(f"media root is not a directory: {configured}")
    return {
        "configured": str(configured),
        "physical": str(physical),
        "device": metadata.st_dev if metadata else None,
        "inode": metadata.st_ino if metadata else None,
    }


def anchor_evidence(path):
    """Inspect media hard-link anchors without opening or hashing their payload."""
    no_symlinks(path)
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode):
        raise InstallError(f"media anchor is not a regular file: {path}")
    return {
        "path": str(path),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "size": metadata.st_size,
        "links": metadata.st_nlink,
        "uid": metadata.st_uid,
        "gid": metadata.st_gid,
        "mode": stat.S_IMODE(metadata.st_mode),
        "evidence_only": True,
    }


def allowed_domains():
    return {f"gui/{os.getuid()}", f"user/{os.getuid()}"}


def validate_transaction(transaction, label=None):
    """Admit only complete, same-user rollback plans before any mutation."""
    if not isinstance(transaction, dict):
        raise InstallError("rollback transaction must be a dictionary")
    required = {
        "version",
        "label",
        "domain",
        "targets",
        "previous_services",
        "binary",
        "previous_binary",
        "plist",
        "had_plist",
        "had_installed_binary",
        "requirement",
    }
    if not required.issubset(transaction):
        raise InstallError("rollback transaction is incomplete")
    saved_label = transaction["label"]
    if (
        type(transaction["version"]) is not int
        or transaction["version"] != 1
        or not isinstance(saved_label, str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", saved_label)
        or (label is not None and saved_label != label)
    ):
        raise InstallError("backup does not match this LaunchAgent")
    domain = transaction["domain"]
    domains = allowed_domains()
    if not isinstance(domain, str) or domain not in domains:
        raise InstallError("rollback transaction domain must be this user's gui or user domain")
    targets = transaction["targets"]
    allowed_targets = {f"{candidate}/{saved_label}" for candidate in domains}
    if (
        not isinstance(targets, list)
        or not 1 <= len(targets) <= 2
        or not all(isinstance(target, str) and target in allowed_targets for target in targets)
        or len(set(targets)) != len(targets)
        or f"{domain}/{saved_label}" not in targets
    ):
        raise InstallError("rollback transaction has invalid or duplicate service targets")
    for key in ("had_plist", "had_installed_binary"):
        if type(transaction[key]) is not bool:
            raise InstallError(f"rollback transaction has an invalid {key}")
    expected_paths = {
        "binary": Path.home() / ".local/lib/telegram-video-downloader/bin" / EXECUTABLE,
        "plist": Path.home() / "Library/LaunchAgents" / f"{saved_label}.plist",
    }
    for key in ("binary", "previous_binary", "plist"):
        value = transaction[key]
        if (
            not isinstance(value, str)
            or not value
            or "\0" in value
            or not Path(value).is_absolute()
        ):
            raise InstallError(f"rollback transaction has an invalid {key} path")
        if key in expected_paths and value != str(expected_paths[key]):
            raise InstallError(f"rollback transaction has an unexpected {key} path")
    requirement = transaction["requirement"]
    if (
        not isinstance(requirement, str)
        or "\0" in requirement
        or f'identifier "{IDENTIFIER}"' not in requirement
        or not re.search(r'certificate (?:root|leaf|0) = H"[a-fA-F0-9]{40}"', requirement)
    ):
        raise InstallError("rollback transaction has an invalid designated requirement")
    services = transaction["previous_services"]
    if not isinstance(services, list) or len(services) > 1:
        raise InstallError("rollback transaction must restore at most one saved service")
    for service in services:
        if (
            not isinstance(service, dict)
            or "target" not in service
            or "pid" not in service
            or not isinstance(service["target"], str)
            or service["target"] not in targets
            or (
                service["pid"] is not None
                and (type(service["pid"]) is not int or not 0 < service["pid"] <= 2**31 - 1)
            )
        ):
            raise InstallError("rollback transaction has an invalid saved service")
    if services and not transaction["had_plist"]:
        raise InstallError("rollback transaction cannot restore a service without a saved plist")
    return copy.deepcopy(transaction)


def capture_log_offsets(paths):
    """Bind pre-bootstrap log objects and their append boundary, without contents."""
    offsets = {}
    for path in paths:
        try:
            metadata = path.stat()
        except FileNotFoundError:
            offsets[str(path)] = {"offset": 0, "device": None, "inode": None}
            continue
        except OSError:
            raise InstallError("cannot inspect the startup log boundary") from None
        if not stat.S_ISREG(metadata.st_mode):
            continue
        offsets[str(path)] = {
            "offset": metadata.st_size,
            "device": metadata.st_dev,
            "inode": metadata.st_ino,
        }
    return offsets


class _ReadinessLogScanner:
    """Scan only appended bytes, keeping bounded parser state for each log object."""

    CHUNK_BYTES = 256 * 1024
    TELEGRAM_STARTED = b"telegram local downloader started"
    STARTED = TELEGRAM_STARTED
    POLLS = {b"telegram polling completed poll=1", b"telegram polling completed poll=2"}
    TELEGRAM_STARTED_PREFIX = TELEGRAM_STARTED + b" concurrency="
    STARTED_PREFIX = TELEGRAM_STARTED_PREFIX
    CLOUD_STARTED_PREFIX = b"telegram local cloud ingress started concurrency="
    CLOUD_RECONCILED = b"cloud ingress reconciliation completed"
    LINE_BYTES = max(len(TELEGRAM_STARTED_PREFIX), len(CLOUD_STARTED_PREFIX)) + 20 + len(
        b" pid="
    ) + 10 + 1

    def __init__(self, paths, offsets, mode="telegram"):
        if mode not in {"telegram", "cloud"}:
            raise InstallError("readiness mode must be telegram or cloud")
        self.mode = mode
        self.paths = list(dict.fromkeys(paths))
        self.boundaries = {}
        self.streams = {}
        self.pid = None
        for path in self.paths:
            boundary = offsets.get(str(path), 0)
            if isinstance(boundary, int):
                # Retain the small direct-call interface; deployments capture object identity.
                boundary = {"offset": boundary, "device": None, "inode": None}
            self.boundaries[path] = dict(boundary)

    def scan(self, expected_pid):
        if type(expected_pid) is not int or not 0 < expected_pid <= 2**31 - 1:
            raise InstallError("readiness requires a valid observed service PID")
        if self.pid is not None and self.pid != expected_pid:
            raise InstallError("service PID changed during readiness log verification")
        self.pid = expected_pid
        pid_suffix = f" pid={self.pid}".encode()
        opened = []
        try:
            for path in self.paths:
                boundary = self.boundaries[path]
                try:
                    metadata = path.stat()
                    if not stat.S_ISREG(metadata.st_mode):
                        if boundary["device"] is not None:
                            raise InstallError("startup log changed during readiness verification")
                        continue
                    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
                    try:
                        opened_metadata = os.fstat(descriptor)
                        if not stat.S_ISREG(opened_metadata.st_mode):
                            raise InstallError("startup log changed during readiness verification")
                        stream = os.fdopen(descriptor, "rb")
                    except BaseException:
                        os.close(descriptor)
                        raise
                except FileNotFoundError:
                    if boundary["device"] is not None:
                        raise InstallError(
                            "startup log changed during readiness verification"
                        ) from None
                    continue
                except OSError:
                    raise InstallError(
                        "startup log cannot be read for readiness verification"
                    ) from None
                opened.append((path, stream))
                metadata = os.fstat(stream.fileno())
                identity = (metadata.st_dev, metadata.st_ino)
                if boundary["device"] is not None and identity != (
                    boundary["device"],
                    boundary["inode"],
                ):
                    raise InstallError("startup log changed during readiness verification")
                boundary["device"], boundary["inode"] = identity
                state = self.streams.get(identity)
                if state is None:
                    state = {
                        "offset": boundary["offset"],
                        "line": b"",
                        "overflow": False,
                        "started": False,
                        "polls": set(),
                        "cloud_reconciled": False,
                        "initialized": False,
                    }
                    self.streams[identity] = state
                elif not state["initialized"]:
                    # Aliases to one log object share the furthest captured boundary.
                    state["offset"] = max(state["offset"], boundary["offset"])
            scanned = set()
            for _, stream in opened:
                metadata = os.fstat(stream.fileno())
                identity = (metadata.st_dev, metadata.st_ino)
                if identity in scanned:
                    continue
                scanned.add(identity)
                state = self.streams[identity]
                # Device/inode protects the object; size protects the append boundary.
                # Normal appends may change all timestamps and are deliberately allowed.
                if metadata.st_size < state["offset"]:
                    raise InstallError("startup log changed during readiness verification")
                if not state["initialized"]:
                    if state["offset"]:
                        stream.seek(state["offset"] - 1)
                        state["overflow"] = stream.read(1) != b"\n"
                    state["initialized"] = True
                stream.seek(state["offset"])
                data = stream.read(self.CHUNK_BYTES)
                state["offset"] += len(data)
                fragments = data.split(b"\n")
                for index, fragment in enumerate(fragments):
                    line = state["line"] + fragment
                    if index < len(fragments) - 1:
                        if not state["overflow"] and len(line) <= self.LINE_BYTES:
                            line = line.removesuffix(b"\r")
                            if line.endswith(pid_suffix):
                                marker = line[: -len(pid_suffix)]
                                if self.mode == "telegram":
                                    state["started"] |= marker == self.TELEGRAM_STARTED or (
                                        marker.startswith(self.TELEGRAM_STARTED_PREFIX)
                                        and marker[len(self.TELEGRAM_STARTED_PREFIX) :].isdigit()
                                    )
                                    if marker in self.POLLS:
                                        state["polls"].add(marker)
                                else:
                                    state["started"] |= marker.startswith(
                                        self.CLOUD_STARTED_PREFIX
                                    ) and marker[len(self.CLOUD_STARTED_PREFIX) :].isdigit()
                                    state["cloud_reconciled"] |= marker == self.CLOUD_RECONCILED
                        state["line"], state["overflow"] = b"", False
                    else:
                        state["overflow"] |= len(line) > self.LINE_BYTES
                        state["line"] = line[-self.LINE_BYTES :]
            for path, stream in opened:
                try:
                    current = path.stat()
                except OSError:
                    raise InstallError(
                        "startup log changed during readiness verification"
                    ) from None
                opened_metadata = os.fstat(stream.fileno())
                if (current.st_dev, current.st_ino) != (
                    opened_metadata.st_dev,
                    opened_metadata.st_ino,
                ) or current.st_size < self.streams[(current.st_dev, current.st_ino)]["offset"]:
                    raise InstallError("startup log changed during readiness verification")
        finally:
            for _, stream in opened:
                stream.close()
        started = any(state["started"] for state in self.streams.values())
        if self.mode == "cloud":
            return started and any(
                state.get("cloud_reconciled", False) for state in self.streams.values()
            )
        polls = set().union(*(state["polls"] for state in self.streams.values()))
        return started and self.POLLS.issubset(polls)


class Deployment:
    def __init__(self, args):
        self.args = args
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", args.label):
            raise InstallError(
                "LaunchAgent label must contain only letters, digits, dot, underscore or dash"
            )
        if (
            not isinstance(args.domain, str)
            or args.domain not in allowed_domains()
            or (args.legacy_domain and args.legacy_domain not in allowed_domains())
        ):
            raise InstallError("production deployment requires this user's gui or user domain")
        self.home = Path.home()
        self.repo = absolute(args.repo, Path.cwd())
        self.readiness_mode = "telegram"
        self.launchd_cloud_shared_secret = None
        self.input_binary = absolute(args.binary, Path.cwd()) if args.binary else None
        self.binary = self.home / ".local/lib/telegram-video-downloader/bin" / EXECUTABLE
        self.backups = self.home / ".local/lib/telegram-video-downloader/backups"
        self.plist_path = self.home / "Library/LaunchAgents" / f"{args.label}.plist"
        self.targets = [f"{args.domain}/{args.label}"]
        if args.legacy_domain and args.legacy_domain != args.domain:
            self.targets.append(f"{args.legacy_domain}/{args.label}")
        no_symlinks(self.plist_path)
        if self.plist_path.exists():
            self.plist_binding, self.old_plist_bytes = file_binding(self.plist_path, capture=True)
        else:
            self.plist_binding, self.old_plist_bytes = None, None
        self.old_plist = plistlib.loads(self.old_plist_bytes) if self.old_plist_bytes else None
        environment = self.old_plist.get("EnvironmentVariables", {}) if self.old_plist else {}
        if not isinstance(environment, dict) or (
            "HOME" in environment and not isinstance(environment["HOME"], str)
        ):
            raise InstallError("existing LaunchAgent HOME must be a string")
        self.runtime_home = (
            Path(environment["HOME"])
            if environment.get("HOME")
            else None
            if "HOME" in environment
            else self.home
        )
        if self.old_plist and self.old_plist.get("Label") != args.label:
            raise InstallError("existing plist Label does not match the selected service")
        if self.old_plist and "Program" in self.old_plist:
            raise InstallError(
                "existing plist has an explicit Program override; normalize it first"
            )
        if self.old_plist:
            stderr = self.old_plist.get("StandardErrorPath")
            if (
                not isinstance(stderr, str)
                or not stderr
                or "\0" in stderr
                or not Path(stderr).is_absolute()
            ):
                raise InstallError(
                    "existing LaunchAgent requires an absolute StandardErrorPath "
                    "to capture startup and readiness records; configure it before deployment"
                )
        old_args = self.old_plist.get("ProgramArguments", []) if self.old_plist else []
        if self.old_plist and len(old_args) != 2:
            raise InstallError("expected an existing binary and config ProgramArguments pair")
        if self.old_plist:
            cwd = self.old_plist.get("WorkingDirectory")
            if not isinstance(cwd, str) or not cwd or "\0" in cwd or not Path(cwd).is_absolute():
                raise InstallError(
                    "existing LaunchAgent requires an explicit absolute WorkingDirectory"
                )
            self.workdir = Path(cwd)
        else:
            self.workdir = self.repo
        self.old_binary = (
            resolve_launchd_executable(old_args[0], self.workdir) if old_args else self.binary
        )
        self.config = (
            plist_path(old_args[1], self.workdir)
            if old_args
            else absolute(args.config or self.repo / "config.toml", self.repo)
        )
        if args.config and self.old_plist and absolute(args.config, Path.cwd()) != self.config:
            raise InstallError("existing service config path is preserved; change it separately")
        if self.old_plist:
            self.log_paths = [
                plist_path(self.old_plist[key], self.workdir)
                for key in ["StandardOutPath", "StandardErrorPath"]
                if self.old_plist.get(key)
            ]
            self.log_paths = list(dict.fromkeys(self.log_paths))
            self.stderr_path = Path(self.old_plist["StandardErrorPath"])
        else:
            self.workdir = self.config.parent
            logs = absolute(
                args.log_dir or self.home / "Library/Logs/Telegram-Video-Downloader", self.repo
            )
            self.log_paths = [logs / "stdout.log", logs / "stderr.log"]
            self.stderr_path = self.log_paths[1]
        validate_stderr_path(self.stderr_path)

    def audit(self):
        validate_stderr_path(self.stderr_path)
        for path in [
            self.binary,
            self.backups,
        ]:
            no_symlinks(path)
        self.workdir_binding = directory_binding(self.workdir)
        self.config_binding, contents = file_binding(self.config, capture=True)
        self.config_parent = Path(self.config_binding["physical"]).parent
        try:
            self.document = tomllib.loads(contents.decode("utf-8"))
        except (UnicodeError, tomllib.TOMLDecodeError):
            raise InstallError("configuration is not valid UTF-8 TOML") from None
        cloud_config = self.document.get("cloud")
        if cloud_config is not None and not isinstance(cloud_config, dict):
            raise InstallError("cloud configuration must be a TOML table")
        self.readiness_mode = "cloud" if cloud_config is not None else "telegram"
        self.launchd_cloud_shared_secret = None
        if self.readiness_mode == "cloud":
            shared_secret = os.environ.get(LAUNCHD_CLOUD_SHARED_SECRET)
            # Match the runtime's non-empty bearer requirement without replacing a
            # valid value already saved in an existing plist with an empty override.
            if shared_secret is not None and shared_secret.strip():
                self.launchd_cloud_shared_secret = shared_secret
        auth = self.document.get("bilibili", {}).get("auth", {})
        self.auth_paths = {}
        for key, default in [
            ("state_path", "bilibili-auth.json"),
            ("credential_file", "bbdown-credentials.json"),
        ]:
            if self.runtime_home is not None:
                default = str(
                    self.runtime_home / ".local/state/telegram-video-downloader" / default
                )
            value = auth.get(key, default)
            if not isinstance(value, str):
                raise InstallError("configured auth paths must be strings")
            source = config_path(value, self.config_parent, self.runtime_home)
            no_symlinks(source)
            try:
                metadata = source.stat()
            except FileNotFoundError:
                metadata = None
            except OSError:
                raise InstallError("configured auth path cannot be inspected") from None
            if metadata and not stat.S_ISREG(metadata.st_mode):
                raise InstallError("configured auth paths must be regular files when present")
            self.auth_paths[key] = source
        downloads = self.document.get("downloads", {})
        self.media = {}
        for kind, home_default, fallback in (
            ("video", "Movies/Downloads", "video-downloads"),
            ("pdf", "Documents/Downloads", "pdf-downloads"),
        ):
            default = (
                str(self.runtime_home / home_default) if self.runtime_home is not None else fallback
            )
            value = downloads.get(f"{kind}_dir", default)
            if not isinstance(value, str):
                raise InstallError("configured media paths must be strings")
            self.media[kind] = config_path(value, self.config_parent, self.runtime_home)
        self.media_bindings = {
            kind: media_binding(directory) for kind, directory in self.media.items()
        }
        if self.old_plist or self.old_binary.exists():
            self.old_binary_binding, _ = file_binding(self.old_binary)
            self.requirement, self.identity = signature(Path(self.old_binary_binding["physical"]))
        else:
            self.old_binary_binding = None
            self.requirement, self.identity = initial_identity()
        self.dotnet_root = (
            detect_dotnet_root(self.args.dotnet_root, self.home) if not self.old_plist else None
        )
        self.state_inventory()
        registered = [info for target in self.targets if (info := service_info(target))]
        if len({info["pid"] for info in registered if info["pid"]}) > 1:
            raise InstallError(
                "multiple service instances are already polling; "
                "stop the duplicate before deployment"
            )
        known = {info["pid"] for info in registered if info["pid"]}
        if set(bot_processes(expected_executables=(self.old_binary, self.binary))) - known:
            raise InstallError(
                "another downloader executable is running; wait for workers to finish "
                "or stop unmanaged instances before deployment"
            )
        self.registered = registered
        return {
            "binary": str(self.binary),
            "config": str(self.config),
            "working_directory": str(self.workdir),
            "logs": [str(path) for path in self.log_paths],
            "source_binary": str(self.old_binary),
            "media": {key: str(value) for key, value in self.media.items()},
            "signing_identity": self.identity,
            "readiness_mode": self.readiness_mode,
            "backups": str(self.backups),
            "state": "configuration and media transactional state stay at their existing paths",
        }

    def new_plist(self):
        if self.old_plist:
            document = copy.deepcopy(self.old_plist)
            document["ProgramArguments"][0] = str(self.binary)
            if self.launchd_cloud_shared_secret is not None:
                environment = dict(document.get("EnvironmentVariables", {}))
                environment[LAUNCHD_CLOUD_SHARED_SECRET] = self.launchd_cloud_shared_secret
                document["EnvironmentVariables"] = environment
            return plistlib.dumps(document)
        environment = {
            "HOME": str(self.home),
            "RUST_LOG": "info",
            "PATH": (
                f"/opt/homebrew/bin:{self.home}/.dotnet:{self.home}/.dotnet/tools:"
                f"{self.home}/.local/bin:{self.home}/.cargo/bin:"
                "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
            ),
        }
        if self.dotnet_root:
            environment.update(
                {"DOTNET_ROOT": str(self.dotnet_root), "DOTNET_ROOT_ARM64": str(self.dotnet_root)}
            )
            environment["PATH"] = f"{self.dotnet_root}:{environment['PATH']}"
        if self.launchd_cloud_shared_secret is not None:
            environment[LAUNCHD_CLOUD_SHARED_SECRET] = self.launchd_cloud_shared_secret
        return plistlib.dumps(
            {
                "Label": self.args.label,
                "ProgramArguments": [str(self.binary), str(self.config)],
                "WorkingDirectory": str(self.workdir),
                "EnvironmentVariables": environment,
                "RunAtLoad": True,
                "KeepAlive": True,
                "StandardOutPath": str(self.log_paths[0]),
                "StandardErrorPath": str(self.log_paths[1]),
            }
        )

    def plist_contains_cloud_shared_secret(self):
        if self.launchd_cloud_shared_secret is not None:
            return True
        environment = self.old_plist.get("EnvironmentVariables", {}) if self.old_plist else {}
        return LAUNCHD_CLOUD_SHARED_SECRET in environment

    def revalidate_media_bindings(self):
        for kind, directory in self.media.items():
            if media_binding(directory) != self.media_bindings[kind]:
                raise InstallError(f"media root binding changed: {directory}")

    def revalidate_runtime_bindings(self):
        if directory_binding(self.workdir) != self.workdir_binding:
            raise InstallError("working directory binding changed after audit")
        current, _ = file_binding(self.config)
        if current != self.config_binding:
            raise InstallError("configuration binding or content changed after audit")
        if self.old_binary_binding is not None:
            current, _ = file_binding(self.old_binary)
            if current != self.old_binary_binding:
                raise InstallError("trusted executable binding or content changed after audit")
        if self.plist_binding is None:
            if self.plist_path.exists() or self.plist_path.is_symlink():
                raise InstallError("LaunchAgent plist appeared after audit")
        else:
            no_symlinks(self.plist_path)
            current, _ = file_binding(self.plist_path)
            if current != self.plist_binding:
                raise InstallError("LaunchAgent plist binding or content changed after audit")

    def state_inventory(self, deadline=None):
        """Require complete enumeration before stopping and again before copying."""
        deadline = deadline or time.monotonic() + SNAPSHOT_SECONDS
        self.revalidate_runtime_bindings()
        self.revalidate_media_bindings()
        candidates = {Path(self.config_binding["physical"])}
        anchors = {}
        for source in self.auth_paths.values():
            no_symlinks(source)
            try:
                with os.scandir(source.parent) as entries:
                    for entry in entries:
                        if time.monotonic() >= deadline:
                            raise InstallError("auth state inventory exceeded its time budget")
                        path = Path(entry.path)
                        if entry.name.startswith(source.name) or auth_recovery_entry(path):
                            candidates.add(path)
            except FileNotFoundError:
                pass
            except OSError:
                raise InstallError(
                    "auth state directory could not be completely enumerated"
                ) from None
        physical_roots = {Path(binding["physical"]) for binding in self.media_bindings.values()}
        for directory in physical_roots:
            try:
                directory.stat()
            except FileNotFoundError:
                continue
            except OSError:
                raise InstallError("media state directory cannot be inspected") from None
            for name in [
                ".telegram-video-downloader-queue",
                ".telegram-video-downloader.lock",
                ".telegram-video-downloader.recovery",
            ]:
                candidates.add(directory / name)
            visited = 0
            for root, dirs, files in strict_walk(directory):
                if time.monotonic() >= deadline:
                    raise InstallError("media metadata inventory exceeded its two-minute budget")
                visited += len(dirs) + len(files)
                if visited > 200_000:
                    raise InstallError("media metadata inventory exceeded its 200000-entry bound")
                for name in dirs[:]:
                    child = Path(root) / name
                    if child.is_symlink():
                        if any(
                            part.startswith(".telegram-video-downloader")
                            for part in child.relative_to(directory).parts
                        ):
                            raise InstallError(f"symbolic link selected as media metadata: {child}")
                        dirs.remove(name)
                for name in files:
                    path = Path(root) / name
                    hidden = any(
                        part.startswith(".telegram-video-downloader")
                        for part in path.relative_to(directory).parts[:-1]
                    )
                    # These names are hard links to complete media outputs, not
                    # recovery metadata. Never copy, hash, or budget their bytes.
                    if hidden and (
                        name == "output.anchor" or name.startswith(".committed-output-")
                    ):
                        anchors[str(path)] = anchor_evidence(path)
                        continue
                    if (
                        name.startswith(".telegram-video-downloader-task-")
                        or name.endswith(".pdf.lock")
                        or (
                            hidden
                            and (
                                name.startswith(".")
                                or name.endswith(".json")
                                or name in {"output-lock", "recovery-state", "recovery-state.next"}
                            )
                        )
                    ):
                        candidates.add(path)
        selected = []
        for path in sorted(candidates, key=lambda entry: (len(entry.parts), str(entry))):
            no_symlinks(path)
            try:
                path.stat()
            except FileNotFoundError:
                continue
            except OSError:
                raise InstallError("runtime state entry cannot be inspected") from None
            if not any(parent in selected for parent in path.parents):
                check_tree(path)
                selected.append(path)
        return selected, anchors

    def snapshot_state(self, backup):
        """Copy state metadata; media payload anchors remain inode evidence only."""
        deadline = time.monotonic() + SNAPSHOT_SECONDS
        selected, anchors = self.state_inventory(deadline)
        manifest = []
        hardlinks = {}
        total = 0
        for index, path in enumerate(selected):
            originals = file_manifest(path, SNAPSHOT_BYTES - total, deadline)
            total += sum(entry["size"] for entry in originals)
            if total > SNAPSHOT_BYTES or time.monotonic() >= deadline:
                raise InstallError("state snapshot exceeded its 256-MiB or two-minute budget")
            destination = backup / "state" / f"{index:04d}" / path.name
            preserve_copy(path, destination)
            if originals != file_manifest(
                path, SNAPSHOT_BYTES - total + sum(entry["size"] for entry in originals), deadline
            ):
                raise InstallError(f"runtime state changed while service was stopped: {path}")
            for entry in originals:
                original = Path(entry["path"])
                copied = destination / original.relative_to(path) if path.is_dir() else destination
                identity = (entry["device"], entry["inode"])
                if entry["links"] > 1 and identity in hardlinks:
                    copied.unlink()
                    os.link(hardlinks[identity], copied)
                else:
                    hardlinks[identity] = copied
            manifest.append({"source": str(path), "backup": str(destination), "files": originals})
        for path, evidence in anchors.items():
            current = anchor_evidence(Path(path))
            if (current["device"], current["inode"]) != (evidence["device"], evidence["inode"]):
                raise InstallError(f"media anchor identity changed: {path}")
        self.revalidate_media_bindings()
        self.revalidate_runtime_bindings()
        atomic_bytes(
            backup / "state-manifest.json",
            json.dumps(
                {
                    "restore_policy": "manual; preserve live state and inode-bound lock identities",
                    "entries": manifest,
                    "media_roots": self.media_bindings,
                    "anchors": list(anchors.values()),
                    "runtime_bindings": {
                        "config": self.config_binding,
                        "working_directory": self.workdir_binding,
                        "previous_binary": self.old_binary_binding,
                    },
                },
                indent=2,
            ).encode(),
        )
        return len(manifest)

    def wait_ready(self, offsets):
        deadline = time.monotonic() + self.args.ready_timeout
        observed_pid = None
        logs = _ReadinessLogScanner([self.stderr_path], offsets, mode=self.readiness_mode)
        while time.monotonic() < deadline:
            current = service_info(self.targets[0])
            if current and current["pid"]:
                if observed_pid is not None and current["pid"] != observed_pid:
                    raise InstallError(
                        "service PID changed during startup/poll verification: "
                        f"expected {observed_pid}, observed {current['pid']}"
                    )
                observed_pid = current["pid"]
                processes = bot_processes(expected_executables=(self.old_binary, self.binary))
                if not processes:
                    # launchd can publish the PID before exec updates its process name.
                    time.sleep(1)
                    continue
                if processes != [current["pid"]]:
                    raise InstallError(
                        "expected exactly one downloader process after bootstrap: "
                        f"expected PID {current['pid']}, observed PIDs {sorted(processes)}"
                    )
                if logs.scan(current["pid"]):
                    listing = run(
                        ["lsof", "-nP", "-a", "-p", str(current["pid"]), "-d", "cwd,txt", "-Ffn"]
                    ).stdout
                    names, descriptor = {"cwd": set(), "txt": set()}, None
                    for line in listing.splitlines():
                        if line.startswith("f"):
                            descriptor = line[1:]
                        elif line.startswith("n") and descriptor in names:
                            names[descriptor].add(line[1:])
                    binding = getattr(self, "workdir_binding", None)
                    try:
                        configured = self.workdir.stat()
                        expected = (
                            (binding["device"], binding["inode"])
                            if binding
                            else (configured.st_dev, configured.st_ino)
                        )
                        valid_cwd = (
                            stat.S_ISDIR(configured.st_mode)
                            and (configured.st_dev, configured.st_ino) == expected
                            and any(
                                stat.S_ISDIR(metadata.st_mode)
                                and (metadata.st_dev, metadata.st_ino) == expected
                                for path in names["cwd"]
                                for metadata in [Path(path).stat()]
                            )
                        )
                    except OSError:
                        raise InstallError(
                            "running working directory cannot be inspected"
                        ) from None
                    if str(self.binary) not in names["txt"] or not valid_cwd:
                        raise InstallError(
                            "running executable or working directory "
                            "differs from the installed plist"
                        )
                    confirmed = service_info(self.targets[0])
                    if not confirmed or confirmed["pid"] != observed_pid:
                        raise InstallError("service PID changed before readiness was confirmed")
                    return current["pid"]
            time.sleep(1)
        if self.readiness_mode == "cloud":
            raise InstallError(
                "cloud startup and initial REST ingress reconciliation readiness "
                "were not observed before timeout"
            )
        raise InstallError(
            "startup-ready and two successful Telegram polling cycles "
            "were not observed before timeout"
        )

    def deploy(self):
        self.revalidate_runtime_bindings()
        stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%S.%fZ")
        backup = self.backups / stamp
        no_symlinks(backup)
        backup.mkdir(parents=True, mode=0o700)
        # Capture trusted executable and exact plist before Cargo runs.
        if self.old_binary_binding is not None:
            preserve_copy(Path(self.old_binary_binding["physical"]), backup / "previous-binary")
        if self.old_plist_bytes:
            preserve_copy(self.plist_path, backup / "previous.plist")
        atomic_bytes(backup / "designated-requirement.txt", self.requirement.encode())
        previous_installed = self.binary.exists()
        restore_services = [info for info in self.registered if info["pid"]] or self.registered[:1]
        transaction = {
            "version": 1,
            "label": self.args.label,
            "domain": self.args.domain,
            "targets": self.targets,
            "previous_services": restore_services,
            "binary": str(self.binary),
            "previous_binary": str(self.old_binary),
            "plist": str(self.plist_path),
            "had_plist": bool(self.old_plist),
            "had_installed_binary": previous_installed,
            "requirement": self.requirement,
        }
        atomic_bytes(backup / "transaction.json", json.dumps(transaction, indent=2).encode())
        # Prove the retained fallback is usable before building or stopping the service.
        transaction = validate_transaction(transaction, self.args.label)
        validate_saved_plist(backup, transaction)
        self.revalidate_runtime_bindings()
        staged = backup / "staged-binary"
        source = self.input_binary or self.repo / "target/release" / EXECUTABLE
        try:
            if not self.args.no_build:
                built_source = build_release(self.repo, backup)
                if not self.args.binary:
                    source = built_source
            if not source.is_file() or not os.access(source, os.X_OK):
                raise InstallError(f"input artifact is not executable: {source}")
            trusted = (
                Path(self.old_binary_binding["physical"])
                if self.old_binary_binding
                else self.old_binary
            )
            stage_artifact(source, staged, trusted)
            run(
                ["codesign", "--force", "--sign", self.identity, "--identifier", IDENTIFIER, staged]
            )
            run(
                [
                    "codesign",
                    "--verify",
                    "--strict",
                    "--verbose=2",
                    "--test-requirement",
                    "=" + self.requirement,
                    staged,
                ]
            )
            requirement, identity = signature(staged)
            if identity != self.identity or requirement != self.requirement:
                raise InstallError("staged signing identity or designated requirement changed")
        finally:
            # These unique directories belong exclusively to this deployment.
            for generated in [backup / "build", backup / "cargo-build"]:
                if generated.exists():
                    no_symlinks(generated)
                    shutil.rmtree(generated)
        stopped = False
        try:
            validate_stderr_path(self.stderr_path)
            self.state_inventory()
            # Set this before bootout: a later domain may fail after the first stopped.
            stopped = True
            stop_services(self.targets)
            wait_for_no_bot_processes(expected_executables=(self.old_binary, self.binary))
            state_entries = self.snapshot_state(backup)
            offsets = capture_log_offsets([self.stderr_path])
            atomic_install(staged, self.binary)
            run(
                [
                    "codesign",
                    "--verify",
                    "--strict",
                    "--test-requirement",
                    "=" + self.requirement,
                    self.binary,
                ]
            )
            mode = (
                stat.S_IMODE(self.plist_path.stat().st_mode) if self.plist_path.exists() else 0o644
            )
            plist_bytes = self.new_plist()
            if self.plist_contains_cloud_shared_secret():
                mode = 0o600
            atomic_bytes(self.plist_path, plist_bytes, mode)
            for path in self.log_paths:
                path.resolve(strict=False).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            run(["launchctl", "bootstrap", self.args.domain, self.plist_path])
            pid = self.wait_ready(offsets)
            atomic_bytes(
                backup / "complete.json",
                json.dumps(
                    {
                        "pid": pid,
                        "state_entries": state_entries,
                        "readiness_mode": self.readiness_mode,
                    }
                ).encode(),
            )
            result = {
                "backup": str(backup),
                "pid": pid,
                "startup_ready": True,
                "readiness_mode": self.readiness_mode,
            }
            if self.readiness_mode == "cloud":
                result["cloud_ingress_reconciled"] = True
            else:
                result["successful_poll_cycles"] = 2
            return result
        except BaseException as failure:
            if stopped:
                failure_reason = (
                    str(failure) if isinstance(failure, InstallError) else type(failure).__name__
                )
                try:
                    restore_binary_and_plist(backup, transaction)
                except (InstallError, OSError, subprocess.TimeoutExpired) as restoration:
                    restoration_reason = (
                        str(restoration)
                        if isinstance(restoration, InstallError)
                        else type(restoration).__name__
                    )
                    raise InstallError(
                        f"deployment failed: {failure_reason}; "
                        f"rollback failed: {restoration_reason}; "
                        f"service readiness UNCONFIRMED; durable backup: {backup}"
                    ) from failure
                restoration_status = (
                    f"previous executable/plist restored at fixed path {self.binary}"
                    if self.old_plist or (backup / "previous-binary").exists()
                    else "incomplete first installation removed"
                )
                raise InstallError(
                    f"deployment failed: {failure_reason}; {restoration_status}; "
                    f"service readiness UNCONFIRMED; durable backup: {backup}"
                ) from failure
            raise


def validate_saved_plist(backup, transaction):
    """Validate rollback inputs completely before stopping or replacing anything."""
    binary = backup / "previous-binary"
    if binary.exists():
        no_symlinks(binary)
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise InstallError("saved previous executable is not a regular executable file")
        run(
            [
                "codesign",
                "--verify",
                "--strict",
                "--test-requirement",
                "=" + transaction["requirement"],
                binary,
            ]
        )
    elif transaction["had_plist"] or transaction["had_installed_binary"]:
        raise InstallError("saved previous executable is missing")
    if not transaction["had_plist"]:
        return None
    source = backup / "previous.plist"
    no_symlinks(source)
    try:
        with source.open("rb") as stream:
            metadata = os.fstat(stream.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > 1024 * 1024:
                raise InstallError("saved previous plist is not a supported regular plist")
            contents = stream.read(1024 * 1024 + 1)
        document = plistlib.loads(contents)
    except InstallError:
        raise
    except Exception:
        raise InstallError("saved previous plist is missing or invalid") from None

    def valid_string(value):
        return isinstance(value, str) and bool(value) and "\0" not in value

    if not isinstance(document, dict):
        raise InstallError("saved previous plist must be a dictionary")
    if document.get("Label") != transaction["label"]:
        raise InstallError("saved previous plist Label does not match the selected service")
    arguments = document.get("ProgramArguments")
    if (
        not isinstance(arguments, list)
        or len(arguments) != 2
        or not all(valid_string(argument) for argument in arguments)
        or "Program" in document
    ):
        raise InstallError("saved previous plist has unsupported ProgramArguments or Program")
    for key in ("WorkingDirectory", "StandardOutPath", "StandardErrorPath"):
        if key in document and (
            not valid_string(document[key]) or not Path(document[key]).is_absolute()
        ):
            raise InstallError(f"saved previous plist has an unsupported {key}")
    if "WorkingDirectory" not in document:
        raise InstallError("saved previous plist requires an explicit absolute WorkingDirectory")
    resolve_launchd_executable(
        arguments[0], document["WorkingDirectory"], captured=transaction["previous_binary"]
    )
    environment = document.get("EnvironmentVariables", {})
    if not isinstance(environment, dict) or not all(
        valid_string(key) and isinstance(value, str) and "\0" not in value
        for key, value in environment.items()
    ):
        raise InstallError("saved previous plist has unsupported environment values")
    return {"bytes": contents, "document": document, "mode": stat.S_IMODE(metadata.st_mode)}


def validate_original_entrypoint(backup, transaction, saved=None):
    """Admit an explicitly requested old entry only while its trusted bytes exist."""
    if not transaction["had_plist"] or not (backup / "previous-binary").is_file():
        raise InstallError("original entrypoint rollback requires a saved executable and plist")
    if saved is None:
        saved = validate_saved_plist(backup, transaction)
    previous_plist = saved["bytes"]
    previous = saved["document"]
    arguments = previous.get("ProgramArguments", [])
    if len(arguments) != 2 or "Program" in previous:
        raise InstallError("saved plist has no supported original entrypoint")
    original = resolve_launchd_executable(arguments[0], previous["WorkingDirectory"])
    if original != Path(transaction["previous_binary"]):
        raise InstallError("saved original entrypoint does not match the backup transaction")
    binding, _ = file_binding(original)
    physical = Path(binding["physical"])
    if not os.access(physical, os.X_OK):
        raise InstallError(f"original entrypoint is missing or not executable: {original}")
    run(
        [
            "codesign",
            "--verify",
            "--strict",
            "--test-requirement",
            "=" + transaction["requirement"],
            physical,
        ]
    )
    with physical.open("rb") as current, (backup / "previous-binary").open("rb") as saved:
        if (
            hashlib.file_digest(current, "sha256").digest()
            != hashlib.file_digest(saved, "sha256").digest()
        ):
            raise InstallError("original entrypoint differs from the saved previous executable")
    current_binding, _ = file_binding(original)
    if current_binding != binding:
        raise InstallError("original entrypoint changed during rollback preflight")
    return original, previous_plist


def restore_binary_and_plist(backup, transaction, *, original_entrypoint=False):
    """Restore executable/plist only, retaining every newer runtime write."""
    transaction = validate_transaction(transaction)
    saved = validate_saved_plist(backup, transaction)
    if original_entrypoint:
        # Missing/changed original binaries must fail before interrupting any service.
        original, _ = validate_original_entrypoint(backup, transaction, saved)
    stop_services(transaction["targets"])
    wait_for_no_bot_processes(
        expected_executables=(transaction["previous_binary"], transaction["binary"])
    )
    binary = original if original_entrypoint else Path(transaction["binary"])
    if original_entrypoint:
        # Revalidate after stopping before launchd is allowed to execute this path.
        validate_original_entrypoint(backup, transaction, saved)
    elif (backup / "previous-binary").exists():
        atomic_install(backup / "previous-binary", binary)
        run(
            [
                "codesign",
                "--verify",
                "--strict",
                "--test-requirement",
                "=" + transaction["requirement"],
                binary,
            ]
        )
    elif binary.exists():
        binary.unlink()
    plist = Path(transaction["plist"])
    if original_entrypoint:
        atomic_bytes(plist, saved["bytes"], saved["mode"])
    elif transaction["had_plist"]:
        old = copy.deepcopy(saved["document"])
        old["ProgramArguments"][0] = str(binary)
        atomic_bytes(plist, plistlib.dumps(old), saved["mode"])
    elif plist.exists():
        plist.unlink()
    for service in transaction["previous_services"]:
        domain = service["target"].rsplit("/", 1)[0]
        run(["launchctl", "bootstrap", domain, plist])
    return binary


def rollback(args):
    if not args.backup:
        raise InstallError("rollback requires --backup PATH")
    backup = absolute(args.backup, Path.cwd())
    check_tree(backup)
    transaction = validate_transaction(
        json.loads((backup / "transaction.json").read_text()), args.label
    )
    # Reject incomplete rollback inputs before retaining files or changing service state.
    validate_saved_plist(backup, transaction)
    retained = backup / datetime.datetime.now(datetime.UTC).strftime(
        "pre-rollback-%Y%m%dT%H%M%S.%fZ"
    )
    retained.mkdir(mode=0o700)
    for source, name in [
        (Path(transaction["binary"]), "binary"),
        (Path(transaction["plist"]), "agent.plist"),
    ]:
        if source.exists():
            preserve_copy(source, retained / name)
    entrypoint = restore_binary_and_plist(
        backup, transaction, original_entrypoint=args.original_entrypoint
    )
    return {
        "rollback": str(backup),
        "retained_newer_files": str(retained),
        "runtime_state": "left unchanged",
        "entrypoint": str(entrypoint),
        "readiness": "UNCONFIRMED",
    }


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("action", choices=["plan", "install", "deploy", "rollback"])
    result.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]))
    result.add_argument("--label", default=IDENTIFIER)
    result.add_argument("--domain", default=f"gui/{os.getuid()}")
    result.add_argument("--legacy-domain")
    result.add_argument("--config")
    result.add_argument(
        "--binary", help="Input release artifact; never the direct LaunchAgent target"
    )
    result.add_argument("--no-build", action="store_true")
    result.add_argument("--dotnet-root")
    result.add_argument("--log-dir")
    result.add_argument("--backup", type=Path)
    result.add_argument(
        "--original-entrypoint",
        action="store_true",
        help="Restore the exact old plist only if its old executable still matches backup",
    )
    result.add_argument("--ready-timeout", type=int, default=180)
    return result


def main(argv=None):
    try:
        arguments = parser().parse_args(argv)
        if arguments.original_entrypoint and arguments.action != "rollback":
            raise InstallError("--original-entrypoint is only valid with rollback")
        with deployment_lock() if arguments.action != "plan" else nullcontext():
            if arguments.action == "rollback":
                result = rollback(arguments)
            else:
                deployment = Deployment(arguments)
                result = deployment.audit()
                if arguments.action != "plan":
                    result.update(deployment.deploy())
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0
    except (InstallError, OSError, ValueError, subprocess.TimeoutExpired) as error:
        diagnostic = str(error) if isinstance(error, InstallError) else type(error).__name__
        print(f"error: {diagnostic}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
