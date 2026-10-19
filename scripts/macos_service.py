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
STOP_TIMEOUT = 15
SNAPSHOT_SECONDS = 120
SNAPSHOT_BYTES = 256 * 1024 * 1024


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
    if value == "$HOME" or value.startswith("$HOME/"):
        value = str(Path.home()) + value[5:]
    value = os.path.expanduser(value)
    if "$" in value:
        raise InstallError("unsupported variable in a configured path")
    return Path(os.path.abspath(Path(base) / value))


def no_symlinks(path):
    """Protect path identity: never copy or replace through symbolic links."""
    for entry in [Path(path), *Path(path).parents]:
        if entry.is_symlink():
            raise InstallError(f"symbolic link is unsupported: {entry}")


def check_tree(path):
    no_symlinks(path)
    if Path(path).exists() and not (Path(path).is_file() or Path(path).is_dir()):
        raise InstallError(f"unsupported filesystem entry: {path}")
    if Path(path).is_dir():
        for root, dirs, files in os.walk(path, followlinks=False):
            for name in dirs + files:
                child = Path(root) / name
                mode = child.lstat().st_mode
                if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                    raise InstallError(f"unsupported state entry: {child}")


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
            Path(root) / name for root, dirs, files in os.walk(source) for name in dirs + files
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
    """Read executable paths, not macOS's truncated names or process arguments."""
    result = run(["/bin/ps", "-axww", "-o", "pid=,ppid=,comm="], check=False)
    if result.returncode:
        raise InstallError("cannot inspect downloader processes")
    inventory = {}
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        fields = line.strip().split(None, 2)
        if len(fields) != 3 or not all(re.fullmatch(r"[0-9]+", value) for value in fields[:2]):
            raise InstallError("unexpected downloader process inventory")
        pid, parent, executable = int(fields[0]), int(fields[1]), Path(fields[2])
        if pid in inventory:
            raise InstallError("duplicate PID in downloader process inventory")
        inventory[pid] = {"parent": parent, "executable": executable}
    return inventory


def bot_processes(include_workers=False):
    inventory = process_inventory()
    downloaders = {
        pid
        for pid, process in inventory.items()
        if pid > 0 and process["executable"].name == EXECUTABLE
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


def wait_for_no_bot_processes():
    """Do not replace or restart until old service and worker images have exited."""
    deadline = time.monotonic() + STOP_TIMEOUT
    while True:
        remaining = bot_processes(include_workers=True)
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
        files = [Path(directory) / name for directory, _, names in os.walk(root) for name in names]
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
        self.binary = self.home / ".local/lib/telegram-video-downloader/bin" / EXECUTABLE
        self.backups = self.home / ".local/lib/telegram-video-downloader/backups"
        self.plist_path = self.home / "Library/LaunchAgents" / f"{args.label}.plist"
        self.targets = [f"{args.domain}/{args.label}"]
        if args.legacy_domain and args.legacy_domain != args.domain:
            self.targets.append(f"{args.legacy_domain}/{args.label}")
        no_symlinks(self.plist_path)
        self.old_plist_bytes = self.plist_path.read_bytes() if self.plist_path.exists() else None
        self.old_plist = plistlib.loads(self.old_plist_bytes) if self.old_plist_bytes else None
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
                    "to capture startup and polling readiness; configure it before deployment"
                )
        old_args = self.old_plist.get("ProgramArguments", []) if self.old_plist else []
        if self.old_plist and len(old_args) != 2:
            raise InstallError("expected an existing binary and config ProgramArguments pair")
        self.workdir = (
            absolute(self.old_plist.get("WorkingDirectory", self.repo), self.repo)
            if self.old_plist
            else self.repo
        )
        self.old_binary = absolute(old_args[0], self.workdir) if old_args else self.binary
        self.config = (
            absolute(old_args[1], self.workdir)
            if old_args
            else absolute(args.config or self.repo / "config.toml", self.repo)
        )
        if args.config and self.old_plist and absolute(args.config, Path.cwd()) != self.config:
            raise InstallError("existing service config path is preserved; change it separately")
        if self.old_plist:
            self.log_paths = [
                absolute(self.old_plist[key], self.workdir)
                for key in ["StandardOutPath", "StandardErrorPath"]
                if self.old_plist.get(key)
            ]
            self.log_paths = list(dict.fromkeys(self.log_paths))
        else:
            self.workdir = self.config.parent
            logs = absolute(
                args.log_dir or self.home / "Library/Logs/Telegram-Video-Downloader", self.repo
            )
            self.log_paths = [logs / "stdout.log", logs / "stderr.log"]

    def audit(self):
        for path in [
            self.binary,
            self.backups,
            self.config,
            self.old_binary,
            self.workdir,
            *self.log_paths,
        ]:
            no_symlinks(path)
        if not self.config.is_file():
            raise InstallError(f"configuration is missing: {self.config}")
        try:
            self.document = tomllib.loads(self.config.read_text())
        except (UnicodeError, tomllib.TOMLDecodeError):
            raise InstallError("configuration is not valid UTF-8 TOML") from None
        downloads = self.document.get("downloads", {})
        self.media = {
            "video": absolute(downloads.get("video_dir", "~/Movies/Downloads"), self.config.parent),
            "pdf": absolute(downloads.get("pdf_dir", "~/Documents/Downloads"), self.config.parent),
        }
        self.media_bindings = {
            kind: media_binding(directory) for kind, directory in self.media.items()
        }
        if self.old_plist or self.old_binary.exists():
            self.requirement, self.identity = signature(self.old_binary)
        else:
            self.requirement, self.identity = initial_identity()
        registered = [info for target in self.targets if (info := service_info(target))]
        if len({info["pid"] for info in registered if info["pid"]}) > 1:
            raise InstallError(
                "multiple service instances are already polling; "
                "stop the duplicate before deployment"
            )
        known = {info["pid"] for info in registered if info["pid"]}
        if set(bot_processes()) - known:
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
            "backups": str(self.backups),
            "state": "configuration and media transactional state stay at their existing paths",
        }

    def new_plist(self):
        if self.old_plist:
            document = copy.deepcopy(self.old_plist)
            document["ProgramArguments"][0] = str(self.binary)
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
        if self.args.dotnet_root:
            environment.update(
                {"DOTNET_ROOT": self.args.dotnet_root, "DOTNET_ROOT_ARM64": self.args.dotnet_root}
            )
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

    def revalidate_media_bindings(self):
        for kind, directory in self.media.items():
            if media_binding(directory) != self.media_bindings[kind]:
                raise InstallError(f"media root binding changed: {directory}")

    def snapshot_state(self, backup):
        """Back up config/auth/queue and recovery metadata, never media payloads.

        Original lock inode identities are in the manifest. Copies are forensic
        evidence, not valid replacements for inode-bound lock/owner records.
        """
        deadline = time.monotonic() + SNAPSHOT_SECONDS
        self.revalidate_media_bindings()
        candidates = {self.config}
        anchors = {}
        auth = self.document.get("bilibili", {}).get("auth", {})
        for key, default in [
            ("state_path", "~/.local/state/telegram-video-downloader/bilibili-auth.json"),
            ("credential_file", "~/.local/state/telegram-video-downloader/bbdown-credentials.json"),
        ]:
            source = absolute(auth.get(key, default), self.config.parent)
            no_symlinks(source)
            if source.parent.exists():
                candidates.update(source.parent.glob(source.name + "*"))
        physical_roots = {Path(binding["physical"]) for binding in self.media_bindings.values()}
        for directory in physical_roots:
            if not directory.exists():
                continue
            for name in [
                ".telegram-video-downloader-queue",
                ".telegram-video-downloader.lock",
                ".telegram-video-downloader.recovery",
            ]:
                candidates.add(directory / name)
            visited = 0
            for root, dirs, files in os.walk(directory, followlinks=False):
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
            if path.exists() and not any(parent in selected for parent in path.parents):
                check_tree(path)
                selected.append(path)
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
        atomic_bytes(
            backup / "state-manifest.json",
            json.dumps(
                {
                    "restore_policy": "manual; preserve live state and inode-bound lock identities",
                    "entries": manifest,
                    "media_roots": self.media_bindings,
                    "anchors": list(anchors.values()),
                },
                indent=2,
            ).encode(),
        )
        return len(manifest)

    def wait_ready(self, offsets):
        deadline = time.monotonic() + self.args.ready_timeout
        observed_pid = None
        while time.monotonic() < deadline:
            current = service_info(self.targets[0])
            if current and current["pid"]:
                if observed_pid is not None and current["pid"] != observed_pid:
                    raise InstallError(
                        "service PID changed during startup/poll verification: "
                        f"expected {observed_pid}, observed {current['pid']}"
                    )
                observed_pid = current["pid"]
                processes = bot_processes()
                if not processes:
                    # launchd can publish the PID before exec updates its process name.
                    time.sleep(1)
                    continue
                if processes != [current["pid"]]:
                    raise InstallError(
                        "expected exactly one downloader process after bootstrap: "
                        f"expected PID {current['pid']}, observed PIDs {sorted(processes)}"
                    )
                output = ""
                for path in self.log_paths:
                    if path.is_file():
                        with path.open("rb") as stream:
                            stream.seek(offsets.get(str(path), 0))
                            output += stream.read(2 * 1024 * 1024).decode(errors="replace")
                if (
                    "telegram local downloader started" in output
                    and output.count("telegram polling completed") >= 2
                ):
                    listing = run(
                        ["lsof", "-nP", "-a", "-p", str(current["pid"]), "-d", "cwd,txt", "-Ffn"]
                    ).stdout
                    names, descriptor = {"cwd": set(), "txt": set()}, None
                    for line in listing.splitlines():
                        if line.startswith("f"):
                            descriptor = line[1:]
                        elif line.startswith("n") and descriptor in names:
                            names[descriptor].add(line[1:])
                    if (
                        str(self.binary) not in names["txt"]
                        or str(self.workdir) not in names["cwd"]
                    ):
                        raise InstallError(
                            "running executable or working directory "
                            "differs from the installed plist"
                        )
                    return current["pid"]
            time.sleep(1)
        raise InstallError(
            "startup-ready and two successful Telegram polling cycles "
            "were not observed before timeout"
        )

    def deploy(self):
        stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%S.%fZ")
        backup = self.backups / stamp
        no_symlinks(backup)
        backup.mkdir(parents=True, mode=0o700)
        # Capture trusted executable and exact plist before Cargo runs.
        if self.old_binary.exists():
            preserve_copy(self.old_binary, backup / "previous-binary")
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
        staged = backup / "staged-binary"
        source = (
            absolute(self.args.binary, Path.cwd())
            if self.args.binary
            else self.repo / "target/release" / EXECUTABLE
        )
        try:
            if not self.args.no_build:
                built_source = build_release(self.repo, backup)
                if not self.args.binary:
                    source = built_source
            if not source.is_file() or not os.access(source, os.X_OK):
                raise InstallError(f"input artifact is not executable: {source}")
            stage_artifact(source, staged, self.old_binary)
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
            # Set this before bootout: a later domain may fail after the first stopped.
            stopped = True
            stop_services(self.targets)
            wait_for_no_bot_processes()
            state_entries = self.snapshot_state(backup)
            offsets = {
                str(path): path.stat().st_size if path.exists() else 0 for path in self.log_paths
            }
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
            atomic_bytes(self.plist_path, self.new_plist(), mode)
            for path in self.log_paths:
                path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            run(["launchctl", "bootstrap", self.args.domain, self.plist_path])
            pid = self.wait_ready(offsets)
            atomic_bytes(
                backup / "complete.json",
                json.dumps({"pid": pid, "state_entries": state_entries}).encode(),
            )
            return {
                "backup": str(backup),
                "pid": pid,
                "startup_ready": True,
                "successful_poll_cycles": 2,
            }
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
    elif transaction["had_plist"]:
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
    original = absolute(arguments[0], previous.get("WorkingDirectory", Path.home()))
    if original != Path(transaction["previous_binary"]):
        raise InstallError("saved original entrypoint does not match the backup transaction")
    no_symlinks(original)
    if not original.is_file() or not os.access(original, os.X_OK):
        raise InstallError(f"original entrypoint is missing or not executable: {original}")
    run(
        [
            "codesign",
            "--verify",
            "--strict",
            "--test-requirement",
            "=" + transaction["requirement"],
            original,
        ]
    )
    with original.open("rb") as current, (backup / "previous-binary").open("rb") as saved:
        if (
            hashlib.file_digest(current, "sha256").digest()
            != hashlib.file_digest(saved, "sha256").digest()
        ):
            raise InstallError("original entrypoint differs from the saved previous executable")
    return original, previous_plist


def restore_binary_and_plist(backup, transaction, *, original_entrypoint=False):
    """Restore executable/plist only, retaining every newer runtime write."""
    transaction = validate_transaction(transaction)
    saved = validate_saved_plist(backup, transaction)
    if original_entrypoint:
        # Missing/changed original binaries must fail before interrupting any service.
        original, _ = validate_original_entrypoint(backup, transaction, saved)
    stop_services(transaction["targets"])
    wait_for_no_bot_processes()
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
