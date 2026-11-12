#!/usr/bin/env python3
"""Plan or explicitly install one profile's private Python Music runtime."""

from __future__ import annotations

import argparse
import os
import pwd
import selectors
import stat
import subprocess
import sys
import time
from pathlib import Path


DEPENDENCIES = (
    "Telethon==1.45.0",
    "pyaes==1.6.1",
    "rsa==4.9.1",
    "pyasn1==0.6.5",
)
OUTPUT_LIMIT = 64 * 1024
COMMAND_TIMEOUT = 600


class InstallError(Exception):
    """A safe installation diagnostic."""


def actual_home() -> Path:
    try:
        value = Path(pwd.getpwuid(os.getuid()).pw_dir)
    except (KeyError, OSError):
        raise InstallError("current account home directory is unavailable") from None
    if not value.is_absolute():
        raise InstallError("current account home directory is invalid")
    return value


def profile_root(home: Path, profile: str) -> Path:
    if profile not in {"test", "production"}:
        raise InstallError("profile must be test or production")
    suffix = "telegram-video-downloader-test" if profile == "test" else "telegram-video-downloader"
    return home / ".local/lib" / suffix


def plan(home: Path, profile: str, python: Path) -> dict[str, Path | str]:
    root = profile_root(home, profile)
    return {
        "profile": profile,
        "python": python,
        "runtime": root / "music-runtime",
        "interpreter": root / "music-runtime/bin/python3",
        "helper": root / "libexec/music_transfer.py",
        "dependencies": ", ".join(DEPENDENCIES),
    }


def _run_bounded(argv: list[str], *, timeout: int, cwd: Path | None = None) -> str:
    process = None
    selector = None
    streams: dict[str, bytearray] = {"stdout": bytearray(), "stderr": bytearray()}
    try:
        process = subprocess.Popen(
            argv,
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={
                "HOME": str(actual_home()),
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                "PYTHONNOUSERSITE": "1",
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONUNBUFFERED": "1",
            },
        )
        assert process.stdout is not None and process.stderr is not None
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ, "stdout")
        selector.register(process.stderr, selectors.EVENT_READ, "stderr")
        deadline = time.monotonic() + timeout
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise InstallError("runtime installation command exceeded its time limit")
            for key, _ in selector.select(min(remaining, 0.25)):
                chunk = os.read(key.fileobj.fileno(), 8192)
                if not chunk:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                output = streams[key.data]
                if len(output) + len(chunk) > OUTPUT_LIMIT:
                    raise InstallError("runtime installation command exceeded its output limit")
                output.extend(chunk)
        return_code = process.wait(timeout=max(0.1, deadline - time.monotonic()))
        selector.close()
        selector = None
        if return_code != 0:
            raise InstallError("runtime installation command failed")
        return (streams["stdout"] + streams["stderr"]).decode("utf-8", errors="replace")
    except InstallError:
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
        raise InstallError("runtime installation command could not complete") from None


def _check_python(python: Path) -> None:
    if not python.is_absolute() or not python.is_file():
        raise InstallError("selected Python interpreter must be an absolute file path")
    output = _run_bounded([str(python), "--version"], timeout=10)
    if "Python 3.14." not in output:
        raise InstallError("Music runtime installation requires Python 3.14")


def _ensure_owned_directory(path: Path, mode: int, *, create: bool) -> None:
    try:
        if create:
            path.mkdir(mode=mode)
        info = path.lstat()
    except FileExistsError:
        info = path.lstat()
    except OSError:
        raise InstallError("Music runtime destination directory is unavailable") from None
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != mode
    ):
        raise InstallError("Music runtime directories must be user-owned and private")


def _ensure_libexec(path: Path) -> None:
    try:
        path.mkdir(mode=0o755)
    except FileExistsError:
        pass
    except OSError:
        raise InstallError("profile libexec directory is unavailable") from None
    try:
        info = path.lstat()
    except OSError:
        raise InstallError("profile libexec directory is unavailable") from None
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) & 0o022
    ):
        raise InstallError("profile libexec directory is not protected from other writers")


def _copy_helper(source: Path, destination: Path) -> None:
    try:
        source_info = source.lstat()
        if not stat.S_ISREG(source_info.st_mode):
            raise InstallError("source Music transfer helper is not a regular file")
        data = source.read_bytes()
        if not data or len(data) > 1024 * 1024:
            raise InstallError("source Music transfer helper has an invalid size")
        existing = destination.lstat()
        if not stat.S_ISREG(existing.st_mode) or existing.st_uid != os.getuid():
            raise InstallError("installed Music transfer helper is not a user-owned regular file")
    except FileNotFoundError:
        pass
    except InstallError:
        raise
    except OSError:
        raise InstallError("Music transfer helper could not be inspected") from None
    else:
        pass
    if "data" not in locals():
        try:
            data = source.read_bytes()
        except OSError:
            raise InstallError("source Music transfer helper could not be read") from None
    temp = destination.with_name(destination.name + ".installing")
    try:
        temp_info = temp.lstat()
    except FileNotFoundError:
        temp_info = None
    except OSError:
        raise InstallError("temporary Music helper path could not be inspected") from None
    if temp_info is not None:
        if not stat.S_ISREG(temp_info.st_mode) or temp_info.st_uid != os.getuid():
            raise InstallError("temporary Music helper path is not safe")
        try:
            temp.unlink()
        except OSError:
            raise InstallError("temporary Music helper could not be cleared") from None
    descriptor = None
    try:
        descriptor = os.open(
            temp,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        os.fchmod(descriptor, 0o600)
        view = memoryview(data)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short write")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        os.replace(temp, destination)
        directory_fd = os.open(destination.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError:
        raise InstallError("Music transfer helper could not be installed safely") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def apply_install(home: Path, profile: str, python: Path) -> dict[str, Path | str]:
    selected = plan(home, profile, python)
    root = profile_root(home, profile)
    try:
        root_info = root.lstat()
    except OSError:
        raise InstallError("fixed application profile is unavailable") from None
    if (
        not stat.S_ISDIR(root_info.st_mode)
        or root_info.st_uid != os.getuid()
        or stat.S_IMODE(root_info.st_mode) & 0o022
    ):
        raise InstallError("fixed application profile is not protected from other writers")
    _check_python(python)

    runtime = Path(selected["runtime"])
    _ensure_owned_directory(runtime, 0o700, create=True)
    libexec = Path(selected["helper"]).parent
    _ensure_libexec(libexec)

    try:
        import venv

        venv.EnvBuilder(with_pip=True, clear=False, symlinks=True).create(runtime)
    except Exception:
        raise InstallError("private Music Python runtime could not be created") from None
    _ensure_owned_directory(runtime, 0o700, create=False)
    interpreter = Path(selected["interpreter"])
    if not interpreter.is_absolute() or not interpreter.exists():
        raise InstallError("private Music Python interpreter was not created")

    requirements = Path(__file__).resolve().with_name("music-requirements.txt")
    source_helper = Path(__file__).resolve().with_name("music_transfer.py")
    try:
        content = requirements.read_text(encoding="utf-8").splitlines()
    except OSError:
        raise InstallError("pinned Music runtime requirements are unavailable") from None
    if content != list(DEPENDENCIES):
        raise InstallError("Music runtime requirements do not match the pinned Telethon dependency set")
    _run_bounded(
        [
            str(interpreter),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-input",
            "--no-cache-dir",
            "--only-binary=:all:",
            # This pinned pure-Python release is distributed only as an sdist.
            "--no-binary=pyaes",
            "--requirement",
            str(requirements),
        ],
        timeout=COMMAND_TIMEOUT,
    )
    package_names = [dependency.split("==", 1)[0] for dependency in DEPENDENCIES]
    package_info = _run_bounded(
        [str(interpreter), "-m", "pip", "show", *package_names], timeout=30
    )
    versions: dict[str, str] = {}
    current_name = None
    for line in package_info.splitlines():
        if line.startswith("Name: "):
            current_name = line[6:].strip().lower()
        elif line.startswith("Version: ") and current_name is not None:
            versions[current_name] = line[9:].strip()
    expected_versions = {
        dependency.split("==", 1)[0].lower(): dependency.split("==", 1)[1]
        for dependency in DEPENDENCIES
    }
    if versions != expected_versions:
        raise InstallError("installed Music runtime dependency versions did not match the pins")
    _copy_helper(source_helper, Path(selected["helper"]))
    return selected


def _arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--apply-test", action="store_true")
    modes.add_argument("--apply-production", action="store_true")
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _arguments(argv)
    profile = "test" if args.apply_test else "production"
    home = actual_home()
    selected = plan(home, profile, args.python)
    if not args.apply_test and not args.apply_production:
        print(
            "\n".join(
                [
                    f"mode=plan profile={selected['profile']}",
                    f"python={selected['python']}",
                    f"runtime={selected['runtime']}",
                    f"interpreter={selected['interpreter']}",
                    f"helper={selected['helper']}",
                    f"dependencies={selected['dependencies']}",
                    f"apply with --apply-{selected['profile']} to create or update this profile",
                ]
            )
        )
        return 0
    installed = apply_install(home, profile, args.python)
    print(
        f"installed profile={installed['profile']} runtime={installed['runtime']} helper={installed['helper']}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except InstallError as exc:
        print(f"Music runtime installer failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
