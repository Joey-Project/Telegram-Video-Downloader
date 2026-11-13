#!/usr/bin/env python3
"""Plan or explicitly install the signed, profile-local FLAC converter helper."""

from __future__ import annotations

import argparse
import hashlib
import os
import pwd
import re
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

CONVERTER_IDENTIFIER = "io.github.telegram-local-downloader.bot.converter"
PROFILES = {
    "production": ".local/lib/telegram-video-downloader",
    "test": ".local/lib/telegram-video-downloader-test",
}
MAX_SOURCE_BYTES = 512 * 1024 * 1024
OUTPUT_LIMIT = 64 * 1024
COMMAND_TIMEOUT = 60


class InstallError(Exception):
    """A safe diagnostic for converter installation failures."""


def actual_home() -> Path:
    try:
        home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    except (KeyError, OSError):
        raise InstallError("current account home directory is unavailable") from None
    if not home.is_absolute():
        raise InstallError("current account home directory is invalid")
    return home


def profile_root(home: Path, profile: str) -> Path:
    try:
        suffix = PROFILES[profile]
    except KeyError:
        raise InstallError("profile must be test or production") from None
    return home / suffix


def _run(argv: list[str], *, timeout: int = COMMAND_TIMEOUT) -> subprocess.CompletedProcess:
    try:
        result = subprocess.run(
            [str(value) for value in argv],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False,
            env={
                "HOME": str(actual_home()),
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                "LC_ALL": "C",
            },
        )
    except (OSError, subprocess.TimeoutExpired):
        raise InstallError(f"{Path(argv[0]).name} could not complete") from None
    if len(result.stdout) + len(result.stderr) > OUTPUT_LIMIT:
        raise InstallError(f"{Path(argv[0]).name} output exceeded its limit")
    if result.returncode:
        raise InstallError(f"{Path(argv[0]).name} failed (exit {result.returncode})")
    return result


def _source_info(source: Path) -> os.stat_result:
    if not source.is_absolute():
        raise InstallError("converter source must be an absolute path")
    try:
        info = source.lstat()
    except OSError:
        raise InstallError("converter source is unavailable") from None
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_nlink != 1
        or not stat.S_IMODE(info.st_mode) & 0o111
        or info.st_size <= 0
        or info.st_size > MAX_SOURCE_BYTES
    ):
        raise InstallError("converter source must be a user-owned regular executable")
    return info


def _dependency_paths(output: str) -> list[str]:
    lines = output.splitlines()
    if not lines or not lines[0].endswith(":"):
        raise InstallError("converter source is not a supported Mach-O executable")
    dependencies = []
    for line in lines[1:]:
        entry = line.strip()
        if not entry:
            continue
        match = re.fullmatch(r"(.+?) \(compatibility version [^,]+, current version [^)]+\)", entry)
        if match is None:
            raise InstallError("converter dependency output is not recognized")
        dependencies.append(match.group(1))
    if not dependencies:
        raise InstallError("converter dependency list is empty")
    return dependencies


def _validate_system_dependencies(source: Path) -> list[str]:
    output = _run(["/usr/bin/otool", "-L", source]).stdout
    dependencies = _dependency_paths(output)
    unexpected = [
        item
        for item in dependencies
        if not item.startswith(("/usr/lib/", "/System/Library/"))
    ]
    if unexpected:
        raise InstallError("converter has a non-system runtime dependency")
    return dependencies


def plan(home: Path, profile: str, source: Path, identity: str) -> dict[str, str]:
    if re.fullmatch(r"[A-Fa-f0-9]{40}", identity) is None:
        raise InstallError("signing identity must be a 40-character certificate fingerprint")
    _source_info(source)
    dependencies = _validate_system_dependencies(source)
    root = profile_root(home, profile)
    return {
        "mode": "plan",
        "profile": profile,
        "source": str(source),
        "destination": str(root / "libexec/flac-alac-converter"),
        "identifier": CONVERTER_IDENTIFIER,
        "signing_identity": identity.upper(),
        "dependencies": ", ".join(dependencies),
    }


def _ensure_profile(root: Path, home: Path) -> None:
    _no_symlinks(root, home)
    try:
        info = root.lstat()
    except OSError:
        raise InstallError("fixed application profile is unavailable") from None
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) & 0o022
    ):
        raise InstallError("fixed application profile is not protected from other writers")


def _ensure_libexec(path: Path, root: Path) -> None:
    _no_symlinks(path, root)
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


def _copy_source(source: Path, destination: Path) -> None:
    descriptor = None
    output_descriptor = None
    try:
        before = _source_info(source)
        descriptor = os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise InstallError("converter source changed while opening")
        output_descriptor = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o700,
        )
        os.fchmod(output_descriptor, 0o700)
        copied_digest = hashlib.sha256()
        total = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            total += len(chunk)
            if total > MAX_SOURCE_BYTES:
                raise InstallError("converter source exceeded its size limit")
            copied_digest.update(chunk)
            view = memoryview(chunk)
            while view:
                written = os.write(output_descriptor, view)
                if written <= 0:
                    raise OSError("short write")
                view = view[written:]
        os.fsync(output_descriptor)

        # Content stability is the protected property. Re-read the same open object and
        # compare its digest; inode and size alone would miss an in-place rewrite.
        os.lseek(descriptor, 0, os.SEEK_SET)
        source_digest = hashlib.sha256()
        read_total = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            read_total += len(chunk)
            if read_total > MAX_SOURCE_BYTES:
                raise InstallError("converter source exceeded its size limit")
            source_digest.update(chunk)
        after = os.fstat(descriptor)
        current = source.lstat()
        if (
            (after.st_dev, after.st_ino) != (opened.st_dev, opened.st_ino)
            or (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino)
            or total != opened.st_size
            or read_total != opened.st_size
            or copied_digest.digest() != source_digest.digest()
        ):
            raise InstallError("converter source changed while being installed")
    except InstallError:
        raise
    except OSError:
        raise InstallError("converter source could not be copied safely") from None
    finally:
        if output_descriptor is not None:
            os.close(output_descriptor)
        if descriptor is not None:
            os.close(descriptor)


def _verify_signed_helper(path: Path, identity: str) -> None:
    requirement = (
        f'identifier "{CONVERTER_IDENTIFIER}" and certificate leaf = H"{identity.lower()}"'
    )
    _run(
        [
            "/usr/bin/codesign",
            "--verify",
            "--strict",
            "--test-requirement",
            "=" + requirement,
            path,
        ]
    )
    result = _run(["/usr/bin/codesign", "-dr", "-", path])
    output = result.stdout + result.stderr
    match = re.search(r"designated => (.+)", output)
    if not match or f'identifier "{CONVERTER_IDENTIFIER}"' not in match.group(1):
        raise InstallError("installed converter has the wrong designated identifier")


def apply_install(
    home: Path, profile: str, source: Path, identity: str, *, sign: bool = True
) -> dict[str, str]:
    selected = plan(home, profile, source, identity)
    root = profile_root(home, profile)
    _ensure_profile(root, home)
    destination = Path(selected["destination"])
    _ensure_libexec(destination.parent, root)
    license_sources = {
        "FLAC-LICENSE.txt": Path(__file__).resolve().parents[1]
        / "docs/licenses/FLAC-LICENSE.txt",
        "Ogg-LICENSE.txt": Path(__file__).resolve().parents[1]
        / "docs/licenses/Ogg-LICENSE.txt",
    }
    try:
        current = destination.lstat()
    except FileNotFoundError:
        current = None
    except OSError:
        raise InstallError("installed converter destination is unavailable") from None
    if current is not None and (
        not stat.S_ISREG(current.st_mode)
        or current.st_uid != os.getuid()
        or current.st_nlink != 1
    ):
        raise InstallError("installed converter must be a user-owned regular file")

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".flac-alac-converter.", suffix=".installing", dir=destination.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        temporary.unlink()
        _copy_source(source, temporary)
        if sign:
            _run(
                [
                    "/usr/bin/codesign",
                    "--force",
                    "--options",
                    "runtime",
                    "--identifier",
                    CONVERTER_IDENTIFIER,
                    "--sign",
                    identity.upper(),
                    temporary,
                ],
                timeout=COMMAND_TIMEOUT,
            )
        _verify_signed_helper(temporary, identity)
        _validate_system_dependencies(temporary)
        os.chmod(temporary, 0o755)
        for name, license_source in license_sources.items():
            _install_license(license_source, destination.parent / name)
        directory_fd = os.open(
            destination.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.replace(temporary, destination)
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass
        raise
    return {**selected, "mode": "installed"}


def _install_license(source: Path, destination: Path) -> None:
    _no_symlinks(destination, destination.parent)
    try:
        source_info = source.lstat()
    except OSError:
        raise InstallError("tracked converter license text is unavailable") from None
    if (
        not stat.S_ISREG(source_info.st_mode)
        or source_info.st_size <= 0
        or source_info.st_size > 16 * 1024
    ):
        raise InstallError("converter license source has invalid metadata")
    try:
        current = None
        content = source.read_bytes()
    except OSError:
        raise InstallError("converter license text could not be read") from None
    try:
        current = destination.lstat()
    except FileNotFoundError:
        current = None
    except OSError:
        raise InstallError("converter license text could not be inspected") from None
    if current is not None and (
        not stat.S_ISREG(current.st_mode)
        or current.st_uid != os.getuid()
        or current.st_nlink != 1
    ):
        raise InstallError("installed converter license must be a user-owned regular file")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".installing", dir=destination.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o644)
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short write")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, destination)
        directory_fd = os.open(
            destination.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError:
        raise InstallError("converter license text could not be installed") from None
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass


def _no_symlinks(path: Path, root: Path | None = None) -> None:
    entries = [path, *path.parents]
    if root is not None:
        bound = root.absolute()
        if bound not in entries:
            raise InstallError("converter install path escaped its fixed profile")
        entries = entries[: entries.index(bound) + 1]
    for entry in entries:
        try:
            if entry.is_symlink():
                raise InstallError("symbolic links are unsupported in the converter install path")
        except OSError:
            raise InstallError("converter install path could not be inspected") from None


def _arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=tuple(PROFILES), required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--identity", required=True, help="trusted signing certificate SHA-1")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="sign and atomically install the helper; default is read-only planning",
    )
    parser.add_argument(
        "--apply-signed",
        action="store_true",
        help="verify and atomically install an already-signed staged helper",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _arguments(argv)
    home = actual_home()
    if args.apply and args.apply_signed:
        raise InstallError("--apply and --apply-signed are mutually exclusive")
    if args.apply or args.apply_signed:
        selected = apply_install(
            home,
            args.profile,
            args.source,
            args.identity,
            sign=args.apply,
        )
    else:
        selected = plan(home, args.profile, args.source, args.identity)
    print("\n".join(f"{key}={value}" for key, value in selected.items()))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except InstallError as error:
        print(f"FLAC converter installer failed: {error}", file=sys.stderr)
        raise SystemExit(2) from None
