"""Private Instagram cookie backup and recovery helper.

The script is also embedded by the bot and run with ``python -I -c``. Keep all
paths derived from the fixed private config location and do not depend on
``__file__``.
"""

import argparse
from contextlib import closing, contextmanager
from datetime import datetime
import hashlib
import hmac
import inspect
import json
import logging
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import sys
import tempfile
import time
from typing import Sequence
from urllib.parse import parse_qsl, urlsplit


CONFIG_DIR = Path.home() / ".config/telegram-video-downloader"
BACKUP_DIR = CONFIG_DIR / "instagram-cookie-backups"
ROLE_CHOICES = ("primary", "fallback")
PROFILE_PROVENANCE_KEY = "instagram_session_profile_sha256"

STATUS_OK = 0
STATUS_ABSENT = 20
STATUS_UNAVAILABLE = 21
STATUS_BACKUP_UNUSABLE = 22

CHROMIUM_EPOCH_OFFSET = 11_644_473_600
SNAPSHOT_RE = re.compile(
    r"(?P<role>primary|fallback)-\d{8}T\d{12}[+-]\d{4}\.sqlite3\Z"
)
USERNAME_RE = re.compile(r"[A-Za-z0-9._]{1,30}\Z")
SHORTCODE_RE = re.compile(r"[A-Za-z0-9_-]{1,90}\Z")
NUMERIC_ID_RE = re.compile(r"[0-9]{1,24}\Z")
RESERVED_PROFILE_NAMES = {
    "about",
    "accounts",
    "api",
    "direct",
    "explore",
    "p",
    "reel",
    "s",
    "stories",
    "tv",
}
last_backup_name: str | None = None


class SessionUnavailable(Exception):
    """A local session check could not establish whether a login is usable."""


def _validate_role(role: str) -> None:
    if role not in ROLE_CHOICES:
        raise ValueError("Unknown Instagram profile role")


def _lstat_private(path: Path, *, directory: bool, mode: int) -> os.stat_result:
    metadata = path.lstat()
    is_expected_type = stat.S_ISDIR(metadata.st_mode) if directory else stat.S_ISREG(metadata.st_mode)
    if (
        not is_expected_type
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != mode
    ):
        raise SessionUnavailable("Private path permissions are invalid")
    return metadata


def _validate_config_directory() -> None:
    _lstat_private(CONFIG_DIR, directory=True, mode=0o700)


def _validate_backup_directory(*, create: bool = False) -> None:
    if create:
        try:
            BACKUP_DIR.lstat()
        except FileNotFoundError:
            BACKUP_DIR.mkdir(mode=0o700)
    _lstat_private(BACKUP_DIR, directory=True, mode=0o700)


def profile_path(role: str) -> Path:
    """Return the configured Chrome profile directory for one role."""
    _validate_role(role)
    _validate_config_directory()
    config_path = CONFIG_DIR / "instagram-profiles.json"
    _lstat_private(config_path, directory=False, mode=0o600)

    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
        value = config[role]["cookies_from_browser"]
    except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError) as error:
        raise SessionUnavailable("Instagram profile configuration is invalid") from error

    if not isinstance(value, str) or not value.startswith("chrome:/"):
        raise SessionUnavailable("Expected a dedicated Chrome profile")
    path = Path(value.removeprefix("chrome:"))
    if not path.is_absolute():
        raise SessionUnavailable("Expected an absolute Chrome profile path")
    try:
        resolved = path.resolve()
    except OSError as error:
        raise SessionUnavailable("Configured Chrome profile is unavailable") from error
    if resolved.exists() and not resolved.is_dir():
        raise SessionUnavailable("Configured Chrome profile is unavailable")
    return resolved


def configured_profile_path(role: str, requested_path: str) -> Path:
    """Require an app-supplied profile path to match the current role config."""
    configured = profile_path(role)
    if not isinstance(requested_path, str):
        raise SessionUnavailable("Instagram profile path is invalid")
    candidate = Path(requested_path)
    if (
        not candidate.is_absolute()
        or "::" in requested_path
        or any(part == ".." for part in candidate.parts)
        or any(character in requested_path for character in "\r\n\x00")
    ):
        raise SessionUnavailable("Instagram profile path is invalid")
    # Rust selected the browser path before invoking the original downloader.
    # Re-resolving it here would follow a link that may have changed meanwhile.
    if candidate != configured:
        raise SessionUnavailable("Instagram profile path does not match its role")
    return configured


def profile_provenance(profile: Path) -> str:
    return hashlib.sha256(os.fsencode(profile)).hexdigest()


def cookie_db(profile: Path) -> Path:
    """Find the configured profile's current Chrome cookie database."""
    profile = Path(profile)
    candidates = (profile / "Network" / "Cookies", profile / "Cookies")
    found: list[tuple[int, Path]] = []
    for path in candidates:
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            continue
        except OSError as error:
            raise SessionUnavailable("Chrome cookie database is unavailable") from error
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
            raise SessionUnavailable("Chrome cookie database is not an owner-owned file")
        found.append((metadata.st_mtime_ns, path))
    if not found:
        raise SessionUnavailable("Chrome cookie database is missing")
    return max(found, key=lambda item: (item[0], item[1].as_posix()))[1]


def _instagram_domain_sql() -> str:
    return "(lower(host_key) = 'instagram.com' OR lower(host_key) = '.instagram.com' OR lower(host_key) LIKE '%.instagram.com')"


def _live_session_rows(database: Path) -> list[int]:
    """Read only Instagram session-cookie expiry metadata from a DB."""
    try:
        uri = database.resolve(strict=True).as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=4)) as connection:
            rows = connection.execute(
                "SELECT expires_utc FROM cookies WHERE "
                + _instagram_domain_sql()
                + " AND name = ?",
                ("sessionid",),
            ).fetchall()
    except (OSError, sqlite3.Error, ValueError) as error:
        raise SessionUnavailable("Could not inspect Instagram cookie database") from error

    now = time.time()
    active: list[int] = []
    for row in rows:
        if len(row) != 1 or row[0] is None:
            raise SessionUnavailable("Instagram session-cookie expiry is invalid")
        try:
            expires_utc = int(row[0])
        except (TypeError, ValueError, OverflowError) as error:
            raise SessionUnavailable("Instagram session-cookie expiry is invalid") from error
        if expires_utc == 0 or expires_utc / 1_000_000 - CHROMIUM_EPOCH_OFFSET > now:
            active.append(expires_utc)
    return active


def _is_instagram_cookie(cookie: object) -> bool:
    domain = getattr(cookie, "domain", "")
    if not isinstance(domain, str):
        return False
    normalized = domain.lower().lstrip(".")
    return normalized == "instagram.com" or normalized.endswith(".instagram.com")


def _native_v11_supported(cookies_module: object) -> bool:
    """Avoid wrapping a future gallery-dl decryptor that already handles v11."""
    try:
        source = inspect.getsource(cookies_module.MacChromiumCookieDecryptor.decrypt)
    except (AttributeError, OSError, TypeError):
        return False
    return "v11" in source.lower()


@contextmanager
def use_chrome_158_cookie_reader(profile: Path):
    """Add v11 decoding for gallery-dl 1.32.21, scoped to one profile root."""
    from gallery_dl import cookies, version

    if (
        sys.platform != "darwin"
        or version.__version__ != "1.32.21"
        or _native_v11_supported(cookies)
    ):
        yield
        return

    original = cookies._chromium_cookie_decryptor
    scoped_root = str(Path(profile).resolve(strict=True).parent)

    def create(browser_root, keyring_name, keyring=None, meta_version=0):
        decoder = original(browser_root, keyring_name, keyring, meta_version)
        try:
            requested_root = str(Path(browser_root).resolve(strict=True))
        except (OSError, TypeError, ValueError):
            return decoder
        if requested_root != scoped_root:
            return decoder

        default_decrypt = decoder.decrypt
        custom_key = None

        def decrypt(ciphertext):
            nonlocal custom_key
            if not ciphertext.startswith(b"v11"):
                return default_decrypt(ciphertext)
            if custom_key is None:
                secret = cookies._get_mac_keyring_password(keyring_name)
                if not secret:
                    return None
                custom_key = hashlib.pbkdf2_hmac(
                    "sha1", secret, b"pepper", 1003, 16
                )
            try:
                return cookies._decrypt_aes_cbc(
                    ciphertext[3:], custom_key, decoder._offset
                )
            except Exception:
                return None

        decoder.decrypt = decrypt
        return decoder

    cookies._chromium_cookie_decryptor = create
    try:
        yield
    finally:
        cookies._chromium_cookie_decryptor = original


def decoded_cookies(profile: Path) -> list[object]:
    """Decode only Instagram cookies from the specified Chrome profile."""
    from gallery_dl import cookies

    previous_disable = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        with use_chrome_158_cookie_reader(profile):
            return cookies.load_cookies(
                ["chrome", str(Path(profile).resolve(strict=True)), None, None, "instagram.com"]
            )
    except Exception as error:
        raise SessionUnavailable("Could not decode Instagram session cookies") from error
    finally:
        logging.disable(previous_disable)


def inspect_session(profile: Path) -> tuple[int, str | None]:
    """Return (status, in-memory cookie value) without printing cookie data."""
    database = cookie_db(profile)
    active_rows = _live_session_rows(database)
    if not active_rows:
        return STATUS_ABSENT, None
    if len(active_rows) != 1:
        return STATUS_UNAVAILABLE, None

    cookies = decoded_cookies(profile)
    matches = [
        cookie
        for cookie in cookies
        if getattr(cookie, "name", None) == "sessionid" and _is_instagram_cookie(cookie)
    ]
    active_cookies = []
    for cookie in matches:
        try:
            expired = cookie.is_expired()
        except Exception as error:
            raise SessionUnavailable("Decoded Instagram cookie expiry is invalid") from error
        if not expired:
            active_cookies.append(cookie)
    if len(active_cookies) != 1:
        return STATUS_UNAVAILABLE, None
    value = getattr(active_cookies[0], "value", None)
    if not isinstance(value, str) or not value:
        return STATUS_UNAVAILABLE, None
    return STATUS_OK, value


def _readonly_sqlite_uri(database: Path) -> str:
    return database.resolve(strict=True).as_uri() + "?mode=ro"


def _snapshot_provenance(snapshot_path: Path) -> str | None:
    try:
        with closing(sqlite3.connect(_readonly_sqlite_uri(snapshot_path), uri=True, timeout=4)) as snapshot:
            has_meta = snapshot.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta'"
            ).fetchone()
            if has_meta is None:
                return None
            columns = {
                row[1] for row in snapshot.execute("PRAGMA table_info(meta)").fetchall()
            }
            if not {"key", "value"}.issubset(columns):
                raise SessionUnavailable("Snapshot metadata table is invalid")
            rows = snapshot.execute(
                "SELECT value FROM meta WHERE key = ?", (PROFILE_PROVENANCE_KEY,)
            ).fetchall()
    except (OSError, sqlite3.Error, ValueError) as error:
        if isinstance(error, SessionUnavailable):
            raise
        raise SessionUnavailable("Could not inspect snapshot provenance") from error
    if not rows:
        return None
    if len(rows) != 1 or not isinstance(rows[0][0], str):
        raise SessionUnavailable("Snapshot provenance is ambiguous")
    value = rows[0][0]
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise SessionUnavailable("Snapshot provenance is invalid")
    return value


def _make_snapshot(source: Path, destination: Path, source_profile_hash: str) -> None:
    try:
        with closing(sqlite3.connect(_readonly_sqlite_uri(source), uri=True, timeout=4)) as active:
            with closing(sqlite3.connect(destination, timeout=4)) as snapshot:
                active.backup(snapshot, pages=64)
                snapshot.execute("PRAGMA journal_mode=DELETE")
                snapshot.execute("PRAGMA secure_delete=ON")
                tables = snapshot.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
                table_names = {row[0] for row in tables}
                if "cookies" not in table_names:
                    raise SessionUnavailable("Cookie database has no cookies table")
                for (table_name,) in tables:
                    if table_name not in {"cookies", "meta", "sqlite_sequence"}:
                        escaped = table_name.replace('"', '""')
                        snapshot.execute(f'DROP TABLE "{escaped}"')
                snapshot.execute(
                    "DELETE FROM cookies WHERE host_key IS NULL OR NOT "
                    + _instagram_domain_sql()
                )
                if "meta" not in table_names:
                    snapshot.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
                else:
                    columns = {
                        row[1]
                        for row in snapshot.execute("PRAGMA table_info(meta)").fetchall()
                    }
                    if not {"key", "value"}.issubset(columns):
                        raise SessionUnavailable("Cookie metadata table is invalid")
                snapshot.execute("DELETE FROM meta WHERE key = ?", (PROFILE_PROVENANCE_KEY,))
                snapshot.execute(
                    "INSERT INTO meta(key, value) VALUES (?, ?)",
                    (PROFILE_PROVENANCE_KEY, source_profile_hash),
                )
                snapshot.commit()
                snapshot.execute("VACUUM")
                result = snapshot.execute("PRAGMA quick_check").fetchone()
                if result != ("ok",):
                    raise SessionUnavailable("Cookie snapshot failed SQLite validation")
    except (OSError, sqlite3.Error, ValueError) as error:
        raise SessionUnavailable("Could not create private cookie snapshot") from error


def _copy_snapshot_to_profile(snapshot: Path, profile: Path) -> Path:
    profile.mkdir(mode=0o700, parents=True, exist_ok=False)
    network = profile / "Network"
    network.mkdir(mode=0o700)
    destination = network / "Cookies"
    shutil.copyfile(snapshot, destination)
    destination.chmod(0o600)
    return profile


def _validate_snapshot(snapshot: Path, role: str) -> None:
    _validate_role(role)
    if snapshot.parent.resolve() != BACKUP_DIR.resolve():
        raise SessionUnavailable("Snapshot is outside the private backup directory")
    if SNAPSHOT_RE.fullmatch(snapshot.name) is None or not snapshot.name.startswith(role + "-"):
        raise SessionUnavailable("Snapshot name is invalid")
    _lstat_private(snapshot, directory=False, mode=0o400)


def _snapshot_candidates(role: str) -> list[Path]:
    _validate_role(role)
    try:
        BACKUP_DIR.lstat()
    except FileNotFoundError:
        return []
    _validate_backup_directory()
    candidates = []
    for path in BACKUP_DIR.iterdir():
        if path.name.startswith(role + "-") and path.name.endswith(".sqlite3"):
            if SNAPSHOT_RE.fullmatch(path.name) is not None:
                candidates.append(path)
    return candidates


def latest_backup(
    role: str, *, profile: Path | None = None, require_bound: bool = False
) -> Path | None:
    """Return the newest private snapshot that belongs to the selected profile."""
    candidates = _snapshot_candidates(role)
    if not candidates:
        return None
    expected = profile_provenance(profile if profile is not None else profile_path(role))
    for candidate in sorted(candidates, key=lambda path: (path.lstat().st_mtime_ns, path.name), reverse=True):
        try:
            _validate_snapshot(candidate, role)
            provenance = _snapshot_provenance(candidate)
        except (OSError, SessionUnavailable):
            continue
        if provenance == expected or (provenance is None and not require_bound):
            return candidate
    raise SessionUnavailable("No Instagram backup matches the selected profile")


def _checked_snapshot(snapshot: Path, role: str) -> tuple[int, str | None]:
    """Decode a snapshot from a private temporary copy, then remove the copy."""
    _validate_snapshot(snapshot, role)
    with tempfile.TemporaryDirectory(prefix="instagram-cookie-check-") as raw_root:
        root = Path(raw_root)
        root.chmod(0o700)
        profile = _copy_snapshot_to_profile(snapshot, root / "ChromeProfile")
        return inspect_session(profile)


def status(role: str, requested_profile_path: str | None = None) -> int:
    """Check one configured live profile without making network requests."""
    try:
        profile = (
            profile_path(role)
            if requested_profile_path is None
            else configured_profile_path(role, requested_profile_path)
        )
        code, _cookie = inspect_session(profile)
    except Exception:
        return STATUS_UNAVAILABLE
    return code


def backup(role: str) -> int:
    """Create an owner-only, Instagram-only SQLite cookie snapshot."""
    global last_backup_name
    last_backup_name = None
    try:
        profile = profile_path(role)
        source_profile_hash = profile_provenance(profile)
        live_status, live_cookie = inspect_session(profile)
        if live_status != STATUS_OK or live_cookie is None:
            return live_status
        _validate_backup_directory(create=True)
        with tempfile.TemporaryDirectory(prefix=".snapshot-", dir=BACKUP_DIR) as raw_scratch:
            scratch = Path(raw_scratch)
            scratch.chmod(0o700)
            destination = scratch / "Cookies"
            _make_snapshot(cookie_db(profile), destination, source_profile_hash)
            destination.chmod(0o400)
            copied_profile = scratch / "ChromeProfile"
            _copy_snapshot_to_profile(destination, copied_profile)
            copy_status, copy_cookie = inspect_session(copied_profile)
            if (
                copy_status != STATUS_OK
                or copy_cookie is None
                or not hmac.compare_digest(live_cookie, copy_cookie)
                or _snapshot_provenance(destination) != source_profile_hash
            ):
                return STATUS_UNAVAILABLE

            stamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%f%z")
            snapshot = BACKUP_DIR / f"{role}-{stamp}.sqlite3"
            os.link(destination, snapshot)
            snapshot.chmod(0o400)
            _lstat_private(snapshot, directory=False, mode=0o400)
            last_backup_name = snapshot.name
        return STATUS_OK
    except Exception:
        return STATUS_UNAVAILABLE


def check_backup(role: str) -> int:
    """Check the newest owner-only snapshot for a role."""
    try:
        snapshot = latest_backup(role)
        if snapshot is None:
            return STATUS_ABSENT
        code, _cookie = _checked_snapshot(snapshot, role)
        return code
    except Exception:
        return STATUS_UNAVAILABLE


def _validate_instagram_url(url: str) -> None:
    if not isinstance(url, str) or any(character in url for character in "\r\n\x00"):
        raise ValueError("Invalid Instagram URL")
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as error:
        raise ValueError("Invalid Instagram URL") from error
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.instagram.com"
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or not parsed.path.startswith("/")
        or parsed.fragment
    ):
        raise ValueError("Invalid Instagram URL")

    has_trailing_slash = parsed.path.endswith("/")
    parts = parsed.path.strip("/").split("/")
    if "//" in parsed.path or not parts or any(not part for part in parts):
        raise ValueError("Unsupported Instagram URL path")

    if len(parts) == 2 and parts[0] in {"p", "reel", "tv"}:
        if has_trailing_slash and SHORTCODE_RE.fullmatch(parts[1]) and not parsed.query:
            return
    if len(parts) == 2 and parts[0] == "stories":
        if (
            has_trailing_slash
            and USERNAME_RE.fullmatch(parts[1])
            and parts[1] not in RESERVED_PROFILE_NAMES
            and not parsed.query
        ):
            return
    if len(parts) == 3 and parts[0] == "stories":
        if (
            has_trailing_slash
            and USERNAME_RE.fullmatch(parts[1])
            and NUMERIC_ID_RE.fullmatch(parts[2])
            and not parsed.query
        ):
            return
    if len(parts) == 2 and parts[0] == "s":
        ids = [
            value
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if key == "story_media_id"
        ]
        if (
            not has_trailing_slash
            and SHORTCODE_RE.fullmatch(parts[1])
            and parts[1].startswith("aGlnaGxpZ2h0")
            and len(ids) == 1
            and NUMERIC_ID_RE.fullmatch(ids[0])
        ):
            return
    if len(parts) == 1 and USERNAME_RE.fullmatch(parts[0]):
        if parts[0] not in RESERVED_PROFILE_NAMES and not parsed.query:
            return
    if len(parts) == 2 and parts[1] == "avatar":
        if (
            has_trailing_slash
            and USERNAME_RE.fullmatch(parts[0])
            and parts[0] not in RESERVED_PROFILE_NAMES
            and not parsed.query
        ):
            return
    raise ValueError("Unsupported Instagram URL path")


def run_gallery_dl(argv: Sequence[str], profile: Path) -> int:
    """Invoke gallery-dl in-process so v11 compatibility sees the temp profile."""
    from gallery_dl import main as gallery_dl_main

    original_argv = sys.argv
    sys.argv = ["gallery-dl", *argv]
    try:
        with use_chrome_158_cookie_reader(profile):
            try:
                result = gallery_dl_main()
            except SystemExit as error:
                result = error.code
    finally:
        sys.argv = original_argv
    if result is None:
        return 0
    if isinstance(result, int):
        return result
    return 1


def _restore_preflight_error(marker: str, code: int) -> int:
    print(marker, file=sys.stderr)
    return code


def restore(
    role: str,
    url: str,
    directory: str,
    range_value: str,
    filename: str,
    simulate: bool = False,
    requested_profile_path: str | None = None,
) -> int:
    """Run gallery-dl against the newest verified private backup copy."""
    try:
        _validate_role(role)
        _validate_instagram_url(url)
    except ValueError:
        print("Instagram session recovery: invalid URL or role", file=sys.stderr)
        return 2

    try:
        automatic = requested_profile_path is not None
        expected_profile = (
            profile_path(role)
            if requested_profile_path is None
            else configured_profile_path(role, requested_profile_path)
        )
        snapshot = latest_backup(role, profile=expected_profile, require_bound=automatic)
    except Exception:
        return _restore_preflight_error(
            "Instagram session recovery: backup unavailable", STATUS_UNAVAILABLE
        )
    if snapshot is None:
        return _restore_preflight_error(
            "Instagram session recovery: no backup", STATUS_UNAVAILABLE
        )

    try:
        temporary = tempfile.TemporaryDirectory(prefix="instagram-session-restore-")
    except Exception:
        return _restore_preflight_error(
            "Instagram session recovery: backup unavailable", STATUS_UNAVAILABLE
        )

    with temporary as raw_root:
        root = Path(raw_root)
        try:
            root.chmod(0o700)
            profile = _copy_snapshot_to_profile(snapshot, root / "ChromeProfile")
            session_status, _cookie = inspect_session(profile)
        except Exception:
            return _restore_preflight_error(
                "Instagram session recovery: backup unavailable", STATUS_UNAVAILABLE
            )
        if session_status == STATUS_ABSENT:
            return _restore_preflight_error(
                "Instagram session recovery: backup has no usable login",
                STATUS_BACKUP_UNUSABLE,
            )
        if session_status != STATUS_OK:
            return _restore_preflight_error(
                "Instagram session recovery: backup unavailable", STATUS_UNAVAILABLE
            )

        cache_path = root / "gallery-dl-cache.sqlite3"
        gallery_argv = [
            "--config-ignore",
            "--no-input",
            "--retries",
            "0",
            f"--directory={directory}",
            f"--range={range_value}",
            f"--filename={filename}",
            f"--cookies-from-browser=chrome/instagram.com:{profile}",
            f"--cache-file={cache_path}",
        ]
        if simulate:
            gallery_argv.append("--simulate")
        gallery_argv.extend(("--", url))
        return run_gallery_dl(gallery_argv, profile)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)

    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("role", choices=ROLE_CHOICES)
    status_parser.add_argument("--profile-path")

    backup_parser = subparsers.add_parser("backup")
    backup_parser.add_argument("role", choices=ROLE_CHOICES)

    check_parser = subparsers.add_parser("check-backup")
    check_parser.add_argument("role", choices=ROLE_CHOICES)

    restore_parser = subparsers.add_parser("restore")
    restore_parser.add_argument("role", choices=ROLE_CHOICES)
    restore_parser.add_argument("url")
    restore_parser.add_argument("--directory", required=True)
    restore_parser.add_argument("--range", dest="range_value", required=True)
    restore_parser.add_argument("--filename", required=True)
    restore_parser.add_argument("--simulate", action="store_true")
    restore_parser.add_argument("--profile-path")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    os.umask(0o077)
    if args.action == "status":
        code = status(args.role, args.profile_path)
        label = {
            STATUS_OK: "readable",
            STATUS_ABSENT: "missing or expired",
            STATUS_UNAVAILABLE: "unavailable",
        }.get(code, "unavailable")
        print("session_cookie_status=" + label)
        return code
    if args.action == "backup":
        code = backup(args.role)
        if code == STATUS_OK and last_backup_name:
            print("backup_created=true backup_file=" + last_backup_name)
        else:
            print("backup_created=false")
        return code
    if args.action == "check-backup":
        code = check_backup(args.role)
        label = {
            STATUS_OK: "readable",
            STATUS_ABSENT: "missing or expired",
            STATUS_UNAVAILABLE: "unavailable",
        }.get(code, "unavailable")
        print("backup_session_cookie_status=" + label)
        return code
    return restore(
        args.role,
        args.url,
        args.directory,
        args.range_value,
        args.filename,
        args.simulate,
        args.profile_path,
    )


if __name__ == "__main__":
    raise SystemExit(main())
