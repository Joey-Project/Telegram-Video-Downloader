from __future__ import annotations

import contextlib
import io
import json
import os
import sqlite3
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import instagram_session


class SyntheticCookie:
    def __init__(
        self,
        *,
        name: str = "sessionid",
        value: str = "synthetic-only",
        expired: bool = False,
    ):
        self.name = name
        self.value = value
        self.domain = ".instagram.com"
        self._expired = expired

    def is_expired(self) -> bool:
        return self._expired


class InstagramSessionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name).resolve()
        self.config_dir = self.root / "config"
        self.backup_dir = self.root / "backups"
        self.config_dir.mkdir(mode=0o700)
        self.backup_dir.mkdir(mode=0o700)
        self.primary_profile = self.root / "browser" / "Primary"
        self.fallback_profile = self.root / "browser" / "Fallback"
        self._make_profile(self.primary_profile)
        self._make_profile(self.fallback_profile)
        self.config_path = self.config_dir / "instagram-profiles.json"
        self._write_config()
        self.config_before = self._file_state(self.config_path)

        self.patches = [
            mock.patch.object(instagram_session, "CONFIG_DIR", self.config_dir),
            mock.patch.object(instagram_session, "BACKUP_DIR", self.backup_dir),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    @staticmethod
    def _file_state(path: Path) -> tuple[bytes, int, int, int, int]:
        info = path.stat()
        return (
            path.read_bytes(),
            stat.S_IMODE(info.st_mode),
            info.st_uid,
            info.st_ino,
            info.st_mtime_ns,
        )

    def _make_profile(self, profile: Path) -> None:
        (profile / "Network").mkdir(parents=True, mode=0o700)
        os.chmod(profile.parent, 0o700)
        os.chmod(profile, 0o700)
        database = profile / "Network" / "Cookies"
        self._create_cookie_database(database, [])

    def _write_config(
        self,
        *,
        primary_profile: Path | None = None,
        fallback_profile: Path | None = None,
    ) -> None:
        profiles = {
            "primary": {
                "cookies_from_browser": f"chrome:{primary_profile or self.primary_profile}"
            },
            "fallback": {
                "cookies_from_browser": f"chrome:{fallback_profile or self.fallback_profile}"
            },
        }
        self.config_path.write_text(json.dumps(profiles), encoding="utf-8")
        os.chmod(self.config_path, 0o600)
        os.chmod(self.config_dir, 0o700)

    @staticmethod
    def _create_cookie_database(
        database: Path,
        cookies: list[tuple[str, str, str, bytes, str, int, int]],
    ) -> None:
        database.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with contextlib.closing(sqlite3.connect(database)) as connection:
            with connection:
                connection.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT, value TEXT)")
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS cookies ("
                    "host_key TEXT, name TEXT, value TEXT, encrypted_value BLOB, "
                    "path TEXT, expires_utc INTEGER, is_secure INTEGER"
                    ")"
                )
                connection.execute("DELETE FROM cookies")
                connection.executemany(
                    "INSERT INTO cookies("
                    "host_key, name, value, encrypted_value, path, expires_utc, is_secure"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                    cookies,
                )
        os.chmod(database, 0o600)

    def _set_cookies(
        self,
        profile: Path,
        cookies: list[tuple[str, str, str, bytes, str, int, int]],
    ) -> Path:
        database = instagram_session.cookie_db(profile)
        self._create_cookie_database(database, cookies)
        return database

    @staticmethod
    def _live_cookie(
        *,
        host: str = ".instagram.com",
        name: str = "sessionid",
        expires: int = 14_000_000_000_000_000,
    ) -> tuple[str, str, str, bytes, str, int, int]:
        return host, name, "", b"synthetic encrypted bytes", "/", expires, 1

    def _successful_decode(self, _profile: object) -> list[SyntheticCookie]:
        return [SyntheticCookie()]

    def _run_cli(self, argv: list[str]) -> int:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return instagram_session.main(argv)

    def _backup(self, role: str) -> int:
        with mock.patch.object(
            instagram_session,
            "decoded_cookies",
            side_effect=self._successful_decode,
        ):
            return self._run_cli(["backup", role])

    def test_status_distinguishes_absent_or_expired_from_unavailable(self) -> None:
        primary_db = instagram_session.cookie_db(self.primary_profile)
        absent_before = self._file_state(primary_db)

        with mock.patch.object(instagram_session, "decoded_cookies") as decode:
            self.assertEqual(self._run_cli(["status", "primary"]), 20)
            decode.assert_not_called()

            expired = self._set_cookies(
                self.primary_profile,
                [self._live_cookie(expires=1)],
            )
            self.assertEqual(self._run_cli(["status", "primary"]), 20)
            decode.assert_not_called()

        self.assertNotEqual(self._file_state(primary_db), absent_before)
        self._set_cookies(self.primary_profile, [self._live_cookie()])
        with mock.patch.object(
            instagram_session,
            "decoded_cookies",
            side_effect=PermissionError("synthetic decrypt failure"),
        ):
            self.assertEqual(self._run_cli(["status", "primary"]), 21)

        with mock.patch.object(
            instagram_session,
            "decoded_cookies",
            side_effect=self._successful_decode,
        ):
            self.assertEqual(self._run_cli(["status", "primary"]), 0)

        self.assertTrue(expired.exists())

    def test_backup_is_role_scoped_owner_only_sqlite_and_leaves_live_files_unchanged(self) -> None:
        primary_db = self._set_cookies(
            self.primary_profile,
            [self._live_cookie()],
        )
        fallback_db = self._set_cookies(
            self.fallback_profile,
            [self._live_cookie(expires=14_000_000_000_000_100)],
        )
        primary_before = self._file_state(primary_db)
        fallback_before = self._file_state(fallback_db)
        config_before = self._file_state(self.config_path)

        self.assertEqual(self._backup("primary"), 0)

        snapshot = instagram_session.latest_backup("primary")
        self.assertIsNotNone(snapshot)
        snapshot = Path(snapshot)
        self.assertTrue(snapshot.name.startswith("primary-"))
        self.assertEqual(snapshot.suffix, ".sqlite3")
        snapshot_info = snapshot.stat()
        self.assertEqual(stat.S_IMODE(snapshot_info.st_mode), 0o400)
        self.assertEqual(snapshot_info.st_uid, os.getuid())
        self.assertNotEqual(snapshot_info.st_ino, primary_db.stat().st_ino)
        with contextlib.closing(sqlite3.connect(snapshot)) as connection:
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone(), ("ok",))
            rows = connection.execute("SELECT name FROM cookies").fetchall()
        self.assertEqual(rows, [("sessionid",)])
        self.assertEqual(list(self.backup_dir.glob("fallback-*.sqlite3")), [])
        self.assertEqual(self._file_state(primary_db), primary_before)
        self.assertEqual(self._file_state(fallback_db), fallback_before)
        self.assertEqual(self._file_state(self.config_path), config_before)
        self.assertEqual(config_before, self.config_before)

    def test_fallback_backup_stays_separate_from_primary(self) -> None:
        self._set_cookies(self.primary_profile, [self._live_cookie()])
        self._set_cookies(
            self.fallback_profile,
            [self._live_cookie(expires=14_000_000_000_000_100)],
        )

        self.assertEqual(self._backup("primary"), 0)
        self.assertEqual(self._backup("fallback"), 0)

        primary_snapshot = Path(instagram_session.latest_backup("primary"))
        fallback_snapshot = Path(instagram_session.latest_backup("fallback"))
        with contextlib.closing(sqlite3.connect(primary_snapshot)) as connection:
            primary_rows = connection.execute("SELECT expires_utc FROM cookies").fetchall()
        with contextlib.closing(sqlite3.connect(fallback_snapshot)) as connection:
            fallback_rows = connection.execute("SELECT expires_utc FROM cookies").fetchall()
        self.assertEqual(primary_rows, [(14_000_000_000_000_000,)])
        self.assertEqual(fallback_rows, [(14_000_000_000_000_100,)])
        for snapshot in (primary_snapshot, fallback_snapshot):
            info = snapshot.stat()
            self.assertEqual(stat.S_IMODE(info.st_mode), 0o400)
            self.assertEqual(info.st_uid, os.getuid())

    def test_fallback_restore_uses_its_snapshot_and_cleans_the_temporary_profile(self) -> None:
        primary_db = self._set_cookies(self.primary_profile, [self._live_cookie()])
        fallback_db = self._set_cookies(
            self.fallback_profile,
            [self._live_cookie(expires=14_000_000_000_000_100)],
        )
        primary_before = self._file_state(primary_db)
        fallback_before = self._file_state(fallback_db)
        self.assertEqual(self._backup("primary"), 0)
        self.assertEqual(self._backup("fallback"), 0)
        restored_profiles: list[Path] = []

        def mock_gallery_dl(_argv: list[str], profile: Path) -> int:
            profile = Path(profile)
            restored_profiles.append(profile)
            database = instagram_session.cookie_db(profile)
            with contextlib.closing(sqlite3.connect(database)) as connection:
                rows = connection.execute("SELECT expires_utc FROM cookies").fetchall()
            self.assertEqual(rows, [(14_000_000_000_000_100,)])
            self.assertEqual(stat.S_IMODE(database.stat().st_mode), 0o600)
            self.assertEqual(database.stat().st_uid, os.getuid())
            return 0

        with mock.patch.object(
            instagram_session,
            "decoded_cookies",
            side_effect=self._successful_decode,
        ), mock.patch.object(
            instagram_session,
            "run_gallery_dl",
            side_effect=mock_gallery_dl,
        ):
            code = self._run_cli(
                [
                    "restore",
                    "fallback",
                    "https://www.instagram.com/owner/",
                    "--directory",
                    str(self.root / "fallback-downloads"),
                    "--range",
                    "1",
                    "--filename",
                    "{category}_{id}.{extension}",
                    "--simulate",
                ]
            )

        self.assertEqual(code, 0)
        self.assertEqual(len(restored_profiles), 1)
        self.assertFalse(restored_profiles[0].exists())
        self.assertEqual(self._file_state(primary_db), primary_before)
        self.assertEqual(self._file_state(fallback_db), fallback_before)

    def test_check_backup_uses_synthetic_decode_and_does_not_touch_live_profiles(self) -> None:
        primary_db = self._set_cookies(self.primary_profile, [self._live_cookie()])
        source_before = self._file_state(primary_db)
        self.assertEqual(self._backup("primary"), 0)

        with mock.patch.object(
            instagram_session,
            "decoded_cookies",
            side_effect=self._successful_decode,
        ) as decode:
            self.assertEqual(self._run_cli(["check-backup", "primary"]), 0)

        decode.assert_called()
        self.assertEqual(self._file_state(primary_db), source_before)
        self.assertEqual(self._file_state(self.config_path), self.config_before)

    def test_restore_uses_temporary_profile_and_cleans_it_up(self) -> None:
        primary_db = self._set_cookies(self.primary_profile, [self._live_cookie()])
        primary_before = self._file_state(primary_db)
        self.assertEqual(self._backup("primary"), 0)
        backup = Path(instagram_session.latest_backup("primary"))
        backup_before = self._file_state(backup)
        destination = self.root / "downloads"
        observed_profiles: list[Path] = []
        runner_calls: list[tuple[list[str], object]] = []

        def mock_gallery_dl(argv: list[str], profile: object) -> int:
            profile_path = Path(profile)
            observed_profiles.append(profile_path)
            runner_calls.append((argv, profile))
            self.assertNotEqual(profile_path, self.primary_profile)
            self.assertNotEqual(profile_path, self.fallback_profile)
            self.assertTrue(profile_path.exists())
            temporary_db = instagram_session.cookie_db(profile_path)
            self.assertTrue(temporary_db.is_file())
            self.assertEqual(stat.S_IMODE(temporary_db.stat().st_mode), 0o600)
            return 0

        with mock.patch.object(
            instagram_session,
            "decoded_cookies",
            side_effect=self._successful_decode,
        ), mock.patch.object(
            instagram_session,
            "run_gallery_dl",
            side_effect=mock_gallery_dl,
        ):
            code = self._run_cli(
                [
                    "restore",
                    "primary",
                    "https://www.instagram.com/p/SYNTHETIC/",
                    "--directory",
                    str(destination),
                    "--range",
                    "1-2",
                    "--filename",
                    "{category}_{id}.{extension}",
                    "--simulate",
                ]
            )

        self.assertEqual(code, 0)
        self.assertTrue(runner_calls)
        self.assertTrue(all(not profile.exists() for profile in observed_profiles))
        self.assertEqual(self._file_state(primary_db), primary_before)
        self.assertEqual(self._file_state(backup), backup_before)
        self.assertEqual(self._file_state(self.config_path), self.config_before)
        self.assertFalse(destination.exists())

    def test_restore_accepts_story_collection_shared_link_and_avatar_routes(self) -> None:
        self._set_cookies(self.primary_profile, [self._live_cookie()])
        self.assertEqual(self._backup("primary"), 0)
        routes = (
            "https://www.instagram.com/stories/owner/",
            "https://www.instagram.com/s/aGlnaGxpZ2h0fixture?story_media_id=123",
            "https://www.instagram.com/owner/",
        )
        runner_calls: list[tuple[list[str], Path]] = []

        def mock_gallery_dl(argv: list[str], profile: Path) -> int:
            runner_calls.append((argv, Path(profile)))
            return 0

        with mock.patch.object(
            instagram_session,
            "decoded_cookies",
            side_effect=self._successful_decode,
        ), mock.patch.object(
            instagram_session,
            "run_gallery_dl",
            side_effect=mock_gallery_dl,
        ):
            for route in routes:
                with self.subTest(route=route):
                    self.assertEqual(
                        self._run_cli(
                            [
                                "restore",
                                "primary",
                                route,
                                "--directory",
                                str(self.root / "downloads"),
                                "--range",
                                "1",
                                "--filename",
                                "{category}_{id}.{extension}",
                                "--simulate",
                            ]
                        ),
                        0,
                    )

        self.assertEqual(len(runner_calls), len(routes))
        for (argv, profile), route in zip(runner_calls, routes, strict=True):
            self.assertIn(route, argv)
            self.assertFalse(profile.exists())

    def test_profile_path_override_must_match_current_role_config(self) -> None:
        self._set_cookies(self.primary_profile, [self._live_cookie()])
        with mock.patch.object(
            instagram_session,
            "decoded_cookies",
            side_effect=self._successful_decode,
        ):
            self.assertEqual(
                self._run_cli(
                    ["status", "primary", "--profile-path", str(self.primary_profile)]
                ),
                0,
            )
            self.assertEqual(
                self._run_cli(
                    ["status", "primary", "--profile-path", str(self.fallback_profile)]
                ),
                21,
            )

    def test_role_swap_rejects_old_snapshot_until_new_profile_is_backed_up(self) -> None:
        self._set_cookies(self.primary_profile, [self._live_cookie()])
        self._set_cookies(
            self.fallback_profile,
            [self._live_cookie(expires=14_000_000_000_000_100)],
        )
        self.assertEqual(self._backup("primary"), 0)
        old_snapshot = Path(instagram_session.latest_backup("primary"))
        old_provenance = instagram_session._snapshot_provenance(old_snapshot)
        self.assertEqual(
            old_provenance,
            instagram_session.profile_provenance(self.primary_profile),
        )

        self._write_config(
            primary_profile=self.fallback_profile,
            fallback_profile=self.primary_profile,
        )
        unavailable = io.StringIO()
        runner = mock.Mock(return_value=0)
        restore_args = [
            "restore",
            "primary",
            "https://www.instagram.com/p/SYNTHETIC/",
            "--directory",
            str(self.root / "downloads"),
            "--range",
            "1",
            "--filename",
            "{category}_{id}.{extension}",
            "--simulate",
            "--profile-path",
            str(self.fallback_profile),
        ]
        with mock.patch.object(
            instagram_session,
            "decoded_cookies",
            side_effect=self._successful_decode,
        ), mock.patch.object(instagram_session, "run_gallery_dl", runner), contextlib.redirect_stderr(unavailable), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(instagram_session.main(restore_args), 21)
        self.assertEqual(
            unavailable.getvalue().strip(),
            "Instagram session recovery: backup unavailable",
        )
        runner.assert_not_called()

        self.assertEqual(self._backup("primary"), 0)
        new_snapshot = Path(instagram_session.latest_backup("primary"))
        self.assertNotEqual(new_snapshot, old_snapshot)
        self.assertEqual(
            instagram_session._snapshot_provenance(new_snapshot),
            instagram_session.profile_provenance(self.fallback_profile),
        )
        with mock.patch.object(
            instagram_session,
            "decoded_cookies",
            side_effect=self._successful_decode,
        ), mock.patch.object(instagram_session, "run_gallery_dl", runner):
            self.assertEqual(self._run_cli(restore_args), 0)
        runner.assert_called_once()

        # Switching back should find the older matching snapshot, even if the
        # original live profile directory is no longer present.
        self._write_config(
            primary_profile=self.primary_profile,
            fallback_profile=self.fallback_profile,
        )
        self.primary_profile.rename(self.root / "moved-primary-profile")

        def restored_from_old_snapshot(_argv, profile):
            copied = instagram_session.cookie_db(Path(profile))
            self.assertEqual(instagram_session._snapshot_provenance(copied), old_provenance)
            return 0

        with mock.patch.object(
            instagram_session, "decoded_cookies", side_effect=self._successful_decode,
        ), mock.patch.object(
            instagram_session, "run_gallery_dl", side_effect=restored_from_old_snapshot,
        ) as restored:
            self.assertEqual(
                self._run_cli([*restore_args[:-1], str(self.primary_profile.resolve())]), 0
            )
        restored.assert_called_once()

    def test_retargeted_profile_link_does_not_match_original_selection(self) -> None:
        link = self.root / "configured-profile"
        link.symlink_to(self.primary_profile, target_is_directory=True)
        self._write_config(primary_profile=link, fallback_profile=self.fallback_profile)
        selected = link.resolve()
        link.unlink()
        link.symlink_to(self.fallback_profile, target_is_directory=True)

        with mock.patch.object(instagram_session, "inspect_session") as inspect_session:
            self.assertEqual(
                self._run_cli(["status", "primary", "--profile-path", str(selected)]), 21
            )
        inspect_session.assert_not_called()

    def test_automatic_restore_rejects_legacy_unbound_backup_but_manual_allows_it(self) -> None:
        self._set_cookies(self.primary_profile, [self._live_cookie()])
        self.assertEqual(self._backup("primary"), 0)
        snapshot = Path(instagram_session.latest_backup("primary"))
        snapshot.chmod(0o600)
        with contextlib.closing(sqlite3.connect(snapshot)) as connection:
            connection.execute(
                "DELETE FROM meta WHERE key = ?",
                (instagram_session.PROFILE_PROVENANCE_KEY,),
            )
            connection.commit()
        snapshot.chmod(0o400)

        base_args = [
            "restore",
            "primary",
            "https://www.instagram.com/p/SYNTHETIC/",
            "--directory",
            str(self.root / "downloads"),
            "--range",
            "1",
            "--filename",
            "{category}_{id}.{extension}",
            "--simulate",
        ]
        runner = mock.Mock(return_value=0)
        unavailable = io.StringIO()
        with mock.patch.object(
            instagram_session,
            "decoded_cookies",
            side_effect=self._successful_decode,
        ), mock.patch.object(instagram_session, "run_gallery_dl", runner), contextlib.redirect_stderr(unavailable):
            self.assertEqual(
                instagram_session.main(
                    [*base_args, "--profile-path", str(self.primary_profile)]
                ),
                21,
            )
            runner.assert_not_called()
            self.assertEqual(self._run_cli(base_args), 0)
        runner.assert_called_once()
        self.assertEqual(
            unavailable.getvalue().strip(),
            "Instagram session recovery: backup unavailable",
        )


if __name__ == "__main__":
    unittest.main()
