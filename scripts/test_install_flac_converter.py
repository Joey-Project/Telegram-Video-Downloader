from __future__ import annotations

import io
import os
import stat
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import install_flac_converter as installer


IDENTITY = "A" * 40
SYSTEM_DEPENDENCIES = (
    "/usr/lib/libSystem.B.dylib (compatibility version 1.0.0, current version 1351.0.0)",
    "/System/Library/Frameworks/AudioToolbox.framework/Versions/A/AudioToolbox "
    "(compatibility version 1.0.0, current version 1.0.0)",
)


class FlacConverterInstallerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="flac-converter-installer-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.home = self.root / "home"
        self.home.mkdir()
        self.source = self.root / "staging/flac-alac-converter"
        self.source.parent.mkdir()
        self.source.write_bytes(b"synthetic Mach-O converter")
        self.source.chmod(0o700)

    def test_plan_uses_fixed_profile_paths_and_is_read_only(self):
        with mock.patch.object(
            installer, "_validate_system_dependencies", return_value=["/usr/lib/libSystem.B.dylib"]
        ):
            test = installer.plan(self.home, "test", self.source, IDENTITY)
            production = installer.plan(self.home, "production", self.source, IDENTITY)
        self.assertEqual(
            Path(test["destination"]),
            self.home / ".local/lib/telegram-video-downloader-test/libexec/flac-alac-converter",
        )
        self.assertEqual(
            Path(production["destination"]),
            self.home / ".local/lib/telegram-video-downloader/libexec/flac-alac-converter",
        )
        self.assertEqual(test["identifier"], installer.CONVERTER_IDENTIFIER)
        self.assertFalse((self.home / ".local").exists())

    def test_otool_parser_accepts_system_paths_only(self):
        output = "/tmp/helper:\n" + "\n".join(f"\t{item}" for item in SYSTEM_DEPENDENCIES)
        self.assertEqual(installer._dependency_paths(output), [
            "/usr/lib/libSystem.B.dylib",
            "/System/Library/Frameworks/AudioToolbox.framework/Versions/A/AudioToolbox",
        ])

    def test_dependency_check_rejects_homebrew_and_rpath_dependencies(self):
        for dependency in (
            "/opt/homebrew/opt/flac/lib/libFLAC.14.dylib",
            "@rpath/libFLAC.14.dylib",
        ):
            with self.subTest(dependency=dependency):
                output = (
                    "/tmp/helper:\n"
                    f"\t{dependency} (compatibility version 1.0.0, current version 1.0.0)\n"
                )
                with mock.patch.object(
                    installer, "_run", return_value=mock.Mock(stdout=output)
                ), self.assertRaisesRegex(installer.InstallError, "non-system"):
                    installer._validate_system_dependencies(self.source)

    def test_source_must_be_user_owned_regular_executable(self):
        with self.assertRaisesRegex(installer.InstallError, "regular executable"):
            installer._source_info(self.root)
        self.source.chmod(0o600)
        with self.assertRaisesRegex(installer.InstallError, "regular executable"):
            installer._source_info(self.source)

    def test_explicit_install_signs_copies_and_installs_not_the_source(self):
        root = installer.profile_root(self.home, "test")
        root.mkdir(mode=0o700, parents=True)
        calls = []

        def fake_run(argv, **kwargs):
            calls.append([str(value) for value in argv])
            return mock.Mock(stdout="", stderr="", returncode=0)

        with (
            mock.patch.object(installer, "_validate_system_dependencies", return_value=["/usr/lib/libSystem.B.dylib"]),
            mock.patch.object(installer, "_run", side_effect=fake_run),
            mock.patch.object(installer, "_verify_signed_helper"),
        ):
            result = installer.apply_install(self.home, "test", self.source, IDENTITY)

        destination = Path(result["destination"])
        self.assertEqual(destination.read_bytes(), b"synthetic Mach-O converter")
        self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o755)
        self.assertEqual(self.source.read_bytes(), b"synthetic Mach-O converter")
        self.assertTrue(any(
            call[:2] == ["/usr/bin/codesign", "--force"]
            and installer.CONVERTER_IDENTIFIER in call
            and IDENTITY in call
            for call in calls
        ))
        for name in ("FLAC-LICENSE.txt", "Ogg-LICENSE.txt"):
            installed = destination.parent / name
            tracked = REPO_ROOT / "docs/licenses" / name
            self.assertEqual(installed.read_bytes(), tracked.read_bytes())
            self.assertEqual(stat.S_IMODE(installed.stat().st_mode), 0o644)

    def test_pre_signed_install_verifies_without_signing_again(self):
        root = installer.profile_root(self.home, "production")
        root.mkdir(mode=0o700, parents=True)
        calls = []

        def fake_run(argv, **kwargs):
            calls.append([str(value) for value in argv])
            return mock.Mock(stdout="", stderr="", returncode=0)

        with (
            mock.patch.object(
                installer,
                "_validate_system_dependencies",
                return_value=["/usr/lib/libSystem.B.dylib"],
            ),
            mock.patch.object(installer, "_run", side_effect=fake_run),
            mock.patch.object(installer, "_verify_signed_helper"),
        ):
            result = installer.apply_install(
                self.home, "production", self.source, IDENTITY, sign=False
            )

        self.assertEqual(result["mode"], "installed")
        self.assertFalse(any("--sign" in call for call in calls))

    def test_install_rejects_symlink_target_without_touching_its_referent(self):
        root = installer.profile_root(self.home, "production")
        libexec = root / "libexec"
        libexec.mkdir(mode=0o755, parents=True)
        target = libexec / "outside"
        target.write_bytes(b"keep")
        destination = libexec / "flac-alac-converter"
        destination.symlink_to(target)
        with (
            mock.patch.object(installer, "_validate_system_dependencies", return_value=["/usr/lib/libSystem.B.dylib"]),
            mock.patch.object(installer, "_run", return_value=mock.Mock(stdout="", stderr="", returncode=0)),
            mock.patch.object(installer, "_verify_signed_helper"),
            self.assertRaisesRegex(installer.InstallError, "user-owned regular file"),
        ):
            installer.apply_install(self.home, "production", self.source, IDENTITY)
        self.assertEqual(target.read_bytes(), b"keep")

    def test_default_cli_is_a_read_only_plan(self):
        output = io.StringIO()
        with (
            mock.patch.object(installer, "actual_home", return_value=self.home),
            mock.patch.object(
                installer, "_validate_system_dependencies", return_value=["/usr/lib/libSystem.B.dylib"]
            ),
            redirect_stdout(output),
        ):
            self.assertEqual(
                installer.main([
                    "--profile", "production", "--source", str(self.source), "--identity", IDENTITY,
                ]),
                0,
            )
        self.assertIn("mode=plan", output.getvalue())
        self.assertFalse((self.home / ".local").exists())


if __name__ == "__main__":
    unittest.main()
