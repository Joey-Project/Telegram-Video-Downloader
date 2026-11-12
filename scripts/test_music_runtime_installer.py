import contextlib
import io
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import install_music_runtime


class MusicRuntimeInstallerTests(unittest.TestCase):
    def test_default_mode_only_prints_a_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            output = io.StringIO()
            with mock.patch.object(install_music_runtime, "actual_home", return_value=home):
                with contextlib.redirect_stdout(output):
                    self.assertEqual(install_music_runtime.main([]), 0)
            self.assertIn("mode=plan profile=production", output.getvalue())
            self.assertIn("Telethon==1.45.0", output.getvalue())
            self.assertIn("pyaes==1.6.1", output.getvalue())
            self.assertFalse((home / ".local").exists())

    def test_plan_paths_are_profile_specific(self):
        home = Path("/Users/example")
        test = install_music_runtime.plan(home, "test", Path("/python"))
        production = install_music_runtime.plan(home, "production", Path("/python"))
        self.assertEqual(
            test["runtime"],
            home / ".local/lib/telegram-video-downloader-test/music-runtime",
        )
        self.assertEqual(
            production["helper"],
            home / ".local/lib/telegram-video-downloader/libexec/music_transfer.py",
        )
        self.assertEqual(
            production["dependencies"],
            "Telethon==1.45.0, pyaes==1.6.1, rsa==4.9.1, pyasn1==0.6.5",
        )
        self.assertNotEqual(test["runtime"], production["runtime"])

    def test_apply_modes_are_mutually_exclusive(self):
        error_output = io.StringIO()
        with contextlib.redirect_stderr(error_output):
            with self.assertRaises(SystemExit):
                install_music_runtime._arguments(["--apply-test", "--apply-production"])

    def test_helper_copy_is_private_and_replaces_only_regular_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.py"
            destination = root / "libexec/music_transfer.py"
            destination.parent.mkdir(mode=0o700)
            source.write_text("pass\n", encoding="utf-8")
            install_music_runtime._copy_helper(source, destination)
            info = destination.lstat()
            self.assertTrue(stat.S_ISREG(info.st_mode))
            self.assertEqual(stat.S_IMODE(info.st_mode), 0o600)
            self.assertEqual(destination.read_text(encoding="utf-8"), "pass\n")

    def test_helper_copy_rejects_symlink_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.py"
            destination = root / "libexec/music_transfer.py"
            destination.parent.mkdir(mode=0o700)
            source.write_text("pass\n", encoding="utf-8")
            target = root / "outside.py"
            target.write_text("keep\n", encoding="utf-8")
            destination.symlink_to(target)
            with self.assertRaises(install_music_runtime.InstallError):
                install_music_runtime._copy_helper(source, destination)
            self.assertEqual(target.read_text(encoding="utf-8"), "keep\n")


if __name__ == "__main__":
    unittest.main()
