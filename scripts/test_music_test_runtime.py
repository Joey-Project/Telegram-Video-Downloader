from __future__ import annotations

import importlib.util
import contextlib
import io
import json
import plistlib
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SPEC = importlib.util.spec_from_file_location(
    "music_test_runtime", Path(__file__).with_name("music_test_runtime.py")
)
runtime = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runtime)


class MusicTestRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="music-test-runtime-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.home = self.root / "home"
        self.home.mkdir()
        self.binary = self.root / "cargo/release/telegram-video-downloader"
        self.binary.parent.mkdir(parents=True)
        self.binary.write_bytes(b"synthetic executable")
        self.binary.chmod(0o700)
        self.patches = [
            mock.patch.object(Path, "home", return_value=self.home),
            mock.patch.object(runtime, "_real_home", return_value=self.home),
            mock.patch.object(runtime, "_embedded_info_plist", return_value={
                "CFBundleIdentifier": runtime.TEST_IDENTIFIER,
                "CFBundleExecutable": runtime.TEST_BINARY.name,
                "NSAppleEventsUsageDescription": "Authorize Music access for a test.",
            }),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_plan_validates_embedded_test_identity_without_external_mutation(self):
        with mock.patch.object(runtime, "_run", side_effect=AssertionError("external command")):
            result = runtime.plan(self.binary)
        self.assertTrue(result["valid"])
        self.assertFalse(result["installed"])
        self.assertEqual(result["identifier"], "io.github.telegram-local-downloader.bot.test")

    def test_otool_fixture_decodes_words_in_mach_o_byte_order(self):
        # Captured from the public /usr/bin/osascript binary; no private plist values.
        fixture = (
            "/usr/bin/osascript:\n"
            "Contents of (__TEXT,__info_plist) section\n"
            "00000001000047e3\t6d783f3c 6576206c 6f697372 31223d6e \n"
        )
        decoded = runtime._otool_dump_bytes(fixture)
        self.assertEqual(decoded, b'<?xml version="1')

    def test_process_detection_uses_anchored_fixed_path_only(self):
        with mock.patch.object(runtime, "_run", return_value=subprocess.CompletedProcess(
            ["pgrep"], 1, "", "",
        )) as run:
            self.assertFalse(runtime._active_test_process())
        argv = run.call_args.args[0]
        self.assertEqual(argv[:2], ["/usr/bin/pgrep", "-f"])
        self.assertEqual(
            argv[2],
            "^" + runtime.re.escape(str(self.home / runtime.TEST_BINARY)) + r"([[:space:]]|$)",
        )

    def test_process_detection_flags_full_fixed_path_command(self):
        with mock.patch.object(runtime, "_run", return_value=subprocess.CompletedProcess(
            ["pgrep"], 0, "4242\n", "",
        )):
            self.assertTrue(runtime._active_test_process())

    def test_command_output_is_capped_while_the_producer_runs(self):
        with mock.patch.object(runtime, "OUTPUT_LIMIT", 32):
            with self.assertRaisesRegex(runtime.RuntimeErrorSafe, "output exceeded"):
                runtime._run([sys.executable, "-c", "print('x' * 1000000)"])

    def test_validation_rejects_wrong_bundle_id(self):
        with mock.patch.object(runtime, "_embedded_info_plist", return_value={
            "CFBundleIdentifier": runtime.TEST_IDENTIFIER + ".wrong",
            "CFBundleExecutable": runtime.TEST_BINARY.name,
            "NSAppleEventsUsageDescription": "reason",
        }):
            with self.assertRaisesRegex(runtime.RuntimeErrorSafe, "wrong test bundle"):
                runtime._validate_binary(self.binary)

    def test_install_signs_and_installs_only_fixed_test_path(self):
        entitlements = self.root / "entitlements.plist"
        entitlements.write_bytes(plistlib.dumps({"com.apple.security.automation.apple-events": True}))
        calls = []

        def fake_run(argv, **kwargs):
            calls.append([str(value) for value in argv])
            if "find-identity" in argv:
                return subprocess.CompletedProcess(argv, 0, stdout=runtime.TRUSTED_FINGERPRINT, stderr="")
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

        with (
            mock.patch.object(runtime, "ENTITLEMENTS", entitlements),
            mock.patch.object(runtime, "_run", side_effect=fake_run),
            mock.patch.object(runtime, "_require_test_requirement"),
            mock.patch.object(runtime, "_active_test_process", return_value=False),
        ):
            result = runtime.install(self.binary)
        installed = self.home / runtime.TEST_BINARY
        self.assertEqual(Path(result["binary"]), installed)
        self.assertEqual(installed.read_bytes(), b"synthetic executable")
        self.assertEqual(installed.stat().st_mode & 0o777, 0o755)
        self.assertTrue(any("--sign" in call for call in calls))
        self.assertNotEqual(installed, self.home / runtime.PRODUCTION_BINARY)

    def test_active_or_unknown_job_is_reported_pending_without_restart(self):
        state, stdout_path, _ = runtime._private_paths()
        stdout_path.write_text('{"old": true}\n')
        stdout_path.chmod(0o600)
        with (
            mock.patch.object(runtime, "_require_test_requirement"),
            mock.patch.object(runtime, "_launchd_status", return_value={
                "loaded": True, "pid": None, "last_exit_code": None,
            }),
            mock.patch.object(runtime, "_run", side_effect=AssertionError("must not restart")),
        ):
            result = runtime.launch_action("authorize")
        self.assertEqual(result["state"], "pending")
        self.assertEqual(stdout_path.read_text(), '{"old": true}\n')

    def test_completed_job_is_booted_out_and_new_receipt_is_cleared(self):
        _, stdout_path, plist_path = runtime._private_paths()
        stdout_path.write_text('{"previous": true}\n')
        stdout_path.chmod(0o600)
        calls = []

        def fake_run(argv, **kwargs):
            calls.append([str(value) for value in argv])
            if argv[1:2] == ["bootstrap"]:
                return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

        with (
            mock.patch.object(runtime, "_require_test_requirement"),
            mock.patch.object(runtime, "_launchd_status", return_value={
                "loaded": True, "pid": None, "last_exit_code": 0,
            }),
            mock.patch.object(runtime, "_run", side_effect=fake_run),
        ):
            result = runtime.launch_action("check")
        self.assertEqual(result["state"], "launched")
        self.assertEqual(stdout_path.read_bytes(), b"")
        self.assertEqual(stdout_path.stat().st_mode & 0o777, 0o600)
        agent = plistlib.loads(plist_path.read_bytes())
        self.assertEqual(agent["ProgramArguments"], [
            str(self.home / runtime.TEST_BINARY), "--music-check",
        ])
        self.assertTrue(agent["RunAtLoad"])
        self.assertFalse(agent["KeepAlive"])
        self.assertTrue(any(call[1] == "bootout" for call in calls))

    def test_status_parses_only_bounded_json_receipt(self):
        _, stdout_path, _ = runtime._private_paths()
        stdout_path.write_text(json.dumps({"ok": True}) + "\n")
        stdout_path.chmod(0o600)
        with mock.patch.object(runtime, "_launchd_status", return_value={
            "loaded": False, "pid": None, "last_exit_code": None,
        }):
            result = runtime.status()
        self.assertEqual(result["record"], {"ok": True})

    def test_status_exposes_bounded_native_error_for_completed_run(self):
        _, stdout_path, _ = runtime._private_paths()
        stdout_path.write_bytes(
            b"x" * 2500 + b"\nError: AppleEvents permission check exceeded 7 seconds\n"
        )
        stdout_path.chmod(0o600)
        with mock.patch.object(runtime, "_launchd_status", return_value={
            "loaded": True, "pid": None, "last_exit_code": 1,
        }):
            result = runtime.status()
        self.assertIsNone(result["record"])
        self.assertLessEqual(len(result["diagnostic"]), 2048)
        self.assertTrue(
            result["diagnostic"].endswith(
                "Error: AppleEvents permission check exceeded 7 seconds"
            )
        )

    def test_status_omits_non_json_diagnostic_while_job_is_running(self):
        _, stdout_path, _ = runtime._private_paths()
        stdout_path.write_text("authorization is still pending\n")
        stdout_path.chmod(0o600)
        with mock.patch.object(runtime, "_launchd_status", return_value={
            "loaded": True, "pid": 4321, "last_exit_code": None,
        }):
            result = runtime.status()
        self.assertIsNone(result["record"])
        self.assertNotIn("diagnostic", result)

    def create_request(self, payload=None, *, filename="17.json"):
        import_dir = self.home / ".local/lib/telegram-video-downloader-test/music-import"
        requests_dir = import_dir / "requests"
        requests_dir.mkdir(mode=0o700, parents=True)
        import_dir.chmod(0o700)
        requests_dir.chmod(0o700)
        request = payload or {
            "schema": 1,
            "source_path": str(self.root / "Music/track.mp3"),
            "expected_sha256": "a" * 64,
            "expected_size": 12345,
        }
        path = requests_dir / filename
        path.write_text(json.dumps(request))
        path.chmod(0o600)
        return path

    def test_import_action_maps_valid_request_to_fixed_test_binary(self):
        request = self.create_request()
        _, _, plist_path = runtime._private_paths()
        with (
            mock.patch.object(runtime, "_require_test_requirement"),
            mock.patch.object(runtime, "_launchd_status", return_value={
                "loaded": False, "pid": None, "last_exit_code": None,
            }),
            mock.patch.object(runtime, "_active_test_process", return_value=False),
            mock.patch.object(runtime, "_run", return_value=subprocess.CompletedProcess(
                ["launchctl"], 0, "", "",
            )),
        ):
            result = runtime.launch_action("import", request)
        agent = plistlib.loads(plist_path.read_bytes())
        self.assertEqual(result["state"], "launched")
        self.assertEqual(agent["ProgramArguments"], [
            str(self.home / runtime.TEST_BINARY), "--music-import", str(request),
        ])
        self.assertEqual(agent["Label"], runtime.TEST_LABEL)

    def test_invalid_import_manifest_is_rejected_before_launch(self):
        request = self.create_request({
            "schema": 2,
            "source_path": str(self.root / "Music/track.mp3"),
            "expected_sha256": "a" * 64,
            "expected_size": 12345,
        })
        with (
            mock.patch.object(runtime, "_require_test_requirement") as requirement,
            mock.patch.object(runtime, "_run", side_effect=AssertionError("must not launch")),
        ):
            with self.assertRaisesRegex(runtime.RuntimeErrorSafe, "schema must be 1"):
                runtime.launch_action("import", request)
        requirement.assert_not_called()

    def test_active_job_keeps_existing_single_flight_job(self):
        request = self.create_request()
        _, stdout_path, plist_path = runtime._private_paths()
        stdout_path.write_text("existing job log\n")
        stdout_path.chmod(0o600)
        plist_path.write_text("existing job plist\n")
        plist_path.chmod(0o600)
        with (
            mock.patch.object(runtime, "_require_test_requirement"),
            mock.patch.object(runtime, "_launchd_status", return_value={
                "loaded": True, "pid": 1234, "last_exit_code": None,
            }),
            mock.patch.object(runtime, "_run", side_effect=AssertionError("must not relaunch")),
        ):
            result = runtime.launch_action("import", request)
        self.assertEqual(result["state"], "pending")
        self.assertEqual(stdout_path.read_text(), "existing job log\n")
        self.assertEqual(plist_path.read_text(), "existing job plist\n")

    def test_request_option_is_limited_to_import_actions(self):
        parsed = runtime.parser().parse_args(["import-status", "--request", "/tmp/1.json"])
        self.assertEqual(parsed.action, "import-status")
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(runtime.main(["status", "--request", "/tmp/1.json"]), 1)
            self.assertEqual(runtime.main(["import-status"]), 1)

    def test_quit_maps_to_fixed_test_binary_and_rejects_request(self):
        _, _, plist_path = runtime._private_paths()
        with (
            mock.patch.object(runtime, "_require_test_requirement"),
            mock.patch.object(runtime, "_launchd_status", return_value={
                "loaded": False, "pid": None, "last_exit_code": None,
            }),
            mock.patch.object(runtime, "_active_test_process", return_value=False),
            mock.patch.object(runtime, "_run", return_value=subprocess.CompletedProcess(
                ["launchctl"], 0, "", "",
            )),
        ):
            result = runtime.launch_action("quit")
        agent = plistlib.loads(plist_path.read_bytes())
        self.assertEqual(result["state"], "launched")
        self.assertEqual(agent["Label"], runtime.TEST_LABEL)
        self.assertEqual(agent["ProgramArguments"], [
            str(self.home / runtime.TEST_BINARY), "--music-quit",
        ])
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(runtime.main(["quit", "--request", "/tmp/1.json"]), 1)


if __name__ == "__main__":
    unittest.main()
