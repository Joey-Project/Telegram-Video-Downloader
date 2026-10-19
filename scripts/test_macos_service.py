#!/usr/bin/env python3
"""Test binary-only service installation without mutating any macOS service."""

from __future__ import annotations

import importlib.util
import json
import os
import plistlib
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SPEC = importlib.util.spec_from_file_location(
    "macos_service", Path(__file__).with_name("macos_service.py")
)
service = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(service)

FINGERPRINT = "A" * 40
REQUIREMENT = f'identifier "{service.IDENTIFIER}" and certificate leaf = H"{FINGERPRINT}"'
PRIVATE_MARKER = "private-configuration-must-not-be-printed"
# Synthetic-token catalog joey-private-v3: access-a, active access role.
SYNTHETIC_ACCESS_TOKEN = "codex_synth_v1_access_a"


class InstallationTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="telegram-service-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.home = self.root / "home"
        self.repo = self.root / "repo"
        self.home.mkdir()
        self.repo.mkdir()
        self.old_binary = self.repo / "target/release" / service.EXECUTABLE
        self.old_binary.parent.mkdir(parents=True)
        self.old_binary.write_bytes(b"old signed executable")
        self.old_binary.chmod(0o700)
        self.artifact = self.root / "release-artifact"
        self.artifact.write_bytes(b"new unsigned executable")
        self.artifact.chmod(0o700)
        self.config = self.repo / "config.toml"
        self.config.write_text(
            f"# {PRIVATE_MARKER}\n"
            f'[telegram]\ntoken = "{SYNTHETIC_ACCESS_TOKEN}"\n'
            '[downloads]\nvideo_dir = "media/videos"\npdf_dir = "media/pdfs"\n'
            '[bilibili.auth]\nstate_path = "auth/state.json"\n'
            'credential_file = "auth/credentials.json"\n'
        )
        self.config.chmod(0o600)
        self.label = service.IDENTIFIER
        self.gui = f"gui/123/{self.label}"
        self.legacy = f"user/123/{self.label}"
        self.plist = self.home / "Library/LaunchAgents" / f"{self.label}.plist"
        self.plist.parent.mkdir(parents=True)
        self.original_plist = {
            "Label": self.label,
            "ProgramArguments": [str(self.old_binary), str(self.config)],
            "WorkingDirectory": str(self.repo),
            "StandardOutPath": str(self.root / "existing-logs/stdout.log"),
            "StandardErrorPath": str(self.root / "existing-logs/stderr.log"),
            "EnvironmentVariables": {"PATH": "/fixture/bin:/usr/bin", "RUST_LOG": "info"},
            "RunAtLoad": True,
            "KeepAlive": {"SuccessfulExit": False},
            "ProcessType": "Background",
            "CustomRetainedProperty": ["preserve", {"nested": True}],
        }
        self.plist.write_bytes(plistlib.dumps(self.original_plist))
        self.plist.chmod(0o600)
        self.state_files = []
        for relative in [
            "auth/state.json",
            "auth/credentials.json",
            "media/videos/.telegram-video-downloader-queue/index.json",
            "media/videos/.telegram-video-downloader-queue/owner.lock",
            "media/videos/.telegram-video-downloader-task-completed.json",
            "media/videos/.telegram-video-downloader-control/owner.json",
        ]:
            path = self.repo / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("unchanged private runtime fixture")
            path.chmod(0o600)
            self.state_files.append(path)
        auth_lock = Path(
            str(self.repo / "auth/credentials.json") + ".telegram-video-downloader.auth.lock"
        )
        auth_lock.write_bytes(b"unchanged lock object")
        auth_lock.chmod(0o600)
        anchor = Path(str(auth_lock) + ".anchor")
        os.link(auth_lock, anchor)
        self.state_files.extend([auth_lock, anchor])
        self.before_state = self.state_snapshot()
        self.addCleanup(mock.patch.stopall)
        mock.patch.object(Path, "home", return_value=self.home).start()
        mock.patch.dict(os.environ, {"HOME": str(self.home)}).start()
        mock.patch.object(service, "service_info", return_value=None).start()
        mock.patch.object(service, "bot_processes", return_value=[]).start()
        self.external = mock.patch.object(
            service, "run", side_effect=AssertionError("unexpected external command")
        ).start()

    def deployment(self, action="plan", *options):
        return service.Deployment(
            service.parser().parse_args(
                [
                    action,
                    "--repo",
                    str(self.repo),
                    "--domain",
                    "gui/123",
                    "--legacy-domain",
                    "user/123",
                    *options,
                ]
            )
        )

    def state_snapshot(self):
        return {
            str(path): (
                path.read_bytes(),
                path.stat().st_ino,
                path.stat().st_uid,
                path.stat().st_gid,
                stat.S_IMODE(path.stat().st_mode),
            )
            for path in [self.config, *self.state_files]
        }

    def audit(self, deployment):
        with mock.patch.object(service, "signature", return_value=(REQUIREMENT, FINGERPRINT)):
            return deployment.audit()

    def test_audit_keeps_configuration_and_runtime_files_at_original_paths(self):
        deployment = self.deployment()
        plan = self.audit(deployment)
        self.assertEqual(Path(plan["config"]), self.config)
        self.assertEqual(
            Path(plan["binary"]),
            self.home / ".local/lib/telegram-video-downloader/bin" / service.EXECUTABLE,
        )
        self.assertNotIn(PRIVATE_MARKER, json.dumps(plan))
        self.assertEqual(self.state_snapshot(), self.before_state)
        self.assertEqual(self.old_binary.read_bytes(), b"old signed executable")
        self.external.assert_not_called()

    def test_plist_changes_only_program_arguments_binary(self):
        deployment = self.deployment()
        self.audit(deployment)
        document = plistlib.loads(deployment.new_plist())
        expected = dict(self.original_plist)
        expected["ProgramArguments"] = [str(deployment.binary), str(self.config)]
        self.assertEqual(document, expected)
        self.assertEqual(self.state_snapshot(), self.before_state)

    def snapshot_runner(self, *, on_copy=None, forbidden=()):
        def copied(argv, **_):
            self.assertEqual(argv[0], "ditto")
            source, destination = map(Path, argv[-2:])
            for payload in forbidden:
                self.assertNotEqual(source, payload, "media payload must not be copied")
                self.assertNotIn(source, payload.parents, "media payload parent must not be copied")
            destination.parent.mkdir(parents=True, exist_ok=True)
            if source.is_dir():
                shutil.copytree(source, destination)
            else:
                shutil.copy2(source, destination)
            if on_copy:
                on_copy(source)
            return subprocess.CompletedProcess(argv, 0, "", "")

        return mock.patch.object(service, "run", side_effect=copied)

    def aliased_media_fixture(self):
        configured = self.repo / "media/videos"
        physical = self.root / "physical-videos"
        configured.rename(physical)
        configured.symlink_to(physical, target_is_directory=True)
        return configured, physical

    def test_media_alias_is_preserved_while_snapshot_uses_its_physical_directory(self):
        configured, physical = self.aliased_media_fixture()
        original_config = self.config.read_bytes()
        deployment = self.deployment()
        plan = self.audit(deployment)
        self.assertEqual(plan["media"]["video"], str(configured))
        self.assertEqual(deployment.media["video"], configured)
        self.assertEqual(plan["working_directory"], str(self.repo))
        backup = self.root / "aliased-snapshot"
        backup.mkdir()
        with self.snapshot_runner():
            deployment.snapshot_state(backup)
        document = json.loads((backup / "state-manifest.json").read_text())
        sources = {Path(entry["source"]) for entry in document["entries"]}
        self.assertTrue(any(source.is_relative_to(physical) for source in sources))
        self.assertFalse(any(source.is_relative_to(configured) for source in sources))
        self.assertEqual(document["media_roots"]["video"]["configured"], str(configured))
        self.assertEqual(document["media_roots"]["video"]["physical"], str(physical))
        self.assertEqual(self.config.read_bytes(), original_config)
        self.assertEqual(self.state_snapshot(), self.before_state)
        expected = dict(self.original_plist)
        expected["ProgramArguments"] = [str(deployment.binary), str(self.config)]
        self.assertEqual(plistlib.loads(deployment.new_plist()), expected)

    def test_snapshot_rejects_media_alias_retargeted_during_copy(self):
        configured, physical = self.aliased_media_fixture()
        replacement = self.root / "different-media-root"
        replacement.mkdir()
        deployment = self.deployment()
        self.audit(deployment)
        backup = self.root / "retargeted-snapshot"
        backup.mkdir()
        retargeted = False

        def retarget(_):
            nonlocal retargeted
            if not retargeted:
                configured.unlink()
                configured.symlink_to(replacement, target_is_directory=True)
                retargeted = True

        with self.snapshot_runner(on_copy=retarget), self.assertRaises(service.InstallError):
            deployment.snapshot_state(backup)
        self.assertTrue(retargeted)
        self.assertTrue((physical / ".telegram-video-downloader-queue/index.json").is_file())
        self.assertEqual(list(replacement.iterdir()), [])

    def test_media_binding_allows_child_churn_and_rejects_root_replacement(self):
        _, physical = self.aliased_media_fixture()
        deployment = self.deployment()
        self.audit(deployment)
        (physical / "new-child-entry").write_bytes(b"benign directory content change")
        deployment.revalidate_media_bindings()
        previous = self.root / "previous-physical-videos"
        physical.rename(previous)
        physical.mkdir()
        with self.assertRaises(service.InstallError):
            deployment.revalidate_media_bindings()

    def test_sparse_media_anchors_are_metadata_only_and_do_not_consume_snapshot_budget(self):
        media = self.repo / "media/videos"
        payload = media / "large-video.mp4"
        with payload.open("wb") as stream:
            stream.truncate(service.SNAPSHOT_BYTES + 4096)
        control_anchor = media / ".telegram-video-downloader-control/output.anchor"
        overwrite = media / ".telegram-video-downloader-overwrite-fixture"
        overwrite.mkdir()
        overwrite_anchor = overwrite / ".committed-output-0"
        os.link(payload, control_anchor)
        os.link(payload, overwrite_anchor)
        (overwrite / ".transaction.json").write_text('{"version":4}\n')
        protected = {payload, control_anchor, overwrite_anchor}
        original_open = Path.open

        def metadata_only(path, *args, **kwargs):
            self.assertNotIn(Path(path), protected, "media anchor must not be opened or hashed")
            return original_open(path, *args, **kwargs)

        deployment = self.deployment()
        self.audit(deployment)
        backup = self.root / "anchor-snapshot"
        backup.mkdir()
        with (
            self.snapshot_runner(forbidden=protected),
            mock.patch.object(Path, "open", metadata_only),
        ):
            deployment.snapshot_state(backup)
        document = json.loads((backup / "state-manifest.json").read_text())
        anchors = {Path(entry["path"]): entry for entry in document["anchors"]}
        self.assertEqual(set(anchors), {control_anchor, overwrite_anchor})
        expected = payload.stat()
        for entry in anchors.values():
            self.assertEqual(entry["size"], service.SNAPSHOT_BYTES + 4096)
            self.assertEqual(entry["device"], expected.st_dev)
            self.assertEqual(entry["inode"], expected.st_ino)
            self.assertEqual(entry["links"], 3)
            self.assertTrue(entry["evidence_only"])
            self.assertNotIn("sha256", entry)
            self.assertNotIn("backup", entry)
        copied_paths = {
            Path(item["path"]) for entry in document["entries"] for item in entry["files"]
        }
        self.assertTrue(copied_paths.isdisjoint(protected))
        self.assertEqual(self.state_snapshot(), self.before_state)

    def test_symlink_source_binary_is_rejected_before_external_commands(self):
        actual = self.repo / "actual-binary"
        self.old_binary.rename(actual)
        self.old_binary.symlink_to(actual)
        with self.assertRaises(service.InstallError):
            self.audit(self.deployment())
        self.external.assert_not_called()

    def test_unmanaged_process_rejects_migration_before_state_changes(self):
        with (
            mock.patch.object(service, "bot_processes", return_value=[999999]),
            self.assertRaises(service.InstallError),
        ):
            self.audit(self.deployment())
        self.assertEqual(self.state_snapshot(), self.before_state)
        self.external.assert_not_called()

    def test_atomic_write_preserves_owner_and_mode(self):
        destination = self.root / "private-file"
        destination.write_bytes(b"old")
        destination.chmod(0o600)
        previous = destination.stat()
        service.atomic_bytes(destination, b"new", 0o600)
        current = destination.stat()
        self.assertEqual(destination.read_bytes(), b"new")
        self.assertEqual(stat.S_IMODE(current.st_mode), 0o600)
        self.assertEqual((current.st_uid, current.st_gid), (previous.st_uid, previous.st_gid))
        self.assertNotEqual(current.st_ino, previous.st_ino)
        self.assertEqual(list(self.root.glob(".private-file.*")), [])

    def transaction_environment(self, deployment):
        """Model external tools while real file operations stay under a temporary home."""
        events = []
        registry = {self.gui: 101, self.legacy: None}

        def command(argv, **kwargs):
            args = [str(value) for value in argv]
            events.append(args)
            if args[0] == "codesign":
                if "--sign" in args:
                    target = Path(args[-1])
                    self.assertNotIn(target, [self.artifact, self.old_binary, deployment.binary])
                    target.write_bytes(target.read_bytes() + b"|signed")
                output = "designated => " + REQUIREMENT if "-dr" in args else ""
                return subprocess.CompletedProcess(args, 0, output, "")
            if args[0] == "security":
                return subprocess.CompletedProcess(args, 0, FINGERPRINT, "")
            if args[0] == "ditto":
                source, target = map(Path, args[-2:])
                if source == self.config or source in self.state_files:
                    self.assertEqual(
                        registry, {}, "runtime snapshots must happen after both services stop"
                    )
                target.parent.mkdir(parents=True, exist_ok=True)
                if source.is_dir():
                    shutil.copytree(source, target)
                else:
                    shutil.copy2(source, target)
                return subprocess.CompletedProcess(args, 0, "", "")
            if args[0] == "cargo":
                self.assertEqual(self.old_binary.read_bytes(), b"old signed executable")
                build = Path(args[args.index("--target-dir") + 1])
                self.assertNotEqual(build, self.repo / "target")
                self.assertEqual(kwargs["env"]["CARGO_TARGET_DIR"], str(build))
                self.assertNotEqual(
                    kwargs["env"]["CARGO_BUILD_BUILD_DIR"], str(self.repo / "target")
                )
                output = build / "release" / service.EXECUTABLE
                output.parent.mkdir(parents=True)
                output.write_bytes(b"new unsigned executable")
                output.chmod(0o700)
                return subprocess.CompletedProcess(args, 0, "", "")
            if args[:2] == ["launchctl", "bootout"]:
                registry.pop(args[2], None)
                return subprocess.CompletedProcess(args, 0, "", "")
            if args[:2] == ["launchctl", "bootstrap"]:
                registry[f"{args[2]}/{self.label}"] = 303
                return subprocess.CompletedProcess(args, 0, "", "")
            self.fail(f"unmocked external command: {args[0]}")

        def info(target):
            return {"target": target, "pid": registry[target]} if target in registry else None

        def build(repo, backup):
            events.append(["cargo", "mocked-build"])
            self.assertEqual(repo, self.repo)
            self.assertEqual(self.old_binary.read_bytes(), b"old signed executable")
            output = backup / "build/release" / service.EXECUTABLE
            output.parent.mkdir(parents=True)
            shutil.copy2(self.artifact, output)
            return output

        patches = [
            mock.patch.object(service, "run", side_effect=command),
            mock.patch.object(service, "service_info", side_effect=info),
            mock.patch.object(
                service,
                "bot_processes",
                side_effect=lambda: [pid for pid in registry.values() if pid],
            ),
            mock.patch.object(
                service, "pid_exists", side_effect=lambda pid: pid in registry.values()
            ),
            mock.patch.object(service, "build_release", side_effect=build),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        return events, registry

    def test_deploy_signs_staged_copy_and_preserves_all_runtime_state(self):
        deployment = self.deployment("deploy", "--no-build", "--binary", str(self.artifact))
        events, registry = self.transaction_environment(deployment)
        deployment.audit()
        with mock.patch.object(deployment, "wait_ready", return_value=303):
            result = deployment.deploy()
        self.assertEqual(deployment.binary.read_bytes(), b"new unsigned executable|signed")
        self.assertEqual(self.artifact.read_bytes(), b"new unsigned executable")
        self.assertEqual(self.old_binary.read_bytes(), b"old signed executable")
        self.assertEqual(self.state_snapshot(), self.before_state)
        self.assertEqual(registry, {self.gui: 303})
        self.assertEqual(result["successful_poll_cycles"], 2)
        backup = Path(result["backup"])
        self.assertEqual((backup / "previous-binary").read_bytes(), b"old signed executable")
        self.assertTrue((backup / "state-manifest.json").is_file())
        self.assertTrue((backup / "complete.json").is_file())
        expected = dict(self.original_plist)
        expected["ProgramArguments"] = [str(deployment.binary), str(self.config)]
        self.assertEqual(plistlib.loads(self.plist.read_bytes()), expected)
        self.assertFalse(any(event[0] == "cargo" for event in events))

    def test_build_is_isolated_after_trusted_requirement_is_captured(self):
        deployment = self.deployment("deploy")
        events, _ = self.transaction_environment(deployment)
        deployment.audit()
        with mock.patch.object(deployment, "wait_ready", return_value=303):
            deployment.deploy()
        build_index = next(index for index, event in enumerate(events) if event[0] == "cargo")
        old_signature = ["codesign", "-dr", "-", str(self.old_binary)]
        self.assertIn(old_signature, events[:build_index])
        self.assertEqual(self.old_binary.read_bytes(), b"old signed executable")
        self.assertEqual(self.state_snapshot(), self.before_state)

    def test_failed_readiness_restores_binary_and_plist_without_reverting_runtime(self):
        deployment = self.deployment("deploy", "--no-build", "--binary", str(self.artifact))
        _, registry = self.transaction_environment(deployment)
        deployment.audit()
        changed_record = self.state_files[2]

        def failure(_):
            changed_record.write_bytes(b"newer runtime state must survive rollback")
            raise service.InstallError("simulated startup failure")

        with (
            mock.patch.object(deployment, "wait_ready", side_effect=failure),
            self.assertRaises(service.InstallError),
        ):
            deployment.deploy()
        self.assertEqual(deployment.binary.read_bytes(), b"old signed executable")
        expected = dict(self.original_plist)
        expected["ProgramArguments"] = [str(deployment.binary), str(self.config)]
        self.assertEqual(plistlib.loads(self.plist.read_bytes()), expected)
        self.assertEqual(set(registry), {self.gui})
        self.assertEqual(changed_record.read_bytes(), b"newer runtime state must survive rollback")
        self.assertEqual(self.config.read_bytes(), self.before_state[str(self.config)][0])

    def test_snapshot_failure_restores_old_service_before_any_new_bootstrap(self):
        deployment = self.deployment("deploy", "--no-build", "--binary", str(self.artifact))
        events, registry = self.transaction_environment(deployment)
        deployment.audit()
        with (
            mock.patch.object(
                deployment, "snapshot_state", side_effect=service.InstallError("snapshot failed")
            ),
            mock.patch.object(deployment, "wait_ready") as ready,
            self.assertRaises(service.InstallError),
        ):
            deployment.deploy()
        ready.assert_not_called()
        self.assertEqual(deployment.binary.read_bytes(), b"old signed executable")
        self.assertEqual(registry, {self.gui: 303})
        self.assertEqual(self.state_snapshot(), self.before_state)
        bootstraps = [event for event in events if event[:2] == ["launchctl", "bootstrap"]]
        self.assertEqual(len(bootstraps), 1)

    def test_corrupted_backup_copy_is_rejected_before_it_can_be_used(self):
        destination = self.root / "corrupted-backup"

        def corrupt(argv, **_):
            shutil.copy2(self.old_binary, destination)
            destination.write_bytes(b"corrupted executable")
            return subprocess.CompletedProcess(argv, 0, "", "")

        with (
            mock.patch.object(service, "run", side_effect=corrupt),
            self.assertRaises(service.InstallError),
        ):
            service.preserve_copy(self.old_binary, destination)
        self.assertEqual(self.old_binary.read_bytes(), b"old signed executable")

    def test_staging_normalizes_artifact_group_without_changing_its_input(self):
        destination = self.root / "staged-executable"
        source_alias = self.root / "input-hard-link"
        os.link(self.artifact, source_alias)
        self.artifact.chmod(0o710)
        before = self.artifact.stat()
        original_stat = Path.stat
        owners = {
            self.artifact: (os.geteuid(), 0),
            self.old_binary: (os.geteuid(), 20),
            destination: (os.geteuid(), 0),
        }

        def metadata(path, *args, **kwargs):
            values = list(original_stat(path, *args, **kwargs))
            if Path(path) in owners:
                values[4], values[5] = owners[Path(path)]
            return os.stat_result(values)

        def copy_with_inherited_metadata(argv, **_):
            self.assertEqual(Path(argv[-2]), self.artifact)
            self.assertEqual(Path(argv[-1]), destination)
            shutil.copyfile(self.artifact, destination)
            destination.chmod(0o600)
            return subprocess.CompletedProcess(argv, 0, "", "")

        def normalize_owner(path, uid, gid, **_):
            self.assertEqual(Path(path), destination)
            owners[Path(path)] = (uid, gid)

        with (
            mock.patch.object(Path, "stat", metadata),
            mock.patch.object(service, "run", side_effect=copy_with_inherited_metadata),
            mock.patch.object(service.os, "chown", side_effect=normalize_owner) as chown,
        ):
            service.stage_artifact(self.artifact, destination, self.old_binary)
            self.assertEqual(
                (destination.stat().st_uid, destination.stat().st_gid), (os.geteuid(), 20)
            )
            self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o755)
        self.assertEqual(chown.call_args.args[:3], (destination, os.geteuid(), 20))
        after = self.artifact.stat()
        self.assertEqual(
            (after.st_ino, after.st_uid, after.st_gid, after.st_mode, after.st_nlink),
            (before.st_ino, before.st_uid, before.st_gid, before.st_mode, before.st_nlink),
        )
        self.assertEqual(source_alias.stat().st_ino, after.st_ino)
        self.assertEqual(destination.read_bytes(), b"new unsigned executable")
        self.assertEqual(self.artifact.read_bytes(), b"new unsigned executable")
        self.assertEqual(self.state_snapshot(), self.before_state)

    def test_state_backup_rejects_a_copied_group_change(self):
        source = self.state_files[1]
        destination = self.root / "wrong-group-state-backup"
        original_stat = Path.stat

        def wrong_group(path, *args, **kwargs):
            values = list(original_stat(path, *args, **kwargs))
            if Path(path) == destination:
                values[5] = source.stat().st_gid + 1
            return os.stat_result(values)

        def copied(argv, **_):
            shutil.copyfile(source, destination)
            destination.chmod(0o600)
            return subprocess.CompletedProcess(argv, 0, "", "")

        with (
            mock.patch.object(Path, "stat", wrong_group),
            mock.patch.object(service, "run", side_effect=copied),
            mock.patch.object(service.os, "chown") as chown,
            self.assertRaises(service.InstallError),
        ):
            service.preserve_copy(source, destination)
        chown.assert_not_called()
        self.assertEqual(self.state_snapshot(), self.before_state)

    def test_partial_stop_failure_restores_the_previously_running_service(self):
        deployment = self.deployment("deploy", "--no-build", "--binary", str(self.artifact))
        _, registry = self.transaction_environment(deployment)
        deployment.audit()
        stop = service.stop_services
        calls = 0

        def partial_failure(targets):
            nonlocal calls
            calls += 1
            if calls == 1:
                registry.pop(self.gui)
                raise service.InstallError("legacy domain bootout failed")
            return stop(targets)

        with (
            mock.patch.object(service, "stop_services", side_effect=partial_failure),
            self.assertRaises(service.InstallError),
        ):
            deployment.deploy()
        self.assertEqual(registry, {self.gui: 303})
        self.assertEqual(deployment.binary.read_bytes(), b"old signed executable")
        self.assertEqual(self.state_snapshot(), self.before_state)

    def test_repeated_deploy_uses_installed_binary_and_keeps_existing_state(self):
        first = self.deployment("deploy", "--no-build", "--binary", str(self.artifact))
        self.transaction_environment(first)
        first.audit()
        with mock.patch.object(first, "wait_ready", return_value=303):
            first.deploy()
        second = self.deployment("deploy", "--no-build", "--binary", str(self.artifact))
        self.assertEqual(second.old_binary, first.binary)
        self.transaction_environment(second)
        second.audit()
        with mock.patch.object(second, "wait_ready", return_value=303):
            result = second.deploy()
        self.assertEqual(self.state_snapshot(), self.before_state)
        self.assertEqual(
            (Path(result["backup"]) / "previous-binary").read_bytes(),
            b"new unsigned executable|signed",
        )

    def test_explicit_rollback_survives_cargo_target_removal(self):
        deployment = self.deployment("deploy", "--no-build", "--binary", str(self.artifact))
        self.transaction_environment(deployment)
        deployment.audit()
        with mock.patch.object(deployment, "wait_ready", return_value=303):
            result = deployment.deploy()
        self.old_binary.unlink()
        arguments = service.parser().parse_args(
            [
                "rollback",
                "--label",
                self.label,
                "--backup",
                result["backup"],
            ]
        )
        rolled_back = service.rollback(arguments)
        self.assertEqual(deployment.binary.read_bytes(), b"old signed executable")
        self.assertEqual(
            plistlib.loads(self.plist.read_bytes())["ProgramArguments"][0], str(deployment.binary)
        )
        self.assertEqual(self.state_snapshot(), self.before_state)
        self.assertEqual(rolled_back["runtime_state"], "left unchanged")

    def test_default_rollback_rejects_invalid_saved_plist_before_stopping_or_replacing_binary(self):
        deployment, arguments, events, registry, _ = self.original_rollback_fixture()
        arguments.original_entrypoint = False
        saved = arguments.backup / "previous.plist"
        missing_arguments = dict(self.original_plist)
        missing_arguments.pop("ProgramArguments")
        non_string_argument = dict(self.original_plist)
        non_string_argument["ProgramArguments"] = [str(self.old_binary), 123]
        explicit_program = {**self.original_plist, "Program": "/unexpected/override"}
        invalid_documents = {
            "missing": None,
            "unparseable": b"not a saved property list",
            "non_mapping": plistlib.dumps(["unexpected", "list"]),
            "missing_arguments": plistlib.dumps(missing_arguments),
            "non_string_argument": plistlib.dumps(non_string_argument),
            "explicit_program": plistlib.dumps(explicit_program),
        }
        installed_bytes = deployment.binary.read_bytes()
        live_plist = self.plist.read_bytes()
        for label, contents in invalid_documents.items():
            with self.subTest(saved_plist=label):
                if contents is None:
                    saved.unlink()
                else:
                    saved.write_bytes(contents)
                    saved.chmod(0o600)
                events.clear()
                with self.assertRaises(service.InstallError):
                    service.rollback(arguments)
                self.assertEqual(deployment.binary.read_bytes(), installed_bytes)
                self.assertEqual(self.plist.read_bytes(), live_plist)
                self.assertEqual(registry, {self.gui: 303})
                self.assertFalse(any(event[:2] == ["launchctl", "bootout"] for event in events))
                self.assertEqual(self.state_snapshot(), self.before_state)

    def test_default_rollback_uses_captured_plist_when_backup_changes_after_validation(self):
        deployment, arguments, _, _, _ = self.original_rollback_fixture()
        arguments.original_entrypoint = False
        saved = arguments.backup / "previous.plist"
        stop = service.stop_services

        def change_saved_plist(targets):
            saved.write_bytes(b"invalid replacement after validation")
            return stop(targets)

        with mock.patch.object(service, "stop_services", side_effect=change_saved_plist):
            service.rollback(arguments)
        expected = dict(self.original_plist)
        expected["ProgramArguments"] = [str(deployment.binary), str(self.config)]
        self.assertEqual(plistlib.loads(self.plist.read_bytes()), expected)
        self.assertEqual(self.state_snapshot(), self.before_state)

    def original_rollback_fixture(self):
        original_plist_bytes = self.plist.read_bytes()
        deployment = self.deployment("deploy", "--no-build", "--binary", str(self.artifact))
        events, registry = self.transaction_environment(deployment)
        deployment.audit()
        with mock.patch.object(deployment, "wait_ready", return_value=303):
            result = deployment.deploy()
        arguments = service.parser().parse_args(
            [
                "rollback",
                "--label",
                self.label,
                "--backup",
                result["backup"],
                "--original-entrypoint",
            ]
        )
        events.clear()
        return deployment, arguments, events, registry, original_plist_bytes

    def test_original_entrypoint_rollback_verifies_source_and_restores_exact_plist(self):
        deployment, arguments, events, registry, original_plist = self.original_rollback_fixture()
        result = service.rollback(arguments)
        self.assertEqual(self.plist.read_bytes(), original_plist)
        self.assertEqual(self.old_binary.read_bytes(), b"old signed executable")
        self.assertEqual(self.state_snapshot(), self.before_state)
        self.assertEqual(registry, {self.gui: 303})
        verified = next(
            index
            for index, event in enumerate(events)
            if event[0] == "codesign" and "--verify" in event and event[-1] == str(self.old_binary)
        )
        stopped = next(
            index for index, event in enumerate(events) if event[:2] == ["launchctl", "bootout"]
        )
        self.assertLess(verified, stopped)
        self.assertEqual(result["entrypoint"], str(self.old_binary))
        self.assertEqual(result["readiness"], "UNCONFIRMED")
        self.assertEqual(deployment.config, self.config)

    def test_original_entrypoint_missing_source_is_rejected_before_stopping_service(self):
        deployment, arguments, events, registry, _ = self.original_rollback_fixture()
        self.old_binary.unlink()
        installed_bytes = deployment.binary.read_bytes()
        plist_bytes = self.plist.read_bytes()
        with self.assertRaises(service.InstallError):
            service.rollback(arguments)
        self.assertEqual(deployment.binary.read_bytes(), installed_bytes)
        self.assertEqual(self.plist.read_bytes(), plist_bytes)
        self.assertEqual(registry, {self.gui: 303})
        self.assertFalse(any(event[:2] == ["launchctl", "bootout"] for event in events))
        self.assertEqual(self.state_snapshot(), self.before_state)

    def test_original_entrypoint_changed_source_is_rejected_before_stopping_service(self):
        deployment, arguments, events, registry, _ = self.original_rollback_fixture()
        self.old_binary.write_bytes(b"changed original executable")
        installed_bytes = deployment.binary.read_bytes()
        plist_bytes = self.plist.read_bytes()
        with self.assertRaises(service.InstallError):
            service.rollback(arguments)
        self.assertEqual(deployment.binary.read_bytes(), installed_bytes)
        self.assertEqual(self.plist.read_bytes(), plist_bytes)
        self.assertEqual(registry, {self.gui: 303})
        self.assertFalse(any(event[:2] == ["launchctl", "bootout"] for event in events))
        self.assertEqual(self.state_snapshot(), self.before_state)

    def test_deployment_lock_excludes_another_install_or_rollback(self):
        with (
            service.deployment_lock(),
            self.assertRaises(service.InstallError),
            service.deployment_lock(),
        ):
            self.fail("second deployment acquired the same lock")

    def test_initial_install_uses_trusted_identity_for_unsigned_input_artifact(self):
        self.plist.unlink()
        deployment = self.deployment("plan", "--binary", str(self.artifact))
        with (
            mock.patch.object(
                service, "initial_identity", return_value=(REQUIREMENT, FINGERPRINT)
            ) as identity,
            mock.patch.object(
                service, "signature", side_effect=AssertionError("input artifact is unsigned")
            ),
        ):
            deployment.audit()
        identity.assert_called_once_with()
        self.assertEqual(deployment.identity, FINGERPRINT)
        self.assertEqual(self.artifact.read_bytes(), b"new unsigned executable")

    def test_tampered_rollback_binary_is_rejected_before_stopping_live_service(self):
        deployment = self.deployment("deploy", "--no-build", "--binary", str(self.artifact))
        events, registry = self.transaction_environment(deployment)
        deployment.audit()
        with mock.patch.object(deployment, "wait_ready", return_value=303):
            result = deployment.deploy()
        backup = Path(result["backup"])
        previous = backup / "previous-binary"
        previous.write_bytes(b"tampered backup binary")
        arguments = service.parser().parse_args(
            [
                "rollback",
                "--label",
                self.label,
                "--backup",
                str(backup),
            ]
        )
        command = service.run.side_effect

        def reject_tampering(argv, **kwargs):
            args = [str(value) for value in argv]
            if args[0] == "codesign" and "--verify" in args and Path(args[-1]) == previous:
                raise service.InstallError("saved binary signature is invalid")
            return command(argv, **kwargs)

        events.clear()
        with (
            mock.patch.object(service, "run", side_effect=reject_tampering),
            self.assertRaises(service.InstallError),
        ):
            service.rollback(arguments)
        self.assertEqual(deployment.binary.read_bytes(), b"new unsigned executable|signed")
        self.assertEqual(registry, {self.gui: 303})
        self.assertFalse(any(event[:2] == ["launchctl", "bootout"] for event in events))
        self.assertEqual(self.state_snapshot(), self.before_state)

    def test_readiness_requires_new_polling_records_and_running_paths(self):
        deployment = self.deployment()
        self.audit(deployment)
        stdout = deployment.log_paths[0]
        stdout.parent.mkdir(parents=True)
        ready_log = (
            "telegram local downloader started\n"
            "telegram polling completed\ntelegram polling completed\n"
        )
        stdout.write_text(ready_log)
        info = {"target": self.gui, "pid": 303}
        with (
            mock.patch.object(service, "service_info", return_value=info),
            mock.patch.object(service, "bot_processes", return_value=[303]),
            mock.patch.object(service.time, "monotonic", side_effect=[0, 0, 999]),
            mock.patch.object(service.time, "sleep"),
            self.assertRaises(service.InstallError),
        ):
            deployment.wait_ready({str(stdout): stdout.stat().st_size})
        self.external.assert_not_called()
        listing = f"p303\nn{deployment.workdir}\nn{deployment.binary}\n"
        with (
            mock.patch.object(service, "service_info", return_value=info),
            mock.patch.object(service, "bot_processes", return_value=[303]),
            mock.patch.object(
                service, "run", return_value=subprocess.CompletedProcess([], 0, listing, "")
            ),
        ):
            self.assertEqual(deployment.wait_ready({str(stdout): 0}), 303)

    def startup_fixture(self):
        deployment = self.deployment()
        self.audit(deployment)
        stdout = deployment.log_paths[0]
        stdout.parent.mkdir(parents=True)
        old_log = "records from the previous service instance\n"
        stdout.write_text(
            old_log
            + "telegram local downloader started\n"
            + "telegram polling completed\ntelegram polling completed\n"
        )
        offsets = {str(stdout): len(old_log.encode())}
        info = {"target": self.gui, "pid": 303}
        listing = f"p303\nn{deployment.workdir}\nn{deployment.binary}\n"
        return deployment, offsets, info, listing

    def test_startup_waits_for_process_inventory_to_catch_up_with_launchctl(self):
        deployment, offsets, info, listing = self.startup_fixture()
        with (
            mock.patch.object(service, "service_info", return_value=info),
            mock.patch.object(service, "bot_processes", side_effect=[[], [303]]) as inventory,
            mock.patch.object(service.time, "sleep") as sleep,
            mock.patch.object(
                service, "run", return_value=subprocess.CompletedProcess([], 0, listing, "")
            ),
        ):
            self.assertEqual(deployment.wait_ready(offsets), 303)
        self.assertEqual(inventory.call_count, 2)
        sleep.assert_called_once()

    def test_startup_times_out_when_process_inventory_remains_empty(self):
        deployment, offsets, info, _ = self.startup_fixture()
        with (
            mock.patch.object(service, "service_info", return_value=info),
            mock.patch.object(service, "bot_processes", return_value=[]),
            mock.patch.object(service.time, "monotonic", side_effect=[0, 0, 999]),
            mock.patch.object(service.time, "sleep"),
            self.assertRaises(service.InstallError) as failure,
        ):
            deployment.wait_ready(offsets)
        self.assertIn("timeout", str(failure.exception))
        self.external.assert_not_called()

    def test_startup_rejects_foreign_or_extra_processes_with_numeric_diagnostic(self):
        deployment, offsets, info, _ = self.startup_fixture()
        for inventory in [[404], [303, 404]]:
            with (
                self.subTest(inventory=inventory),
                mock.patch.object(service, "service_info", return_value=info),
                mock.patch.object(service, "bot_processes", return_value=inventory),
                mock.patch.object(service.time, "sleep") as sleep,
                self.assertRaises(service.InstallError) as failure,
            ):
                deployment.wait_ready(offsets)
            self.assertIn("303", str(failure.exception))
            self.assertIn("404", str(failure.exception))
            sleep.assert_not_called()
        self.external.assert_not_called()


class SignatureTest(unittest.TestCase):
    def test_existing_certificate_and_identifier_are_required(self):
        commands = []

        def command(argv, **_):
            commands.append(list(argv))
            output = REQUIREMENT if "-dr" in argv else FINGERPRINT
            if "-dr" in argv:
                output = "designated => " + output
            return subprocess.CompletedProcess(argv, 0, output, "")

        with mock.patch.object(service, "run", side_effect=command):
            actual = service.signature(Path("/fixture/old-executable"))
        self.assertEqual(actual, (REQUIREMENT, FINGERPRINT))
        self.assertEqual(
            commands[0], ["codesign", "--verify", "--strict", Path("/fixture/old-executable")]
        )
        self.assertTrue(any(argv[:2] == ["security", "find-identity"] for argv in commands))

    def test_ad_hoc_or_other_identifier_signatures_are_rejected(self):
        for requirement in [
            'identifier "other"',
            f'identifier "{service.IDENTIFIER}" and anchor apple',
        ]:
            with self.subTest(requirement=requirement):
                result = subprocess.CompletedProcess([], 0, "designated => " + requirement, "")
                with (
                    mock.patch.object(service, "run", return_value=result),
                    self.assertRaises(service.InstallError),
                ):
                    service.signature(Path("/fixture/old-executable"))

    def test_initial_install_selects_only_the_exact_trusted_identity_name(self):
        result = subprocess.CompletedProcess(
            [],
            0,
            (
                f'1) {FINGERPRINT} "Telegram Video Downloader Local Signing"\n'
                f'2) {"B" * 40} "Unrelated identity"\n'
            ),
            "",
        )
        with mock.patch.object(service, "run", return_value=result):
            requirement, identity = service.initial_identity()
        self.assertEqual(identity, FINGERPRINT)
        self.assertIn(f'identifier "{service.IDENTIFIER}"', requirement)
        self.assertIn(FINGERPRINT.lower(), requirement)
        result.stdout += f'3) {"C" * 40} "Telegram Video Downloader Local Signing"\n'
        with (
            mock.patch.object(service, "run", return_value=result),
            self.assertRaises(service.InstallError),
        ):
            service.initial_identity()


class BuildSupervisorTest(unittest.TestCase):
    def test_release_target_and_build_cache_override_live_environment(self):
        with tempfile.TemporaryDirectory(prefix="telegram-build-test-") as temporary:
            root = Path(temporary).resolve()
            process = mock.Mock(returncode=0)
            process.poll.return_value = 0
            reader, writer = os.pipe()
            os.close(writer)
            process.stdout = os.fdopen(reader, "rb")
            with (
                mock.patch.dict(
                    os.environ,
                    {
                        "CARGO_TARGET_DIR": "/existing/live-target",
                        "CARGO_BUILD_BUILD_DIR": "/existing/live-cache",
                    },
                ),
                mock.patch.object(service.subprocess, "Popen", return_value=process) as popen,
            ):
                result = service.build_release(root / "repo", root)
            command = popen.call_args.args[0]
            environment = popen.call_args.kwargs["env"]
            self.assertEqual(result, root / "build/release" / service.EXECUTABLE)
            self.assertEqual(command[command.index("--target-dir") + 1], str(root / "build"))
            self.assertEqual(environment["CARGO_TARGET_DIR"], str(root / "build"))
            self.assertEqual(environment["CARGO_BUILD_BUILD_DIR"], str(root / "cargo-build"))
            self.assertTrue(popen.call_args.kwargs["start_new_session"])
            self.assertEqual(stat.S_IMODE((root / "build.log").stat().st_mode), 0o600)

    def test_build_deadline_stops_its_entire_process_group(self):
        with tempfile.TemporaryDirectory(prefix="telegram-build-timeout-") as temporary:
            root = Path(temporary).resolve()
            process = mock.Mock(pid=999999)
            process.poll.return_value = None
            reader, writer = os.pipe()
            os.close(writer)
            process.stdout = os.fdopen(reader, "rb")
            with (
                mock.patch.object(service.subprocess, "Popen", return_value=process),
                mock.patch.object(service.time, "monotonic", side_effect=[0, 1801]),
                mock.patch.object(service.os, "killpg") as kill,
                self.assertRaises(service.InstallError),
            ):
                service.build_release(root / "repo", root)
            self.assertEqual(
                kill.call_args_list,
                [
                    mock.call(process.pid, service.signal.SIGTERM),
                    mock.call(process.pid, service.signal.SIGKILL),
                ],
            )
            self.assertEqual(process.wait.call_args_list, [mock.call(timeout=5)] * 2)

    def test_build_timeout_kills_descendants_even_after_cargo_parent_exited(self):
        with tempfile.TemporaryDirectory(prefix="telegram-build-descendant-") as temporary:
            root = Path(temporary).resolve()
            process = mock.Mock(pid=999999, returncode=0)
            process.poll.return_value = 0
            reader, writer = os.pipe()
            self.addCleanup(os.close, writer)
            # The open write end models a descendant retaining Cargo's output pipe.
            process.stdout = os.fdopen(reader, "rb")
            with (
                mock.patch.object(service.subprocess, "Popen", return_value=process),
                mock.patch.object(service.time, "monotonic", side_effect=[0, 1801, 1807, 1813]),
                mock.patch.object(service.time, "sleep"),
                mock.patch.object(service.os, "killpg") as kill,
                self.assertRaises(service.InstallError),
            ):
                service.build_release(root / "repo", root)
            self.assertEqual(
                kill.call_args_list,
                [
                    mock.call(process.pid, service.signal.SIGTERM),
                    mock.call(process.pid, service.signal.SIGKILL),
                ],
            )
            self.assertTrue(process.stdout.closed)


class ServiceControlTest(unittest.TestCase):
    def test_stop_waits_for_both_registration_and_old_process_exit(self):
        gui, legacy = "gui/123/example", "user/123/example"
        checks = {gui: 0, legacy: 0}
        events = []

        def info(target):
            checks[target] += 1
            if checks[target] == 1:
                return {"target": target, "pid": 101 if target == gui else 102}
            return None

        def command(argv, **_):
            events.append(list(argv))
            return subprocess.CompletedProcess(argv, 0, "", "")

        with (
            mock.patch.object(service, "service_info", side_effect=info),
            mock.patch.object(service, "pid_exists", side_effect=[True, False, False, False]),
            mock.patch.object(service, "run", side_effect=command),
            mock.patch.object(service.time, "sleep") as sleep,
        ):
            stopped = service.stop_services([gui, legacy])
        self.assertEqual(events, [["launchctl", "bootout", gui], ["launchctl", "bootout", legacy]])
        self.assertEqual(len(stopped), 2)
        sleep.assert_called_once()

    def test_unexpected_launchctl_query_error_does_not_count_as_absent(self):
        result = subprocess.CompletedProcess(["launchctl"], 42, "", PRIVATE_MARKER)
        with (
            mock.patch.object(service, "run", return_value=result),
            self.assertRaises(service.InstallError) as failure,
        ):
            service.service_info("gui/123/example")
        self.assertNotIn(PRIVATE_MARKER, str(failure.exception))


if __name__ == "__main__":
    unittest.main()
