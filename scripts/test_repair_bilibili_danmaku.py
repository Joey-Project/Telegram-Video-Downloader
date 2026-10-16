import gzip
import hashlib
import json
import os
import tempfile
import unittest
import zlib
from pathlib import Path
from unittest import mock

import repair_bilibili_danmaku as repair


def xml_fixture() -> bytes:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<i><chatid>1</chatid>'
        '<d p="1.25,1,25,16711680,0,0,0,0">中文 &amp; {world}\\</d>'
        '<d p="2,5,30,255,0,0,0,0">top</d>'
        '<d p="3,4,18,65280,0,0,0,0">bottom</d>'
        '<d p="4,6,25,0,0,0,0,0">reverse</d>'
        '<d p="5,7,25,16777215,0,0,0,0">advanced</d>'
        '</i>'
    ).encode("utf-8")


def raw_deflate(data: bytes) -> bytes:
    compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    return compressor.compress(data) + compressor.flush()


class DecodeAndRenderTests(unittest.TestCase):
    def test_detects_plain_raw_deflate_zlib_and_gzip(self) -> None:
        xml = xml_fixture()
        payloads = (
            ("plain XML", xml),
            ("raw DEFLATE", raw_deflate(xml)),
            ("zlib", zlib.compress(xml)),
            ("gzip", gzip.compress(xml)),
        )
        for expected_format, payload in payloads:
            with self.subTest(expected_format=expected_format):
                detected, decoded = repair.decode_xml_payload(payload)
                self.assertEqual(detected, expected_format)
                self.assertEqual(decoded, xml)

    def test_rendering_preserves_layout_timing_color_chinese_and_escaping(self) -> None:
        root = repair._parse_xml_document(xml_fixture())
        comments, invalid, unsupported, normal = repair._parse_danmaku(root)
        ass, count, skipped = repair.render_ass(comments)

        self.assertEqual((invalid, normal), (0, 4))
        self.assertEqual(unsupported, {7: 1})
        self.assertEqual(skipped, {7: 1})
        self.assertEqual(count, 4)
        self.assertEqual(ass.count("Dialogue:"), 4)
        self.assertIn("0:00:01.25,0:00:09.25", ass)
        self.assertIn("0:00:02.00,0:00:06.00", ass)
        self.assertIn(r"中文 & \{world\}\\", ass)
        self.assertIn(r"\an8\pos(960,92)", ass)
        self.assertIn(r"\an2\pos(960,936)", ass)
        self.assertIn(r"\move(-", ass)
        self.assertIn("&H0000FF&", ass)
        self.assertNotIn("advanced", ass)

    def test_malformed_xml_dtd_invalid_comments_and_size_limits_are_rejected_or_counted(self) -> None:
        with self.assertRaisesRegex(repair.RepairError, "invalid XML"):
            repair.decode_xml_payload(b"<i><d></i>")
        with self.assertRaisesRegex(repair.RepairError, "DTD and entity"):
            repair.decode_xml_payload(b"<!DOCTYPE i [<!ENTITY x 'x'>]><i>&x;</i>")

        root = repair._parse_xml_document(
            b'<i><d p="nan,1,25,0">bad time</d><d p="1,99,25,0">unknown mode</d>'
            b'<d p="1,1,25,0">valid</d><d>missing p</d></i>'
        )
        comments, invalid, unsupported, normal = repair._parse_danmaku(root)
        ass, count, skipped = repair.render_ass(comments)
        self.assertEqual(invalid, 2)
        self.assertEqual(unsupported, {99: 1})
        self.assertEqual(normal, 1)
        self.assertEqual((count, skipped), (1, {99: 1}))
        self.assertEqual(ass.count("Dialogue:"), 1)

        with self.assertRaisesRegex(repair.RepairError, "encoded input exceeds"):
            repair.decode_xml_payload(b"x" * (repair.MAX_FILE_BYTES + 1))
        bomb = gzip.compress(b"<i>" + b"x" * (repair.MAX_FILE_BYTES + 1) + b"</i>")
        with self.assertRaisesRegex(repair.RepairError, "safety limit"):
            repair.decode_xml_payload(bomb)

    def test_rendered_ass_is_capped_while_comments_are_appended(self) -> None:
        comment = repair.Danmaku(1.0, 1, 25.0, 0xFFFFFF, "\\" * 1024)
        with mock.patch.object(repair, "MAX_FILE_BYTES", 2048):
            with self.assertRaisesRegex(repair.RepairError, "generated ASS exceeds"):
                repair.render_ass([comment])


class FileOperationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.xml_path = self.root / "episode.xml"
        self.ass_path = self.root / "episode.ass"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_dry_run_makes_no_filesystem_changes(self) -> None:
        original = raw_deflate(xml_fixture())
        self.xml_path.write_bytes(original)
        before = sorted(path.name for path in self.root.iterdir())

        result = repair.repair_file(str(self.xml_path))

        self.assertFalse(result["applied"])
        self.assertEqual(result["compression"], "raw DEFLATE")
        self.assertEqual(result["comments_rendered"], 4)
        self.assertEqual(self.xml_path.read_bytes(), original)
        self.assertFalse(self.ass_path.exists())
        self.assertEqual(sorted(path.name for path in self.root.iterdir()), before)

    def test_apply_backs_up_exact_originals_and_replaces_both_files(self) -> None:
        original_xml = raw_deflate(xml_fixture())
        original_ass = b""
        self.xml_path.write_bytes(original_xml)
        self.ass_path.write_bytes(original_ass)
        backup_dir = self.root / "repair-backup"

        result = repair.repair_file(str(self.xml_path), apply=True, backup_dir=str(backup_dir))

        self.assertTrue(result["applied"])
        self.assertEqual((backup_dir / "original.xml").read_bytes(), original_xml)
        self.assertEqual((backup_dir / "original.ass").read_bytes(), original_ass)
        manifest = json.loads((backup_dir / "manifest.json").read_text("utf-8"))
        self.assertEqual(manifest["source_sha256"], hashlib.sha256(original_xml).hexdigest())
        normalized = self.xml_path.read_bytes()
        self.assertTrue(normalized.startswith(b"<?xml version='1.0' encoding='utf-8'?>"))
        self.assertIn(b"<d p=\"1.25,1,25,16711680,0,0,0,0\">", normalized)
        self.assertEqual(self.ass_path.read_text("utf-8").count("Dialogue:"), 4)
        self.assertEqual(list(self.root.glob(".tvd-danmaku-*.tmp")), [])

    def test_nonempty_ass_requires_explicit_replacement_and_existing_backup_is_refused(self) -> None:
        self.xml_path.write_bytes(xml_fixture())
        old_ass = b"user-edited subtitles\n"
        self.ass_path.write_bytes(old_ass)
        backup_dir = self.root / "backup"

        dry_run = repair.repair_file(str(self.xml_path))
        self.assertFalse(dry_run["applied"])
        self.assertEqual(dry_run["existing_ass_bytes"], len(old_ass))
        self.assertEqual(self.xml_path.read_bytes(), xml_fixture())
        self.assertEqual(self.ass_path.read_bytes(), old_ass)
        self.assertFalse(backup_dir.exists())

        with self.assertRaisesRegex(repair.RepairError, "--replace-existing-ass"):
            repair.repair_file(str(self.xml_path), apply=True, backup_dir=str(backup_dir))
        self.assertFalse(backup_dir.exists())
        self.assertEqual(self.ass_path.read_bytes(), old_ass)

        result = repair.repair_file(
            str(self.xml_path), apply=True, backup_dir=str(backup_dir), replace_existing_ass=True
        )
        self.assertTrue(result["applied"])
        self.assertEqual((backup_dir / "original.ass").read_bytes(), old_ass)
        self.assertEqual(self.ass_path.read_text("utf-8").count("Dialogue:"), 4)

        with self.assertRaisesRegex(repair.RepairError, "already exists"):
            repair.repair_file(
                str(self.xml_path), apply=True, backup_dir=str(backup_dir), replace_existing_ass=True
            )

    def test_apply_rejects_source_mutation_after_backup_before_commit(self) -> None:
        original_xml = raw_deflate(xml_fixture())
        self.xml_path.write_bytes(original_xml)
        original_stage = repair._stage_file_at
        calls = 0

        def mutate_during_staging(directory_fd: int, data: bytes, mode: int):
            nonlocal calls
            calls += 1
            staged = original_stage(directory_fd, data, mode)
            if calls == 2:
                with self.xml_path.open("wb") as stream:
                    stream.write(b"externally changed")
            return staged

        with mock.patch.object(repair, "_stage_file_at", side_effect=mutate_during_staging):
            with self.assertRaisesRegex(repair.RepairError, "changed before commit"):
                repair.repair_file(
                    str(self.xml_path), apply=True, backup_dir=str(self.root / "backup")
                )
        self.assertEqual(self.xml_path.read_bytes(), b"externally changed")
        self.assertFalse(self.ass_path.exists())
        self.assertTrue((self.root / "backup" / "original.xml").exists())
        self.assertEqual(list(self.root.glob(".tvd-danmaku-*.tmp")), [])

    def test_metadata_change_is_allowed_but_content_change_and_replacement_are_rejected(self) -> None:
        self.xml_path.write_bytes(b"stable content")
        parent_fd, directory_identity = repair._open_directory(str(self.root))
        try:
            expected = repair._read_file_at(parent_fd, self.xml_path.name)
            assert expected is not None

            os.utime(self.xml_path, ns=(1_000_000_000, 1_000_000_001))
            repair._verify_directory_path(str(self.root), directory_identity)
            repair._verify_snapshot(parent_fd, self.xml_path.name, expected)

            with self.xml_path.open("r+b") as stream:
                stream.seek(0)
                stream.write(b"changed")
            with self.assertRaisesRegex(repair.RepairError, "identity or content changed"):
                repair._verify_snapshot(parent_fd, self.xml_path.name, expected)

            self.xml_path.write_bytes(b"stable content")
            expected = repair._read_file_at(parent_fd, self.xml_path.name)
            assert expected is not None
            replacement = self.root / "replacement"
            replacement.write_bytes(b"stable content")
            os.replace(replacement, self.xml_path)
            with self.assertRaisesRegex(repair.RepairError, "identity or content changed"):
                repair._verify_snapshot(parent_fd, self.xml_path.name, expected)
        finally:
            os.close(parent_fd)

    def test_staging_write_failure_leaves_target_files_unchanged(self) -> None:
        original_xml = raw_deflate(xml_fixture())
        original_ass = b""
        self.xml_path.write_bytes(original_xml)
        self.ass_path.write_bytes(original_ass)
        original_stage = repair._stage_file_at
        calls = 0

        def fail_second_stage(directory_fd: int, data: bytes, mode: int):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("simulated staging write failure")
            return original_stage(directory_fd, data, mode)

        with mock.patch.object(repair, "_stage_file_at", side_effect=fail_second_stage):
            with self.assertRaisesRegex(repair.RepairError, "staging write failure"):
                repair.repair_file(
                    str(self.xml_path), apply=True, backup_dir=str(self.root / "backup")
                )
        self.assertEqual(self.xml_path.read_bytes(), original_xml)
        self.assertEqual(self.ass_path.read_bytes(), original_ass)
        self.assertEqual(list(self.root.glob(".tvd-danmaku-*.tmp")), [])

    def test_apply_rejects_old_ass_mutation_after_backup_before_commit(self) -> None:
        original_xml = raw_deflate(xml_fixture())
        original_ass = b"user's old subtitles"
        self.xml_path.write_bytes(original_xml)
        self.ass_path.write_bytes(original_ass)
        original_stage = repair._stage_file_at
        calls = 0

        def mutate_ass_during_staging(directory_fd: int, data: bytes, mode: int):
            nonlocal calls
            calls += 1
            staged = original_stage(directory_fd, data, mode)
            if calls == 2:
                with self.ass_path.open("wb") as stream:
                    stream.write(b"externally changed subtitles")
            return staged

        with mock.patch.object(repair, "_stage_file_at", side_effect=mutate_ass_during_staging):
            with self.assertRaisesRegex(repair.RepairError, "changed before commit"):
                repair.repair_file(
                    str(self.xml_path),
                    apply=True,
                    backup_dir=str(self.root / "backup"),
                    replace_existing_ass=True,
                )
        self.assertEqual(self.xml_path.read_bytes(), original_xml)
        self.assertEqual(self.ass_path.read_bytes(), b"externally changed subtitles")
        self.assertEqual((self.root / "backup" / "original.ass").read_bytes(), original_ass)
        self.assertEqual(list(self.root.glob(".tvd-danmaku-*.tmp")), [])

    def test_second_replace_failure_rolls_back_first_replacement(self) -> None:
        original_xml = raw_deflate(xml_fixture())
        original_ass = b""
        self.xml_path.write_bytes(original_xml)
        self.ass_path.write_bytes(original_ass)
        original_replace = os.replace

        def fail_ass_replace(src, dst, *args, **kwargs):
            if dst == self.ass_path.name:
                raise OSError("simulated ASS replacement failure")
            return original_replace(src, dst, *args, **kwargs)

        with mock.patch.object(repair.os, "replace", side_effect=fail_ass_replace):
            with self.assertRaisesRegex(repair.RepairError, "original files were restored"):
                repair.repair_file(
                    str(self.xml_path), apply=True, backup_dir=str(self.root / "backup")
                )
        self.assertEqual(self.xml_path.read_bytes(), original_xml)
        self.assertEqual(self.ass_path.read_bytes(), original_ass)
        self.assertEqual(list(self.root.glob(".tvd-danmaku-*.tmp")), [])


if __name__ == "__main__":
    unittest.main()
