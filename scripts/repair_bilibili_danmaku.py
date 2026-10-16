#!/usr/bin/env python3
"""Restore a Bilibili XML danmaku file and regenerate its sibling ASS file."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import sys
import unicodedata
import uuid
import xml.etree.ElementTree as ET
import zlib
from dataclasses import dataclass
from typing import Any


MAX_FILE_BYTES = 16 * 1024 * 1024
PLAY_RES_X = 1920
PLAY_RES_Y = 1080
DEFAULT_FONT_SIZE = 42.0
SCROLL_DURATION_SECONDS = 8.0
FIXED_DURATION_SECONDS = 4.0
TOP_MARGIN = 40.0
LINE_HEIGHT = 52.0
SCROLL_LANES = 15
FIXED_LANES = 8
XML_FORBIDDEN_DECLARATION = re.compile(rb"<!\s*(?:DOCTYPE|ENTITY)\b", re.IGNORECASE)


class RepairError(Exception):
    """A safe, actionable repair failure."""


@dataclass(frozen=True)
class Snapshot:
    data: bytes
    identity: tuple[int, int, int]
    mode: int

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.data).hexdigest()


@dataclass(frozen=True)
class Danmaku:
    start_seconds: float
    mode: int
    font_size: float
    color: int
    text: str


def _file_identity(file_stat: os.stat_result) -> tuple[int, int, int]:
    return (file_stat.st_dev, file_stat.st_ino, stat.S_IFMT(file_stat.st_mode))


def _is_zlib_header(data: bytes) -> bool:
    if len(data) < 2:
        return False
    cmf, flags = data[0], data[1]
    return (cmf & 0x0F) == 8 and (cmf >> 4) <= 7 and ((cmf << 8) | flags) % 31 == 0


def _parse_xml_document(data: bytes) -> ET.Element:
    if len(data) > MAX_FILE_BYTES:
        raise RepairError(f"XML exceeds the {MAX_FILE_BYTES}-byte safety limit")
    if XML_FORBIDDEN_DECLARATION.search(data):
        raise RepairError("DTD and entity declarations are not accepted")
    try:
        xml_text = data.decode("utf-8-sig", errors="strict")
    except UnicodeDecodeError as exc:
        raise RepairError("XML is not valid UTF-8") from exc

    declaration = re.match(r"\s*<\?xml\s+[^?]*encoding\s*=\s*(['\"])([^'\"]+)\1", xml_text, re.I)
    if declaration and declaration.group(2).lower().replace("-", "") != "utf8":
        raise RepairError("XML declaration does not specify UTF-8")

    parser = ET.XMLParser(target=ET.TreeBuilder(insert_comments=True, insert_pis=True))
    try:
        root = ET.fromstring(xml_text, parser=parser)
    except ET.ParseError as exc:
        raise RepairError(f"invalid XML: {exc}") from exc
    if root.tag != "i":
        raise RepairError(f"expected Bilibili <i> root, found <{root.tag}>")
    return root


def _inflate(data: bytes, wbits: int) -> bytes:
    decoder = zlib.decompressobj(wbits)
    try:
        decoded = decoder.decompress(data, MAX_FILE_BYTES + 1)
        if len(decoded) > MAX_FILE_BYTES or decoder.unconsumed_tail:
            raise RepairError(f"decompressed XML exceeds the {MAX_FILE_BYTES}-byte safety limit")
        decoded += decoder.flush(MAX_FILE_BYTES + 1 - len(decoded))
    except zlib.error as exc:
        raise RepairError("invalid compressed stream") from exc
    if len(decoded) > MAX_FILE_BYTES:
        raise RepairError(f"decompressed XML exceeds the {MAX_FILE_BYTES}-byte safety limit")
    if not decoder.eof:
        raise RepairError("compressed stream is truncated")
    if decoder.unused_data:
        raise RepairError("compressed stream has trailing data")
    return decoded


def decode_xml_payload(data: bytes) -> tuple[str, bytes]:
    """Detect plain UTF-8, raw DEFLATE, zlib, or gzip without unbounded output."""
    if len(data) > MAX_FILE_BYTES:
        raise RepairError(f"encoded input exceeds the {MAX_FILE_BYTES}-byte safety limit")

    probe = data.lstrip(b"\xef\xbb\xbf \t\r\n")
    plain_error: RepairError | None = None
    if probe.startswith(b"<"):
        try:
            _parse_xml_document(data)
            return "plain XML", data
        except RepairError as exc:
            plain_error = exc

    if data.startswith(b"\x1f\x8b"):
        formats = (("gzip", zlib.MAX_WBITS | 16), ("raw DEFLATE", -zlib.MAX_WBITS))
    elif _is_zlib_header(data):
        formats = (("zlib", zlib.MAX_WBITS), ("raw DEFLATE", -zlib.MAX_WBITS))
    else:
        formats = (("raw DEFLATE", -zlib.MAX_WBITS),)

    decode_errors: list[str] = []
    for label, wbits in formats:
        try:
            decoded = _inflate(data, wbits)
            _parse_xml_document(decoded)
            return label, decoded
        except RepairError as exc:
            decode_errors.append(str(exc))
    if plain_error is not None:
        raise plain_error
    limit_error = next((error for error in decode_errors if "safety limit" in error), None)
    if limit_error is not None:
        raise RepairError(limit_error)
    detail = decode_errors[-1] if decode_errors else "unsupported encoding"
    raise RepairError(f"input is neither valid plain XML nor a supported compressed XML stream ({detail})")


def _parse_danmaku(root: ET.Element) -> tuple[list[Danmaku], int, dict[int, int], int]:
    comments: list[Danmaku] = []
    invalid_count = 0
    unsupported_modes: dict[int, int] = {}
    normal_count = 0

    for element in root.iter("d"):
        parameters = element.attrib.get("p")
        if parameters is None:
            invalid_count += 1
            continue
        fields = parameters.split(",")
        if len(fields) < 4:
            invalid_count += 1
            continue
        try:
            start_seconds = float(fields[0])
            mode = int(fields[1])
            raw_font_size = float(fields[2])
            raw_color = int(fields[3], 10)
        except ValueError:
            invalid_count += 1
            continue
        if (
            not math.isfinite(start_seconds)
            or start_seconds < 0
            or not math.isfinite(raw_font_size)
            or raw_font_size <= 0
            or not 0 <= raw_color <= 0xFFFF_FFFF
            or not 0 <= mode <= 255
        ):
            invalid_count += 1
            continue
        text = "".join(element.itertext())
        if not text:
            invalid_count += 1
            continue

        font_size = min(96.0, max(12.0, raw_font_size))
        comments.append(Danmaku(start_seconds, mode, font_size, raw_color & 0xFF_FFFF, text))
        if mode in (1, 2, 3, 4, 5, 6):
            normal_count += 1
        else:
            unsupported_modes[mode] = unsupported_modes.get(mode, 0) + 1

    return comments, invalid_count, unsupported_modes, normal_count


def _ass_timestamp(seconds: float) -> str:
    safe_seconds = min(3_599_999.0, max(0.0, seconds))
    centiseconds = int((safe_seconds + 0.005) * 100)
    total_seconds, centisecond = divmod(centiseconds, 100)
    hour, remainder = divmod(total_seconds, 3600)
    minute, second = divmod(remainder, 60)
    return f"{hour}:{minute:02}:{second:02}.{centisecond:02}"


def _top_lane_y(index: int, lane_count: int) -> float:
    return TOP_MARGIN + (index % lane_count) * LINE_HEIGHT


def _estimated_text_width(text: str, font_size: float) -> float:
    return max(max(1, len(text)) * font_size * 0.72, font_size * 2.0)


def _ass_color(color: int) -> str:
    red = (color >> 16) & 0xFF
    green = (color >> 8) & 0xFF
    blue = color & 0xFF
    return f"&H{blue:02X}{green:02X}{red:02X}&"


def _ass_escape(text: str, byte_budget: int) -> str:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    output: list[str] = []
    chunk: list[str] = []
    output_bytes = 0
    for character in normalized:
        if character == "\n":
            escaped = r"\N"
        elif character == "\\":
            escaped = r"\\"
        elif character == "{":
            escaped = r"\{"
        elif character == "}":
            escaped = r"\}"
        elif unicodedata.category(character) != "Cc":
            escaped = character
        else:
            continue

        escaped_bytes = len(escaped.encode("utf-8"))
        if output_bytes + escaped_bytes > byte_budget:
            raise RepairError(f"generated ASS exceeds the {MAX_FILE_BYTES}-byte safety limit")
        output_bytes += escaped_bytes
        chunk.append(escaped)
        if len(chunk) >= 4096:
            output.append("".join(chunk))
            chunk.clear()
    if chunk:
        output.append("".join(chunk))
    return "".join(output)


def render_ass(comments: list[Danmaku]) -> tuple[str, int, dict[int, int]]:
    output: list[str] = []
    output_bytes = 0

    def append_bounded(part: str) -> None:
        nonlocal output_bytes
        part_bytes = len(part.encode("utf-8"))
        if output_bytes + part_bytes > MAX_FILE_BYTES:
            raise RepairError(f"generated ASS exceeds the {MAX_FILE_BYTES}-byte safety limit")
        output.append(part)
        output_bytes += part_bytes

    for part in [
        "[Script Info]\n",
        "ScriptType: v4.00+\n",
        "WrapStyle: 2\n",
        "ScaledBorderAndShadow: yes\n",
        f"PlayResX: {PLAY_RES_X}\n",
        f"PlayResY: {PLAY_RES_Y}\n",
        "\n",
        "[V4+ Styles]\n",
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n",
        "Style: Danmaku,Arial,42,&H00FFFFFF,&H000000FF,&H00000000,&H64000000,0,0,0,0,100,100,0,0,1,2,0,7,20,20,20,1\n",
        "\n",
        "[Events]\n",
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n",
    ]:
        append_bounded(part)
    dialogue_count = 0
    unsupported_modes: dict[int, int] = {}

    for index, comment in enumerate(comments):
        if comment.mode not in (1, 2, 3, 4, 5, 6):
            unsupported_modes[comment.mode] = unsupported_modes.get(comment.mode, 0) + 1
            continue
        duration = FIXED_DURATION_SECONDS if comment.mode in (4, 5) else SCROLL_DURATION_SECONDS
        start = _ass_timestamp(comment.start_seconds)
        end = _ass_timestamp(comment.start_seconds + duration)
        font_size = math.floor(comment.font_size + 0.5)
        color = _ass_color(comment.color)
        if comment.mode == 4:
            y = PLAY_RES_Y - TOP_MARGIN - (index % FIXED_LANES) * LINE_HEIGHT
            override = f"{{\\an2\\pos({PLAY_RES_X // 2},{y:.0f})\\fs{font_size}\\c{color}}}"
        elif comment.mode == 5:
            y = _top_lane_y(index, FIXED_LANES)
            override = f"{{\\an8\\pos({PLAY_RES_X // 2},{y:.0f})\\fs{font_size}\\c{color}}}"
        else:
            y = _top_lane_y(index, SCROLL_LANES)
            width = _estimated_text_width(comment.text, font_size)
            if comment.mode == 6:
                x1, x2 = -width, PLAY_RES_X + width
            else:
                x1, x2 = PLAY_RES_X + width, -width
            override = f"{{\\move({x1:.0f},{y:.0f},{x2:.0f},{y:.0f})\\fs{font_size}\\c{color}}}"
        prefix = f"Dialogue: 0,{start},{end},Danmaku,,0,0,0,,{override}"
        byte_budget = MAX_FILE_BYTES - output_bytes - len((prefix + "\n").encode("utf-8"))
        escaped = _ass_escape(comment.text, byte_budget)
        if not escaped:
            continue
        append_bounded(f"{prefix}{escaped}\n")
        dialogue_count += 1

    return "".join(output), dialogue_count, unsupported_modes


def _directory_flags() -> int:
    required = getattr(os, "O_DIRECTORY", 0)
    if not required or not getattr(os, "O_NOFOLLOW", 0):
        raise RepairError("this platform lacks safe no-follow directory operations")
    return os.O_RDONLY | required | os.O_NOFOLLOW


def _open_directory(path: str) -> tuple[int, tuple[int, int, int]]:
    try:
        fd = os.open(path, _directory_flags())
    except OSError as exc:
        raise RepairError(f"cannot open directory safely: {path}: {exc}") from exc
    file_stat = os.fstat(fd)
    if not stat.S_ISDIR(file_stat.st_mode):
        os.close(fd)
        raise RepairError(f"not a directory: {path}")
    return fd, _file_identity(file_stat)


def _read_file_at(directory_fd: int, name: str, *, missing_ok: bool = False) -> Snapshot | None:
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(name, flags, dir_fd=directory_fd)
    except FileNotFoundError:
        if missing_ok:
            return None
        raise
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise RepairError(f"refusing non-regular file: {name}")
        if before.st_size > MAX_FILE_BYTES:
            raise RepairError(f"file exceeds the {MAX_FILE_BYTES}-byte safety limit: {name}")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(fd, min(64 * 1024, MAX_FILE_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > MAX_FILE_BYTES:
                raise RepairError(f"file exceeds the {MAX_FILE_BYTES}-byte safety limit: {name}")
        after = os.fstat(fd)
        if _file_identity(before) != _file_identity(after):
            raise RepairError(f"file identity changed while reading: {name}")
        return Snapshot(b"".join(chunks), _file_identity(before), stat.S_IMODE(before.st_mode))
    finally:
        os.close(fd)


def _verify_snapshot(directory_fd: int, name: str, expected: Snapshot) -> None:
    current = _read_file_at(directory_fd, name)
    assert current is not None
    if current.identity != expected.identity or current.sha256 != expected.sha256:
        raise RepairError(f"file identity or content changed before commit: {name}")


def _verify_missing(directory_fd: int, name: str) -> None:
    current = _read_file_at(directory_fd, name, missing_ok=True)
    if current is not None:
        raise RepairError(f"expected output to remain absent before commit: {name}")


def _verify_directory_path(path: str, expected: tuple[int, int, int]) -> None:
    try:
        current_stat = os.stat(path)
    except OSError as exc:
        raise RepairError(f"input directory became unavailable before commit: {exc}") from exc
    if not stat.S_ISDIR(current_stat.st_mode) or _file_identity(current_stat) != expected:
        raise RepairError("input directory identity changed before commit")


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError("short write")
        view = view[written:]


def _write_new_file_at(directory_fd: int, name: str, data: bytes, mode: int = 0o600) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0)
    fd = os.open(name, flags, mode, dir_fd=directory_fd)
    try:
        os.fchmod(fd, mode)
        _write_all(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)


def _stage_file_at(directory_fd: int, data: bytes, mode: int) -> tuple[str, tuple[int, int, int]]:
    for _ in range(5):
        name = f".tvd-danmaku-{uuid.uuid4().hex}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0)
        try:
            fd = os.open(name, flags, 0o600, dir_fd=directory_fd)
            break
        except FileExistsError:
            continue
    else:
        raise RepairError("could not allocate a temporary output file")
    try:
        os.fchmod(fd, mode)
        _write_all(fd, data)
        os.fsync(fd)
        return name, _file_identity(os.fstat(fd))
    except Exception:
        try:
            os.unlink(name, dir_fd=directory_fd)
        except OSError:
            pass
        raise
    finally:
        os.close(fd)


def _open_backup_dir(path: str) -> tuple[int, int, str, str]:
    absolute = os.path.abspath(path)
    parent_path, name = os.path.split(absolute)
    if not name or name in (".", ".."):
        raise RepairError("backup directory must be a new directory path")
    parent_fd, _ = _open_directory(parent_path)
    try:
        os.mkdir(name, 0o700, dir_fd=parent_fd)
    except OSError as exc:
        os.close(parent_fd)
        if isinstance(exc, FileExistsError):
            raise RepairError(f"backup directory already exists: {absolute}") from exc
        raise RepairError(f"cannot create backup directory {absolute}: {exc}") from exc
    try:
        backup_fd = os.open(name, _directory_flags(), dir_fd=parent_fd)
    except OSError as exc:
        os.close(parent_fd)
        raise RepairError(f"cannot open new backup directory {absolute}: {exc}") from exc
    return parent_fd, backup_fd, name, absolute


def _backup_originals(
    backup_dir: str,
    source: Snapshot,
    old_ass: Snapshot | None,
    manifest: dict[str, Any],
) -> str:
    parent_fd, backup_fd, name, absolute = _open_backup_dir(backup_dir)
    try:
        _write_new_file_at(backup_fd, "original.xml", source.data)
        if old_ass is not None:
            _write_new_file_at(backup_fd, "original.ass", old_ass.data)
        manifest_data = (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        _write_new_file_at(backup_fd, "manifest.json", manifest_data)
        os.fsync(backup_fd)
        os.fsync(parent_fd)
    except Exception as exc:
        raise RepairError(f"backup creation failed; any partial backup is at {absolute}: {exc}") from exc
    finally:
        os.close(backup_fd)
        os.close(parent_fd)
    return absolute


def _rollback_one(
    directory_fd: int,
    name: str,
    staged_identity: tuple[int, int, int],
    generated: bytes,
    original: Snapshot | None,
) -> None:
    current = _read_file_at(directory_fd, name, missing_ok=original is None)
    if current is None:
        return
    if current.identity != staged_identity or current.sha256 != hashlib.sha256(generated).hexdigest():
        raise RepairError(f"cannot roll back {name}: installed output changed after replacement")
    if original is None:
        os.unlink(name, dir_fd=directory_fd)
    else:
        temp_name, _ = _stage_file_at(directory_fd, original.data, original.mode)
        os.replace(temp_name, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)


def repair_file(
    input_path: str,
    *,
    apply: bool = False,
    backup_dir: str | None = None,
    replace_existing_ass: bool = False,
) -> dict[str, Any]:
    absolute = os.path.abspath(input_path)
    parent_path, xml_name = os.path.split(absolute)
    if not xml_name or not xml_name.lower().endswith(".xml"):
        raise RepairError("input must be one explicit .xml file")
    ass_name = xml_name[:-4] + ".ass"

    directory_fd, directory_identity = _open_directory(parent_path)
    temp_names: list[str] = []
    try:
        source = _read_file_at(directory_fd, xml_name)
        assert source is not None
        compression, xml_data = decode_xml_payload(source.data)
        root = _parse_xml_document(xml_data)
        comments, invalid_count, parse_unsupported, normal_count = _parse_danmaku(root)
        ass_text, dialogue_count, render_unsupported = render_ass(comments)
        unsupported_modes = dict(parse_unsupported)
        for mode, count in render_unsupported.items():
            unsupported_modes[mode] = count
        if normal_count and dialogue_count == 0:
            raise RepairError("normal comments were found, but none could be rendered; refusing an empty ASS result")
        ass_data = ass_text.encode("utf-8")
        normalized_xml = ET.tostring(root, encoding="utf-8", xml_declaration=True) + b"\n"
        if len(normalized_xml) > MAX_FILE_BYTES or len(ass_data) > MAX_FILE_BYTES:
            raise RepairError(f"generated output exceeds the {MAX_FILE_BYTES}-byte safety limit")

        old_ass = _read_file_at(directory_fd, ass_name, missing_ok=True)
        if apply and old_ass is not None and old_ass.data and not replace_existing_ass:
            raise RepairError(f"refusing to overwrite non-empty ASS file without --replace-existing-ass: {ass_name}")

        report: dict[str, Any] = {
            "input": absolute,
            "ass_output": os.path.join(parent_path, ass_name),
            "compression": compression,
            "original_bytes": len(source.data),
            "normalized_xml_bytes": len(normalized_xml),
            "ass_bytes": len(ass_data),
            "comments_rendered": dialogue_count,
            "comments_invalid_or_empty": invalid_count,
            "unsupported_modes": {str(mode): count for mode, count in sorted(unsupported_modes.items())},
            "existing_ass_bytes": None if old_ass is None else len(old_ass.data),
            "applied": False,
        }
        if not apply:
            return report
        if backup_dir is None:
            raise RepairError("--backup-dir is required with --apply")

        backup_manifest = {
            "input_name": xml_name,
            "ass_name": ass_name,
            "compression": compression,
            "source_sha256": source.sha256,
            "source_bytes": len(source.data),
            "existing_ass_sha256": None if old_ass is None else old_ass.sha256,
            "existing_ass_bytes": None if old_ass is None else len(old_ass.data),
            "generated_dialogues": dialogue_count,
        }
        created_backup_dir = _backup_originals(backup_dir, source, old_ass, backup_manifest)
        report["backup_dir"] = created_backup_dir

        try:
            xml_temp, xml_temp_identity = _stage_file_at(directory_fd, normalized_xml, source.mode)
            temp_names.append(xml_temp)
            ass_mode = source.mode if old_ass is None else old_ass.mode
            ass_temp, ass_temp_identity = _stage_file_at(directory_fd, ass_data, ass_mode)
            temp_names.append(ass_temp)

            _verify_directory_path(parent_path, directory_identity)
            _verify_snapshot(directory_fd, xml_name, source)
            if old_ass is None:
                _verify_missing(directory_fd, ass_name)
            else:
                _verify_snapshot(directory_fd, ass_name, old_ass)
        except Exception as exc:
            raise RepairError(
                f"pre-commit check failed; target files were not changed by this repair, "
                f"and originals remain in {created_backup_dir}: {exc}"
            ) from exc

        committed: list[tuple[str, tuple[int, int, int], bytes, Snapshot | None]] = []
        try:
            os.replace(xml_temp, xml_name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
            temp_names.remove(xml_temp)
            committed.append((xml_name, xml_temp_identity, normalized_xml, source))
            os.replace(ass_temp, ass_name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
            temp_names.remove(ass_temp)
            committed.append((ass_name, ass_temp_identity, ass_data, old_ass))
            os.fsync(directory_fd)
        except Exception as commit_error:
            rollback_errors: list[str] = []
            for item in reversed(committed):
                try:
                    _rollback_one(directory_fd, *item)
                except Exception as rollback_error:  # preserve the backup and describe partial recovery
                    rollback_errors.append(str(rollback_error))
            if rollback_errors:
                raise RepairError(
                    f"commit failed ({commit_error}); rollback was incomplete ({'; '.join(rollback_errors)}); "
                    f"original files are preserved in {created_backup_dir}"
                ) from commit_error
            raise RepairError(f"commit failed and original files were restored: {commit_error}") from commit_error

        report["applied"] = True
        return report
    finally:
        for temp_name in temp_names:
            try:
                os.unlink(temp_name, dir_fd=directory_fd)
            except OSError:
                pass
        os.close(directory_fd)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_xml", help="one explicit Bilibili XML danmaku file")
    parser.add_argument("--apply", action="store_true", help="write normalized XML and regenerate the sibling ASS")
    parser.add_argument("--backup-dir", help="new directory for exact originals and a manifest; required with --apply")
    parser.add_argument(
        "--replace-existing-ass",
        action="store_true",
        help="allow replacing a non-empty sibling ASS file",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.backup_dir and not args.apply:
        parser.error("--backup-dir can only be used with --apply")
    try:
        report = repair_file(
            args.input_xml,
            apply=args.apply,
            backup_dir=args.backup_dir,
            replace_existing_ass=args.replace_existing_ass,
        )
    except (RepairError, OSError) as exc:
        print(f"repair failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not args.apply:
        print("Dry run only. Add --apply --backup-dir /new/path to write changes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
