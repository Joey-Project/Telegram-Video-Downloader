#!/usr/bin/env python3
"""Send one verified original file as a reply from the configured Telegram bot."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import pwd
import re
import select
import stat
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


MAX_REQUEST_BYTES = 64 * 1024
MAX_CREDENTIAL_BYTES = 8 * 1024
REQUEST_READ_TIMEOUT_SECONDS = 10
PART_SIZE = 512 * 1024
# Conservative fixed cap: Telegram documents 4,000 default/non-Premium parts.
MAX_FILE_PARTS = 4000
MAX_FILE_BYTES = PART_SIZE * MAX_FILE_PARTS
BIG_FILE_THRESHOLD = 10 * 1024 * 1024
MAX_DEADLINE_SECONDS = 30 * 60
RPC_TIMEOUT_SECONDS = 45
DISCONNECT_TIMEOUT_SECONDS = 5
UPLOAD_CONCURRENCY = 4
API_HASH = re.compile(r"[0-9a-fA-F]{32}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
REQUEST_FIELDS = frozenset(
    {
        "bot_token",
        "credentials_path",
        "chat_id",
        "reply_to_message_id",
        "random_id",
        "file_path",
        "file_name",
        "expected_sha256",
    }
)


class ReplyError(Exception):
    """A failure represented by one content-free result category."""

    def __init__(self, category: str):
        self.category = category
        super().__init__(category)


@dataclass(frozen=True)
class ReplyRequest:
    bot_token: str
    credentials_path: Path
    chat_id: int
    reply_to_message_id: int
    random_id: int
    file_path: Path
    file_name: str
    expected_sha256: str


@dataclass(frozen=True)
class TelegramBindings:
    client_type: type
    memory_session_type: type
    user_migrate_error_type: type[Exception]
    input_peer_user_type: type
    input_media_uploaded_document_type: type
    document_attribute_filename_type: type
    input_reply_to_message_type: type
    send_media_request_type: type
    save_file_part_request_type: type
    save_big_file_part_request_type: type
    input_file_type: type
    input_file_big_type: type


def _load_telethon() -> TelegramBindings:
    try:
        from telethon import TelegramClient
        from telethon.errors import UserMigrateError
        from telethon.sessions import MemorySession
        from telethon.tl.functions.messages import SendMediaRequest
        from telethon.tl.functions.upload import (
            SaveBigFilePartRequest,
            SaveFilePartRequest,
        )
        from telethon.tl.types import (
            DocumentAttributeFilename,
            InputFile,
            InputFileBig,
            InputMediaUploadedDocument,
            InputPeerUser,
            InputReplyToMessage,
        )
    except Exception:
        raise ReplyError("dependency_unavailable") from None
    return TelegramBindings(
        client_type=TelegramClient,
        memory_session_type=MemorySession,
        user_migrate_error_type=UserMigrateError,
        input_peer_user_type=InputPeerUser,
        input_media_uploaded_document_type=InputMediaUploadedDocument,
        document_attribute_filename_type=DocumentAttributeFilename,
        input_reply_to_message_type=InputReplyToMessage,
        send_media_request_type=SendMediaRequest,
        save_file_part_request_type=SaveFilePartRequest,
        save_big_file_part_request_type=SaveBigFilePartRequest,
        input_file_type=InputFile,
        input_file_big_type=InputFileBig,
    )


def _real_home() -> Path:
    try:
        home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    except (KeyError, OSError):
        raise ReplyError("credentials_unavailable") from None
    if not home.is_absolute():
        raise ReplyError("credentials_unavailable")
    return home


def _path_text(value: Any, category: str = "invalid_request") -> Path:
    if not isinstance(value, str) or not value or "\0" in value:
        raise ReplyError(category)
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        raise ReplyError(category) from None
    path = Path(value)
    if len(encoded) > 4096 or not path.is_absolute() or ".." in path.parts:
        raise ReplyError(category)
    return path


def validate_request(value: Any, home: Path | None = None) -> ReplyRequest:
    if not isinstance(value, dict) or set(value) != REQUEST_FIELDS:
        raise ReplyError("invalid_request")
    home = home or _real_home()
    token = value["bot_token"]
    chat_id = value["chat_id"]
    reply_id = value["reply_to_message_id"]
    random_id = value["random_id"]
    file_name = value["file_name"]
    expected_sha256 = value["expected_sha256"]
    if (
        not isinstance(token, str)
        or not token
        or len(token) > 256
        or any(character.isspace() for character in token)
    ):
        raise ReplyError("invalid_request")
    if type(chat_id) is not int or not 0 < chat_id <= (1 << 63) - 1:
        raise ReplyError("invalid_request")
    if type(reply_id) is not int or not 0 < reply_id <= (1 << 31) - 1:
        raise ReplyError("invalid_request")
    if type(random_id) is not int or not -(1 << 63) <= random_id < (1 << 63) or random_id == 0:
        raise ReplyError("invalid_request")
    if (
        not isinstance(file_name, str)
        or not file_name
        or file_name in {".", ".."}
        or "/" in file_name
        or "\\" in file_name
        or any(ord(character) < 32 or ord(character) == 127 for character in file_name)
    ):
        raise ReplyError("invalid_request")
    try:
        if len(file_name.encode("utf-8")) > 255:
            raise ReplyError("invalid_request")
    except UnicodeError:
        raise ReplyError("invalid_request") from None
    if not isinstance(expected_sha256, str) or not SHA256.fullmatch(expected_sha256):
        raise ReplyError("invalid_request")

    credentials_path = _path_text(value["credentials_path"], "credentials_unavailable")
    expected_credentials = home / ".config/telegram-video-downloader/mtproto.json"
    if credentials_path != expected_credentials:
        raise ReplyError("invalid_request")

    return ReplyRequest(
        bot_token=token,
        credentials_path=credentials_path,
        chat_id=chat_id,
        reply_to_message_id=reply_id,
        random_id=random_id,
        file_path=_path_text(value["file_path"]),
        file_name=file_name,
        expected_sha256=expected_sha256,
    )


def _open_directory_path(path: Path) -> int:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptor = os.open(path.anchor, flags)
    try:
        for component in path.parts[1:]:
            next_descriptor = os.open(component, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
        return descriptor
    except OSError:
        os.close(descriptor)
        raise


def _check_private_directory(descriptor: int) -> None:
    info = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise ReplyError("storage_unavailable")


def _read_bounded(descriptor: int, limit: int) -> bytes:
    chunks: list[bytes] = []
    remaining = limit + 1
    while remaining:
        chunk = os.read(descriptor, min(remaining, 4096))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    result = b"".join(chunks)
    if len(result) > limit:
        raise ReplyError("credentials_unavailable")
    return result


def read_credentials(path: Path, home: Path | None = None) -> dict[str, Any]:
    home = home or _real_home()
    if path != home / ".config/telegram-video-downloader/mtproto.json":
        raise ReplyError("credentials_unavailable")
    directory_fd = None
    descriptor = None
    try:
        directory_fd = _open_directory_path(path.parent)
        _check_private_directory(directory_fd)
        descriptor = os.open(
            path.name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
            dir_fd=directory_fd,
        )
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
            or info.st_size > MAX_CREDENTIAL_BYTES
        ):
            raise ReplyError("credentials_unavailable")
        raw = _read_bounded(descriptor, MAX_CREDENTIAL_BYTES)
        values = json.loads(raw.decode("utf-8"))
    except ReplyError:
        raise
    except Exception:
        raise ReplyError("credentials_unavailable") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if directory_fd is not None:
            os.close(directory_fd)
    if (
        not isinstance(values, dict)
        or set(values) != {"api_id", "api_hash"}
        or type(values.get("api_id")) is not int
        or not 0 < values["api_id"] <= (1 << 31) - 1
        or not isinstance(values.get("api_hash"), str)
        or not API_HASH.fullmatch(values["api_hash"])
    ):
        raise ReplyError("credentials_unavailable")
    return {"api_id": values["api_id"], "api_hash": values["api_hash"]}


def _open_source_file(path: Path) -> tuple[int, int]:
    directory_fd = None
    descriptor = None
    try:
        directory_fd = _open_directory_path(path.parent)
        _check_private_directory(directory_fd)
        descriptor = os.open(
            path.name,
            os.O_RDONLY
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NONBLOCK", 0),
            dir_fd=directory_fd,
        )
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
        ):
            raise ReplyError("file_unavailable")
        if info.st_size <= 0:
            raise ReplyError("file_unavailable")
        if info.st_size > MAX_FILE_BYTES:
            raise ReplyError("file_too_large")
        return descriptor, info.st_size
    except ReplyError:
        if descriptor is not None:
            os.close(descriptor)
        raise
    except Exception:
        if descriptor is not None:
            os.close(descriptor)
        raise ReplyError("file_unavailable") from None
    finally:
        if directory_fd is not None:
            os.close(directory_fd)


def _safe_rpc_error(error: BaseException, phase: str) -> ReplyError:
    if isinstance(error, ReplyError):
        return error
    name = type(error).__name__.casefold()
    if "randomidduplicate" in name:
        return ReplyError("delivery_unknown")
    if "flood" in name:
        return ReplyError("rate_limited")
    if isinstance(error, (asyncio.TimeoutError, TimeoutError)):
        return ReplyError("delivery_unknown" if phase == "send" else "request_timeout")
    return ReplyError("delivery_unknown" if phase == "send" else "telegram_error")


def _bot_id_from_token(token: str) -> int | None:
    prefix, separator, secret = token.partition(":")
    if (
        not separator
        or not prefix.isascii()
        or not prefix.isdigit()
        or not secret
    ):
        return None
    try:
        bot_id = int(prefix)
    except ValueError:
        return None
    return bot_id if 0 < bot_id <= (1 << 63) - 1 else None


async def _invoke(client: Any, request: Any, deadline: float, phase: str) -> Any:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ReplyError("deadline_exceeded")
    try:
        return await asyncio.wait_for(client(request), min(RPC_TIMEOUT_SECONDS, remaining))
    except asyncio.CancelledError:
        raise
    except BaseException as error:
        raise _safe_rpc_error(error, phase) from None


async def _flush_uploads(pending: list[asyncio.Task[Any]]) -> None:
    if not pending:
        return
    results = await asyncio.gather(*pending, return_exceptions=True)
    pending.clear()
    for result in results:
        if isinstance(result, asyncio.CancelledError):
            raise result
        if isinstance(result, BaseException):
            raise result
        if result is False:
            raise ReplyError("telegram_error")


async def _upload_parts(
    client: Any,
    descriptor: int,
    size: int,
    expected_sha256: str,
    bindings: TelegramBindings,
    deadline: float,
) -> tuple[int, int, str, str]:
    part_count = (size + PART_SIZE - 1) // PART_SIZE
    if not 1 <= part_count <= MAX_FILE_PARTS:
        raise ReplyError("file_too_large")
    file_id = int.from_bytes(os.urandom(8), "big") & ((1 << 63) - 1)
    if file_id == 0:
        file_id = 1
    sha256 = hashlib.sha256()
    md5 = hashlib.md5(usedforsecurity=False)
    pending: list[asyncio.Task[Any]] = []
    request_type = (
        bindings.save_big_file_part_request_type
        if size > BIG_FILE_THRESHOLD
        else bindings.save_file_part_request_type
    )
    for part_index in range(part_count):
        offset = part_index * PART_SIZE
        length = min(PART_SIZE, size - offset)
        try:
            part = await asyncio.to_thread(os.pread, descriptor, length, offset)
        except OSError:
            raise ReplyError("file_unavailable") from None
        if len(part) != length:
            raise ReplyError("file_changed")
        sha256.update(part)
        md5.update(part)
        if size > BIG_FILE_THRESHOLD:
            request = request_type(
                file_id=file_id,
                file_part=part_index,
                file_total_parts=part_count,
                bytes=part,
            )
        else:
            request = request_type(file_id=file_id, file_part=part_index, bytes=part)
        pending.append(asyncio.create_task(_invoke(client, request, deadline, "upload")))
        if len(pending) >= UPLOAD_CONCURRENCY:
            await _flush_uploads(pending)
    await _flush_uploads(pending)
    digest = sha256.hexdigest()
    if digest != expected_sha256:
        raise ReplyError("file_integrity_error")
    return file_id, part_count, digest, md5.hexdigest()


async def _authenticate_bot(client: Any, token: str, bindings: TelegramBindings) -> Any:
    try:
        await client.start(bot_token=token)
    except bindings.user_migrate_error_type as migration:
        # Telethon switches DC before exhausting request_retries=0. Retry only
        # authentication, after confirming the switch completed.
        if migration.new_dc <= 0 or client.session.dc_id != migration.new_dc:
            raise
        await client.start(bot_token=token)
    return await client.get_me()


async def _send_reply(
    request: ReplyRequest,
    credentials: dict[str, Any],
    bindings: TelegramBindings | None = None,
) -> None:
    bindings = bindings or _load_telethon()
    descriptor, size = _open_source_file(request.file_path)
    client = None
    deadline = time.monotonic() + MAX_DEADLINE_SECONDS
    try:
        token_bot_id = _bot_id_from_token(request.bot_token)
        if token_bot_id is None:
            raise ReplyError("invalid_request")
        logging.getLogger("telethon").disabled = True
        client = bindings.client_type(
            bindings.memory_session_type(),
            credentials["api_id"],
            credentials["api_hash"],
            timeout=RPC_TIMEOUT_SECONDS,
            request_retries=0,
            connection_retries=0,
            retry_delay=0,
            auto_reconnect=False,
            flood_sleep_threshold=0,
            raise_last_call_error=True,
            receive_updates=False,
            catch_up=False,
        )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ReplyError("deadline_exceeded")
        try:
            bot = await asyncio.wait_for(
                _authenticate_bot(client, request.bot_token, bindings),
                min(RPC_TIMEOUT_SECONDS, remaining),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            raise _safe_rpc_error(error, "auth") from None
        if bot is None or getattr(bot, "bot", None) is not True:
            raise ReplyError("bot_identity_invalid")
        if getattr(bot, "id", None) != token_bot_id:
            raise ReplyError("bot_identity_invalid")

        uploaded_id, part_count, digest, md5_hex = await _upload_parts(
            client,
            descriptor,
            size,
            request.expected_sha256,
            bindings,
            deadline,
        )
        if digest != request.expected_sha256:
            raise ReplyError("file_integrity_error")
        if size > BIG_FILE_THRESHOLD:
            input_file = bindings.input_file_big_type(
                id=uploaded_id,
                parts=part_count,
                name=request.file_name,
            )
        else:
            input_file = bindings.input_file_type(
                id=uploaded_id,
                parts=part_count,
                name=request.file_name,
                md5_checksum=md5_hex,
            )
        peer = bindings.input_peer_user_type(user_id=request.chat_id, access_hash=0)
        media = bindings.input_media_uploaded_document_type(
            file=input_file,
            mime_type="application/octet-stream",
            attributes=[bindings.document_attribute_filename_type(file_name=request.file_name)],
            force_file=True,
        )
        reply_to = bindings.input_reply_to_message_type(
            reply_to_msg_id=request.reply_to_message_id
        )
        send_request = bindings.send_media_request_type(
            peer=peer,
            reply_to=reply_to,
            media=media,
            message="",
            random_id=request.random_id,
        )
        await _invoke(client, send_request, deadline, "send")
    finally:
        if client is not None:
            try:
                await asyncio.wait_for(client.disconnect(), DISCONNECT_TIMEOUT_SECONDS)
            except BaseException:
                pass
        os.close(descriptor)


async def send_reply(request: ReplyRequest, credentials: dict[str, Any], bindings: TelegramBindings | None = None) -> None:
    try:
        await asyncio.wait_for(
            _send_reply(request, credentials, bindings),
            timeout=MAX_DEADLINE_SECONDS,
        )
    except asyncio.TimeoutError:
        raise ReplyError("deadline_exceeded") from None


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate field")
        result[key] = value
    return result


def read_request(stream: Any) -> Any:
    try:
        descriptor = stream.fileno()
    except Exception:
        descriptor = None
    if descriptor is None:
        try:
            raw = stream.read(MAX_REQUEST_BYTES + 1)
        except Exception:
            raise ReplyError("invalid_request") from None
    else:
        deadline = time.monotonic() + REQUEST_READ_TIMEOUT_SECONDS
        chunks: list[bytes] = []
        total = 0
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ReplyError("request_timeout")
            try:
                ready, _, _ = select.select([descriptor], [], [], remaining)
                if not ready:
                    raise ReplyError("request_timeout")
                chunk = os.read(descriptor, min(4096, MAX_REQUEST_BYTES + 1 - total))
            except ReplyError:
                raise
            except Exception:
                raise ReplyError("invalid_request") from None
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > MAX_REQUEST_BYTES:
                raise ReplyError("invalid_request")
        raw = b"".join(chunks)
    if len(raw) > MAX_REQUEST_BYTES:
        raise ReplyError("invalid_request")
    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except Exception:
        raise ReplyError("invalid_request") from None


def main() -> int:
    logging.disable(logging.CRITICAL)
    try:
        request = validate_request(read_request(sys.stdin.buffer))
        credentials = read_credentials(request.credentials_path)
        asyncio.run(send_reply(request, credentials))
        result = {"ok": True}
    except ReplyError as error:
        result = {"ok": False, "error": error.category}
    except BaseException:
        result = {"ok": False, "error": "internal_error"}
    try:
        sys.stdout.write(json.dumps(result, separators=(",", ":")) + "\n")
        sys.stdout.flush()
    except Exception:
        return 1
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
