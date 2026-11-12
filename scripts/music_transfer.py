#!/usr/bin/env python3
"""One-shot, bounded Telegram file_id to MTProto transfer helper."""

from __future__ import annotations

import asyncio
import base64
from collections import defaultdict
import fcntl
import hashlib
import ipaddress
import json
import logging
import os
import pwd
import re
import stat
import struct
import sys
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, BinaryIO


TELETHON_VERSION = "1.45.0"
CHUNK_SIZE = 1024 * 1024
MAX_FILE_SIZE = 512 * 1024 * 1024
MAX_FILE_ID_BYTES = 8192
MAX_REQUEST_BYTES = 64 * 1024
MAX_CREDENTIAL_BYTES = 8 * 1024
MAX_DEADLINE_SECONDS = 1800
RPC_TIMEOUT_SECONDS = 45
MAX_RPC_CALLS = MAX_FILE_SIZE // CHUNK_SIZE + 24
BOOTSTRAP_DC = 2
BOOTSTRAP_IP = "149.154.167.51"
PORT = 443
FILE_REFERENCE_FLAG = 1 << 25
WEB_LOCATION_FLAG = 1 << 24
PHOTO_TYPES = {0, 1, 2, 11, 12}
REQUEST_FIELDS = frozenset(
    {
        "schema",
        "bot_token",
        "credentials_path",
        "file_id",
        "identity_sha256",
        "expected_size",
        "staging_root",
        "destination",
        "deadline_seconds",
    }
)
HEX_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
API_HASH = re.compile(r"[0-9a-fA-F]{32}\Z")


class ErrorCode(str, Enum):
    INVALID_REQUEST = "invalid_request"
    CREDENTIALS_UNAVAILABLE = "credentials_unavailable"
    UNSUPPORTED_FILE = "unsupported_file"
    REFRESH_REQUIRED = "refresh_required"
    RATE_LIMITED = "rate_limited"
    DEADLINE_EXCEEDED = "deadline_exceeded"
    REQUEST_TIMEOUT = "request_timeout"
    TRANSPORT_ERROR = "transport_error"
    RPC_ERROR = "rpc_error"
    RPC_DENIED = "rpc_denied"
    UNEXPECTED_RESPONSE = "unexpected_response"
    INTEGRITY_ERROR = "integrity_error"
    FILE_EXISTS = "file_exists"
    MATERIALIZATION_PENDING = "materialization_pending"
    STORAGE_ERROR = "storage_error"
    BUSY = "busy"
    INTERNAL_ERROR = "internal_error"


class TransferError(Exception):
    """A deliberately content-free transfer failure."""

    def __init__(self, code: ErrorCode):
        self.code = code
        super().__init__(code.value)


class MigrationRequired(TransferError):
    def __init__(self, new_dc: int):
        self.new_dc = new_dc
        super().__init__(ErrorCode.TRANSPORT_ERROR)


class FloodWaitRequired(TransferError):
    def __init__(self, seconds: int):
        self.seconds = seconds
        super().__init__(ErrorCode.RATE_LIMITED)


class RefreshRequired(TransferError):
    def __init__(self):
        super().__init__(ErrorCode.REFRESH_REQUIRED)


@dataclass(frozen=True)
class DecodedFileId:
    file_type: int
    dc_id: int
    file_reference: bytes
    media_id: int
    access_hash: int


@dataclass(frozen=True)
class TransferRequest:
    bot_token: str
    credentials_path: Path
    file_id: str
    identity_sha256: str
    expected_size: int
    staging_root: Path
    job_id: str
    destination: Path
    deadline_seconds: int


@dataclass(frozen=True)
class ProtocolBindings:
    sender_type: type
    auth_key_type: type
    connection_type: type
    invoke_without_updates_type: type
    invoke_with_layer_type: type
    init_connection_type: type
    get_config_type: type
    import_bot_authorization_type: type
    export_authorization_type: type
    import_authorization_type: type
    get_file_type: type
    input_document_file_location_type: type
    config_response_type: type
    exported_authorization_response_type: type
    authorization_response_type: type
    file_response_type: type
    layer: int


def _load_protocol() -> ProtocolBindings:
    try:
        from telethon import __version__
        from telethon.crypto import AuthKey
        from telethon.network.connection.tcpfull import ConnectionTcpFull
        from telethon.network.mtprotosender import MTProtoSender
        from telethon.tl.alltlobjects import LAYER
        from telethon.tl.functions import (
            InitConnectionRequest,
            InvokeWithoutUpdatesRequest,
            InvokeWithLayerRequest,
        )
        from telethon.tl.functions.auth import (
            ExportAuthorizationRequest,
            ImportAuthorizationRequest,
            ImportBotAuthorizationRequest,
        )
        from telethon.tl.functions.help import GetConfigRequest
        from telethon.tl.functions.upload import GetFileRequest
        from telethon.tl.types import Config, InputDocumentFileLocation
        from telethon.tl.types.auth import Authorization, ExportedAuthorization
        from telethon.tl.types.upload import File
    except Exception:
        raise TransferError(ErrorCode.TRANSPORT_ERROR) from None
    if __version__ != TELETHON_VERSION:
        raise TransferError(ErrorCode.TRANSPORT_ERROR)
    return ProtocolBindings(
        sender_type=MTProtoSender,
        auth_key_type=AuthKey,
        connection_type=ConnectionTcpFull,
        invoke_without_updates_type=InvokeWithoutUpdatesRequest,
        invoke_with_layer_type=InvokeWithLayerRequest,
        init_connection_type=InitConnectionRequest,
        get_config_type=GetConfigRequest,
        import_bot_authorization_type=ImportBotAuthorizationRequest,
        export_authorization_type=ExportAuthorizationRequest,
        import_authorization_type=ImportAuthorizationRequest,
        get_file_type=GetFileRequest,
        input_document_file_location_type=InputDocumentFileLocation,
        config_response_type=Config,
        exported_authorization_response_type=ExportedAuthorization,
        authorization_response_type=Authorization,
        file_response_type=File,
        layer=LAYER,
    )


def _rle_zero_decode(data: bytes) -> bytes:
    output = bytearray()
    index = 0
    while index < len(data):
        byte = data[index]
        index += 1
        if byte == 0:
            if index >= len(data):
                raise TransferError(ErrorCode.UNSUPPORTED_FILE)
            count = data[index]
            index += 1
            if count == 0:
                raise TransferError(ErrorCode.UNSUPPORTED_FILE)
            output.extend(b"\0" * count)
        else:
            output.append(byte)
        if len(output) > MAX_FILE_ID_BYTES:
            raise TransferError(ErrorCode.UNSUPPORTED_FILE)
    return bytes(output)


def decode_file_id(file_id: str) -> DecodedFileId:
    """Decode the bounded Bot API document/audio FileId representation."""
    try:
        if not isinstance(file_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", file_id):
            raise ValueError
        compact = base64.b64decode(
            file_id + "=" * (-len(file_id) % 4), altchars=b"-_", validate=True
        )
        decoded = _rle_zero_decode(compact)
        if len(decoded) < 14:
            raise ValueError
        major, minor = decoded[-1], decoded[-2]
        payload = memoryview(decoded[:-2])
        file_type_flags, dc_id = struct.unpack_from("<ii", payload, 0)
        has_web_location = bool(file_type_flags & WEB_LOCATION_FLAG)
        has_file_reference = bool(file_type_flags & FILE_REFERENCE_FLAG)
        file_type = file_type_flags & ~(WEB_LOCATION_FLAG | FILE_REFERENCE_FLAG)
        if has_web_location or file_type in PHOTO_TYPES:
            raise TransferError(ErrorCode.UNSUPPORTED_FILE)
        offset = 8
        file_reference = b""
        if has_file_reference:
            first = payload[offset]
            offset += 1
            if first == 254:
                size = int.from_bytes(payload[offset : offset + 3], "little")
                offset += 3
                padding = -size % 4
            else:
                size = first
                padding = -(size + 1) % 4
            if size > MAX_FILE_ID_BYTES or offset + size + padding > len(payload):
                raise ValueError
            file_reference = bytes(payload[offset : offset + size])
            offset += size + padding
        media_id, access_hash = struct.unpack_from("<qq", payload, offset)
        offset += 16
        remaining = len(payload) - offset
        if remaining not in (0, 8):
            raise ValueError
        if remaining == 8:
            inner_minor, inner_major = struct.unpack_from("<ii", payload, offset)
            if inner_minor != minor or inner_major != major:
                raise ValueError
        if major < 4 or dc_id not in range(1, 6):
            raise ValueError
        if file_type not in {3, 4, 5, 6, 7, 8, 9, 10, 13, 14, 15, 16, 17}:
            raise TransferError(ErrorCode.UNSUPPORTED_FILE)
        if not has_file_reference or not file_reference:
            raise TransferError(ErrorCode.UNSUPPORTED_FILE)
        return DecodedFileId(file_type, dc_id, file_reference, media_id, access_hash)
    except TransferError:
        raise
    except Exception:
        raise TransferError(ErrorCode.UNSUPPORTED_FILE) from None


def _is_update_rpc(request: Any) -> bool:
    name = type(request).__name__
    module = type(request).__module__
    return module.startswith("telethon.tl.functions.updates") or name.startswith(
        ("Update", "Updates", "GetDifference", "GetState")
    )


def _request_leaf(request: Any, bindings: ProtocolBindings) -> Any:
    while type(request) in {
        bindings.invoke_without_updates_type,
        bindings.invoke_with_layer_type,
        bindings.init_connection_type,
    }:
        request = request.query
    return request


def validate_rpc_tree(request: Any, bindings: ProtocolBindings) -> None:
    """Require the exact audited RPC classes under an outer no-updates wrapper."""
    no_updates = False
    in_layer = False
    current = request
    while True:
        if _is_update_rpc(current):
            raise TransferError(ErrorCode.RPC_DENIED)
        current_type = type(current)
        if current_type is bindings.invoke_without_updates_type:
            if no_updates or in_layer:
                raise TransferError(ErrorCode.RPC_DENIED)
            no_updates = True
            current = current.query
            continue
        if current_type is bindings.invoke_with_layer_type:
            if not no_updates or in_layer or current.layer != bindings.layer:
                raise TransferError(ErrorCode.RPC_DENIED)
            in_layer = True
            current = current.query
            continue
        if current_type is bindings.init_connection_type:
            if not no_updates or not in_layer:
                raise TransferError(ErrorCode.RPC_DENIED)
            in_layer = False
            current = current.query
            continue
        if not no_updates or in_layer:
            raise TransferError(ErrorCode.RPC_DENIED)
        allowed = {
            bindings.get_config_type,
            bindings.import_bot_authorization_type,
            bindings.export_authorization_type,
            bindings.import_authorization_type,
            bindings.get_file_type,
        }
        if current_type not in allowed:
            raise TransferError(ErrorCode.RPC_DENIED)
        if current_type is bindings.get_file_type:
            if (
                type(current.offset) is not int
                or current.offset < 0
                or current.limit != CHUNK_SIZE
                or current.precise is not True
                or current.cdn_supported is not False
                or type(current.location) is not bindings.input_document_file_location_type
            ):
                raise TransferError(ErrorCode.RPC_DENIED)
        return


def _contains_forbidden_response(value: Any, seen: set[int] | None = None) -> bool:
    if value is None or isinstance(value, (str, bytes, int, float, bool)):
        return False
    seen = seen or set()
    identity = id(value)
    if identity in seen:
        return False
    seen.add(identity)
    name = type(value).__name__.replace("_", "").lower()
    module = type(value).__module__
    if (
        name.startswith(("update", "updates"))
        or "cdnredirect" in name
        or module.startswith("telethon.tl.types.updates")
    ):
        return True
    if isinstance(value, dict):
        children = value.values()
    elif isinstance(value, (list, tuple, set)):
        children = value
    else:
        children = list(vars(value).values()) if hasattr(value, "__dict__") else []
        for cls in type(value).__mro__:
            slots = getattr(cls, "__slots__", ())
            if isinstance(slots, str):
                slots = (slots,)
            for name in slots:
                if name.startswith("_"):
                    continue
                try:
                    children.append(getattr(value, name))
                except Exception:
                    continue
    return any(_contains_forbidden_response(child, seen) for child in children)


def _validate_response(request: Any, response: Any, bindings: ProtocolBindings) -> None:
    if _contains_forbidden_response(response):
        raise TransferError(ErrorCode.UNEXPECTED_RESPONSE)
    leaf = _request_leaf(request, bindings)
    if type(leaf) is bindings.get_config_type:
        expected = bindings.config_response_type
    elif type(leaf) is bindings.import_bot_authorization_type:
        expected = bindings.authorization_response_type
    elif type(leaf) is bindings.export_authorization_type:
        expected = bindings.exported_authorization_response_type
    elif type(leaf) is bindings.import_authorization_type:
        expected = bindings.authorization_response_type
    elif type(leaf) is bindings.get_file_type:
        expected = bindings.file_response_type
        if "redirect" in type(response).__name__.lower() or "cdn" in type(response).__name__.lower():
            raise TransferError(ErrorCode.UNEXPECTED_RESPONSE)
    else:
        raise TransferError(ErrorCode.RPC_DENIED)
    if type(response) is not expected:
        raise TransferError(ErrorCode.UNEXPECTED_RESPONSE)


async def guarded_send(
    sender: Any,
    request: Any,
    bindings: ProtocolBindings,
    request_counter: list[int],
) -> Any:
    validate_rpc_tree(request, bindings)
    request_counter[0] += 1
    if request_counter[0] > MAX_RPC_CALLS:
        raise TransferError(ErrorCode.RPC_DENIED)
    try:
        result = await asyncio.wait_for(sender.send(request), RPC_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        raise TransferError(ErrorCode.REQUEST_TIMEOUT) from None
    except (TransferError, asyncio.CancelledError):
        raise
    except Exception as exc:
        name = type(exc).__name__.replace("_", "").lower()
        if "filereference" in name or name in {"filereferenceexpired", "filereferenceinvalid"}:
            raise RefreshRequired() from None
        migration_dc = getattr(exc, "new_dc", None)
        if type(migration_dc) is int:
            leaf = _request_leaf(request, bindings)
            if (
                type(leaf) is bindings.get_file_type
                and "filemigrate" not in name
            ):
                raise TransferError(ErrorCode.RPC_ERROR) from None
            raise MigrationRequired(migration_dc) from None
        if "floodwait" in name:
            seconds = getattr(exc, "seconds", None)
            raise FloodWaitRequired(seconds if type(seconds) is int else 0) from None
        raise TransferError(ErrorCode.RPC_ERROR) from None
    _validate_response(request, result, bindings)
    return result


def _no_duplicate_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _reject_json_constant(_: str) -> None:
    raise ValueError("invalid JSON constant")


def read_request(stream: BinaryIO) -> dict[str, Any]:
    try:
        raw = stream.read(MAX_REQUEST_BYTES + 1)
        if len(raw) > MAX_REQUEST_BYTES:
            raise TransferError(ErrorCode.INVALID_REQUEST)
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_no_duplicate_object,
            parse_constant=_reject_json_constant,
        )
    except TransferError:
        raise
    except Exception:
        raise TransferError(ErrorCode.INVALID_REQUEST) from None
    if not isinstance(value, dict) or set(value) != REQUEST_FIELDS:
        raise TransferError(ErrorCode.INVALID_REQUEST)
    return value


def _real_home() -> Path:
    try:
        home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    except (KeyError, OSError):
        raise TransferError(ErrorCode.INVALID_REQUEST) from None
    if not home.is_absolute():
        raise TransferError(ErrorCode.INVALID_REQUEST)
    return home


def _valid_path_text(value: Any) -> Path:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 4096:
        raise TransferError(ErrorCode.INVALID_REQUEST)
    if "\0" in value:
        raise TransferError(ErrorCode.INVALID_REQUEST)
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise TransferError(ErrorCode.INVALID_REQUEST)
    return path


def validate_request(value: dict[str, Any], home: Path | None = None) -> TransferRequest:
    home = home or _real_home()
    schema = value.get("schema")
    size = value.get("expected_size")
    deadline = value.get("deadline_seconds")
    token = value.get("bot_token")
    file_id = value.get("file_id")
    identity = value.get("identity_sha256")
    if type(schema) is not int or schema != 1:
        raise TransferError(ErrorCode.INVALID_REQUEST)
    if type(size) is not int or not 0 < size <= MAX_FILE_SIZE:
        raise TransferError(ErrorCode.INVALID_REQUEST)
    if type(deadline) is not int or not 1 <= deadline <= MAX_DEADLINE_SECONDS:
        raise TransferError(ErrorCode.INVALID_REQUEST)
    if not isinstance(token, str) or not token or len(token) > 256 or any(c.isspace() for c in token):
        raise TransferError(ErrorCode.INVALID_REQUEST)
    if not isinstance(file_id, str) or len(file_id) > MAX_FILE_ID_BYTES:
        raise TransferError(ErrorCode.INVALID_REQUEST)
    if not isinstance(identity, str) or not HEX_SHA256.fullmatch(identity):
        raise TransferError(ErrorCode.INVALID_REQUEST)
    credential_path = _valid_path_text(value.get("credentials_path"))
    expected_credentials = home / ".config/telegram-video-downloader/mtproto.json"
    if credential_path != expected_credentials:
        raise TransferError(ErrorCode.INVALID_REQUEST)
    staging_root = _valid_path_text(value.get("staging_root"))
    destination = _valid_path_text(value.get("destination"))
    job_id = destination.parent.name
    if (
        staging_root.name != "music"
        or staging_root.parent.name != ".telegram-video-downloader-staging"
        or destination.parent != staging_root / job_id
        or not re.fullmatch(r"mj1-[0-9a-f]{24}", job_id)
        or destination.name not in {"original.m4a", "original.mp3", "original.aac"}
    ):
        raise TransferError(ErrorCode.INVALID_REQUEST)
    try:
        root_info = os.stat(staging_root, follow_symlinks=False)
        if not stat.S_ISDIR(root_info.st_mode):
            raise TransferError(ErrorCode.STORAGE_ERROR)
        # Canonicalize trusted ancestors such as macOS /tmp -> /private/tmp;
        # all opened components from the selected root downward remain no-follow.
        staging_root = staging_root.resolve(strict=True)
        destination = staging_root / job_id / destination.name
    except TransferError:
        raise
    except Exception:
        raise TransferError(ErrorCode.STORAGE_ERROR) from None
    return TransferRequest(
        bot_token=token,
        credentials_path=credential_path,
        file_id=file_id,
        identity_sha256=identity,
        expected_size=size,
        staging_root=staging_root,
        job_id=job_id,
        destination=destination,
        deadline_seconds=deadline,
    )


def _open_directory_path(path: Path) -> int:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
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


def _check_private_dir(descriptor: int) -> None:
    info = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise TransferError(ErrorCode.STORAGE_ERROR)


def _read_credentials(path: Path, home: Path) -> dict[str, Any]:
    credentials_dir = path.parent
    directory_fd = None
    descriptor = None
    try:
        directory_fd = _open_directory_path(credentials_dir)
        _check_private_dir(directory_fd)
        descriptor = os.open(
            path.name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
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
            raise TransferError(ErrorCode.CREDENTIALS_UNAVAILABLE)
        raw = os.read(descriptor, MAX_CREDENTIAL_BYTES + 1)
        if len(raw) > MAX_CREDENTIAL_BYTES:
            raise TransferError(ErrorCode.CREDENTIALS_UNAVAILABLE)
        values = json.loads(raw.decode("utf-8"))
    except TransferError:
        raise
    except Exception:
        raise TransferError(ErrorCode.CREDENTIALS_UNAVAILABLE) from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if directory_fd is not None:
            os.close(directory_fd)
    if (
        not isinstance(values, dict)
        or set(values) != {"api_id", "api_hash"}
        or type(values.get("api_id")) is not int
        or values["api_id"] <= 0
        or not isinstance(values.get("api_hash"), str)
        or not API_HASH.fullmatch(values["api_hash"])
    ):
        raise TransferError(ErrorCode.CREDENTIALS_UNAVAILABLE)
    return {"api_id": values["api_id"], "api_hash": values["api_hash"]}


def _dc_endpoints(config: Any) -> dict[int, tuple[str, int]]:
    candidates: dict[int, set[int]] = {}
    for option in getattr(config, "dc_options", []):
        dc_id = getattr(option, "id", None)
        raw_ip = getattr(option, "ip_address", None)
        port = getattr(option, "port", None)
        secret = getattr(option, "secret", None)
        if (
            type(dc_id) is not int
            or not 1 <= dc_id <= 5
            or not isinstance(raw_ip, str)
            or type(port) is not int
            or port != PORT
            or bool(getattr(option, "cdn", False))
            or bool(getattr(option, "media_only", False))
            or bool(getattr(option, "tcpo_only", False))
            or (secret is not None and secret != b"")
        ):
            continue
        try:
            address = ipaddress.ip_address(raw_ip)
        except ValueError:
            continue
        if address.version != 4 or not address.is_global:
            continue
        candidates.setdefault(dc_id, set()).add(int(address))
    return {
        dc_id: (str(ipaddress.IPv4Address(min(addresses))), PORT)
        for dc_id, addresses in candidates.items()
    }


class RawBotTransport:
    """Raw MTProto transport with one ephemeral auth key per process."""

    def __init__(self, api_id: int, api_hash: str, bot_token: str, bindings: ProtocolBindings):
        self.api_id = api_id
        self.api_hash = api_hash
        self.bot_token = bot_token
        self.bindings = bindings
        self.endpoints: dict[int, tuple[str, int]] = {}
        self.senders: list[Any] = []
        self.request_counter = [0]
        self.boot_config: Any = None

    async def _new_sender(self, dc_id: int, endpoint: tuple[str, int]) -> Any:
        logger = logging.getLogger("telethon")
        logger.disabled = True
        sender_logger = logging.getLogger("music-transfer")
        sender_logger.disabled = True
        loggers = defaultdict(lambda: sender_logger)
        try:
            sender = self.bindings.sender_type(
                self.bindings.auth_key_type(None),
                loggers=loggers,
                retries=0,
                auto_reconnect=False,
                updates_queue=None,
                connect_timeout=RPC_TIMEOUT_SECONDS,
            )
            connection = self.bindings.connection_type(
                endpoint[0], endpoint[1], dc_id, loggers=loggers
            )
            self.senders.append(sender)
            await asyncio.wait_for(sender.connect(connection), RPC_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            raise TransferError(ErrorCode.REQUEST_TIMEOUT) from None
        except TransferError:
            raise
        except Exception:
            raise TransferError(ErrorCode.TRANSPORT_ERROR) from None
        return sender

    def _init_query(self, query: Any) -> Any:
        return self.bindings.init_connection_type(
            api_id=self.api_id,
            device_model="Music transfer helper",
            system_version=sys.platform,
            app_version="music-transfer/1",
            system_lang_code="en",
            lang_pack="",
            lang_code="en",
            query=query,
        )

    async def _send(self, sender: Any, request: Any) -> Any:
        return await guarded_send(sender, request, self.bindings, self.request_counter)

    async def _initialize(self, sender: Any, query: Any, first: bool = False) -> Any:
        if first:
            wrapped = self.bindings.invoke_with_layer_type(
                self.bindings.layer, self._init_query(query)
            )
            request = self.bindings.invoke_without_updates_type(wrapped)
        else:
            request = self.bindings.invoke_without_updates_type(query)
        return await self._send(sender, request)

    async def open(self, target_dc: int) -> Any:
        bootstrap = await self._new_sender(BOOTSTRAP_DC, (BOOTSTRAP_IP, PORT))
        config = await self._initialize(
            bootstrap,
            self.bindings.get_config_type(),
            first=True,
        )
        self.boot_config = config
        self.endpoints = _dc_endpoints(config)
        if BOOTSTRAP_DC not in self.endpoints:
            self.endpoints[BOOTSTRAP_DC] = (BOOTSTRAP_IP, PORT)
        if target_dc not in self.endpoints:
            raise TransferError(ErrorCode.TRANSPORT_ERROR)
        bot_authorization = self.bindings.import_bot_authorization_type(
            flags=0,
            api_id=self.api_id,
            api_hash=self.api_hash,
            bot_auth_token=self.bot_token,
        )
        authenticated = bootstrap
        authenticated_dc = BOOTSTRAP_DC
        try:
            await self._initialize(bootstrap, bot_authorization)
        except MigrationRequired as migration:
            new_dc = migration.new_dc
            if type(new_dc) is not int or not 1 <= new_dc <= 5 or new_dc == BOOTSTRAP_DC:
                raise TransferError(ErrorCode.TRANSPORT_ERROR) from None
            endpoint = self.endpoints.get(new_dc)
            if endpoint is None:
                raise TransferError(ErrorCode.TRANSPORT_ERROR) from None
            authenticated = await self._new_sender(new_dc, endpoint)
            authenticated_dc = new_dc
            try:
                await self._initialize(authenticated, bot_authorization, first=True)
            except MigrationRequired:
                raise TransferError(ErrorCode.TRANSPORT_ERROR) from None
        if target_dc == authenticated_dc:
            return authenticated
        return await self._authorize_dc(authenticated, target_dc)

    async def _authorize_dc(self, source_sender: Any, target_dc: int) -> Any:
        endpoint = self.endpoints.get(target_dc)
        if type(target_dc) is not int or not 1 <= target_dc <= 5 or endpoint is None:
            raise TransferError(ErrorCode.TRANSPORT_ERROR)
        exported = await self._send(
            source_sender,
            self.bindings.invoke_without_updates_type(
                self.bindings.export_authorization_type(dc_id=target_dc)
            ),
        )
        target = await self._new_sender(target_dc, endpoint)
        await self._initialize(
            target,
            self.bindings.import_authorization_type(
                id=exported.id,
                bytes=exported.bytes,
            ),
            first=True,
        )
        return target

    async def migrate_file_dc(self, sender: Any, target_dc: int) -> Any:
        return await self._authorize_dc(sender, target_dc)

    async def close(self) -> None:
        for sender in reversed(self.senders):
            try:
                await asyncio.wait_for(sender.disconnect(), 5)
            except Exception:
                continue
        self.senders.clear()


def _stat_entry(dir_fd: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError:
        raise TransferError(ErrorCode.STORAGE_ERROR) from None


def _open_destination_lock(dir_fd: int) -> int:
    name = ".download.lock"
    existing = _stat_entry(dir_fd, name)
    if existing is not None:
        _validate_owned_regular(existing)
    flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    if existing is None:
        try:
            descriptor = os.open(
                name,
                flags | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=dir_fd,
            )
            os.fchmod(descriptor, 0o600)
            os.fsync(descriptor)
            os.fsync(dir_fd)
        except FileExistsError:
            try:
                descriptor = os.open(name, flags, dir_fd=dir_fd)
            except OSError:
                raise TransferError(ErrorCode.STORAGE_ERROR) from None
        except OSError:
            raise TransferError(ErrorCode.STORAGE_ERROR) from None
    else:
        try:
            descriptor = os.open(name, flags, dir_fd=dir_fd)
        except OSError:
            raise TransferError(ErrorCode.STORAGE_ERROR) from None
    try:
        opened = os.fstat(descriptor)
        _validate_owned_regular(opened)
        current = _stat_entry(dir_fd, name)
        if (
            current is None
            or current.st_dev != opened.st_dev
            or current.st_ino != opened.st_ino
        ):
            raise TransferError(ErrorCode.STORAGE_ERROR)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return descriptor
    except BlockingIOError:
        os.close(descriptor)
        raise TransferError(ErrorCode.BUSY) from None
    except TransferError:
        os.close(descriptor)
        raise
    except OSError:
        os.close(descriptor)
        raise TransferError(ErrorCode.STORAGE_ERROR) from None


def _validate_owned_regular(
    info: os.stat_result,
    *,
    exact_mode: int = 0o600,
    expected_links: int = 1,
) -> None:
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != exact_mode
        or info.st_nlink != expected_links
    ):
        raise TransferError(ErrorCode.STORAGE_ERROR)


def _assert_same_entry(dir_fd: int, name: str, descriptor: int) -> os.stat_result:
    path_info = _stat_entry(dir_fd, name)
    fd_info = os.fstat(descriptor)
    if (
        path_info is None
        or path_info.st_dev != fd_info.st_dev
        or path_info.st_ino != fd_info.st_ino
    ):
        raise TransferError(ErrorCode.STORAGE_ERROR)
    _validate_owned_regular(fd_info)
    return fd_info


def _write_all(descriptor: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        count = os.write(descriptor, view)
        if count <= 0:
            raise TransferError(ErrorCode.STORAGE_ERROR)
        view = view[count:]


def _read_all(descriptor: int, limit: int) -> bytes:
    result = bytearray()
    while len(result) <= limit:
        chunk = os.read(descriptor, min(4096, limit + 1 - len(result)))
        if not chunk:
            break
        result.extend(chunk)
    if len(result) > limit:
        raise TransferError(ErrorCode.STORAGE_ERROR)
    return bytes(result)


def _strict_metadata(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("ascii"), object_pairs_hook=_no_duplicate_object)
    except Exception:
        raise TransferError(ErrorCode.INTEGRITY_ERROR) from None
    expected_fields = {
        "schema",
        "identity_sha256",
        "expected_size",
        "offset",
        "prefix_sha256",
    }
    if not isinstance(value, dict) or set(value) != expected_fields:
        raise TransferError(ErrorCode.INTEGRITY_ERROR)
    if (
        type(value["schema"]) is not int
        or value["schema"] != 1
        or not isinstance(value["identity_sha256"], str)
        or not HEX_SHA256.fullmatch(value["identity_sha256"])
        or type(value["expected_size"]) is not int
        or type(value["offset"]) is not int
        or value["offset"] < 0
        or not isinstance(value["prefix_sha256"], str)
        or not HEX_SHA256.fullmatch(value["prefix_sha256"])
    ):
        raise TransferError(ErrorCode.INTEGRITY_ERROR)
    return value


def _open_metadata(dir_fd: int, name: str) -> dict[str, Any] | None:
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=dir_fd,
        )
    except FileNotFoundError:
        return None
    except OSError:
        raise TransferError(ErrorCode.STORAGE_ERROR) from None
    try:
        info = os.fstat(descriptor)
        _validate_owned_regular(info)
        if info.st_size > 4096:
            raise TransferError(ErrorCode.INTEGRITY_ERROR)
        raw = _read_all(descriptor, 4096)
        return _strict_metadata(raw)
    except TransferError:
        raise
    except OSError:
        raise TransferError(ErrorCode.STORAGE_ERROR) from None
    finally:
        os.close(descriptor)


def _read_completion_receipt(dir_fd: int, name: str) -> dict[str, Any] | None:
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=dir_fd,
        )
    except FileNotFoundError:
        return None
    except OSError:
        raise TransferError(ErrorCode.STORAGE_ERROR) from None
    try:
        info = os.fstat(descriptor)
        _validate_owned_regular(info)
        if info.st_size > 4096:
            raise TransferError(ErrorCode.INTEGRITY_ERROR)
        try:
            value = json.loads(
                _read_all(descriptor, 4096).decode("ascii"),
                object_pairs_hook=_no_duplicate_object,
            )
        except Exception:
            raise TransferError(ErrorCode.INTEGRITY_ERROR) from None
        fields = {
            "schema",
            "identity_sha256",
            "expected_size",
            "size",
            "sha256",
            "source_dev",
            "source_ino",
        }
        if (
            not isinstance(value, dict)
            or set(value) != fields
            or type(value["schema"]) is not int
            or value["schema"] != 1
            or not isinstance(value["identity_sha256"], str)
            or not HEX_SHA256.fullmatch(value["identity_sha256"])
            or type(value["expected_size"]) is not int
            or type(value["size"]) is not int
            or type(value["source_dev"]) is not int
            or type(value["source_ino"]) is not int
            or value["source_dev"] < 0
            or value["source_ino"] < 0
            or not isinstance(value["sha256"], str)
            or not HEX_SHA256.fullmatch(value["sha256"])
        ):
            raise TransferError(ErrorCode.INTEGRITY_ERROR)
        return value
    except TransferError:
        raise
    except OSError:
        raise TransferError(ErrorCode.STORAGE_ERROR) from None
    finally:
        os.close(descriptor)


def _remove_stale_temp(dir_fd: int, name: str) -> None:
    info = _stat_entry(dir_fd, name)
    if info is None:
        return
    _validate_owned_regular(info)
    try:
        os.unlink(name, dir_fd=dir_fd)
    except OSError:
        raise TransferError(ErrorCode.STORAGE_ERROR) from None


def _write_metadata(dir_fd: int, name: str, metadata: dict[str, Any]) -> None:
    temp_name = name + ".tmp"
    _remove_stale_temp(dir_fd, temp_name)
    raw = json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode("ascii")
    descriptor = None
    try:
        descriptor = os.open(
            temp_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=dir_fd,
        )
        os.fchmod(descriptor, 0o600)
        _validate_owned_regular(os.fstat(descriptor))
        _write_all(descriptor, raw)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        existing = _stat_entry(dir_fd, name)
        if existing is not None:
            _validate_owned_regular(existing)
        os.replace(temp_name, name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        os.fsync(dir_fd)
    except TransferError:
        raise
    except OSError:
        raise TransferError(ErrorCode.STORAGE_ERROR) from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _hash_prefix(descriptor: int, size: int) -> Any:
    digest = hashlib.sha256()
    offset = 0
    while offset < size:
        try:
            block = os.pread(descriptor, min(CHUNK_SIZE, size - offset), offset)
        except OSError:
            raise TransferError(ErrorCode.STORAGE_ERROR) from None
        if not block:
            raise TransferError(ErrorCode.INTEGRITY_ERROR)
        digest.update(block)
        offset += len(block)
    return digest


def _is_dataless(info: os.stat_result) -> bool:
    if sys.platform != "darwin":
        return False
    flags = getattr(info, "st_flags", None)
    if type(flags) is not int:
        return True
    return bool(flags & getattr(stat, "SF_DATALESS", 0x40000000))


def _hash_existing_file(
    dir_fd: int,
    name: str,
    expected_size: int,
    expected_links: int = 1,
) -> tuple[Any, os.stat_result]:
    before = _stat_entry(dir_fd, name)
    if before is None:
        raise TransferError(ErrorCode.FILE_EXISTS)
    _validate_owned_regular(before, expected_links=expected_links)
    if before.st_size != expected_size:
        raise TransferError(ErrorCode.FILE_EXISTS)
    if _is_dataless(before):
        raise TransferError(ErrorCode.MATERIALIZATION_PENDING)
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=dir_fd,
        )
    except OSError:
        raise TransferError(ErrorCode.MATERIALIZATION_PENDING) from None
    try:
        opened = os.fstat(descriptor)
        _validate_owned_regular(opened, expected_links=expected_links)
        if (
            opened.st_dev != before.st_dev
            or opened.st_ino != before.st_ino
            or opened.st_size != expected_size
        ):
            raise TransferError(ErrorCode.FILE_EXISTS)
        digest = _hash_prefix(descriptor, expected_size)
        after = os.fstat(descriptor)
        current_path = _stat_entry(dir_fd, name)
        if (
            current_path is None
            or current_path.st_dev != after.st_dev
            or current_path.st_ino != after.st_ino
            or after.st_size != expected_size
            or after.st_nlink != opened.st_nlink
        ):
            raise TransferError(ErrorCode.FILE_EXISTS)
        _validate_owned_regular(after, expected_links=expected_links)
        return digest, after
    except TransferError:
        raise
    except OSError:
        raise TransferError(ErrorCode.STORAGE_ERROR) from None
    finally:
        os.close(descriptor)


def _completion_value(
    identity_sha256: str,
    expected_size: int,
    digest: str,
    info: os.stat_result,
) -> dict[str, Any]:
    return {
        "schema": 1,
        "identity_sha256": identity_sha256,
        "expected_size": expected_size,
        "size": expected_size,
        "sha256": digest,
        "source_dev": info.st_dev,
        "source_ino": info.st_ino,
    }


def _recover_existing_destination(
    dir_fd: int,
    destination_name: str,
    part_name: str,
    metadata_name: str,
    receipt_name: str,
    identity_sha256: str,
    expected_size: int,
) -> tuple[int, str] | None:
    destination_info = _stat_entry(dir_fd, destination_name)
    receipt = _read_completion_receipt(dir_fd, receipt_name)
    if destination_info is None:
        return None
    part_info = _stat_entry(dir_fd, part_name)
    expected_links = 2 if part_info is not None else 1
    _validate_owned_regular(destination_info, expected_links=expected_links)
    if destination_info.st_size != expected_size:
        raise TransferError(ErrorCode.FILE_EXISTS)

    metadata = _open_metadata(dir_fd, metadata_name)
    if receipt is None:
        if (
            part_info is None
            or part_info.st_dev != destination_info.st_dev
            or part_info.st_ino != destination_info.st_ino
            or metadata is None
            or metadata["identity_sha256"] != identity_sha256
            or metadata["expected_size"] != expected_size
            or metadata["offset"] != expected_size
        ):
            raise TransferError(ErrorCode.FILE_EXISTS)
        if _is_dataless(destination_info):
            raise TransferError(ErrorCode.MATERIALIZATION_PENDING)
        digest, current = _hash_existing_file(
            dir_fd, destination_name, expected_size, expected_links
        )
        if digest.hexdigest() != metadata["prefix_sha256"]:
            raise TransferError(ErrorCode.FILE_EXISTS)
        receipt = _completion_value(identity_sha256, expected_size, digest.hexdigest(), current)
        _write_metadata(dir_fd, receipt_name, receipt)
    else:
        if (
            receipt["identity_sha256"] != identity_sha256
            or receipt["expected_size"] != expected_size
            or receipt["size"] != expected_size
        ):
            raise TransferError(ErrorCode.FILE_EXISTS)
        if part_info is not None and (
            part_info.st_dev != destination_info.st_dev
            or part_info.st_ino != destination_info.st_ino
        ):
            raise TransferError(ErrorCode.FILE_EXISTS)
        if metadata is not None and (
            metadata["identity_sha256"] != identity_sha256
            or metadata["expected_size"] != expected_size
            or metadata["offset"] != expected_size
            or metadata["prefix_sha256"] != receipt["sha256"]
        ):
            raise TransferError(ErrorCode.FILE_EXISTS)
        if _is_dataless(destination_info):
            raise TransferError(ErrorCode.MATERIALIZATION_PENDING)
        digest, current = _hash_existing_file(
            dir_fd, destination_name, expected_size, expected_links
        )
        if digest.hexdigest() != receipt["sha256"]:
            raise TransferError(ErrorCode.FILE_EXISTS)
        if (
            current.st_dev != receipt["source_dev"]
            or current.st_ino != receipt["source_ino"]
        ):
            receipt = _completion_value(
                identity_sha256, expected_size, digest.hexdigest(), current
            )
            _write_metadata(dir_fd, receipt_name, receipt)

    if part_info is not None:
        current_part = _stat_entry(dir_fd, part_name)
        if (
            current_part is None
            or current_part.st_dev != destination_info.st_dev
            or current_part.st_ino != destination_info.st_ino
        ):
            raise TransferError(ErrorCode.FILE_EXISTS)
        try:
            os.unlink(part_name, dir_fd=dir_fd)
            if metadata is not None:
                os.unlink(metadata_name, dir_fd=dir_fd)
            os.fsync(dir_fd)
        except OSError:
            raise TransferError(ErrorCode.STORAGE_ERROR) from None
    elif metadata is not None:
        try:
            os.unlink(metadata_name, dir_fd=dir_fd)
            os.fsync(dir_fd)
        except OSError:
            raise TransferError(ErrorCode.STORAGE_ERROR) from None
    return expected_size, receipt["sha256"]


def _open_partial(
    dir_fd: int,
    part_name: str,
    metadata_name: str,
    identity_sha256: str,
    expected_size: int,
) -> tuple[int, Any, int]:
    metadata = _open_metadata(dir_fd, metadata_name)
    existing = _stat_entry(dir_fd, part_name)
    if existing is None and metadata is not None:
        raise TransferError(ErrorCode.INTEGRITY_ERROR)
    if existing is not None and _is_dataless(existing):
        raise TransferError(ErrorCode.MATERIALIZATION_PENDING)
    if existing is not None:
        _validate_owned_regular(existing)
    flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
    if existing is None:
        flags |= os.O_CREAT | os.O_EXCL
    descriptor = None
    try:
        descriptor = os.open(part_name, flags, 0o600, dir_fd=dir_fd)
        os.fchmod(descriptor, 0o600)
        info = os.fstat(descriptor)
        _validate_owned_regular(info)
        if existing is not None and (
            info.st_dev != existing.st_dev or info.st_ino != existing.st_ino
        ):
            raise TransferError(ErrorCode.STORAGE_ERROR)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except TransferError:
        if descriptor is not None:
            os.close(descriptor)
        raise
    except OSError:
        if descriptor is not None:
            os.close(descriptor)
        raise TransferError(ErrorCode.STORAGE_ERROR) from None
    try:
        if metadata is None:
            if existing is not None:
                raise TransferError(ErrorCode.INTEGRITY_ERROR)
            digest = hashlib.sha256()
            metadata = {
                "schema": 1,
                "identity_sha256": identity_sha256,
                "expected_size": expected_size,
                "offset": 0,
                "prefix_sha256": digest.hexdigest(),
            }
            _write_metadata(dir_fd, metadata_name, metadata)
            return descriptor, digest, 0
        offset = metadata["offset"]
        if (
            metadata["identity_sha256"] != identity_sha256
            or metadata["expected_size"] != expected_size
            or offset > expected_size
            or (offset != expected_size and offset % CHUNK_SIZE)
        ):
            raise TransferError(ErrorCode.INTEGRITY_ERROR)
        info = _assert_same_entry(dir_fd, part_name, descriptor)
        if info.st_size < offset or info.st_size > min(expected_size, offset + CHUNK_SIZE):
            raise TransferError(ErrorCode.INTEGRITY_ERROR)
        digest = _hash_prefix(descriptor, offset)
        if digest.hexdigest() != metadata["prefix_sha256"]:
            raise TransferError(ErrorCode.INTEGRITY_ERROR)
        if info.st_size != offset:
            os.ftruncate(descriptor, offset)
            os.fsync(descriptor)
        _assert_same_entry(dir_fd, part_name, descriptor)
        return descriptor, digest, offset
    except TransferError:
        os.close(descriptor)
        raise
    except OSError:
        os.close(descriptor)
        raise TransferError(ErrorCode.STORAGE_ERROR) from None


def _assert_private_root(path: Path) -> int:
    try:
        descriptor = _open_directory_path(path)
        _check_private_dir(descriptor)
        return descriptor
    except TransferError:
        raise
    except Exception:
        raise TransferError(ErrorCode.STORAGE_ERROR) from None


def _assert_root_still_named(path: Path, descriptor: int) -> None:
    try:
        current = _open_directory_path(path)
    except OSError:
        raise TransferError(ErrorCode.STORAGE_ERROR) from None
    try:
        path_info = os.fstat(current)
        fd_info = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(path_info.st_mode)
            or path_info.st_dev != fd_info.st_dev
            or path_info.st_ino != fd_info.st_ino
        ):
            raise TransferError(ErrorCode.STORAGE_ERROR)
    finally:
        os.close(current)


async def _download_file(
    transport: RawBotTransport | None,
    location: Any | None,
    target_dc: int,
    request: TransferRequest,
    bindings: ProtocolBindings | None,
    dir_fd: int,
    part_name: str,
    metadata_name: str,
    deadline: float,
    partial: tuple[int, Any, int],
) -> tuple[int, str, int]:
    descriptor, digest, offset = partial
    sender = None
    if offset < request.expected_size:
        if transport is None or location is None:
            os.close(descriptor)
            raise TransferError(ErrorCode.TRANSPORT_ERROR)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            os.close(descriptor)
            raise TransferError(ErrorCode.DEADLINE_EXCEEDED)
        try:
            sender = await asyncio.wait_for(transport.open(target_dc), remaining)
        except Exception:
            os.close(descriptor)
            raise
    flood_wait_used = False
    file_migration_used = False
    target_dc_current = target_dc
    try:
        while offset < request.expected_size:
            if transport is None or sender is None or location is None or bindings is None:
                raise TransferError(ErrorCode.TRANSPORT_ERROR)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TransferError(ErrorCode.DEADLINE_EXCEEDED)
            _assert_same_entry(dir_fd, part_name, descriptor)
            limit = min(CHUNK_SIZE, request.expected_size - offset)
            get_file = bindings.get_file_type(
                location=location,
                offset=offset,
                limit=CHUNK_SIZE,
                precise=True,
                cdn_supported=False,
            )
            wrapped = bindings.invoke_without_updates_type(get_file)
            try:
                result = await asyncio.wait_for(
                    transport._send(sender, wrapped),
                    min(remaining, RPC_TIMEOUT_SECONDS + 1),
                )
            except MigrationRequired as migration:
                target_dc = migration.new_dc
                if (
                    file_migration_used
                    or type(target_dc) is not int
                    or not 1 <= target_dc <= 5
                    or target_dc == target_dc_current
                    or target_dc not in transport.endpoints
                ):
                    raise TransferError(ErrorCode.TRANSPORT_ERROR) from None
                file_migration_used = True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TransferError(ErrorCode.DEADLINE_EXCEEDED)
                sender = await asyncio.wait_for(
                    transport.migrate_file_dc(sender, target_dc), remaining
                )
                target_dc_current = target_dc
                continue
            except FloodWaitRequired as flood:
                seconds = flood.seconds
                if flood_wait_used or type(seconds) is not int or not 1 <= seconds <= 10:
                    raise TransferError(ErrorCode.RATE_LIMITED) from None
                if deadline - time.monotonic() <= seconds:
                    raise TransferError(ErrorCode.DEADLINE_EXCEEDED) from None
                flood_wait_used = True
                await asyncio.sleep(seconds)
                continue
            except asyncio.TimeoutError:
                raise TransferError(ErrorCode.REQUEST_TIMEOUT) from None
            chunk = getattr(result, "bytes", None)
            if not isinstance(chunk, bytes) or len(chunk) != limit:
                raise TransferError(ErrorCode.INTEGRITY_ERROR)
            if offset + len(chunk) > request.expected_size:
                raise TransferError(ErrorCode.INTEGRITY_ERROR)
            try:
                os.lseek(descriptor, offset, os.SEEK_SET)
                _write_all(descriptor, chunk)
                os.fsync(descriptor)
            except OSError:
                raise TransferError(ErrorCode.STORAGE_ERROR) from None
            digest.update(chunk)
            offset += len(chunk)
            _assert_same_entry(dir_fd, part_name, descriptor)
            _write_metadata(
                dir_fd,
                metadata_name,
                {
                    "schema": 1,
                    "identity_sha256": request.identity_sha256,
                    "expected_size": request.expected_size,
                    "offset": offset,
                    "prefix_sha256": digest.hexdigest(),
                },
            )
        info = _assert_same_entry(dir_fd, part_name, descriptor)
        if info.st_size != request.expected_size:
            raise TransferError(ErrorCode.INTEGRITY_ERROR)
        os.fsync(descriptor)
        completed_descriptor = descriptor
        descriptor = -1
        return request.expected_size, digest.hexdigest(), completed_descriptor
    finally:
        if descriptor >= 0:
            os.close(descriptor)


async def transfer(
    request: TransferRequest,
    home: Path,
    bindings: ProtocolBindings | None = None,
    transport_factory: type[RawBotTransport] = RawBotTransport,
) -> dict[str, Any]:
    try:
        root_info = os.stat(request.staging_root, follow_symlinks=False)
        if not stat.S_ISDIR(root_info.st_mode):
            raise TransferError(ErrorCode.STORAGE_ERROR)
        staging_root = request.staging_root.resolve(strict=True)
    except TransferError:
        raise
    except Exception:
        raise TransferError(ErrorCode.STORAGE_ERROR) from None
    root_fd = _assert_private_root(staging_root)
    try:
        directory_fd = os.open(
            request.job_id,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=root_fd,
        )
    except OSError:
        os.close(root_fd)
        raise TransferError(ErrorCode.STORAGE_ERROR) from None
    try:
        _check_private_dir(directory_fd)
    except Exception:
        os.close(directory_fd)
        os.close(root_fd)
        raise
    part_slot = hashlib.sha256(request.destination.name.encode("utf-8")).hexdigest()[:24]
    part_name = f".music-transfer-{part_slot}.part"
    metadata_name = part_name + ".meta"
    receipt_name = part_name + ".complete"
    transport: RawBotTransport | None = None
    completed_descriptor = -1
    lock_descriptor = -1
    try:
        lock_descriptor = _open_destination_lock(directory_fd)
        recovered = _recover_existing_destination(
            directory_fd,
            request.destination.name,
            part_name,
            metadata_name,
            receipt_name,
            request.identity_sha256,
            request.expected_size,
        )
        if recovered is not None:
            size, digest = recovered
            return {"schema": 1, "ok": True, "size": size, "sha256": digest}
        _remove_stale_temp(directory_fd, metadata_name + ".tmp")
        _remove_stale_temp(directory_fd, receipt_name + ".tmp")
        partial = _open_partial(
            directory_fd,
            part_name,
            metadata_name,
            request.identity_sha256,
            request.expected_size,
        )
        deadline = time.monotonic() + request.deadline_seconds
        location = None
        target_dc = 0
        if partial[2] < request.expected_size:
            credentials = _read_credentials(request.credentials_path, home)
            decoded = decode_file_id(request.file_id)
            bindings = bindings or _load_protocol()
            location = bindings.input_document_file_location_type(
                id=decoded.media_id,
                access_hash=decoded.access_hash,
                file_reference=decoded.file_reference,
                thumb_size="",
            )
            target_dc = decoded.dc_id
            transport = transport_factory(
                credentials["api_id"], credentials["api_hash"], request.bot_token, bindings
            )
        size, digest, completed_descriptor = await _download_file(
            transport,
            location,
            target_dc,
            request,
            bindings,
            directory_fd,
            part_name,
            metadata_name,
            deadline,
            partial,
        )
        _assert_root_still_named(staging_root / request.job_id, directory_fd)
        part_info = _assert_same_entry(directory_fd, part_name, completed_descriptor)
        digest_hex = digest
        _write_metadata(
            directory_fd,
            receipt_name,
            _completion_value(
                request.identity_sha256,
                request.expected_size,
                digest_hex,
                part_info,
            ),
        )
        try:
            os.link(
                part_name,
                request.destination.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except FileExistsError:
            raise TransferError(ErrorCode.FILE_EXISTS) from None
        except OSError:
            raise TransferError(ErrorCode.STORAGE_ERROR) from None
        final_info = _stat_entry(directory_fd, request.destination.name)
        if (
            final_info is None
            or final_info.st_dev != part_info.st_dev
            or final_info.st_ino != part_info.st_ino
            or final_info.st_size != request.expected_size
            or not stat.S_ISREG(final_info.st_mode)
            or final_info.st_uid != os.getuid()
            or stat.S_IMODE(final_info.st_mode) != 0o600
            or final_info.st_nlink != 2
        ):
            raise TransferError(ErrorCode.STORAGE_ERROR)
        try:
            os.unlink(part_name, dir_fd=directory_fd)
            os.unlink(metadata_name, dir_fd=directory_fd)
            os.fsync(directory_fd)
        except OSError:
            raise TransferError(ErrorCode.STORAGE_ERROR) from None
        return {"schema": 1, "ok": True, "size": size, "sha256": digest_hex}
    finally:
        if completed_descriptor >= 0:
            os.close(completed_descriptor)
        if lock_descriptor >= 0:
            os.close(lock_descriptor)
        if transport is not None:
            await transport.close()
        os.close(directory_fd)
        os.close(root_fd)


def _emit(value: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def main() -> int:
    logging.disable(logging.CRITICAL)
    try:
        raw = read_request(sys.stdin.buffer)
        home = _real_home()
        request = validate_request(raw, home)
        result = asyncio.run(
            asyncio.wait_for(
                transfer(request, home),
                timeout=request.deadline_seconds + RPC_TIMEOUT_SECONDS,
            )
        )
        _emit(result)
    except TransferError as exc:
        _emit({"schema": 1, "ok": False, "error": exc.code.value})
    except (asyncio.TimeoutError, TimeoutError):
        _emit({"schema": 1, "ok": False, "error": ErrorCode.DEADLINE_EXCEEDED.value})
    except KeyboardInterrupt:
        _emit({"schema": 1, "ok": False, "error": "cancelled"})
    except BaseException:
        _emit({"schema": 1, "ok": False, "error": ErrorCode.INTERNAL_ERROR.value})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
