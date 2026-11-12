import asyncio
import base64
import hashlib
import importlib.metadata
import io
import json
import os
import stat
import struct
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import music_transfer as transfer


# Synthetic token catalog: active api-key-a for the mocked bot token and
# active api-key-b as the mocked API-hash placeholder; neither reaches Telegram.
SYNTHETIC_BOT_TOKEN = "codex_synth_v1_api_key_a"
SYNTHETIC_API_HASH = "JoeyPrivateV3ApiKeySlotB7Q9M3X5"

def _tl_class(name, module="telethon.tl.functions"):
    def init(self, *args, **kwargs):
        if name == "InvokeWithLayerRequest" and args:
            self.layer, self.query = args
        elif args:
            self.query = args[0]
        self.__dict__.update(kwargs)

    return type(name, (), {"__init__": init, "__module__": module})


def _response_class(name):
    def init(self, **kwargs):
        self.__dict__.update(kwargs)

    return type(name, (), {"__init__": init, "__module__": "telethon.tl.types"})


class FakeBindings:
    def __init__(self):
        self.sender_type = object
        self.auth_key_type = object
        self.connection_type = object
        self.invoke_without_updates_type = _tl_class("InvokeWithoutUpdatesRequest")
        self.invoke_with_layer_type = _tl_class("InvokeWithLayerRequest")
        self.init_connection_type = _tl_class("InitConnectionRequest")
        self.get_config_type = _tl_class("GetConfigRequest")
        self.import_bot_authorization_type = _tl_class("ImportBotAuthorizationRequest")
        self.export_authorization_type = _tl_class("ExportAuthorizationRequest")
        self.import_authorization_type = _tl_class("ImportAuthorizationRequest")
        self.get_file_type = _tl_class("GetFileRequest")
        self.input_document_file_location_type = _tl_class("InputDocumentFileLocation")
        self.config_response_type = _response_class("Config")
        self.exported_authorization_response_type = _response_class("ExportedAuthorization")
        self.authorization_response_type = _response_class("Authorization")
        self.file_response_type = _response_class("File")
        self.layer = 216


def _rle(data):
    result = bytearray()
    offset = 0
    while offset < len(data):
        if data[offset] == 0:
            end = offset
            while end < len(data) and data[end] == 0 and end - offset < 255:
                end += 1
            result.extend((0, end - offset))
            offset = end
        else:
            result.append(data[offset])
            offset += 1
    return bytes(result)


def _file_id(dc_id=4, media_type=5, reference=b"file-reference"):
    flags = media_type | (1 << 25)
    payload = bytearray(struct.pack("<ii", flags, dc_id))
    payload.extend(bytes((len(reference),)))
    payload.extend(reference)
    payload.extend(b"\0" * (-(len(reference) + 1) % 4))
    payload.extend(struct.pack("<qq", 123456789, 987654321))
    payload.extend(struct.pack("<ii", 30, 4))
    payload.extend(struct.pack("<bb", 30, 4))
    return base64.urlsafe_b64encode(_rle(bytes(payload))).decode().rstrip("=")


class FakeRpcError(Exception):
    def __init__(self, new_dc=None, seconds=None):
        self.new_dc = new_dc
        self.seconds = seconds
        super().__init__("private file identifier and token must not escape")


class FileMigrateError(Exception):
    def __init__(self, new_dc):
        self.new_dc = new_dc
        super().__init__("private file identifier must not escape")


class FileReferenceExpiredError(Exception):
    def __init__(self):
        super().__init__("private file identifier must not escape")


class FloodWaitError(Exception):
    def __init__(self, seconds):
        self.seconds = seconds
        super().__init__("private token must not escape")


class FakeSender:
    def __init__(self, transport, dc_id):
        self.transport = transport
        self.dc_id = dc_id

    async def connect(self, _connection):
        return None

    async def disconnect(self):
        return None

    async def send(self, request):
        bindings = self.transport.bindings
        if type(request) is not bindings.invoke_without_updates_type:
            raise AssertionError("every API request must use invokeWithoutUpdates")
        transfer.validate_rpc_tree(request, bindings)
        leaf = transfer._request_leaf(request, bindings)
        self.transport.calls.append((self.dc_id, type(leaf), getattr(leaf, "offset", None)))
        if type(leaf) is bindings.get_config_type:
            return bindings.config_response_type(
                dc_options=[
                    SimpleNamespace(id=dc, ip_address=f"149.154.167.{50 + dc}", port=443, cdn=False)
                    for dc in (2, 3, 4, 5)
                ]
            )
        if type(leaf) is bindings.import_bot_authorization_type:
            migrate = self.transport.bot_auth_migrations.pop(self.dc_id, None)
            if migrate is not None:
                raise FakeRpcError(new_dc=migrate)
            return bindings.authorization_response_type()
        if type(leaf) is bindings.export_authorization_type:
            return bindings.exported_authorization_response_type(id=17, bytes=b"authorization")
        if type(leaf) is bindings.import_authorization_type:
            return bindings.authorization_response_type()
        if type(leaf) is bindings.get_file_type:
            if self.transport.file_reference_error:
                raise FileReferenceExpiredError()
            if self.transport.file_migration is not None:
                target = self.transport.file_migration
                self.transport.file_migration = None
                raise FileMigrateError(new_dc=target)
            if self.transport.flood_waits:
                raise FloodWaitError(self.transport.flood_waits.pop(0))
            if self.transport.fail_offset == leaf.offset:
                raise RuntimeError("secret-bearing RPC diagnostic")
            if self.transport.cdn_redirect:
                return _response_class("FileCdnRedirect")()
            if self.transport.updates_response:
                return _response_class("UpdatesCombined")()
            start = leaf.offset
            limit = leaf.limit
            data = self.transport.payload[start : start + limit]
            if self.transport.invalid_chunk and start == 0:
                data += b"extra"
            return bindings.file_response_type(bytes=data)
        raise AssertionError(f"unexpected request class {type(leaf).__name__}")


class FakeTransport(transfer.RawBotTransport):
    payload = b""
    calls = None
    bot_auth_migrations = {}
    file_migration = None
    file_reference_error = False
    flood_waits = []
    fail_offset = None
    cdn_redirect = False
    updates_response = False
    invalid_chunk = False

    def __init__(self, api_id, api_hash, bot_token, bindings):
        super().__init__(api_id, api_hash, bot_token, bindings)
        self.calls = []
        type(self).last_calls = self.calls
        self.bot_auth_migrations = dict(type(self).bot_auth_migrations)
        self.file_migration = type(self).file_migration
        self.file_reference_error = type(self).file_reference_error
        self.flood_waits = list(type(self).flood_waits)
        self.fail_offset = type(self).fail_offset
        self.cdn_redirect = type(self).cdn_redirect
        self.updates_response = type(self).updates_response
        self.invalid_chunk = type(self).invalid_chunk
        self.payload = type(self).payload

    async def _new_sender(self, dc_id, endpoint):
        sender = FakeSender(self, dc_id)
        self.senders.append(sender)
        return sender


class MusicTransferTests(unittest.TestCase):
    def setUp(self):
        self.bindings = FakeBindings()
        for name in (
            "bot_auth_migrations",
            "file_migration",
            "file_reference_error",
            "flood_waits",
            "fail_offset",
            "cdn_redirect",
            "updates_response",
            "invalid_chunk",
            "payload",
        ):
            setattr(FakeTransport, name, [] if name == "flood_waits" else None if name in {"file_migration", "fail_offset"} else False if name in {"file_reference_error", "cdn_redirect", "updates_response", "invalid_chunk"} else {} if name == "bot_auth_migrations" else b"")
        FakeTransport.last_calls = []

    def _fixture(self, size=64, file_id=None):
        owner = tempfile.TemporaryDirectory()
        base = Path(owner.name)
        home = base / "home"
        home.mkdir(mode=0o700)
        credential_dir = home / ".config/telegram-video-downloader"
        credential_dir.mkdir(parents=True, mode=0o700)
        credential_dir.chmod(0o700)
        credential_path = credential_dir / "mtproto.json"
        credential_path.write_text(
            json.dumps({"api_id": "invalid", "api_hash": SYNTHETIC_API_HASH}),
            encoding="utf-8",
        )
        credential_path.chmod(0o600)
        staging_parent = base / ".telegram-video-downloader-staging"
        staging_root = staging_parent / "music"
        staging_root.mkdir(parents=True, mode=0o700)
        staging_root.chmod(0o700)
        job_id = "mj1-0123456789abcdef01234567"
        job_dir = staging_root / job_id
        job_dir.mkdir(mode=0o700)
        job_dir.chmod(0o700)
        request = transfer.TransferRequest(
            bot_token=SYNTHETIC_BOT_TOKEN,
            credentials_path=credential_path,
            file_id=file_id or _file_id(),
            identity_sha256=hashlib.sha256(b"upload-identity").hexdigest(),
            expected_size=size,
            staging_root=staging_root,
            job_id=job_id,
            destination=job_dir / "original.m4a",
            deadline_seconds=30,
        )
        self.addCleanup(owner.cleanup)
        return home, request, job_dir

    def _run_transfer(self, request, home):
        with mock.patch.object(
            transfer,
            "_read_credentials",
            return_value={"api_id": 1, "api_hash": SYNTHETIC_API_HASH},
        ):
            return asyncio.run(
                transfer.transfer(request, home, self.bindings, FakeTransport)
            )

    def _set_transport(self, *, payload=b"", **values):
        FakeTransport.payload = payload
        for name, value in values.items():
            setattr(FakeTransport, name, value)

    def test_decoder_preserves_reference_and_rejects_unsupported_ids(self):
        parsed = transfer.decode_file_id(_file_id())
        self.assertEqual(parsed.file_type, 5)
        self.assertEqual(parsed.dc_id, 4)
        self.assertEqual(parsed.file_reference, b"file-reference")
        with self.assertRaises(transfer.TransferError):
            transfer.decode_file_id(_file_id(media_type=1))
        with self.assertRaises(transfer.TransferError):
            transfer.decode_file_id("not a file identifier")

    def test_request_json_rejects_unknown_duplicate_and_oversized_input(self):
        valid = {key: "x" for key in transfer.REQUEST_FIELDS}
        valid.update(
            schema=1,
            expected_size=1,
            deadline_seconds=30,
            identity_sha256="a" * 64,
            bot_token=SYNTHETIC_BOT_TOKEN,
            file_id="abc",
        )
        encoded = json.dumps(valid).encode()
        self.assertEqual(transfer.read_request(io.BytesIO(encoded)), valid)
        with self.assertRaises(transfer.TransferError):
            transfer.read_request(io.BytesIO(encoded[:-1] + b',"unknown":1}'))
        with self.assertRaises(transfer.TransferError):
            transfer.read_request(io.BytesIO(b'{"schema":1,"schema":1}'))
        with self.assertRaises(transfer.TransferError):
            transfer.read_request(io.BytesIO(b"x" * (transfer.MAX_REQUEST_BYTES + 1)))

    def test_request_accepts_manager_job_shape_and_denies_arbitrary_destination(self):
        home, request, _ = self._fixture()
        raw = {
            "schema": 1,
            "bot_token": request.bot_token,
            "credentials_path": str(request.credentials_path),
            "file_id": request.file_id,
            "identity_sha256": request.identity_sha256,
            "expected_size": request.expected_size,
            "staging_root": str(request.staging_root),
            "destination": str(request.destination),
            "deadline_seconds": request.deadline_seconds,
        }
        parsed = transfer.validate_request(raw, home)
        self.assertEqual(parsed.destination, request.destination.resolve())
        raw["destination"] = str(request.destination.parent / "voice.m4a")
        with self.assertRaises(transfer.TransferError):
            transfer.validate_request(raw, home)
        raw["destination"] = str(request.destination.parent.parent / "original.m4a")
        with self.assertRaises(transfer.TransferError):
            transfer.validate_request(raw, home)

    def test_private_credentials_require_exact_owner_mode_and_shape(self):
        home, request, _ = self._fixture()
        with self.assertRaises(transfer.TransferError):
            transfer._read_credentials(request.credentials_path, home)
        request.credentials_path.chmod(0o644)
        with self.assertRaises(transfer.TransferError):
            transfer._read_credentials(request.credentials_path, home)

    def test_rpc_guard_denies_bare_updates_and_unknown_requests_before_send(self):
        request = self.bindings.get_config_type()
        with self.assertRaises(transfer.TransferError):
            transfer.validate_rpc_tree(request, self.bindings)
        update = _tl_class("UpdatesGetStateRequest", "telethon.tl.functions.updates")()
        init = self.bindings.init_connection_type(query=update)
        wrapped = self.bindings.invoke_without_updates_type(
            self.bindings.invoke_with_layer_type(self.bindings.layer, init)
        )
        with self.assertRaises(transfer.TransferError):
            transfer.validate_rpc_tree(wrapped, self.bindings)
        unknown = _tl_class("UnknownRequest")()
        with self.assertRaises(transfer.TransferError):
            transfer.validate_rpc_tree(self.bindings.invoke_without_updates_type(unknown), self.bindings)

    def test_response_guard_rejects_updates_and_cdn_redirects(self):
        get_file = self.bindings.get_file_type(
            location=self.bindings.input_document_file_location_type(),
            offset=0,
            limit=transfer.CHUNK_SIZE,
            precise=True,
            cdn_supported=False,
        )
        request = self.bindings.invoke_without_updates_type(get_file)
        for response in (
            _response_class("UpdatesCombined")(),
            _response_class("FileCdnRedirect")(),
        ):
            with self.assertRaises(transfer.TransferError):
                transfer._validate_response(request, response, self.bindings)

    def test_pinned_telethon_response_bindings_use_real_generated_types(self):
        try:
            version = importlib.metadata.version("Telethon")
        except importlib.metadata.PackageNotFoundError:
            self.skipTest("pinned Telethon is not installed in this test interpreter")
        self.assertEqual(version, transfer.TELETHON_VERSION)

        from telethon.tl.types import Config, InputDocumentFileLocation
        from telethon.tl.types.auth import Authorization, ExportedAuthorization
        from telethon.tl.types.upload import File, FileCdnRedirect

        bindings = transfer._load_protocol()
        self.assertIs(bindings.config_response_type, Config)
        self.assertIs(bindings.authorization_response_type, Authorization)
        self.assertIs(bindings.exported_authorization_response_type, ExportedAuthorization)
        self.assertIs(bindings.file_response_type, File)
        self.assertFalse(hasattr(bindings, "bool_true_response_type"))

        authorization = Authorization(user=None)
        bot_request = bindings.invoke_without_updates_type(
            bindings.import_bot_authorization_type(
                flags=0,
                api_id=1,
                api_hash=SYNTHETIC_API_HASH,
                bot_auth_token=SYNTHETIC_BOT_TOKEN,
            )
        )
        transfer._validate_response(bot_request, authorization, bindings)
        with self.assertRaises(transfer.TransferError):
            # Telethon decodes TL Bool constructors as Python bool values, but the
            # authorized import RPCs declare auth.Authorization, never Bool.
            transfer._validate_response(bot_request, True, bindings)

        export_request = bindings.invoke_without_updates_type(
            bindings.export_authorization_type(dc_id=2)
        )
        transfer._validate_response(
            export_request,
            ExportedAuthorization(id=1, bytes=b"synthetic-authorization"),
            bindings,
        )
        import_request = bindings.invoke_without_updates_type(
            bindings.import_authorization_type(id=1, bytes=b"synthetic-authorization")
        )
        transfer._validate_response(import_request, authorization, bindings)

        location = InputDocumentFileLocation(
            id=1,
            access_hash=2,
            file_reference=b"synthetic-reference",
            thumb_size="",
        )
        get_file_request = bindings.invoke_without_updates_type(
            bindings.get_file_type(
                location=location,
                offset=0,
                limit=transfer.CHUNK_SIZE,
                precise=True,
                cdn_supported=False,
            )
        )
        transfer._validate_response(
            get_file_request,
            File(type=None, mtime=0, bytes=b"synthetic-audio-block"),
            bindings,
        )
        with self.assertRaises(transfer.TransferError):
            transfer._validate_response(
                get_file_request,
                FileCdnRedirect(
                    dc_id=2,
                    file_token=b"synthetic-file-token",
                    encryption_key=b"k" * 32,
                    encryption_iv=b"i" * 16,
                    file_hashes=[],
                ),
                bindings,
            )

    def test_pinned_dc_options_select_deterministic_auth_capable_main_endpoints(self):
        try:
            version = importlib.metadata.version("Telethon")
        except importlib.metadata.PackageNotFoundError:
            self.skipTest("pinned Telethon is not installed in this test interpreter")
        self.assertEqual(version, transfer.TELETHON_VERSION)

        from telethon.tl.types import DcOption

        options = [
            DcOption(id=2, ip_address="149.154.170.10", port=443),
            DcOption(id=2, ip_address="149.154.160.20", port=443, static=True),
            DcOption(id=2, ip_address="149.154.170.10", port=443),
            DcOption(id=2, ip_address="149.154.160.30", port=443, media_only=True),
            DcOption(id=2, ip_address="149.154.160.40", port=443, tcpo_only=True),
            DcOption(id=2, ip_address="149.154.160.50", port=443, cdn=True),
            DcOption(
                id=2,
                ip_address="149.154.160.60",
                port=443,
                secret=b"test",
            ),
            DcOption(id=3, ip_address="149.154.160.70", port=443, media_only=True),
        ]
        config = SimpleNamespace(dc_options=options)
        expected = {2: ("149.154.160.20", transfer.PORT)}
        self.assertEqual(transfer._dc_endpoints(config), expected)
        self.assertEqual(
            transfer._dc_endpoints(SimpleNamespace(dc_options=list(reversed(options)))),
            expected,
        )

    def test_raw_auth_migration_is_single_and_all_rpc_calls_are_wrapped(self):
        self._set_transport(bot_auth_migrations={2: 5})
        transport = FakeTransport(1, SYNTHETIC_API_HASH, SYNTHETIC_BOT_TOKEN, self.bindings)
        sender = asyncio.run(transport.open(5))
        self.assertEqual(sender.dc_id, 5)
        self.assertEqual([call[0] for call in transport.calls[:3]], [2, 2, 5])

    def test_file_migration_retries_same_offset_once(self):
        home, request, job_dir = self._fixture(size=4)
        self._set_transport(payload=b"data", file_migration=5)
        result = self._run_transfer(request, home)
        self.assertTrue(result["ok"])
        offsets = [offset for _dc, kind, offset in FakeTransport.last_calls if kind is self.bindings.get_file_type]
        self.assertEqual(offsets, [0, 0])
        self.assertEqual(request.destination.read_bytes(), b"data")

    def test_file_reference_expiry_is_safe_refresh_required_without_retry(self):
        home, request, _ = self._fixture(size=4)
        self._set_transport(payload=b"data", file_reference_error=True)
        with self.assertRaises(transfer.RefreshRequired) as caught:
            self._run_transfer(request, home)
        self.assertEqual(caught.exception.code, transfer.ErrorCode.REFRESH_REQUIRED)
        self.assertEqual([call[2] for call in FakeTransport.last_calls if call[1] is self.bindings.get_file_type], [0])
        self.assertNotIn(request.file_id, str(caught.exception))

    def test_cdn_response_fails_closed_and_keeps_partial(self):
        home, request, job_dir = self._fixture(size=4)
        self._set_transport(payload=b"data", cdn_redirect=True)
        with self.assertRaises(transfer.TransferError) as caught:
            self._run_transfer(request, home)
        self.assertEqual(caught.exception.code, transfer.ErrorCode.UNEXPECTED_RESPONSE)
        part = next(job_dir.glob(".music-transfer-*.part"))
        self.assertEqual(part.stat().st_size, 0)
        self.assertFalse(request.destination.exists())

    def test_bounded_flood_wait_retries_once_at_same_offset(self):
        home, request, _ = self._fixture(size=4)
        self._set_transport(payload=b"data", flood_waits=[1])
        original_sleep = asyncio.sleep

        async def no_wait(_seconds):
            return None

        with mock.patch.object(transfer.asyncio, "sleep", no_wait):
            result = self._run_transfer(request, home)
        self.assertTrue(result["ok"])
        self.assertEqual([call[2] for call in FakeTransport.last_calls if call[1] is self.bindings.get_file_type], [0, 0])
        self.assertIsNotNone(original_sleep)

    def test_overlong_flood_wait_is_rejected_without_retry(self):
        home, request, _ = self._fixture(size=4)
        self._set_transport(payload=b"data", flood_waits=[30])
        with self.assertRaises(transfer.TransferError) as caught:
            self._run_transfer(request, home)
        self.assertEqual(caught.exception.code, transfer.ErrorCode.RATE_LIMITED)
        self.assertEqual(len([call for call in FakeTransport.last_calls if call[1] is self.bindings.get_file_type]), 1)

    def test_large_file_download_uses_exact_one_mebibyte_offsets_and_final_size(self):
        size = 20 * 1024 * 1024 + 8192
        payload = (bytes(range(256)) * ((size + 255) // 256))[:size]
        home, request, _ = self._fixture(size=size)
        self._set_transport(payload=payload)
        result = self._run_transfer(request, home)
        self.assertEqual(result["size"], size)
        self.assertEqual(result["sha256"], hashlib.sha256(payload).hexdigest())
        offsets = [call[2] for call in FakeTransport.last_calls if call[1] is self.bindings.get_file_type]
        self.assertEqual(offsets, list(range(0, size, transfer.CHUNK_SIZE)))
        self.assertEqual(request.destination.stat().st_size, size)
        self.assertEqual(stat.S_IMODE(request.destination.stat().st_mode), 0o600)

    def test_interrupted_large_file_resumes_only_at_committed_chunk_boundary(self):
        size = 20 * 1024 * 1024 + 8192
        payload = (b"resume-check-" * ((size // 13) + 1))[:size]
        home, request, job_dir = self._fixture(size=size)
        self._set_transport(payload=payload, fail_offset=transfer.CHUNK_SIZE)
        with self.assertRaises(transfer.TransferError) as caught:
            self._run_transfer(request, home)
        self.assertEqual(caught.exception.code, transfer.ErrorCode.RPC_ERROR)
        part = next(job_dir.glob(".music-transfer-*.part"))
        self.assertEqual(part.stat().st_size, transfer.CHUNK_SIZE)
        self._set_transport(payload=payload, fail_offset=None)
        result = self._run_transfer(request, home)
        self.assertEqual(result["sha256"], hashlib.sha256(payload).hexdigest())
        offsets = [call[2] for call in FakeTransport.last_calls if call[1] is self.bindings.get_file_type]
        self.assertEqual(offsets[0], transfer.CHUNK_SIZE)
        self.assertFalse(part.exists())

    def test_bad_chunk_leaves_private_partial_and_never_publishes(self):
        home, request, job_dir = self._fixture(size=20 * 1024 * 1024 + 1)
        self._set_transport(payload=b"x" * request.expected_size, invalid_chunk=True)
        with self.assertRaises(transfer.TransferError) as caught:
            self._run_transfer(request, home)
        self.assertEqual(caught.exception.code, transfer.ErrorCode.INTEGRITY_ERROR)
        part = next(job_dir.glob(".music-transfer-*.part"))
        self.assertEqual(part.stat().st_size, 0)
        self.assertFalse(request.destination.exists())

    def test_nonprivate_existing_partial_is_rejected_without_chmod(self):
        _home, request, job_dir = self._fixture(size=4)
        part_slot = hashlib.sha256(request.destination.name.encode()).hexdigest()[:24]
        part_name = f".music-transfer-{part_slot}.part"
        metadata_name = part_name + ".meta"
        partial = job_dir / part_name
        partial.write_bytes(b"")
        partial.chmod(0o644)
        directory_fd = os.open(job_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            with self.assertRaises(transfer.TransferError):
                transfer._open_partial(
                    directory_fd,
                    part_name,
                    metadata_name,
                    request.identity_sha256,
                    request.expected_size,
                )
            self.assertEqual(stat.S_IMODE(partial.stat().st_mode), 0o644)
        finally:
            os.close(directory_fd)

    def test_unknown_existing_destination_is_never_overwritten(self):
        home, request, _ = self._fixture(size=4)
        request.destination.write_bytes(b"keep")
        request.destination.chmod(0o600)
        with self.assertRaises(transfer.TransferError) as caught:
            self._run_transfer(request, home)
        self.assertEqual(caught.exception.code, transfer.ErrorCode.FILE_EXISTS)
        self.assertEqual(request.destination.read_bytes(), b"keep")

    def test_job_lock_is_nonblocking_and_preserves_partial_state(self):
        _home, _request, job_dir = self._fixture(size=4)
        directory_fd = os.open(job_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            first = transfer._open_destination_lock(directory_fd)
            with self.assertRaises(transfer.TransferError) as caught:
                transfer._open_destination_lock(directory_fd)
            self.assertEqual(caught.exception.code, transfer.ErrorCode.BUSY)
            self.assertTrue((job_dir / ".download.lock").is_file())
            self.assertEqual(stat.S_IMODE((job_dir / ".download.lock").stat().st_mode), 0o600)
            os.close(first)
        finally:
            os.close(directory_fd)

    def test_partial_identity_mismatch_is_retained_and_rejected(self):
        home, request, job_dir = self._fixture(size=transfer.CHUNK_SIZE * 2)
        part_slot = hashlib.sha256(request.destination.name.encode()).hexdigest()[:24]
        part_name = f".music-transfer-{part_slot}.part"
        metadata_name = part_name + ".meta"
        initial_dir_fd = os.open(job_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            descriptor, _digest, _ = transfer._open_partial(
                initial_dir_fd,
                part_name,
                metadata_name,
                request.identity_sha256,
                request.expected_size,
            )
        finally:
            os.close(initial_dir_fd)
        os.write(descriptor, b"x" * transfer.CHUNK_SIZE)
        os.fsync(descriptor)
        os.close(descriptor)
        directory_fd = os.open(job_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            transfer._write_metadata(
                directory_fd,
                metadata_name,
                {
                    "schema": 1,
                    "identity_sha256": "0" * 64,
                    "expected_size": request.expected_size,
                    "offset": transfer.CHUNK_SIZE,
                    "prefix_sha256": hashlib.sha256(b"x" * transfer.CHUNK_SIZE).hexdigest(),
                },
            )
        finally:
            os.close(directory_fd)
        with self.assertRaises(transfer.TransferError):
            self._run_transfer(request, home)
        self.assertEqual((job_dir / part_name).stat().st_size, transfer.CHUNK_SIZE)

    def test_completion_receipt_accepts_benign_metadata_delta_and_replaced_inode(self):
        _home, request, job_dir = self._fixture(size=13)
        request.destination.write_bytes(b"complete-data")
        request.destination.chmod(0o600)
        part_slot = hashlib.sha256(request.destination.name.encode()).hexdigest()[:24]
        part_name = f".music-transfer-{part_slot}.part"
        metadata_name = part_name + ".meta"
        receipt_name = part_name + ".complete"
        before = request.destination.stat()
        old_receipt = transfer._completion_value(
            request.identity_sha256,
            request.expected_size,
            hashlib.sha256(b"complete-data").hexdigest(),
            SimpleNamespace(st_dev=before.st_dev + 1, st_ino=before.st_ino + 1),
        )
        directory_fd = os.open(job_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            transfer._write_metadata(directory_fd, receipt_name, old_receipt)
            os.utime(request.destination, ns=(1_000_000_000, 2_000_000_000))
            completed = transfer._recover_existing_destination(
                directory_fd,
                request.destination.name,
                part_name,
                metadata_name,
                receipt_name,
                request.identity_sha256,
                request.expected_size,
            )
            self.assertEqual(completed[1], hashlib.sha256(b"complete-data").hexdigest())
            updated = transfer._read_completion_receipt(directory_fd, receipt_name)
            self.assertEqual(updated["source_ino"], request.destination.stat().st_ino)
        finally:
            os.close(directory_fd)

    def test_completion_receipt_rejects_content_mutation(self):
        _home, request, job_dir = self._fixture(size=13)
        request.destination.write_bytes(b"complete-data")
        request.destination.chmod(0o600)
        part_slot = hashlib.sha256(request.destination.name.encode()).hexdigest()[:24]
        part_name = f".music-transfer-{part_slot}.part"
        receipt_name = part_name + ".complete"
        info = request.destination.stat()
        directory_fd = os.open(job_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            transfer._write_metadata(
                directory_fd,
                receipt_name,
                transfer._completion_value(
                    request.identity_sha256,
                    request.expected_size,
                    hashlib.sha256(b"complete-data").hexdigest(),
                    info,
                ),
            )
            request.destination.write_bytes(b"changed-bytes")
            request.destination.chmod(0o600)
            with self.assertRaises(transfer.TransferError) as caught:
                transfer._recover_existing_destination(
                    directory_fd,
                    request.destination.name,
                    part_name,
                    part_name + ".meta",
                    receipt_name,
                    request.identity_sha256,
                    request.expected_size,
                )
            self.assertEqual(caught.exception.code, transfer.ErrorCode.FILE_EXISTS)
        finally:
            os.close(directory_fd)

    def test_existing_receipt_makes_published_output_idempotent_without_telegram(self):
        home, request, job_dir = self._fixture(size=13)
        request.destination.write_bytes(b"complete-data")
        request.destination.chmod(0o600)
        part_slot = hashlib.sha256(request.destination.name.encode()).hexdigest()[:24]
        part_name = f".music-transfer-{part_slot}.part"
        receipt_name = part_name + ".complete"
        info = request.destination.stat()
        directory_fd = os.open(job_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            transfer._write_metadata(
                directory_fd,
                receipt_name,
                transfer._completion_value(
                    request.identity_sha256,
                    request.expected_size,
                    hashlib.sha256(b"complete-data").hexdigest(),
                    info,
                ),
            )
        finally:
            os.close(directory_fd)
        with mock.patch.object(
            transfer,
            "_read_credentials",
            side_effect=AssertionError("completed output must not start Telegram auth"),
        ), mock.patch.object(FakeTransport, "_new_sender", side_effect=AssertionError("no network")):
            result = asyncio.run(transfer.transfer(request, home, self.bindings, FakeTransport))
        self.assertTrue(result["ok"])
        self.assertEqual(result["sha256"], hashlib.sha256(b"complete-data").hexdigest())

    def test_same_inode_full_part_can_recover_a_published_file_without_receipt(self):
        _home, request, job_dir = self._fixture(size=13)
        part_slot = hashlib.sha256(request.destination.name.encode()).hexdigest()[:24]
        part_name = f".music-transfer-{part_slot}.part"
        metadata_name = part_name + ".meta"
        receipt_name = part_name + ".complete"
        directory_fd = os.open(job_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            partial_fd = os.open(
                part_name,
                os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory_fd,
            )
            os.write(partial_fd, b"complete-data")
            os.fsync(partial_fd)
            part_info = os.fstat(partial_fd)
            digest = hashlib.sha256(b"complete-data").hexdigest()
            transfer._write_metadata(
                directory_fd,
                metadata_name,
                {
                    "schema": 1,
                    "identity_sha256": request.identity_sha256,
                    "expected_size": request.expected_size,
                    "offset": request.expected_size,
                    "prefix_sha256": digest,
                },
            )
            os.link(
                part_name,
                request.destination.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
            self.assertEqual(part_info.st_ino, request.destination.stat().st_ino)
            result = transfer._recover_existing_destination(
                directory_fd,
                request.destination.name,
                part_name,
                metadata_name,
                receipt_name,
                request.identity_sha256,
                request.expected_size,
            )
            self.assertEqual(result, (request.expected_size, digest))
            self.assertFalse((job_dir / part_name).exists())
            self.assertTrue((job_dir / receipt_name).exists())
            self.assertEqual(request.destination.stat().st_nlink, 1)
        finally:
            os.close(directory_fd)
            try:
                os.close(partial_fd)
            except (UnboundLocalError, OSError):
                pass

    def test_dataless_completion_returns_pending_without_opening_audio(self):
        _home, request, job_dir = self._fixture(size=13)
        request.destination.write_bytes(b"complete-data")
        request.destination.chmod(0o600)
        part_slot = hashlib.sha256(request.destination.name.encode()).hexdigest()[:24]
        part_name = f".music-transfer-{part_slot}.part"
        receipt_name = part_name + ".complete"
        info = request.destination.stat()
        directory_fd = os.open(job_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            transfer._write_metadata(
                directory_fd,
                receipt_name,
                transfer._completion_value(
                    request.identity_sha256,
                    request.expected_size,
                    hashlib.sha256(b"complete-data").hexdigest(),
                    info,
                ),
            )
            with mock.patch.object(transfer, "_is_dataless", return_value=True), mock.patch.object(
                transfer, "_hash_existing_file", side_effect=AssertionError("audio must not be opened")
            ):
                with self.assertRaises(transfer.TransferError) as caught:
                    transfer._recover_existing_destination(
                        directory_fd,
                        request.destination.name,
                        part_name,
                        part_name + ".meta",
                        receipt_name,
                        request.identity_sha256,
                        request.expected_size,
                    )
            self.assertEqual(caught.exception.code, transfer.ErrorCode.MATERIALIZATION_PENDING)
        finally:
            os.close(directory_fd)


if __name__ == "__main__":
    unittest.main()
