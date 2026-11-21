import hashlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import social_media_reply as reply


# Synthetic catalog fixture: active api-key-a, used only by mocked TelegramClient.
SYNTHETIC_BOT_TOKEN = "codex_synth_v1_api_key_a"


def _tl_class(name):
    def init(self, **kwargs):
        self.__dict__.update(kwargs)

    return type(name, (), {"__init__": init})


class RandomIdDuplicateError(Exception):
    pass


class UserMigrateError(Exception):
    def __init__(self, new_dc):
        self.new_dc = new_dc


class FakeClient:
    instances = []
    bindings = None
    send_error = None
    migrations = []
    bot_id = 123456

    def __init__(self, session, api_id, api_hash, **options):
        self.session = session
        self.api_id = api_id
        self.api_hash = api_hash
        self.options = options
        self.calls = []
        self.started = False
        self.start_attempts = 0
        self.disconnected = False
        type(self).instances.append(self)

    async def start(self, *, bot_token):
        self.start_attempts += 1
        if type(self).migrations:
            destination, switched = type(self).migrations.pop(0)
            if switched:
                self.session.dc_id = destination
            raise UserMigrateError(destination)
        self.started = True
        self.bot_token = bot_token

    async def get_me(self):
        return SimpleNamespace(id=type(self).bot_id, bot=True)

    async def __call__(self, request):
        self.calls.append(request)
        bindings = type(self).bindings
        if type(request) in {
            bindings.save_file_part_request_type,
            bindings.save_big_file_part_request_type,
        }:
            return True
        if type(request) is bindings.send_media_request_type:
            error = type(self).send_error
            if error is not None:
                raise error
            return SimpleNamespace(updates=[])
        raise AssertionError("unexpected Telegram request")

    async def disconnect(self):
        self.disconnected = True


class FakeBindings:
    def __init__(self):
        self.memory_session_type = type("MemorySession", (), {"dc_id": 2})
        self.input_peer_user_type = _tl_class("InputPeerUser")
        self.input_media_uploaded_document_type = _tl_class("InputMediaUploadedDocument")
        self.document_attribute_filename_type = _tl_class("DocumentAttributeFilename")
        self.input_reply_to_message_type = _tl_class("InputReplyToMessage")
        self.send_media_request_type = _tl_class("SendMediaRequest")
        self.save_file_part_request_type = _tl_class("SaveFilePartRequest")
        self.save_big_file_part_request_type = _tl_class("SaveBigFilePartRequest")
        self.input_file_type = _tl_class("InputFile")
        self.input_file_big_type = _tl_class("InputFileBig")
        self.client_type = FakeClient
        FakeClient.bindings = self
        FakeClient.instances = []
        FakeClient.send_error = None
        FakeClient.migrations = []

    def as_bindings(self):
        return reply.TelegramBindings(
            client_type=self.client_type,
            memory_session_type=self.memory_session_type,
            user_migrate_error_type=UserMigrateError,
            input_peer_user_type=self.input_peer_user_type,
            input_media_uploaded_document_type=self.input_media_uploaded_document_type,
            document_attribute_filename_type=self.document_attribute_filename_type,
            input_reply_to_message_type=self.input_reply_to_message_type,
            send_media_request_type=self.send_media_request_type,
            save_file_part_request_type=self.save_file_part_request_type,
            save_big_file_part_request_type=self.save_big_file_part_request_type,
            input_file_type=self.input_file_type,
            input_file_big_type=self.input_file_big_type,
        )


class SocialMediaReplyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.owner = tempfile.TemporaryDirectory()
        self.addCleanup(self.owner.cleanup)
        self.base = Path(self.owner.name).resolve(strict=True)
        self.home = self.base / "home"
        self.home.mkdir(mode=0o700)
        self.home.chmod(0o700)
        self.credentials_dir = self.home / ".config/telegram-video-downloader"
        self.credentials_dir.mkdir(parents=True, mode=0o700)
        self.credentials_dir.chmod(0o700)
        self.credentials_path = self.credentials_dir / "mtproto.json"
        self.job_dir = self.base / "private-job"
        self.job_dir.mkdir(mode=0o700)
        self.job_dir.chmod(0o700)
        self.file_path = self.job_dir / "original.mp4"
        self.bindings = FakeBindings()
        token_id_patch = mock.patch.object(
            reply,
            "_bot_id_from_token",
            return_value=FakeClient.bot_id,
        )
        token_id_patch.start()
        self.addCleanup(token_id_patch.stop)

    def _request(self, payload=b"verified media bytes", expected_sha256=None):
        self.file_path.write_bytes(payload)
        self.file_path.chmod(0o600)
        value = {
            "bot_token": SYNTHETIC_BOT_TOKEN,
            "credentials_path": str(self.credentials_path),
            "chat_id": 123456,
            "reply_to_message_id": 77,
            "random_id": -987654321,
            "file_path": str(self.file_path),
            "file_name": "original.mp4",
            "expected_sha256": expected_sha256 or hashlib.sha256(payload).hexdigest(),
        }
        return reply.validate_request(value, self.home), value

    async def test_uploads_as_document_and_replies_without_receiving_updates(self):
        request, _ = self._request()

        await reply.send_reply(
            request,
            {"api_id": 123, "api_hash": "test"},
            self.bindings.as_bindings(),
        )

        client = FakeClient.instances[-1]
        self.assertTrue(client.started)
        self.assertTrue(client.disconnected)
        self.assertIsInstance(client.session, self.bindings.memory_session_type)
        self.assertIs(client.options["receive_updates"], False)
        self.assertIs(client.options["catch_up"], False)
        send = client.calls[-1]
        self.assertIs(type(send), self.bindings.send_media_request_type)
        self.assertEqual(send.random_id, request.random_id)
        self.assertEqual(send.peer.user_id, request.chat_id)
        self.assertEqual(send.peer.access_hash, 0)
        self.assertEqual(send.reply_to.reply_to_msg_id, request.reply_to_message_id)
        self.assertEqual(send.media.mime_type, "application/octet-stream")
        self.assertIs(send.media.force_file, True)
        self.assertEqual(send.media.attributes[0].file_name, request.file_name)
        self.assertIs(type(send.media.file), self.bindings.input_file_type)
        self.assertEqual(
            sum(type(call) is self.bindings.save_file_part_request_type for call in client.calls),
            1,
        )

    async def test_large_file_uses_big_parts_and_big_input_file(self):
        request, _ = self._request(b"this is over the patched test threshold")

        with mock.patch.object(reply, "BIG_FILE_THRESHOLD", 3):
            await reply.send_reply(
                request,
                {"api_id": 123, "api_hash": "test"},
                self.bindings.as_bindings(),
            )

        client = FakeClient.instances[-1]
        send = client.calls[-1]
        self.assertIs(type(send.media.file), self.bindings.input_file_big_type)
        self.assertEqual(
            sum(
                type(call) is self.bindings.save_big_file_part_request_type
                for call in client.calls
            ),
            1,
        )

    async def test_hash_mismatch_prevents_send_media(self):
        request, _ = self._request(expected_sha256="0" * 64)

        with self.assertRaises(reply.ReplyError) as raised:
            await reply.send_reply(
                request,
                {"api_id": 123, "api_hash": "test"},
                self.bindings.as_bindings(),
            )

        self.assertEqual(raised.exception.category, "file_integrity_error")
        client = FakeClient.instances[-1]
        self.assertFalse(
            any(type(call) is self.bindings.send_media_request_type for call in client.calls)
        )

    async def test_random_id_duplicate_is_delivery_unknown(self):
        request, _ = self._request()
        FakeClient.send_error = RandomIdDuplicateError()

        with self.assertRaises(reply.ReplyError) as raised:
            await reply.send_reply(
                request,
                {"api_id": 123, "api_hash": "test"},
                self.bindings.as_bindings(),
            )

        self.assertEqual(raised.exception.category, "delivery_unknown")

    async def test_authenticated_bot_must_match_token_prefix_identity(self):
        request, _ = self._request()

        with mock.patch.object(reply, "_bot_id_from_token", return_value=654321):
            with self.assertRaises(reply.ReplyError) as raised:
                await reply.send_reply(
                    request,
                    {"api_id": 123, "api_hash": "test"},
                    self.bindings.as_bindings(),
                )

        self.assertEqual(raised.exception.category, "bot_identity_invalid")
        self.assertEqual(FakeClient.instances[-1].calls, [])

    async def test_auth_retries_once_after_telethon_switches_data_centers(self):
        request, _ = self._request()
        FakeClient.migrations = [(4, True)]

        await reply.send_reply(
            request, {"api_id": 123, "api_hash": "test"}, self.bindings.as_bindings()
        )

        client = FakeClient.instances[-1]
        self.assertEqual(client.start_attempts, 2)
        self.assertEqual(client.session.dc_id, 4)
        self.assertEqual(client.options["request_retries"], 0)
        self.assertIs(type(client.calls[-1]), self.bindings.send_media_request_type)

    async def test_auth_does_not_retry_an_unconfirmed_switch_or_a_second_migration(self):
        request, _ = self._request()
        cases = ((((4, False),), 1), (((4, True), (5, True)), 2))
        for migrations, attempts in cases:
            with self.subTest(migrations=migrations):
                FakeClient.migrations = list(migrations)
                with self.assertRaises(reply.ReplyError) as raised:
                    await reply.send_reply(
                        request, {"api_id": 123, "api_hash": "test"}, self.bindings.as_bindings()
                    )
                self.assertEqual(raised.exception.category, "telegram_error")
                client = FakeClient.instances[-1]
                self.assertEqual(client.start_attempts, attempts)
                self.assertEqual(client.calls, [])

    def test_private_request_validation_requires_positive_dm_id_and_stable_random_id(self):
        _, value = self._request()
        invalid_values = (
            {**value, "chat_id": 0},
            {**value, "chat_id": -123},
            {**value, "reply_to_message_id": 0},
            {**value, "random_id": 0},
            {**value, "expected_sha256": "not-a-digest"},
        )
        for invalid in invalid_values:
            with self.subTest(invalid=invalid):
                with self.assertRaises(reply.ReplyError):
                    reply.validate_request(invalid, self.home)

    def test_source_symlink_is_rejected(self):
        target = self.base / "target"
        target.write_bytes(b"private data")
        target.chmod(0o600)
        self.file_path.symlink_to(target)

        with self.assertRaises(reply.ReplyError) as raised:
            reply._open_source_file(self.file_path)

        self.assertEqual(raised.exception.category, "file_unavailable")

    def test_credentials_require_private_files_and_canonical_location(self):
        self.credentials_path.write_text(
            json.dumps({"api_id": 123, "api_hash": SYNTHETIC_BOT_TOKEN}),
            encoding="utf-8",
        )
        self.credentials_path.chmod(0o600)

        with self.assertRaises(reply.ReplyError) as raised:
            reply.read_credentials(self.credentials_path, self.home)
        self.assertEqual(raised.exception.category, "credentials_unavailable")

        linked_target = self.credentials_dir / "linked-target.json"
        linked_target.write_text(
            json.dumps({"api_id": 123, "api_hash": SYNTHETIC_BOT_TOKEN}),
            encoding="utf-8",
        )
        linked_target.chmod(0o600)
        self.credentials_path.unlink()
        self.credentials_path.symlink_to(linked_target)
        with self.assertRaises(reply.ReplyError) as linked:
            reply.read_credentials(self.credentials_path, self.home)
        self.assertEqual(linked.exception.category, "credentials_unavailable")

    def test_oversize_is_a_separate_safe_category(self):
        self.file_path.write_bytes(b"x" * 8)
        self.file_path.chmod(0o600)

        with mock.patch.object(reply, "MAX_FILE_BYTES", 7):
            with self.assertRaises(reply.ReplyError) as raised:
                reply._open_source_file(self.file_path)
        self.assertEqual(raised.exception.category, "file_too_large")

    def test_duplicate_json_fields_are_rejected(self):
        with self.assertRaises(reply.ReplyError):
            reply.read_request(io.BytesIO(b""))

        with self.assertRaises(reply.ReplyError):
            reply.read_request(io.BytesIO(b'{"chat_id":1,"chat_id":2}'))


if __name__ == "__main__":
    unittest.main()
