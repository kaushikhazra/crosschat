"""Tests for crosschat.core (session cross-talk over NATS).

Uses minimal fakes for the NATS JetStream / KV surface, following the same
pattern as test_nats_bridge.py, so these tests never need a live NATS server.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from crosschat import signing
from crosschat.core import (
    DestinationNotRegisteredError,
    InfraNotReadyError,
    build_envelope,
    connect_with_retry,
    consumer_name,
    deregister_project,
    ensure_infra,
    infra_ready,
    is_registered,
    list_active_projects,
    monitor_consumer_config,
    monitor_loop,
    project_subject,
    register_project,
    send_message,
    resolve_project_id,
    slugify_project_id,
)
from nats.js.api import AckPolicy, DeliverPolicy
from nats.js.errors import BadRequestError, NotFoundError


# ---------------------------------------------------------------------------
# Minimal fakes
# ---------------------------------------------------------------------------


class _FakeKVEntry:
    def __init__(self, value: bytes | None):
        self.value = value


class _FakeKV:
    """Fake JetStream KV bucket."""

    def __init__(self, initial: dict[str, dict] | None = None):
        self._store: dict[str, bytes] = {
            k: json.dumps(v).encode() for k, v in (initial or {}).items()
        }
        self.put_calls: list[tuple[str, bytes]] = []

    async def get(self, key: str) -> _FakeKVEntry:
        if key not in self._store:
            raise KeyError(key)
        return _FakeKVEntry(self._store[key])

    async def put(self, key: str, value: bytes) -> None:
        self._store[key] = value
        self.put_calls.append((key, value))

    async def keys(self) -> list[str]:
        return list(self._store.keys())

    async def delete(self, key: str) -> None:
        self._store.pop(key, None)


class _FakeMsg:
    def __init__(self, data: dict):
        self.data = json.dumps(data).encode()
        self.acked = False

    async def ack(self):
        self.acked = True


class _FakeSub:
    """Fake JetStream subscription: returns queued messages, then times out forever."""

    def __init__(self, messages: list[dict]):
        self._queue = [_FakeMsg(m) for m in messages]
        self._pos = 0
        self.unsubscribed = False

    async def next_msg(self, timeout: float = 5.0) -> _FakeMsg:
        if self._pos < len(self._queue):
            msg = self._queue[self._pos]
            self._pos += 1
            return msg
        raise asyncio.TimeoutError()

    async def unsubscribe(self):
        self.unsubscribed = True


class _FakeJS:
    """Fake JetStream context."""

    def __init__(
        self,
        kv: _FakeKV | None = None,
        sub: _FakeSub | None = None,
        stream_exists: bool = True,
        bucket_exists: bool = True,
        raise_on_add_stream: Exception | None = None,
        raise_on_create_kv: Exception | None = None,
    ):
        self._kv = kv or _FakeKV()
        self._sub = sub
        self.stream_exists = stream_exists
        self.bucket_exists = bucket_exists
        self._raise_on_add_stream = raise_on_add_stream
        self._raise_on_create_kv = raise_on_create_kv
        self.add_stream_calls: list[dict] = []
        self.create_kv_calls: list[dict] = []
        self.publish_calls: list[tuple[str, bytes]] = []
        self.subscribe_calls: list[dict] = []

    async def add_stream(self, **kwargs):
        self.add_stream_calls.append(kwargs)
        if self._raise_on_add_stream:
            raise self._raise_on_add_stream

    async def create_key_value(self, **kwargs):
        self.create_kv_calls.append(kwargs)
        if self._raise_on_create_kv:
            raise self._raise_on_create_kv

    async def stream_info(self, name: str):
        if not self.stream_exists:
            raise NotFoundError()
        return {"name": name}

    async def key_value(self, bucket: str):
        if not self.bucket_exists:
            raise NotFoundError()
        return self._kv

    async def publish(self, subject: str, data: bytes):
        self.publish_calls.append((subject, data))

    async def subscribe(self, subject: str, durable: str = None, config=None):
        self.subscribe_calls.append(
            {"subject": subject, "durable": durable, "config": config}
        )
        return self._sub


# ---------------------------------------------------------------------------
# slugify_project_id
# ---------------------------------------------------------------------------


class TestSlugifyProjectId:
    def test_simple_windows_path(self):
        assert slugify_project_id(r"C:\Projects\velhari") == "velhari"

    def test_simple_posix_path(self):
        assert slugify_project_id("/home/user/velasari") == "velasari"

    def test_trailing_slash(self):
        assert slugify_project_id(r"C:\Projects\velhari\\") == "velhari"

    def test_mixed_case(self):
        assert slugify_project_id(r"C:\Projects\VelHari") == "velhari"

    def test_special_characters_collapsed(self):
        assert slugify_project_id(r"C:\Projects\My Cool Project!!") == "my-cool-project"

    def test_empty_path_raises(self):
        with pytest.raises(ValueError):
            slugify_project_id("")

    def test_path_of_only_separators_raises(self):
        with pytest.raises(ValueError):
            slugify_project_id("///")

    def test_leading_trailing_hyphens_stripped(self):
        assert slugify_project_id(r"C:\Projects\--velhari--") == "velhari"


class TestResolveProjectId:
    """Explicit id beats environment beats path.

    The path-derived rule was originally the only one, which tied a brain's
    address to where it sat on disk: renaming the folder renamed the brain,
    and two brains could not share a directory name anywhere on the machine.
    """

    def test_explicit_id_wins_over_everything(self):
        got = resolve_project_id(
            r"C:\Projects\some-folder",
            explicit="sb-architect",
            env={"CROSSCHAT_PROJECT_ID": "from-env"},
        )
        assert got == "sb-architect"

    def test_env_var_used_when_no_explicit_id(self):
        got = resolve_project_id(
            r"C:\Projects\some-folder", env={"CROSSCHAT_PROJECT_ID": "sb-researcher"}
        )
        assert got == "sb-researcher"

    def test_falls_back_to_path_when_neither_given(self):
        assert resolve_project_id(r"C:\Projects\velhari", env={}) == "velhari"

    def test_blank_env_var_is_ignored(self):
        got = resolve_project_id(
            r"C:\Projects\velhari", env={"CROSSCHAT_PROJECT_ID": "   "}
        )
        assert got == "velhari"

    def test_explicit_id_is_normalized_not_trusted_verbatim(self):
        """A human-chosen id still has to be a legal subject token."""
        got = resolve_project_id(r"C:\Projects\x", explicit="SB Architect!", env={})
        assert got == "sb-architect"

    def test_unusable_explicit_id_raises(self):
        with pytest.raises(ValueError):
            resolve_project_id(r"C:\Projects\x", explicit="!!!", env={})


class TestSubjectAndConsumerNames:
    def test_project_subject(self):
        assert project_subject("velhari") == "crosschat.velhari"

    def test_consumer_name(self):
        assert consumer_name("velhari") == "crosschat-velhari"


# ---------------------------------------------------------------------------
# build_envelope
# ---------------------------------------------------------------------------


class TestBuildEnvelope:
    def test_contains_required_fields(self):
        env = build_envelope("velhari", "velasari", "hello")
        assert env["source_project_id"] == "velhari"
        assert env["dest_project_id"] == "velasari"
        assert env["text"] == "hello"
        assert "sent_at" in env and env["sent_at"]

    def test_sent_at_is_iso_parseable(self):
        from datetime import datetime

        env = build_envelope("a", "b", "hi")
        datetime.fromisoformat(env["sent_at"])


# ---------------------------------------------------------------------------
# connect_with_retry
# ---------------------------------------------------------------------------


class TestConnectWithRetry:
    @pytest.mark.asyncio
    async def test_succeeds_on_first_try(self):
        calls = []

        async def fake_connect(**kwargs):
            calls.append(kwargs)
            return "CONNECTED"

        result = await connect_with_retry("nats://x", connect_fn=fake_connect)
        assert result == "CONNECTED"
        assert len(calls) == 1
        assert calls[0]["allow_reconnect"] is True

    @pytest.mark.asyncio
    async def test_retries_then_succeeds(self):
        attempts = {"n": 0}

        async def flaky_connect(**kwargs):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError("not up yet")
            return "CONNECTED"

        result = await connect_with_retry(
            "nats://x", timeout=30, connect_fn=flaky_connect
        )
        assert result == "CONNECTED"
        assert attempts["n"] == 3

    @pytest.mark.asyncio
    async def test_raises_connection_error_after_timeout(self):
        async def always_fails(**kwargs):
            raise RuntimeError("nats down")

        with pytest.raises(ConnectionError):
            await connect_with_retry("nats://x", timeout=0.01, connect_fn=always_fails)


# ---------------------------------------------------------------------------
# ensure_infra / infra_ready
# ---------------------------------------------------------------------------


class TestEnsureInfra:
    @pytest.mark.asyncio
    async def test_creates_stream_and_bucket(self):
        js = _FakeJS()
        await ensure_infra(js)
        assert js.add_stream_calls[0]["name"] == "CROSSCHAT"
        assert js.add_stream_calls[0]["subjects"] == ["crosschat.>"]
        assert js.create_kv_calls[0]["bucket"] == "crosschat-registry"

    @pytest.mark.asyncio
    async def test_idempotent_when_stream_already_exists(self):
        js = _FakeJS(raise_on_add_stream=BadRequestError("stream name already in use"))
        await ensure_infra(js)  # must not raise

    @pytest.mark.asyncio
    async def test_idempotent_when_bucket_already_exists(self):
        js = _FakeJS(raise_on_create_kv=BadRequestError("bucket name already in use"))
        await ensure_infra(js)  # must not raise

    @pytest.mark.asyncio
    async def test_propagates_unexpected_stream_error(self):
        js = _FakeJS(raise_on_add_stream=RuntimeError("disk full"))
        with pytest.raises(RuntimeError):
            await ensure_infra(js)

    @pytest.mark.asyncio
    async def test_propagates_subject_overlap_instead_of_reporting_success(self):
        """Regression — a subject-overlap rejection must not read as already-exists.

        JetStream raises BadRequestError for BOTH "already exists" and "subjects
        overlap with an existing stream". Swallowing the whole class made `init`
        print success while creating nothing, and every later call then failed
        with a misleading "infra not found". Found live 2026-08-10 against a
        server still carrying the predecessor's `project.>` stream.
        """
        js = _FakeJS(
            raise_on_add_stream=BadRequestError(
                "subjects overlap with an existing stream"
            )
        )
        with pytest.raises(BadRequestError):
            await ensure_infra(js)


class TestInfraReady:
    @pytest.mark.asyncio
    async def test_true_when_both_exist(self):
        js = _FakeJS(stream_exists=True, bucket_exists=True)
        assert await infra_ready(js) is True

    @pytest.mark.asyncio
    async def test_false_when_stream_missing(self):
        js = _FakeJS(stream_exists=False, bucket_exists=True)
        assert await infra_ready(js) is False

    @pytest.mark.asyncio
    async def test_false_when_bucket_missing(self):
        js = _FakeJS(stream_exists=True, bucket_exists=False)
        assert await infra_ready(js) is False


# ---------------------------------------------------------------------------
# register_project / list_active_projects / is_registered
# ---------------------------------------------------------------------------


class TestRegisterProject:
    @pytest.mark.asyncio
    async def test_writes_entry(self):
        js = _FakeJS()
        await register_project(js, "velhari", "Velhari", r"C:\Projects\velhari")
        key, value = js._kv.put_calls[0]
        assert key == "velhari"
        entry = json.loads(value)
        assert entry["project_name"] == "Velhari"
        assert entry["path"] == r"C:\Projects\velhari"
        assert entry["registered_at"] == entry["last_heartbeat"]

    @pytest.mark.asyncio
    async def test_raises_when_infra_missing(self):
        js = _FakeJS(stream_exists=False)
        with pytest.raises(InfraNotReadyError):
            await register_project(js, "velhari", "Velhari", r"C:\Projects\velhari")

    @pytest.mark.asyncio
    async def test_publishes_public_key(self, tmp_path, monkeypatch):
        """Key distribution rides on the registry, which already exists."""
        monkeypatch.setenv("CROSSCHAT_KEY_DIR", str(tmp_path / "keys"))
        js = _FakeJS()
        await register_project(js, "velhari", "Velhari", r"C:\Projects\velhari")
        entry = json.loads(js._kv.put_calls[0][1])
        assert len(entry["public_key"]) == 64, (
            "an Ed25519 public key is 32 bytes of hex"
        )

    @pytest.mark.asyncio
    async def test_never_publishes_the_private_key(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CROSSCHAT_KEY_DIR", str(tmp_path / "keys"))
        js = _FakeJS()
        await register_project(js, "velhari", "Velhari", r"C:\Projects\velhari")
        published = js._kv.put_calls[0][1].decode()
        on_disk = (
            (tmp_path / "keys" / "velhari.key").read_text(encoding="utf-8").strip()
        )
        assert on_disk not in published

    @pytest.mark.asyncio
    async def test_registers_unsigned_when_signing_unavailable(
        self, tmp_path, monkeypatch
    ):
        """A project that cannot sign must still be reachable."""
        monkeypatch.setenv("CROSSCHAT_KEY_DIR", str(tmp_path / "keys"))

        def boom(*_args, **_kwargs):
            raise signing.SigningUnavailableError("no pynacl")

        monkeypatch.setattr(signing, "load_or_create_signing_key", boom)
        js = _FakeJS()
        await register_project(js, "velhari", "Velhari", r"C:\Projects\velhari")
        entry = json.loads(js._kv.put_calls[0][1])
        assert "public_key" not in entry
        assert entry["project_name"] == "Velhari"


class TestSignedDelivery:
    """The end-to-end property: a receiver can tell a real sender from a liar."""

    @pytest.mark.asyncio
    async def test_monitor_marks_a_genuine_message_verified(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("CROSSCHAT_KEY_DIR", str(tmp_path / "keys"))
        _, public = signing.load_or_create_signing_key("kaylan")
        envelope = signing.sign_envelope(
            build_envelope("kaylan", "velasari", "hello"), "kaylan"
        )
        kv = _FakeKV({"kaylan": {"project_name": "Kaylan", "public_key": public}})
        js = _FakeJS(sub=_FakeSub([envelope]), kv=kv)
        emitted = []
        await monitor_loop(js, kv, "velasari", emit=emitted.append, max_iterations=1)
        payload = json.loads(emitted[0][len("CROSSCHAT_MESSAGE ") :])
        assert payload["verification"] == signing.VERIFIED

    @pytest.mark.asyncio
    async def test_monitor_flags_an_impersonator(self, tmp_path, monkeypatch):
        """kaylan signs, relabels itself velhari, and is caught — because the
        signature is checked against velhari's published key, not its own."""
        monkeypatch.setenv("CROSSCHAT_KEY_DIR", str(tmp_path / "keys"))
        signing.load_or_create_signing_key("kaylan")
        _, velhari_public = signing.load_or_create_signing_key("velhari")
        forged = signing.sign_envelope(
            build_envelope("kaylan", "velasari", "trust me"), "kaylan"
        )
        forged["source_project_id"] = "velhari"
        kv = _FakeKV(
            {"velhari": {"project_name": "Velhari", "public_key": velhari_public}}
        )
        js = _FakeJS(sub=_FakeSub([forged]), kv=kv)
        emitted = []
        await monitor_loop(js, kv, "velasari", emit=emitted.append, max_iterations=1)
        payload = json.loads(emitted[0][len("CROSSCHAT_MESSAGE ") :])
        assert payload["verification"] == signing.BAD_SIGNATURE

    @pytest.mark.asyncio
    async def test_forged_message_is_still_delivered(self, tmp_path, monkeypatch):
        """Marked, not dropped. What an unverifiable message is worth is the
        receiving session's judgement, not the listener's."""
        monkeypatch.setenv("CROSSCHAT_KEY_DIR", str(tmp_path / "keys"))
        signing.load_or_create_signing_key("kaylan")
        _, velhari_public = signing.load_or_create_signing_key("velhari")
        forged = signing.sign_envelope(
            build_envelope("kaylan", "velasari", "trust me"), "kaylan"
        )
        forged["source_project_id"] = "velhari"
        kv = _FakeKV(
            {"velhari": {"project_name": "Velhari", "public_key": velhari_public}}
        )
        js = _FakeJS(sub=_FakeSub([forged]), kv=kv)
        emitted = []
        await monitor_loop(js, kv, "velasari", emit=emitted.append, max_iterations=1)
        assert len(emitted) == 1
        assert json.loads(emitted[0][len("CROSSCHAT_MESSAGE ") :])["text"] == "trust me"

    @pytest.mark.asyncio
    async def test_sender_without_published_key_reports_no_key(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("CROSSCHAT_KEY_DIR", str(tmp_path / "keys"))
        signing.load_or_create_signing_key("kaylan")
        envelope = signing.sign_envelope(
            build_envelope("kaylan", "velasari", "hi"), "kaylan"
        )
        kv = _FakeKV({"kaylan": {"project_name": "Kaylan"}})  # registered, no key
        js = _FakeJS(sub=_FakeSub([envelope]), kv=kv)
        emitted = []
        await monitor_loop(js, kv, "velasari", emit=emitted.append, max_iterations=1)
        payload = json.loads(emitted[0][len("CROSSCHAT_MESSAGE ") :])
        assert payload["verification"] == signing.NO_KEY


class TestListActiveProjects:
    @pytest.mark.asyncio
    async def test_lists_all_entries(self):
        kv = _FakeKV(
            {
                "velhari": {"project_name": "Velhari", "path": "C:\\Projects\\velhari"},
                "velasari": {
                    "project_name": "Velasari",
                    "path": "C:\\Projects\\velasari",
                },
            }
        )
        result = await list_active_projects(kv)
        ids = {p["project_id"] for p in result}
        assert ids == {"velhari", "velasari"}

    @pytest.mark.asyncio
    async def test_empty_registry_returns_empty_list(self):
        result = await list_active_projects(_FakeKV())
        assert result == []

    @pytest.mark.asyncio
    async def test_skips_malformed_entry(self):
        kv = _FakeKV()
        await kv.put("bad", b"not json")
        result = await list_active_projects(kv)
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_keys_failure(self):
        class _BrokenKV:
            async def keys(self):
                raise RuntimeError("bucket gone")

        result = await list_active_projects(_BrokenKV())
        assert result == []


class TestIsRegistered:
    @pytest.mark.asyncio
    async def test_true_when_entry_present(self):
        kv = _FakeKV({"velhari": {"project_name": "Velhari"}})
        assert await is_registered(kv, "velhari") is True

    @pytest.mark.asyncio
    async def test_false_when_entry_absent_or_expired(self):
        kv = _FakeKV()
        assert await is_registered(kv, "ghost") is False


class TestDeregisterProject:
    @pytest.mark.asyncio
    async def test_removes_existing_entry(self):
        kv = _FakeKV({"velhari": {"project_name": "Velhari"}})
        await deregister_project(kv, "velhari")
        assert await is_registered(kv, "velhari") is False

    @pytest.mark.asyncio
    async def test_idempotent_on_absent_entry(self):
        kv = _FakeKV()
        await deregister_project(kv, "ghost")  # must not raise

    @pytest.mark.asyncio
    async def test_swallows_unexpected_delete_error(self):
        class _BrokenKV:
            async def delete(self, key):
                raise RuntimeError("bucket unavailable")

        await deregister_project(_BrokenKV(), "velhari")  # must not raise

    @pytest.mark.asyncio
    async def test_does_not_remove_other_entries(self):
        kv = _FakeKV({"velhari": {}, "velasari": {}})
        await deregister_project(kv, "velhari")
        assert await is_registered(kv, "velhari") is False
        assert await is_registered(kv, "velasari") is True


# ---------------------------------------------------------------------------
# send_message
# ---------------------------------------------------------------------------


class TestSendMessage:
    @pytest.mark.asyncio
    async def test_publishes_to_correct_subject(self):
        kv = _FakeKV({"velasari": {"project_name": "Velasari"}})
        js = _FakeJS(kv=kv)
        envelope = build_envelope("velhari", "velasari", "ping")
        await send_message(js, "velasari", envelope, sign=False)
        subject, payload = js.publish_calls[0]
        assert subject == "crosschat.velasari"
        assert json.loads(payload) == envelope

    @pytest.mark.asyncio
    async def test_raises_when_destination_not_registered(self):
        js = _FakeJS(kv=_FakeKV())
        envelope = build_envelope("velhari", "ghost-project", "ping")
        with pytest.raises(DestinationNotRegisteredError):
            await send_message(js, "ghost-project", envelope)
        assert js.publish_calls == []

    @pytest.mark.asyncio
    async def test_returns_immediately_no_blocking(self):
        """Fire-and-forget: send_message never subscribes/waits for a reply."""
        kv = _FakeKV({"velasari": {}})
        js = _FakeJS(kv=kv)
        envelope = build_envelope("velhari", "velasari", "ping")
        await send_message(js, "velasari", envelope)
        assert js.subscribe_calls == []


# ---------------------------------------------------------------------------
# monitor_consumer_config
# ---------------------------------------------------------------------------


class TestMonitorConsumerConfig:
    def test_uses_explicit_ack_and_deliver_all(self):
        cfg = monitor_consumer_config()
        assert cfg.ack_policy == AckPolicy.EXPLICIT
        assert cfg.deliver_policy == DeliverPolicy.ALL


# ---------------------------------------------------------------------------
# monitor_loop
# ---------------------------------------------------------------------------


class TestMonitorLoop:
    @pytest.mark.asyncio
    async def test_subscribes_with_durable_consumer_name(self):
        js = _FakeJS(sub=_FakeSub([]))
        kv = _FakeKV()
        await monitor_loop(js, kv, "velhari", emit=lambda _: None, max_iterations=1)
        call = js.subscribe_calls[0]
        assert call["subject"] == "crosschat.velhari"
        assert call["durable"] == "crosschat-velhari"

    @pytest.mark.asyncio
    async def test_emits_and_acks_each_message(self):
        msgs = [{"source_project_id": "a", "text": "hi", "sent_at": "t"}]
        js = _FakeJS(sub=_FakeSub(msgs))
        kv = _FakeKV()
        emitted = []
        await monitor_loop(js, kv, "velhari", emit=emitted.append, max_iterations=1)
        assert len(emitted) == 1
        assert emitted[0].startswith("CROSSCHAT_MESSAGE ")
        payload = json.loads(emitted[0][len("CROSSCHAT_MESSAGE ") :])
        # The envelope is passed through untouched, plus a verification verdict.
        assert {k: v for k, v in payload.items() if k != "verification"} == msgs[0]
        assert payload["verification"] == "unsigned"
        assert js._sub._queue[0].acked is True

    @pytest.mark.asyncio
    async def test_delivers_messages_in_order_no_replay(self):
        """Regression test for dryrun-2 C1: two messages in quick succession must
        both be delivered, in order — not a replay of the first message only."""
        msgs = [{"n": 1}, {"n": 2}]
        js = _FakeJS(sub=_FakeSub(msgs))
        kv = _FakeKV()
        emitted = []
        await monitor_loop(js, kv, "velhari", emit=emitted.append, max_iterations=2)
        payloads = [json.loads(line[len("CROSSCHAT_MESSAGE ") :]) for line in emitted]
        assert [
            {k: v for k, v in p.items() if k != "verification"} for p in payloads
        ] == msgs

    @pytest.mark.asyncio
    async def test_timeout_does_not_emit_a_line(self):
        js = _FakeJS(sub=_FakeSub([]))  # empty queue -> always times out
        kv = _FakeKV()
        emitted = []
        await monitor_loop(
            js,
            kv,
            "velhari",
            emit=emitted.append,
            max_iterations=3,
            message_timeout=0.001,
        )
        assert emitted == []

    @pytest.mark.asyncio
    async def test_refreshes_heartbeat_when_due(self):
        js = _FakeJS(sub=_FakeSub([]))
        kv = _FakeKV({"velhari": {"project_name": "Velhari", "path": "p"}})
        await monitor_loop(
            js,
            kv,
            "velhari",
            emit=lambda _: None,
            max_iterations=1,
            heartbeat_interval=0,  # due immediately
            message_timeout=0.001,
        )
        assert len(kv.put_calls) == 1
        key, value = kv.put_calls[0]
        assert key == "velhari"
        entry = json.loads(value)
        assert entry["project_name"] == "Velhari"  # preserved
        assert "last_heartbeat" in entry

    @pytest.mark.asyncio
    async def test_does_not_refresh_heartbeat_before_interval(self):
        js = _FakeJS(sub=_FakeSub([]))
        kv = _FakeKV({"velhari": {}})
        await monitor_loop(
            js,
            kv,
            "velhari",
            emit=lambda _: None,
            max_iterations=1,
            heartbeat_interval=3600,  # not due
            message_timeout=0.001,
        )
        assert kv.put_calls == []

    @pytest.mark.asyncio
    async def test_unsubscribes_on_exit(self):
        sub = _FakeSub([])
        js = _FakeJS(sub=sub)
        kv = _FakeKV()
        await monitor_loop(js, kv, "velhari", emit=lambda _: None, max_iterations=1)
        assert sub.unsubscribed is True

    @pytest.mark.asyncio
    async def test_unsubscribes_even_if_loop_body_raises(self):
        sub = _FakeSub([{"bad": "msg"}])

        class _RaisingKV(_FakeKV):
            async def put(self, key, value):
                raise RuntimeError("kv unavailable")

        js = _FakeJS(sub=sub)
        kv = _RaisingKV()
        with pytest.raises(RuntimeError):
            await monitor_loop(
                js,
                kv,
                "velhari",
                emit=lambda _: None,
                max_iterations=1,
                heartbeat_interval=0,
            )
        assert sub.unsubscribed is True

    @pytest.mark.asyncio
    async def test_handles_malformed_message_json(self):
        sub = _FakeSub([])
        # inject a raw non-JSON message directly
        bad_msg = _FakeMsg.__new__(_FakeMsg)
        bad_msg.data = b"not json"
        bad_msg.acked = False

        async def ack():
            bad_msg.acked = True

        bad_msg.ack = ack
        sub._queue = [bad_msg]
        js = _FakeJS(sub=sub)
        kv = _FakeKV()
        emitted = []
        await monitor_loop(js, kv, "velhari", emit=emitted.append, max_iterations=1)
        assert len(emitted) == 1
        assert '"raw": "not json"' in emitted[0]
        assert bad_msg.acked is True
