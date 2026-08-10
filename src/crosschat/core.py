"""Cross-chat: fire-and-forget messaging between Claude Code sessions over NATS.

Client-side logic only — the whole package is a thin, dependency-light layer
over ``nats.py``, exposed through the ``crosschat`` console script (cli.py)
and documented by the crosschat-init/register/send/monitor skills.

Addressing is at the **project** level, not the session level: any session can
message any registered project by its stable ``project_id``, without knowing
which ephemeral session is currently running there.

See .claude/specs/session-cross-chat/design.md for the full design.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import nats
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from nats.js.errors import BadRequestError, NotFoundError

from crosschat import signing

logger = logging.getLogger(__name__)

STREAM_NAME = "CROSSCHAT"
# Namespaced under our own prefix rather than claiming a bare ``project.>``.
# Two reasons: a published package must not squat a generic namespace on
# someone else's NATS server, and JetStream rejects a stream whose subjects
# overlap an existing one — so a greedy prefix makes crosschat un-installable
# alongside anything that got there first.
STREAM_SUBJECTS = ["crosschat.>"]
STREAM_MAX_AGE = 1 * 86400  # 1 day
REGISTRY_BUCKET = "crosschat-registry"
REGISTRY_TTL = 5 * 60  # 5 minutes
HEARTBEAT_INTERVAL = 60  # seconds


class InfraNotReadyError(RuntimeError):
    """Raised when crosschat-init hasn't been run yet (CROSSCHAT stream / registry bucket missing)."""


class DestinationNotRegisteredError(RuntimeError):
    """Raised by send_message() when the destination project has no live registry entry."""


# ---------------------------------------------------------------------------
# Project ID
# ---------------------------------------------------------------------------


def slugify_project_id(path: str) -> str:
    """Derive the canonical project_id from a project directory path.

    Lowercase, non-alphanumeric characters collapsed to single hyphens,
    leading/trailing hyphens stripped. e.g. ``C:\\Projects\\velhari`` -> ``velhari``.
    This is the single derivation point (design.md "Project ID — canonical
    definition") — no other function in this module re-derives it independently.
    """
    normalized = path.replace("\\", "/").rstrip("/")
    name = normalized.rsplit("/", 1)[-1]
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    if not slug:
        raise ValueError(f"Cannot derive a project_id from path: {path!r}")
    return slug


ID_ENV_VAR = "CROSSCHAT_PROJECT_ID"


def normalize_project_id(value: str) -> str:
    """Slugify an explicitly supplied id, applying the same rules as a path.

    An id chosen by a human still has to be a legal NATS subject token and a
    legal durable-consumer name, so it goes through the same normalization
    rather than being trusted verbatim.
    """
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    if not slug:
        raise ValueError(f"Not a usable project_id: {value!r}")
    return slug


def resolve_project_id(
    path: str, explicit: str | None = None, env: dict | None = None
) -> str:
    """Decide a project's id: explicit flag, then environment, then path.

    The path-derived default was the only rule originally, which tied a
    brain's identity to where it happened to sit on disk — renaming the
    folder silently renamed the brain, and two brains could not share a
    directory name. An explicit id is now first-class; the path remains the
    fallback so existing callers are unaffected.

    The environment variable is what makes this usable from the plugin's
    SessionStart hook: a per-project ``.claude/settings.json`` can set
    ``CROSSCHAT_PROJECT_ID`` and the generic hook picks it up without any
    per-instance edit.
    """
    if explicit:
        return normalize_project_id(explicit)
    env = os.environ if env is None else env
    from_env = (env.get(ID_ENV_VAR) or "").strip()
    if from_env:
        return normalize_project_id(from_env)
    return slugify_project_id(path)


def project_subject(project_id: str) -> str:
    return f"crosschat.{project_id}"


def consumer_name(project_id: str) -> str:
    return f"crosschat-{project_id}"


# ---------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------


def build_envelope(source_project_id: str, dest_project_id: str, text: str) -> dict:
    """Build a cross-talk message envelope (design.md "Message envelope")."""
    return {
        "source_project_id": source_project_id,
        "dest_project_id": dest_project_id,
        "text": text,
        "sent_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Connection — initial connect retry (dryrun-3 W1)
# ---------------------------------------------------------------------------


async def connect_with_retry(
    url: str,
    timeout: float = 30.0,
    connect_fn: Callable[..., Awaitable[Any]] | None = None,
) -> Any:
    """Connect to NATS with a bounded retry/backoff loop for the *initial* connect.

    Mirrors NatsBridge._connect() (nats_bridge.py:27-51). Steady-state
    reconnects after a successful initial connect are handled by
    allow_reconnect (passed to nats.connect below), not by this retry loop —
    those two failure modes are distinct (design.md "Initial connection
    failure" vs "Wake contract").
    """
    connect_fn = connect_fn or nats.connect
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return await connect_fn(
                servers=[url],
                allow_reconnect=True,
                max_reconnect_attempts=-1,
                reconnect_time_wait=2,
            )
        except Exception as e:
            last_error = e
            logger.warning("NATS connection attempt failed: %s", e)
            await asyncio.sleep(2)
    raise ConnectionError(f"NATS unavailable at {url} after {timeout}s: {last_error}")


# ---------------------------------------------------------------------------
# crosschat-init — one-time, idempotent infra setup
# ---------------------------------------------------------------------------


def _is_already_exists(err: Exception) -> bool:
    """Whether a JetStream BadRequestError means "this already exists".

    Matched on the server's error text because ``nats.py`` surfaces both
    already-exists and subject-overlap as the same exception class. Covers the
    stream wording ("stream name already in use") and the KV wording
    ("bucket name already in use"), plus the older generic phrasing.
    """
    text = str(err).lower()
    return "already in use" in text or "already exists" in text


async def ensure_infra(js: Any) -> None:
    """Idempotently create the CROSSCHAT stream and crosschat-registry KV bucket.

    Used by crosschat-init. Safe to call repeatedly / from multiple sessions in
    any order — an "already exists" error is treated as success, not failure.

    **Only** already-exists is tolerated. An earlier version swallowed every
    ``BadRequestError`` here, which silently masked a real failure: JetStream
    also raises ``BadRequestError`` when a new stream's subjects overlap an
    existing stream's, so ``init`` reported success while creating nothing and
    every later call failed with a confusing "infra not found". Re-raising
    anything that isn't an already-exists is what makes the success report
    mean something.
    """
    try:
        await js.add_stream(
            name=STREAM_NAME,
            subjects=STREAM_SUBJECTS,
            max_age=STREAM_MAX_AGE,
            storage="file",
        )
        logger.info("Created JetStream stream: %s", STREAM_NAME)
    except BadRequestError as e:
        if not _is_already_exists(e):
            raise
    try:
        await js.create_key_value(bucket=REGISTRY_BUCKET, ttl=REGISTRY_TTL)
        logger.info("Created KV bucket: %s", REGISTRY_BUCKET)
    except BadRequestError as e:
        if not _is_already_exists(e):
            raise


async def infra_ready(js: Any) -> bool:
    """Whether the CROSSCHAT stream and crosschat-registry bucket already exist."""
    try:
        await js.stream_info(STREAM_NAME)
    except NotFoundError:
        return False
    try:
        await js.key_value(REGISTRY_BUCKET)
    except Exception:
        return False
    return True


# ---------------------------------------------------------------------------
# crosschat-register — per-project registration & discovery
# ---------------------------------------------------------------------------


async def register_project(
    js: Any,
    project_id: str,
    project_name: str,
    path: str,
    sign: bool = True,
) -> None:
    """Write this project's initial crosschat-registry entry.

    Raises InfraNotReadyError if crosschat-init hasn't been run yet — does
    not create the infra itself (design.md "crosschat-register" skill).

    When ``sign`` is set, this also creates the project's signing key on first
    use and publishes the **public** half in the registry entry. The registry
    is already the discovery mechanism, so key distribution rides on the thing
    that exists rather than needing anything new. Registration is the only
    place a key is created: it is the one command that already establishes what
    a project *is*.

    A project without PyNaCl registers unsigned rather than failing — signing
    is optional by construction, and a listener that cannot sign must still be
    reachable.
    """
    if not await infra_ready(js):
        raise InfraNotReadyError(
            "crosschat-registry infra not found — run crosschat-init first"
        )
    kv = await js.key_value(REGISTRY_BUCKET)
    now = datetime.now(timezone.utc).isoformat()
    entry = {
        "project_name": project_name,
        "path": path,
        "registered_at": now,
        "last_heartbeat": now,
    }
    if sign:
        try:
            _, public_key = signing.load_or_create_signing_key(project_id)
            entry["public_key"] = public_key
        except signing.SigningUnavailableError:
            logger.info(
                "registering %s without a signing key: %s",
                project_id,
                "PyNaCl not installed",
            )
    await kv.put(project_id, json.dumps(entry).encode())


async def public_key_for(kv: Any, project_id: str) -> str | None:
    """Read a project's published public key from the registry, if it has one."""
    try:
        entry = await kv.get(project_id)
    except Exception:
        return None
    if entry is None or entry.value is None:
        return None
    try:
        return json.loads(entry.value.decode()).get("public_key")
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


async def list_active_projects(kv: Any) -> list[dict]:
    """Read every non-expired entry from crosschat-registry — the entire discovery
    mechanism (design.md "Discovery — JetStream KV registry"): expired entries are
    already gone server-side via the bucket's TTL, so every key returned here is live.
    """
    projects: list[dict] = []
    try:
        keys = await kv.keys()
    except Exception:
        return projects
    for key in keys:
        try:
            raw = await kv.get(key)
        except Exception:
            continue
        if raw is None or raw.value is None:
            continue
        try:
            data = json.loads(raw.value.decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        data["project_id"] = key
        projects.append(data)
    return projects


async def is_registered(kv: Any, project_id: str) -> bool:
    """True if project_id has a live (non-expired) crosschat-registry entry.

    Relies on the KV bucket's own TTL for expiry (design.md W1) — a `.get()`
    on an expired key returns nothing/raises, so no manual `last_heartbeat`
    comparison is needed here.
    """
    try:
        entry = await kv.get(project_id)
    except Exception:
        return False
    return entry is not None and entry.value is not None


async def deregister_project(kv: Any, project_id: str) -> None:
    """Remove project_id's crosschat-registry entry immediately.

    An operational convenience on top of the design's TTL-based expiry (no
    manual cleanup is *required* — see design.md "Listener lifecycle") —
    useful for tearing down test/scratch registrations without waiting out
    the 5-minute bucket TTL. Idempotent: removing an already-absent key is
    not an error.
    """
    try:
        await kv.delete(project_id)
    except Exception:
        pass


async def _refresh_heartbeat_entry(kv: Any, project_id: str) -> None:
    """Rewrite project_id's entry with an updated last_heartbeat, preserving
    the other fields already stored (project_name, path, registered_at)."""
    entry: dict = {}
    try:
        raw = await kv.get(project_id)
        if raw is not None and raw.value is not None:
            entry = json.loads(raw.value.decode())
    except Exception:
        entry = {}
    entry["last_heartbeat"] = datetime.now(timezone.utc).isoformat()
    await kv.put(project_id, json.dumps(entry).encode())


# ---------------------------------------------------------------------------
# crosschat-send — fire-and-forget
# ---------------------------------------------------------------------------


async def send_message(
    js: Any, dest_project_id: str, envelope: dict, sign: bool = True
) -> None:
    """Publish envelope to crosschat.{dest_project_id} and return immediately.

    No blocking wait, no timeout, ever — the fire-and-forget guarantee that
    closes dryrun-1 C1. Raises DestinationNotRegisteredError if the
    destination has no live registry entry, rather than publishing into a
    channel nobody is registered to read.

    When ``sign`` is set and this project holds a signing key, the envelope is
    signed before publishing so the receiver can check that
    ``source_project_id`` is not merely asserted. A project with no key sends
    unsigned rather than failing — see signing.py on why this stays optional.
    """
    kv = await js.key_value(REGISTRY_BUCKET)
    if not await is_registered(kv, dest_project_id):
        raise DestinationNotRegisteredError(
            f"project '{dest_project_id}' is not currently registered/active"
        )
    if sign:
        try:
            envelope = signing.sign_envelope(envelope, envelope["source_project_id"])
        except signing.SigningUnavailableError:
            logger.debug("sending unsigned: PyNaCl not installed")
    await js.publish(project_subject(dest_project_id), json.dumps(envelope).encode())


# ---------------------------------------------------------------------------
# crosschat-monitor — persistent listener
# ---------------------------------------------------------------------------


async def _verify_incoming(kv: Any, data: dict) -> str:
    """Verification outcome for a received envelope.

    Delivery never depends on the result. A message that cannot be verified is
    still emitted, carrying the reason — the receiving session decides what an
    unsigned or unverifiable message is worth, which is a judgement a listener
    has no business making on its behalf.
    """
    source = data.get("source_project_id")
    if not source:
        return signing.UNSIGNED
    return signing.verify_envelope(data, await public_key_for(kv, source))


def monitor_consumer_config() -> ConsumerConfig:
    """Durable consumer config for crosschat-monitor (closes dryrun-2 C1):
    ack_policy EXPLICIT + a durable name (passed separately to js.subscribe)
    means JetStream tracks delivery position server-side across restarts —
    never replays an already-acked message, never skips one.
    """
    return ConsumerConfig(
        deliver_policy=DeliverPolicy.ALL,
        ack_policy=AckPolicy.EXPLICIT,
    )


async def monitor_loop(
    js: Any,
    kv: Any,
    project_id: str,
    emit: Callable[[str], None] = print,
    max_iterations: int | None = None,
    heartbeat_interval: float = HEARTBEAT_INTERVAL,
    message_timeout: float = 5.0,
) -> None:
    """Persistent listener loop for crosschat-monitor.

    Subscribes once with a durable consumer and loops indefinitely (never
    exits on its own — only when cancelled, or in tests, once max_iterations
    is reached). Each incoming message becomes one `CROSSCHAT_MESSAGE <json>`
    line via `emit` (the Monitor tool surfaces each such line as its own
    notification to the live session — design.md "Wake contract").

    `message_timeout` is a purely internal polling tick used to periodically
    check whether the heartbeat is due — it is never surfaced to the caller
    (no "TIMEOUT" line is emitted); the wake contract has exactly one message
    shape, `CROSSCHAT_MESSAGE`.
    """
    subject = project_subject(project_id)
    sub = await js.subscribe(
        subject, durable=consumer_name(project_id), config=monitor_consumer_config()
    )

    last_heartbeat = time.monotonic()
    iterations = 0
    try:
        while max_iterations is None or iterations < max_iterations:
            iterations += 1
            try:
                msg = await sub.next_msg(timeout=message_timeout)
            except asyncio.TimeoutError:
                msg = None

            if msg is not None:
                try:
                    data = json.loads(msg.data.decode())
                except (json.JSONDecodeError, UnicodeDecodeError):
                    data = {"raw": msg.data.decode(errors="replace")}
                else:
                    data["verification"] = await _verify_incoming(kv, data)
                emit("CROSSCHAT_MESSAGE " + json.dumps(data))
                await msg.ack()

            now = time.monotonic()
            if now - last_heartbeat >= heartbeat_interval:
                await _refresh_heartbeat_entry(kv, project_id)
                last_heartbeat = now
    finally:
        await sub.unsubscribe()
