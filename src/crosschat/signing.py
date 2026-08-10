"""Message signing — proving which project sent an envelope.

`source_project_id` in an envelope is a field the sender fills in. Nothing
stops a sender writing someone else's id there, because the transport does not
know or care who is publishing.

This module makes that field checkable. Each project holds an Ed25519 keypair;
the sender signs the envelope and the receiver verifies the signature against
the public key the sender published to the registry.

**Identity lives with the message, not with the routing.** That is the property
worth having: the same scheme works over NATS, HTTP, or a shared folder,
because nothing here knows what a subject is. Binding identity to NATS subject
permissions would have worked too, and would have welded crosschat to one
broker.

**What this gives you, stated honestly: continuity, not authority.** The
registry is unauthenticated — anything that can reach it can write a public key
under any id. So a signature proves *the same holder keeps using the same
name*, and makes a key swap visible. It does not prove that name was ever
entitled to exist. For a mesh whose realistic failure is a project
misaddressing itself rather than an intruder, continuity is the property that
actually matters; anything stronger needs an authenticated registry, which is a
different and much larger design.

Signing is **optional by construction**. An unsigned envelope is delivered and
marked unsigned rather than dropped, so a signing peer and a non-signing peer
keep working together.
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
from typing import Any

logger = logging.getLogger(__name__)

KEY_DIR_ENV = "CROSSCHAT_KEY_DIR"
DEFAULT_KEY_DIR = "~/.crosschat/keys"

#: Verification outcomes carried on a delivered envelope.
VERIFIED = "verified"
UNSIGNED = "unsigned"
BAD_SIGNATURE = "bad-signature"
NO_KEY = "no-key"


class SigningUnavailableError(RuntimeError):
    """Raised when signing is requested but PyNaCl is not installed."""


def _nacl():
    """Import PyNaCl lazily, with an actionable error rather than ImportError.

    Signing is an optional capability: a crosschat that cannot sign must still
    send and receive. Importing at module scope would make the dependency
    mandatory for everyone the moment this module is imported by core.
    """
    try:
        from nacl import signing as nacl_signing  # type: ignore
    except ImportError as e:  # pragma: no cover - exercised via monkeypatch
        raise SigningUnavailableError(
            "signing needs PyNaCl, which crosschat declares as a dependency — "
            "reinstall with `pip install --force-reinstall crosschat`"
        ) from e
    return nacl_signing


def key_dir() -> pathlib.Path:
    """Directory holding this machine's private keys.

    Overridable via ``CROSSCHAT_KEY_DIR``; defaults under the user's home
    rather than inside a project, because a private key that lives in a
    project directory eventually gets committed.
    """
    raw = os.environ.get(KEY_DIR_ENV) or DEFAULT_KEY_DIR
    return pathlib.Path(raw).expanduser()


def key_path(project_id: str) -> pathlib.Path:
    return key_dir() / f"{project_id}.key"


def canonical_bytes(envelope: dict) -> bytes:
    """The exact bytes that get signed and verified.

    Sorted keys, no incidental whitespace, and the ``signature`` and
    ``verification`` fields excluded — a signature cannot cover itself, and
    ``verification`` is added by the receiver after the fact. Both sides must
    derive these bytes identically or every check fails, so this is the single
    definition and neither side may serialise its own.
    """
    payload = {
        k: v for k, v in envelope.items() if k not in ("signature", "verification")
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def load_or_create_signing_key(project_id: str) -> tuple[Any, str]:
    """Return this project's signing key, creating one on first use.

    Returns ``(signing_key, public_key_hex)``. The private key is written with
    owner-only permissions where the platform supports it.
    """
    nacl_signing = _nacl()
    path = key_path(project_id)
    if path.exists():
        seed = bytes.fromhex(path.read_text(encoding="utf-8").strip())
        signing_key = nacl_signing.SigningKey(seed)
    else:
        signing_key = nacl_signing.SigningKey.generate()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(bytes(signing_key).hex(), encoding="utf-8")
        try:
            os.chmod(path, 0o600)
        except OSError:  # pragma: no cover - platform dependent
            logger.debug("could not restrict permissions on %s", path)
        logger.info("created signing key for %s at %s", project_id, path)
    return signing_key, bytes(signing_key.verify_key).hex()


def public_key_hex(project_id: str) -> str | None:
    """This project's public key, or None if it has no key yet.

    Never creates one — used where a missing key means "this project does not
    sign", which is a legitimate state.
    """
    path = key_path(project_id)
    if not path.exists():
        return None
    try:
        nacl_signing = _nacl()
    except SigningUnavailableError:
        return None
    seed = bytes.fromhex(path.read_text(encoding="utf-8").strip())
    return bytes(nacl_signing.SigningKey(seed).verify_key).hex()


def sign_envelope(envelope: dict, project_id: str) -> dict:
    """Return a copy of the envelope carrying a signature over its contents."""
    signing_key, _ = load_or_create_signing_key(project_id)
    signature = signing_key.sign(canonical_bytes(envelope)).signature
    return {**envelope, "signature": signature.hex()}


def verify_envelope(envelope: dict, sender_public_key_hex: str | None) -> str:
    """Check an envelope's signature and return one of the outcome constants.

    Never raises and never drops a message. A receiver's job is to report what
    it can prove, not to decide that an unprovable message did not happen.
    """
    signature_hex = envelope.get("signature")
    if not signature_hex:
        return UNSIGNED
    if not sender_public_key_hex:
        return NO_KEY
    try:
        nacl_signing = _nacl()
        verify_key = nacl_signing.VerifyKey(bytes.fromhex(sender_public_key_hex))
        verify_key.verify(canonical_bytes(envelope), bytes.fromhex(signature_hex))
        return VERIFIED
    except SigningUnavailableError:
        return NO_KEY
    except Exception:
        return BAD_SIGNATURE
