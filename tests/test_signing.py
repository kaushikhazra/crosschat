"""Tests for crosschat.signing — proving who sent an envelope.

Every test points the key directory at a tmp_path, so no test ever reads or
writes a real project's private key.
"""

from __future__ import annotations

import json

import pytest

from crosschat import signing
from crosschat.signing import (
    BAD_SIGNATURE,
    NO_KEY,
    UNSIGNED,
    VERIFIED,
    canonical_bytes,
    load_or_create_signing_key,
    public_key_hex,
    sign_envelope,
    verify_envelope,
)


@pytest.fixture(autouse=True)
def isolated_key_dir(tmp_path, monkeypatch):
    monkeypatch.setenv(signing.KEY_DIR_ENV, str(tmp_path / "keys"))
    return tmp_path / "keys"


def envelope(source="velasari", text="hello"):
    return {
        "source_project_id": source,
        "dest_project_id": "kaylan",
        "text": text,
        "sent_at": "2026-08-10T00:00:00+00:00",
    }


class TestCanonicalBytes:
    def test_key_order_does_not_change_the_bytes(self):
        """Both sides must derive identical bytes or every check fails."""
        a = {"b": 2, "a": 1}
        b = {"a": 1, "b": 2}
        assert canonical_bytes(a) == canonical_bytes(b)

    def test_signature_field_is_excluded(self):
        """A signature cannot cover itself."""
        base = envelope()
        assert canonical_bytes(base) == canonical_bytes({**base, "signature": "ff"})

    def test_verification_field_is_excluded(self):
        """The receiver adds `verification` after the fact; including it would
        make every envelope fail on the second look."""
        base = envelope()
        assert canonical_bytes(base) == canonical_bytes(
            {**base, "verification": VERIFIED}
        )


class TestKeyLifecycle:
    def test_key_is_created_on_first_use_and_reused_after(self):
        _, pub1 = load_or_create_signing_key("velasari")
        _, pub2 = load_or_create_signing_key("velasari")
        assert pub1 == pub2, "a second call must not mint a new identity"

    def test_key_is_written_to_the_configured_directory(self, isolated_key_dir):
        load_or_create_signing_key("velasari")
        assert (isolated_key_dir / "velasari.key").exists()

    def test_different_projects_get_different_keys(self):
        _, a = load_or_create_signing_key("velasari")
        _, b = load_or_create_signing_key("kaylan")
        assert a != b

    def test_public_key_hex_returns_none_without_creating_one(self, isolated_key_dir):
        """Reading must not have the side effect of establishing an identity."""
        assert public_key_hex("nobody") is None
        assert not (isolated_key_dir / "nobody.key").exists()

    def test_private_key_is_never_the_published_value(self, isolated_key_dir):
        signing_key, public = load_or_create_signing_key("velasari")
        on_disk = (isolated_key_dir / "velasari.key").read_text(encoding="utf-8")
        assert public not in on_disk


class TestSignAndVerify:
    def test_round_trip_verifies(self):
        _, public = load_or_create_signing_key("velasari")
        signed = sign_envelope(envelope(), "velasari")
        assert verify_envelope(signed, public) == VERIFIED

    def test_unsigned_envelope_reports_unsigned(self):
        assert verify_envelope(envelope(), "aa" * 32) == UNSIGNED

    def test_signed_but_no_published_key_reports_no_key(self):
        load_or_create_signing_key("velasari")
        signed = sign_envelope(envelope(), "velasari")
        assert verify_envelope(signed, None) == NO_KEY

    def test_tampered_text_fails(self):
        """The point of the exercise: altering the body invalidates it."""
        _, public = load_or_create_signing_key("velasari")
        signed = sign_envelope(envelope(text="transfer 10"), "velasari")
        signed["text"] = "transfer 1000"
        assert verify_envelope(signed, public) == BAD_SIGNATURE

    def test_forged_source_id_fails(self):
        """kaylan signs, then relabels itself velasari — the forgery this exists
        to catch. The signature no longer matches the claimed sender's key."""
        _, velasari_public = load_or_create_signing_key("velasari")
        signed = sign_envelope(envelope(source="kaylan"), "kaylan")
        signed["source_project_id"] = "velasari"
        assert verify_envelope(signed, velasari_public) == BAD_SIGNATURE

    def test_signature_from_the_wrong_key_fails(self):
        _, kaylan_public = load_or_create_signing_key("kaylan")
        signed = sign_envelope(envelope(), "velasari")
        assert verify_envelope(signed, kaylan_public) == BAD_SIGNATURE

    def test_garbage_signature_does_not_raise(self):
        """A malformed field must never crash a listener."""
        _, public = load_or_create_signing_key("velasari")
        assert (
            verify_envelope({**envelope(), "signature": "zzz"}, public) == BAD_SIGNATURE
        )

    def test_garbage_public_key_does_not_raise(self):
        signed = sign_envelope(envelope(), "velasari")
        assert verify_envelope(signed, "not-hex") == BAD_SIGNATURE

    def test_signing_is_stable_across_serialisation(self):
        """Envelopes cross the wire as JSON; a round trip must not break them."""
        _, public = load_or_create_signing_key("velasari")
        signed = sign_envelope(envelope(), "velasari")
        assert verify_envelope(json.loads(json.dumps(signed)), public) == VERIFIED


class TestSigningUnavailable:
    def test_verify_reports_no_key_when_pynacl_missing(self, monkeypatch):
        """Without PyNaCl a receiver degrades to 'cannot check', never to a
        crash and never to a false 'verified'."""

        def boom():
            raise signing.SigningUnavailableError("no pynacl")

        monkeypatch.setattr(signing, "_nacl", boom)
        assert verify_envelope({**envelope(), "signature": "ab"}, "aa" * 32) == NO_KEY

    def test_unsigned_still_reports_unsigned_when_pynacl_missing(self, monkeypatch):
        def boom():
            raise signing.SigningUnavailableError("no pynacl")

        monkeypatch.setattr(signing, "_nacl", boom)
        assert verify_envelope(envelope(), None) == UNSIGNED
