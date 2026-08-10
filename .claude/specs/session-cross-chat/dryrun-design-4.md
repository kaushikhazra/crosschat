# Design Dry-Run Report #4

**Document**: `.claude/specs/session-cross-chat/design.md`
**Reviewed**: 2026-07-25

---

## Critical Gaps (must fix before implementation)

None.

- dryrun-3's C1 (three stale AC cross-references from the pre-rewrite
  numbering) is fixed: line 46 now cites AC-6.4, the "Listener
  lifecycle" section now cites AC-6.1 (both heading and closing
  sentence), and the US-7 verification plan now cites AC-7.2. All three
  now match their actual subject matter in the current requirement.md.

---

## Warnings (should fix, may cause issues)

None.

- dryrun-3's W1 (unspecified initial-connect-failure behavior) is fixed:
  `crosschat-monitor` now has an explicit initial-connect retry-with-
  backoff (mirroring `nats_bridge.py`'s `_connect()`), distinct from the
  steady-state `allow_reconnect` path, with a defined failure-surfacing
  behavior (clear failure line via the Monitor tool, then exit) instead
  of a silent hang/crash. `task.md` has a matching task for it.

---

## Observations (worth discussing)

### [O1] Pass 7: Empty message text is not explicitly addressed

`crosschat-send`'s message text field has no stated behavior for an
empty string — presumably it's just published as-is (an empty `text` in
the envelope is not inherently invalid). Not flagging as a gap: nothing
in the requirement or design implies empty text should be rejected, and
inventing a validation rule the design never asked for would be scope
creep, not a fix. Worth a one-line implementer note if it comes up during
`crosschat-send`'s implementation, not a design blocker.

### [O2] Pass 4: Abandoned-project message expiry — carried forward from dryrun-3 O1, still accepted

Unchanged from the previous pass: a message can outlive an abandoned
project's `crosschat-monitor` by up to the `CROSSCHAT` stream's 1-day
`max_age` before being silently dropped, with no delivery confirmation
back to the sender. This remains consistent with how the existing
`WORKERS`/`VH` streams already behave, and delivery confirmation is
explicitly out of scope. Re-noting only for continuity across dry-run
passes, not as a new finding.

### Pass 9: Design-to-Task-to-AC Traceability

No "Files Changed" table exists in design.md. The one body-level file
prescription found in previous passes is unchanged and still traces
cleanly; the W1 fix (initial-connect retry) references
`nats_bridge.py`'s `_connect()` only as a pattern to mirror (rationale),
not as a file to modify, so it doesn't add a new prescription to check.

| File/Prescription | Task Reference | AC Reference |
|-------------------|---------------|--------------|
| Body §"Verification plan for US-7": `cost-comparison.md` | task.md Verification — "records results in `cost-comparison.md`" | AC-7.1 |

**Result**: The one file-level prescription found traces to both a task
and an AC. No traceability gaps.

---

## Summary

| Critical | Warnings | Observations |
|----------|----------|---------------|
| 0        | 0        | 2 (+ Pass 9 traced clean) |

**Verdict**: PASS

All nine passes ran clean this iteration. The two prior findings
(dryrun-3 C1's stale AC citations, W1's unspecified initial-connect
path) are both fixed and verified against the current text. The design
is internally consistent: the messaging model (fire-and-forget,
symmetric send/reply), the durability model (durable JetStream consumer,
`CROSSCHAT` stream spec), the infra/registration split
(`crosschat-init` vs `crosschat-register`), the wake mechanism
(persistent process + Monitor tool), and the discovery/heartbeat model
(KV registry, 60s internal timer, 5-minute TTL) all hold together
without contradiction, and every AC across all 7 user stories in
requirement.md has a corresponding design element. Ready for
`/implement`.
