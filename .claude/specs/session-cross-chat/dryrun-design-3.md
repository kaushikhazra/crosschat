# Design Dry-Run Report #3

**Document**: `.claude/specs/session-cross-chat/design.md`
**Reviewed**: 2026-07-25

---

## Critical Gaps (must fix before implementation)

### [C1] Three AC cross-references still point at the pre-rewrite numbering — one is actively wrong, one points at a nonexistent AC

- **Pass**: Pass 1 (Completeness Check) / Pass 9 (Design-to-Task-to-AC Traceability)
- **What**: `requirement.md` was rewritten from 3 broad user stories to 7
  granular ones (2026-07-25), which renumbered every AC. `design.md` was
  updated in the same pass to fix its architecture, but three inline
  "closes AC-X" / "matches AC-X" citations were left pointing at the
  **old** numbering scheme and were never updated:
  - Line 46, "Subjects & Durability": *"Matches requirement AC-1.4
    (delivery survives a listener restart)."* — the new requirement.md's
    US-1 (`crosschat-init`) only has AC-1.1 through AC-1.3. **AC-1.4
    does not exist.** The AC this sentence actually describes is now
    AC-6.4 ("a message sent to a project whose listener is transiently
    reconnecting... is not lost").
  - Line 198, "Listener lifecycle" heading: *"(closes requirement
    AC-3.1)"* — the new AC-3.1 is `crosschat-send`'s fire-and-forget/
    no-timeout guarantee (US-3), which is unrelated to listener
    lifecycle. The AC this section actually closes is AC-6.1 ("a
    session's cross-chat listener is on for its entire session
    duration").
  - Line 264, "Verification plan for US-7": *"(AC-2.2 — a cross-chat
    exchange against an otherwise-idle session is the fair
    comparison...)"* — the new AC-2.2 is `crosschat-register`'s
    infra-missing error behavior (US-2), unrelated to cost-comparison
    fairness. The AC this sentence actually describes is now AC-7.2.
- **Risk**: These aren't just broken links — the middle one (line 198)
  actively cites the wrong AC's subject matter, and the first (line 46)
  cites an AC number that doesn't exist at all. Anyone (implementer, a
  future dry-run pass, K) who trusts a "closes AC-X" claim at face value
  without re-deriving it will believe the wrong requirement was
  satisfied, or go looking for an AC-1.4 that isn't there. This is
  exactly the kind of small drift that compounds silently across
  revisions.
- **Fix**: Update all three citations to the current numbering: line 46
  → AC-6.4, line 198 → AC-6.1, line 264 → AC-7.2. Since this is the
  second time a renumbering has left stale references (the first
  revision's "closes C4" / "closes W1" style pass-numbers were internally
  consistent, but this AC-numbering drift wasn't caught until this pass),
  consider a final grep for `AC-\d\.\d` across design.md after any future
  requirement.md renumbering, specifically to catch this class of drift.

---

## Warnings (should fix, may cause issues)

### [W1] `crosschat-monitor`'s behavior on *initial* connection failure (not a reconnect) is unspecified

- **Pass**: Pass 5 (Failure Path Analysis)
- **What**: The design specifies that transient disconnects are handled
  by `nats.py`'s own reconnect (`allow_reconnect=True,
  max_reconnect_attempts=-1`) rather than a bespoke backoff — this
  covers the case where the process is already connected and briefly
  loses the connection. It doesn't say what happens if NATS is down at
  the moment `crosschat-monitor` first launches (`nats.connect()`'s
  initial connection attempt, before there's a connection to lose).
  `nats_bridge.py`'s own connect loop (lines 30-51) explicitly retries
  the *initial* connect with a timeout and backoff — a different code
  path from `allow_reconnect`, which only governs already-established
  connections.
- **Risk**: If `crosschat-register`'s session-start hook launches
  `crosschat-monitor` while NATS happens to be unreachable (server
  restarting, etc.), and the script's `nats.connect()` call isn't given
  its own initial-connect retry, the monitor process could simply crash
  or hang uninformatively, and — since nothing relaunches it anymore
  (that relaunch loop was deliberately removed) — the session would be
  left with no listener for the rest of its life, silently.
- **Suggestion**: Specify that `crosschat-monitor`'s initial
  `nats.connect()` uses the same retry-with-backoff pattern as
  `nats_bridge.py`'s `_connect()` (lines 30-51) — connect_timeout plus a
  bounded retry loop — before falling into the steady-state
  `allow_reconnect` behavior. If it still can't connect after that, it
  should fail loudly (a clear message via the Monitor tool) rather than
  hanging silently, so the live session knows its listener never started.

---

## Observations (worth discussing)

### [O1] Pass 4: Abandoned-project messages age out with the stream, by design — consistent with existing behavior

If a project's `crosschat-monitor` crashes right after `crosschat-send`'s
registry pre-check passes (AC-6.3), and no future session for that
project ever restarts it, the durable consumer's unacked message sits in
the `CROSSCHAT` stream until the 1-day `max_age` retention removes it —
never delivered, no error surfaced to the original sender. This mirrors
how the existing `WORKERS`/`VH` streams already behave (time-based
retention, not delivery-based) and is consistent with the registry's own
5-minute TTL making this a narrow race window, not a common case. Noting
as accepted v1 behavior, not a gap — the alternative (delivery
confirmation back to the sender) is explicitly out of scope per
"Deferred / not designed here."

### Pass 9: Design-to-Task-to-AC Traceability

No "Files Changed" table exists in design.md. Scanning body sections for
file-level prescriptions (the `crosschat_wait.py` usage example from the
previous revision was removed when the wake contract was rewritten to
the persistent-process/Monitor-tool model — nothing to check there now):

| File/Prescription | Task Reference | AC Reference |
|-------------------|---------------|--------------|
| Body §"Verification plan for US-7": `cost-comparison.md` | task.md Verification — "records results in `cost-comparison.md`" | AC-7.1 |

**Result**: The one file-level prescription found traces to both a task
and an AC. No traceability gaps. (`nats_bridge.py` references throughout
the document are rationale/pattern-matching against an existing file —
excluded per the skill's matching rules, consistent with dryrun-2.)

---

## Summary

| Critical | Warnings | Observations |
|----------|----------|---------------|
| 1        | 1        | 2 (incl. Pass 9 traced clean) |

**Verdict**: FAIL — needs revision

The architecture itself is sound this pass — the durable-consumer fix
(dryrun-2 C1), the persistent-monitor model, and the `crosschat-init`
split (dryrun-2 C2) all hold up under this pass's data-flow, state, and
concurrency checks with no new structural issues found. The one Critical
here is bookkeeping, not architecture: three inline AC citations weren't
updated when requirement.md was renumbered, and one of them now points
at an AC that doesn't exist. Cheap to fix (three line edits), but worth
doing before implementation since these citations are exactly what a
task.md/AC-traceability check leans on. W1 is a real but narrow gap —
the initial-connect-failure path is genuinely unspecified, distinct from
the reconnect path the design does cover.
