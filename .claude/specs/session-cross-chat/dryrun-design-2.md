# Design Dry-Run Report #2

**Document**: `.claude/specs/session-cross-chat/design.md`
**Reviewed**: 2026-07-25

---

## Critical Gaps (must fix before implementation)

### [C1] Ephemeral consumer + `DeliverPolicy.ALL` replays the same stale message forever, never advancing

- **Pass**: Pass 6 (Concurrency & Ordering) / Pass 2 (Data Flow Trace)
- **What**: `crosschat-monitor` is specified as a one-shot script that creates a **new ephemeral JetStream consumer** on every launch (`js.subscribe(..., config=ConsumerConfig(deliver_policy=DeliverPolicy.ALL, ack_policy=AckPolicy.NONE))`, copied byte-for-byte from `nats_wait.py`), blocks for one message, and exits. The design's re-arm model then relaunches this same one-shot script every cycle (on message handled, and on plain timeout) for the entire life of the session.

  This is safe in `nats_wait.py`'s original use case because each subject (`workers.{worker_id}.completed`) receives **exactly one message ever** — `DeliverPolicy.ALL` starting "from the beginning" and "the newest message" are the same message. `project.{own_id}` is structurally different: it's a long-lived channel that accumulates many messages over a session's lifetime (every cross-chat message ever sent to that project, for up to the 1-day stream retention).

  An **ephemeral** consumer has no server-persisted delivery cursor — when the process exits (or calls `unsubscribe()`), JetStream forgets what that consumer had already delivered. The *next* relaunch creates a brand-new ephemeral consumer, which with `DeliverPolicy.ALL` starts over from the **oldest** matching message in the stream, not the newest or the next-undelivered one.

- **Risk**: As soon as a second message ever lands on a project's channel while the first is still within the 1-day retention window, the monitor gets stuck replaying message #1 on every relaunch — message #2 (and everything after it) is never delivered until message #1 ages out of the stream. This silently breaks AC-1.2 and AC-1.3 (the whole point of the feature) the first time a project receives more than one cross-chat message in a day, which will be almost immediately in normal use (an initial message + its reply are already two messages on the *replying* project's inbound channel across the session).
- **Fix**: Use a **durable** consumer (not ephemeral) with a stable name per project, e.g. `consumer_name=f"crosschat-{project_id}"`, and `ack_policy=AckPolicy.EXPLICIT` with an explicit `msg.ack()` after the message is captured (before the script exits). A durable consumer's delivery position is tracked server-side and survives across the script's repeated launch/exit cycles — each relaunch resumes exactly where the last one left off, which is the actual requirement here (this is precisely why JetStream distinguishes ephemeral from durable consumers; `nats_wait.py`'s one-message-per-subject pattern never needed to exercise this distinction). Update the Wake contract and `crosschat-monitor` task in task.md accordingly.

### [C2] CROSSCHAT stream-creation ownership left undecided, contradicting the design's own KV-ownership rationale

- **Pass**: Pass 3 (Interface Contract Validation) / Pass 8 (Task Spec Alignment)
- **What**: The "Discovery — JetStream KV registry" section explicitly decides KV access is client-side, inside skill scripts, **not** routed through `nats_bridge.py`, with the stated reason: "the skills are Claude Code session hooks, not Velhari server components" (closes W2 from dryrun-1). The `CROSSCHAT` JetStream *stream*, however, is specified in "Subjects & Durability" with a concrete spec but no stated owner — and `task.md`'s NATS-setup task explicitly punts: "either via `nats_bridge.py`'s `_ensure_streams()`... or via a one-time setup script... decide... before starting." By the project's own CLAUDE.md rule ("Vague design = incomplete implementation. Stop and ask" / L3: architecturally silent tasks get shortcut implementations), this is exactly the kind of undecided fork that should not reach task.md.
- **Risk**: Whoever implements this will guess, and the guess is likely to be wrong in a way that's inconsistent with the KV decision — e.g. wiring `CROSSCHAT` into `nats_bridge.py`'s server-owned `_ensure_streams()` (making it a Velhari MCP-server concern) while the registry stays purely skill-side, splitting cross-chat infra ownership across two different subsystems for no principled reason.
- **Fix**: Apply the same rationale already used for the KV bucket: `CROSSCHAT` is a Claude Code session/skill concern, not a Velhari MCP-server concern (workers/AUDIT/VH streams exist because Velhari itself publishes to and reads them; cross-chat is purely session-to-session). Decide: stream creation is **client-side**, idempotent (`try: add_stream(...) except BadRequestError: pass`, same pattern as `nats_bridge.py:60-69`), invoked from `crosschat-register` on session start alongside the KV bucket ensure-step — not through `nats_bridge.py`. State this explicitly in design.md and remove the "decide... before starting" hedge from task.md.

---

## Warnings (should fix, may cause issues)

### [W1] No backoff specified for `crosschat-monitor` re-arm on `NATS_CONNECT_FAIL`

- **Pass**: Pass 5 (Failure Path Analysis)
- **What**: The re-arm cycle relaunches `crosschat-monitor` "immediately after each exit" on `CROSSCHAT_MESSAGE` and plain `TIMEOUT`. It's unclear whether `NATS_CONNECT_FAIL` also triggers an immediate relaunch. If NATS is down for an extended period and the session naively relaunches on every exit including connect failures, this hot-loops — rapid-fire reconnect attempts with no backoff.
- **Risk**: A NATS outage turns into a reconnect-storm from every registered project simultaneously (each session's monitor spinning as fast as connect-attempt latency allows), adding load right when the server is already unhealthy.
- **Suggestion**: Specify that `NATS_CONNECT_FAIL` is handled differently from `TIMEOUT` in the re-arm cycle — e.g. a short fixed backoff (mirrors `nats_bridge.py`'s own reconnect loop at lines 44-47, which sleeps 2s between attempts) before relaunching, or surface the failure to the live session rather than blindly auto-relaunching.

### [W2] AC-3.1's open question is resolved implicitly by the re-arm cycle but never stated as a decision

- **Pass**: Pass 1 (Completeness Check)
- **What**: requirement.md's AC-3.1 explicitly frames this as an open decision: "always-on for the duration of the session, or opened/closed on demand — decision needed, not yet made." The corrected design (register starts the monitor at session start; the monitor's exit-triggers-relaunch cycle keeps it running until the session itself stops relaunching it) does answer this — it's always-on for the session's duration — but the design document never states this as the resolution to AC-3.1. A reader has to infer it from the mechanics.
- **Risk**: Low on its own, but combined with C1/C2 it's a symptom of the same pattern — decisions that are made in practice but not written down as decisions, which is exactly what CLAUDE.md's "vague design" rule warns against.
- **Suggestion**: Add one explicit sentence, e.g. under "Multi-session-per-project" or a new small "Listener lifecycle (closes AC-3.1)" section: "A session's cross-chat listener is always-on for the duration of the session — opened by `crosschat-register` at session start, kept alive by `crosschat-monitor`'s self-relaunch cycle, and stops only when the session stops relaunching it (session end)."

---

## Observations (worth discussing)

### [O1] Pass 7: A project messaging itself is not disallowed, and doesn't need to be

Nothing in the design prevents `crosschat-send` from publishing to a project's own `project_id`. Harmless (it would just show up as a normal message to its own monitor) — no special-casing needed, noting as verified-fine rather than a gap.

### [O2] Pass 7: Cold-start ordering (register-before-subscribe window) — still correctly covered after the C1 fix

dryrun-1's O2 noted the brief window between KV registration and the listener actually subscribing is covered by JetStream durability. With the C1 fix (durable consumer instead of ephemeral), this remains true and is actually strengthened — a durable consumer's subscription position starts wherever the consumer was last left (or the stream's earliest retained message on first-ever creation), so no message sent in that window is missed.

### Pass 9: Design-to-Task-to-AC Traceability

No "Files Changed" table exists in design.md. Scanning body sections for file-level prescriptions:

| File/Prescription | Task Reference | AC Reference |
|-------------------|---------------|--------------|
| Body §"Wake contract": `crosschat_wait.py` usage (`python crosschat_wait.py <project_id> [timeout_s] [nats_url]`) | task.md `crosschat-monitor` skill — "writes the one-shot listener script" | AC-1.2 |
| Body §"Verification plan for US-2": `cost-comparison.md` | task.md Verification — "records results in `cost-comparison.md`" | AC-2.1 |

**Result**: Both body file-prescriptions found trace to a task and an AC. No traceability gaps. (References to `nats_wait.py` and `nats_bridge.py` elsewhere in the design are analogies/rationale for an existing file, not prescriptions for a new one, and are correctly excluded per the skill's matching rules.)

---

## Summary

| Critical | Warnings | Observations |
|----------|----------|---------------|
| 2        | 2        | 2 (+ Pass 9 traced clean) |

**Verdict**: FAIL — needs revision

The messaging-model fix from the previous revision (fire-and-forget, no request/reply timeout) is sound and closes dryrun-1's C1 correctly. But borrowing `nats_wait.py`'s exact consumer config (ephemeral + `DeliverPolicy.ALL`) without adjusting for the fact that `project.{id}` is a multi-message channel (unlike `nats_wait.py`'s one-message-per-subject world) introduces a new, more subtle correctness bug (C1 here): the monitor will get stuck replaying the same old message and never see newer ones once more than one message accumulates on a channel. This needs a durable consumer, not a config tweak — worth fixing before implementation starts, since it would otherwise surface as "cross-chat mysteriously stops working after the first exchange," which is exactly the kind of bug that's expensive to diagnose after the fact. C2 (stream ownership) is a smaller, cheap fix — apply the same reasoning already used for the KV bucket.
