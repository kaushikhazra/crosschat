# Design Dry-Run Report #1

**Document**: `.claude/specs/session-cross-chat/design.md`
**Reviewed**: 2026-07-25

---

## Critical Gaps (must fix before implementation)

### [C1] Reply-inbox lifetime vs human-paced response time

- **Pass**: Pass 5 (Failure Path Analysis)
- **What**: The design states that NATS request/reply timeout "is just a client-side setting, not a server-enforced limit" and therefore long (multi-minute) response times are fine. This is technically true but mischaracterises the actual failure mode. `nc.request()` creates a subscription on an auto-generated ephemeral inbox (`_INBOX.xxx`) and blocks the caller until either a reply arrives or the timeout expires. When the timeout fires, the client **unsubscribes** from the inbox — the inbox ceases to exist. If the receiving Claude session composes its answer after the sender's timeout has elapsed, `msg.respond(...)` publishes to that inbox subject and the message is silently dropped (core NATS, no JetStream backing on replies — the design itself notes this). Setting an extremely long timeout (e.g. 30 minutes) means the sender's `nc.request()` call blocks the sending script for 30 minutes — which defeats the purpose of async cross-chat and may violate Claude Code's background-task timeout limits.
- **Risk**: Silent reply loss. The sender times out, the responder composes a reply that vanishes. No error surfaces on either side. This is exactly the kind of "works in happy path, fails silently in production" bug that's hardest to diagnose.
- **Fix**: The design must decide between two approaches and specify the choice:
  - **(A) Two-phase publish**: The listener captures the reply-to inbox and stores it. When the session composes a reply, a separate script publishes to that inbox — but the *sender* must still be listening. The sender uses `nc.subscribe()` on the inbox (not `nc.request()`) and manages its own subscription lifecycle independently of the reply latency. This avoids the request-timeout trap but means the sender script must also be a long-lived background process.
  - **(B) Reply via JetStream subject**: Don't use the NATS reply-to inbox for replies at all. Instead, replies go to a JetStream-backed subject (e.g. `project.{source_id}.reply`) that the sender subscribes to durably. This makes both directions durable and decouples reply timing from any client-side timeout. The envelope's `source_project_id` already provides the routing information.
  - Either way, document the chosen approach and its timeout/liveness contract explicitly.

### [C2] JetStream stream for cross-chat not specified enough to implement

- **Pass**: Pass 3 (Interface Contract Validation)
- **What**: The design says the request channel is "backed by a JetStream stream (mirrors the existing WORKERS stream pattern)" but does not specify: stream **name**, **subject filter** (`project.>` vs `project.*` — the wildcard matters for future sub-subjects), **retention policy** (limits vs interest vs work-queue), **max_age**, or **storage type**. The existing `_ensure_streams()` in `nats_bridge.py` (lines 53–69) specifies all five for every stream (WORKERS, AUDIT, VH). An implementer would have to guess each of these.
- **Risk**: Wrong retention policy causes either unbounded storage growth (no max_age) or premature message expiry (too short). Wrong subject filter (`project.*` vs `project.>`) silently fails to capture messages on sub-subjects if the design evolves.
- **Fix**: Add a concrete stream spec, e.g.: stream name `CROSSCHAT`, subjects `["project.>"]`, retention `limits`, max_age `1 * 86400` (1 day — messages older than a session lifetime are stale), storage `file`. Match the pattern established by lines 55–58 of `nats_bridge.py`.

### [C3] `project_id` — no definition of what it is or where it comes from

- **Pass**: Pass 2 (Data Flow Trace)
- **What**: The entire design hinges on `project_id` as the stable addressing key — it appears in subjects (`project.{id}`), KV keys, message envelopes, and every skill's arguments. But the design never defines what a project ID *is*. There is no `project_id` concept in the existing Velhari source code (`src/velhari/`). Is it a filesystem path hash? A name slug? A UUID generated at registration time? A user-supplied label? The KV value includes `project_name` and `path` as separate fields, implying `project_id` is distinct from both — but what generates it?
- **Risk**: Without a canonical definition, each skill script will invent its own project-ID derivation, leading to mismatched keys (sender uses path hash, registry uses name slug → message published to a subject nobody is listening on).
- **Fix**: Define the derivation rule. Suggestion: slugified project directory name (e.g. `velhari`, `velasari`) — simple, stable, human-readable, unique on a single machine. Document it in the design and specify that `crosschat-register` is the single authority that derives and publishes it.

### [C4] task.md does not exist — all file prescriptions are untraced on the task axis

- **Pass**: Pass 9 (Design-to-Task-to-AC Traceability)
- **What**: `task.md` not found in `.claude/specs/session-cross-chat/`. The `/design` skill is required to produce `task.md` before `/dryrun-design` runs. Without it, no file-level prescription can be traced to implementation tasks.
- **Risk**: Prescribed work has no implementation schedule and will be ad-hoc'd.
- **Fix**: Run `/design` to produce `task.md`, or create it manually covering the three skills (crosschat-register, crosschat-monitor, crosschat-send), the JetStream stream/KV setup, the listener script, and the session-start hook wiring.

---

## Warnings (should fix, may cause issues)

### [W1] KV bucket schema: value shape and TTL are underspecified

- **Pass**: Pass 2 (Data Flow Trace)
- **What**: The `crosschat-registry` KV bucket is described with a value shape (`{project_name, path, registered_at, last_heartbeat}`) but important details are missing: (a) value encoding (JSON assumed but not stated), (b) whether the KV bucket uses NATS KV's built-in TTL feature to auto-expire stale entries or relies solely on heartbeat-timestamp comparison by readers, (c) the heartbeat cadence is explicitly called "an implementation detail, not a v1 blocker" but without even a ballpark (10s? 5min?), discovery queries can't distinguish "active 30 seconds ago" from "dead 2 hours ago."
- **Risk**: Without TTL or a specified heartbeat cadence, stale entries accumulate forever and `crosschat-send`'s pre-check ("is destination active?") becomes meaningless — it will always say yes.
- **Suggestion**: Specify: JSON encoding, KV bucket TTL of e.g. 5 minutes, heartbeat interval of e.g. 60 seconds. If the session dies without deregistering, the entry auto-expires within the TTL window.

### [W2] No existing KV bucket usage in this codebase — first-time pattern

- **Pass**: Pass 3 (Interface Contract Validation)
- **What**: `nats_bridge.py` currently manages only JetStream streams (WORKERS, AUDIT, VH). It has no KV bucket creation/access code. The design introduces KV as if it's an extension of an existing pattern, but it's actually a new capability that needs new `nats_bridge` methods (or the skill scripts manage their own NATS connections independently, bypassing `nats_bridge` entirely — which is what the velhari-tracker scripts do, but that's a different repo).
- **Risk**: Implementer uncertainty about where KV bucket creation and access belong — in `nats_bridge.py` (server-side, like streams) or in standalone skill scripts (client-side, like `nats_wait.py`).
- **Suggestion**: Clarify: since the skills are Claude Code session hooks (not Velhari server components), the KV access likely lives in the skill scripts themselves using `nats.py` directly, not via `nats_bridge.py`. State this explicitly so the implementer doesn't try to wire it through the MCP server.

### [W3] Wake mechanism: "same as nats_wait.py" but the interface is unspecified

- **Pass**: Pass 4 (State Machine & Transitions)
- **What**: The design says the listener uses "the existing background-task-notification wake mechanism (identical to how worker completions wake V today)." This refers to `nats_wait.py` in the Velasari project (not this repo). The actual interface — how a background Python script's output becomes a notification that interrupts a Claude Code session's idle state and injects content into its next turn — is treated as fully specified elsewhere. But from this design's perspective, the contract is implicit: the listener script must produce output in a specific format, to a specific fd/channel, that Claude Code's background-task system picks up. None of that is specified here.
- **Risk**: The implementation either copies the nats_wait.py pattern correctly (because the implementer happens to know it) or gets the output format/channel wrong and the session never wakes.
- **Suggestion**: Add a one-paragraph "Wake contract" section: the listener script writes to stdout, Claude Code's background task system surfaces stdout output as a notification to the session, the output format is plain text (or JSON — specify which). Reference the specific nats_wait.py lines or pattern that should be replicated.

### [W4] Design addresses US-1 and US-3 but not US-2

- **Pass**: Pass 1 (Completeness Check)
- **What**: US-2 (cost comparison evidence: token cost and wall-clock for the same workflow run as sub-agent vs cross-chat) has no corresponding design element. It's a measurement/evidence requirement, not an architectural one, so it arguably doesn't need design — but it also can't be verified without at least a plan for what workflow to measure and how.
- **Risk**: US-2's acceptance criteria (AC-2.1, AC-2.2) are testable only after implementation, but without a design-time plan for the comparison, it risks being forgotten.
- **Suggestion**: Add a brief "Verification plan" note: which workflow will be measured, what metrics to capture, and when (post-implementation, as part of task.md).

---

## Observations (worth discussing)

### [O1] Pass 6: Concurrency — multi-session-per-project race is safely deferred

The design correctly identifies that multiple sessions for the same project would cause duplicate message delivery and defers it with the NATS queue-group solution noted. Since v1 assumes single-session-per-project, there's no concurrency gap in the current scope. The KV registry's single-writer-per-key model also avoids write conflicts in v1.

### [O2] Pass 7: Cold-start / first-run — registration before listener

The design specifies that `crosschat-register` fires on session-start and also starts `crosschat-monitor`. There's a brief window where the project is registered in KV (discoverable, messages can be sent to it) but the listener isn't yet subscribed. JetStream durability covers this — the message is stored in the stream and delivered once the listener subscribes. This is the correct design; noting it as verified.

### [O3] Pass 9: No "Files Changed" table and no file-path prescriptions in body

The design is a high-level architecture document that describes skills, subjects, and data flows without prescribing specific file changes (the skills don't exist yet — they're new directories/scripts to create). Body references to existing files (`nats_wait.py`, `nats_bridge.py`) are analogies/rationale, not prescriptions. Traceability check skipped per skill rules. However, this makes task.md even more critical — it's the only place where "create file X" will be specified.

---

## Summary

| Critical | Warnings | Observations |
|----------|----------|--------------|
| 4        | 4        | 3            |

**Verdict**: FAIL — needs revision

The design's core request/reply model (C1) has a silent-failure path where replies composed after the sender's `nc.request()` timeout are silently dropped, which is the primary use case (human-paced Claude sessions don't reply in seconds). The JetStream stream (C2) and project-ID derivation (C3) need concrete specs to implement without guessing. task.md is missing (C4). The locked decisions (JetStream for requests, KV for discovery, project-level addressing) are all sound — the gaps are in the operational details that connect them.
