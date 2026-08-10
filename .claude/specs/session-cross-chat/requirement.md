# Session Cross-Talk via NATS — Requirements

## Background

Sub-agent (Agent tool) fan-out burns a lot of tokens when the real need is
just two Claude Code sessions exchanging messages — not one session paying
for a full nested-context sub-agent every time it needs another session's
input or result.

Kaushik + Sirisha identified the fix: let two independent Claude Code
sessions talk to each other directly over NATS, reusing the durable
JetStream pattern Velhari already runs for worker-completion tracking.
An older, rejected alternative was a Unix-mail-style file-drop mailbox
with polling — NATS is the better mechanism: push-based, low-latency,
already proven in this codebase.

This is a Velhari-shaped capability — orchestration/dispatch primitives
between agent sessions are Velhari's domain, not persona memory (Velasari)
or a one-off research spike.

**Revision note (2026-07-25)**: this requirement was rewritten after two
design dry-run passes (`dryrun-design-1.md`, `dryrun-design-2.md`) forced
several architectural decisions that the original three broad user
stories didn't have room to state as testable acceptance criteria —
notably: fire-and-forget messaging with no request/reply timeout (closes
dryrun-1 C1), a durable JetStream consumer instead of an ephemeral one
(closes dryrun-2 C1), a persistent long-running listener process instead
of relaunch-per-message (K's correction, post dryrun-2), and a separate
one-time `crosschat-init` infra-setup step distinct from per-project
registration (closes dryrun-2 C2, K's correction). The stories below are
split more granularly than the original draft so each piece of the
design has its own testable acceptance criteria.

---

## User Stories

### US-1 — Cross-chat infrastructure setup

> As any Claude Code session, I want to set up the shared cross-chat
> infrastructure once, so that projects can register and exchange
> messages without each one needing to create the underlying NATS
> resources itself.

**Acceptance Criteria:**
- AC-1.1: Running `crosschat-init` when the `CROSSCHAT` JetStream stream
  and `crosschat-registry` KV bucket do not yet exist creates both,
  matching design.md's spec (`CROSSCHAT`: subjects `["project.>"]`,
  retention `limits`, max_age 1 day, storage `file`; `crosschat-registry`:
  5-minute bucket TTL).
- AC-1.2: Running `crosschat-init` again after the infra already exists
  is a no-op — no error, no duplicate resources, safe to run from
  multiple sessions in any order.
- AC-1.3: `crosschat-init` does not register any project and does not
  start any listener — it only ensures the shared infra exists.

### US-2 — Project registration & discovery

> As a Claude Code session, I want my project to register itself and be
> discoverable, so that other sessions can find and message it without
> needing to know it's running in advance.

**Acceptance Criteria:**
- AC-2.1: On session start, `crosschat-register` writes an entry to
  `crosschat-registry` keyed by this project's `project_id` (the
  slugified project directory name), with `project_name`, `path`,
  `registered_at`, `last_heartbeat`.
- AC-2.2: If the `CROSSCHAT` stream or `crosschat-registry` bucket don't
  exist (i.e. `crosschat-init` hasn't been run), `crosschat-register`
  fails with a clear error naming `crosschat-init` as the fix — it does
  not silently create the infra itself.
- AC-2.3: Reading the `crosschat-registry` KV bucket lists every project
  with a non-expired entry — this is the entire discovery mechanism, no
  separate "list active projects" NATS call exists.
- AC-2.4: `project_id` is derived in exactly one place (`crosschat-
  register`, from the project directory path); `crosschat-send` and
  `crosschat-monitor` always take it as an argument or read it from the
  registry/envelope, never recompute it independently.

### US-3 — Fire-and-forget message send

> As a Claude Code session, I want to send a message to another
> project's channel without waiting for a reply, so that sending never
> blocks on how long the other session takes to respond.

**Acceptance Criteria:**
- AC-3.1: `crosschat-send`, given a destination `project_id` and message
  text, publishes an envelope to `project.{dest_id}` on `CROSSCHAT` and
  returns immediately — no blocking wait, no client-side timeout, ever.
- AC-3.2: Before publishing, `crosschat-send` checks `crosschat-registry`
  for the destination `project_id`; if the entry is absent or expired,
  it returns a clear error and does not publish into a channel nobody
  is registered to read.
- AC-3.3: The published envelope contains at minimum
  `source_project_id`, `dest_project_id`, `text`, `sent_at`.
- AC-3.4: `crosschat-send` is used identically for an initial message and
  for a reply — only the destination `project_id` argument differs;
  there is no separate "reply" call or mechanism.

### US-4 — Persistent listener & wake-on-message

> As a Claude Code session, I want to be woken the instant a message
> arrives on my project's channel, so that I don't have to poll and
> don't miss messages sent while I was briefly disconnected.

**Acceptance Criteria:**
- AC-4.1: `crosschat-monitor` subscribes to `project.{own_id}` on
  `CROSSCHAT` using a **durable** JetStream consumer (name
  `crosschat-{project_id}`, explicit ack) — if the process restarts, it
  resumes from the next-undelivered message: it never replays an
  already-acked message and never skips one.
- AC-4.2: On each message, `crosschat-monitor` prints one
  `CROSSCHAT_MESSAGE <json>` line to stdout and acks the message before
  waiting for the next one.
- AC-4.3: The live Claude session is woken on **each** message via the
  Monitor tool's per-stdout-line notification — no polling, and the
  listener script is never relaunched per message.
- AC-4.4: `crosschat-monitor` refreshes this project's
  `crosschat-registry` `last_heartbeat` every 60 seconds via an internal
  timer, independent of whether a message has arrived.
- AC-4.5: `crosschat-monitor` is started exactly once per session (by
  `crosschat-register`) and runs as a single long-running process for
  the session's full duration — not restarted between messages.

### US-5 — Reply as an ordinary message

> As a Claude Code session that received a message, I want to reply
> using the same mechanism I'd use to send any message, so that there's
> no special-cased "reply" path that can drop a late response.

**Acceptance Criteria:**
- AC-5.1: A session that received a message with `source_project_id` X
  replies by calling `crosschat-send` with `dest_project_id` = X — no
  separate reply skill, reply subject, or reply inbox exists.
- AC-5.2: The originating session's `crosschat-monitor` receives the
  reply exactly as it would any other incoming message — same wake path
  (AC-4.3), same envelope shape (AC-3.3).
- AC-5.3: A reply composed several minutes after the original message
  was received is not lost — there is no timeout window during which a
  late reply could be silently dropped (end-to-end verification that
  dryrun-1 C1 and dryrun-2 C1 stay fixed together, not just individually).

### US-6 — Listener lifecycle & cleanup

> As a session participating in cross-chat, I want a listening window
> that matches my session's lifetime exactly, so that messages aren't
> missed while I'm active and I don't leave a listener running forever
> after I'm gone.

**Acceptance Criteria:**
- AC-6.1: A session's cross-chat listener is on for its entire session
  duration — started once at session start (US-4, AC-4.5), no explicit
  "open" action needed, and no per-message re-arming.
- AC-6.2: When a session ends (its `crosschat-monitor` process stops),
  no further heartbeat refresh occurs (AC-4.4 stops), and the project's
  `crosschat-registry` entry expires within 5 minutes with no manual
  deregistration step.
- AC-6.3: A message sent to a project whose entry has expired (listener
  not running) is rejected at send time by `crosschat-send`'s registry
  check (AC-3.2), rather than published into a channel nobody will read.
- AC-6.4: A message sent to a project whose listener is transiently
  reconnecting (NATS blip, not a dead session) is **not** lost — it's
  delivered once the durable consumer (AC-4.1) resumes.

### US-7 — Cost comparison vs sub-agent fan-out

> As Kaushik, I want evidence that session-to-session NATS messaging is
> actually cheaper than Agent-tool sub-agent fan-out for the cases it's
> meant to replace, so that this isn't just moving the cost around.

**Acceptance Criteria:**
- AC-7.1: A measured comparison exists (token cost, wall-clock) for at
  least one real workflow run both ways: as a sub-agent call and as a
  session-to-session NATS exchange (US-3 + US-4 + US-5 end to end).
- AC-7.2: The comparison accounts for the cost of a second live session
  billing concurrently, not just the message-passing overhead — a
  cross-chat exchange against an otherwise-idle session is the fair
  comparison point.

---

## Open Questions (resolved during design — kept here for history)

- ~~Always-on subscriber per session vs spun up on demand?~~ **Resolved**:
  always-on for the session's duration (US-6).
- ~~Message shape/protocol: free text, structured request/reply, or
  MCP tool-call-like semantics?~~ **Resolved**: free-text envelope
  (`source_project_id`, `dest_project_id`, `text`, `sent_at`), no
  structured request/reply protocol — replies are ordinary messages
  (US-5).
- ~~Failure/timeout handling?~~ **Resolved**: no client-side timeouts
  anywhere in the design; delivery durability comes from JetStream, not
  from retry/timeout logic (US-3, US-4).
- **Still open**: does this replace Agent-tool sub-agent fan-out
  entirely, or coexist for cases where a genuinely nested context is
  cheaper than a round-trip? Deferred until US-7's evidence is in.

## Out of Scope (for now)

- Replacing the Agent tool — this is an additional mechanism, not a
  mandated migration, until US-7's cost comparison is in.
- Cross-machine session-to-session (assume same NATS server / same
  Velhari fleet for v1).
- Multi-session-per-project (K: "we don't worry about that" — NATS queue
  groups are the mechanical answer if/when it's needed, per design.md).

---

**Status**: rewritten 2026-07-25 (K + V co-think), second revision, after
`dryrun-design-1.md` and `dryrun-design-2.md`. 7 user stories, each with
design-grounded acceptance criteria. Next step: update `task.md` to match
this story breakdown, then re-run `/dryrun-design`.
