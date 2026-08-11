# Session Cross-Talk via NATS — Design

## Overview

Every Claude Code project gets a project-level session-start hook that
starts a NATS listener scoped to that project. Any Claude session,
anywhere, can send a message to any *project* (not a specific ephemeral
session) by publishing to that project's channel. The receiving side's
listener wakes the live Claude session by streaming notifications from a
persistent background process (via the Monitor tool) — a different wake
mechanism from `nats_wait.py`'s exit-based one, chosen because a
project's channel is multi-message where a worker's completion subject
is single-message (see `crosschat-monitor`, revised post dryrun-2).

Addressing is at the **project level**, not the session level. This
sidesteps session discovery entirely: you don't need to know which
ephemeral session is running, only the stable project ID.

## Messaging model — fire-and-forget, symmetric (supersedes original C1 design)

**Revision note (2026-07-25, post dryrun-design-1 C1)**: the original
design used NATS core `request/reply` on the sender side, blocking on
`nc.request()` with a timeout. That's wrong for this use case and is
removed. Corrected model, confirmed by K:

- Every project runs exactly **one monitor** (`crosschat-monitor`),
  always listening on that project's own channel.
- **Sender never blocks waiting for a reply.** `crosschat-send` publishes
  a message and returns immediately — fire-and-forget, always.
- A "reply" is not a distinct mechanism. It is simply the receiving
  session calling `crosschat-send` again, addressed back to
  `source_project_id` (read from the envelope of the message it
  received). Both directions use the exact same channel type, same
  durability, same skill.
- Consequence: there is no client-side timeout anywhere in this design,
  and therefore no ephemeral-inbox-expiry failure mode. C1 is closed by
  removing the mechanism that caused it, not by hardening it.

## Subjects & Durability

**Revision note (2026-08-10, extraction to the standalone `crosschat`
package)**: the channel was `project.{id}` on a stream claiming `project.>`.
That was defensible for an internal fleet tool on a private NATS server and is
wrong for a published package — see "Subject namespacing" below. Corrected:

- **Channel**: `crosschat.{id}` — one subject per project, used for both
  inbound messages and replies (a reply is just a normal message whose
  destination happens to be the original sender's project).
- Backed by a **JetStream stream** so a message sent while a project's
  listener is transiently reconnecting is not lost. Matches requirement
  AC-6.4 (delivery survives a listener restart).
- **Concrete stream spec**:
  - name: `CROSSCHAT`
  - subjects: `["crosschat.>"]` (wildcard `>` not `*`, so future
    sub-subjects like `crosschat.{id}.control` are covered without a
    stream-def change)
  - retention: `limits`
  - max_age: `1 * 86400` (1 day — a message older than a session
    lifetime is stale)
  - storage: `file`

### Subject namespacing — why not a bare `project.>` (added 2026-08-10)

Two reasons, one principled and one that bit us live during the extraction:

1. **A published package must not squat a generic namespace.** `crosschat` now
   installs onto other people's NATS servers. `project.>` is a name anyone
   might reasonably want; claiming it is antisocial.
2. **JetStream refuses overlapping subjects.** A stream whose subjects overlap
   an existing stream's is rejected outright. With a greedy prefix, crosschat
   is simply un-installable next to anything that got there first — which is
   exactly what happened: on this machine the predecessor `CROSSTALK` stream
   still held `project.>`, so creating `CROSSCHAT` failed.

The failure was invisible because `ensure_infra` swallowed every
`BadRequestError` as "already exists" — see the next section. Both faults were
found together and fixed together.

### `ensure_infra` must not treat every rejection as already-exists

`ensure_infra` is meant to be idempotent, and implemented that as
`except BadRequestError: pass`. But JetStream raises the *same* exception class
for "stream name already in use" and for "subjects overlap with an existing
stream". The consequence: `crosschat init` printed `CROSSCHAT_INIT_OK` while
creating no stream at all, and every subsequent command then failed with a
misleading `CROSSCHAT_INIT_REQUIRED ... infra not found`.

Corrected: match the server's error text and re-raise anything that is not an
already-exists. A success report that cannot fail is not a success report.
Regression test:
`TestEnsureInfra::test_propagates_subject_overlap_instead_of_reporting_success`.

## Sender attribution — signed envelopes (added 2026-08-10)

**Why now.** `source_project_id` is a field the sender fills in; nothing stops
a publisher writing someone else's id there. That was tolerable while every
participant was one person's own project. It stopped being tolerable when a
persona carrying employer data was about to join the mesh — K's own deferral
from August named the trigger as *the day crosstalk crosses a non-Kaushik
boundary*.

**What was rejected, and why.** The first proposal was NATS-native: per-user
subject permissions, with the sender's id moved into the subject
(`cc.<dest>.from.<src>`) so the broker could refuse a publish under anyone
else's name. It works, and K rejected it on two grounds. It is
over-engineered for a single-user machine behind a home firewall, where there
is no intruder — and, decisively, *"it will not make the system messaging
agnostic."* Binding identity to subject permissions welds crosschat to one
broker.

**The model, taken from HTTPS.** TLS binds a name to a public key via a
certificate and has the peer prove possession of the private key. None of that
touches routing, which is exactly why it works over any byte stream. The same
shape here:

- Each project holds an **Ed25519 keypair**, created on first `register`.
- The **public** half is published in that project's `crosschat-registry`
  entry. Distribution rides on the discovery mechanism that already exists;
  nothing new is introduced.
- `send` signs the canonical envelope bytes and attaches `signature`.
- `monitor` reads the sender's published key, verifies, and attaches
  `verification` to the delivered envelope.

**Canonical bytes are a single definition** (`signing.canonical_bytes`): sorted
keys, no incidental whitespace, and `signature`/`verification` excluded — a
signature cannot cover itself, and `verification` is added by the receiver
afterwards. Both sides must derive identical bytes or every check fails, so
neither side serialises its own.

**Outcomes are reported, never enforced.** A message is always delivered,
carrying one of `verified`, `unsigned`, `no-key`, or `bad-signature`. What an
unverifiable message is worth is the receiving session's judgement; a listener
that silently dropped one would be making that call on its behalf, and would
also make a partially-upgraded mesh unusable.

**Optional by construction.** A project without PyNaCl registers without a key
and sends unsigned; a receiver without PyNaCl reports `no-key` rather than
crashing or, worse, reporting `verified`. Signing peers and non-signing peers
interoperate, which is what makes the upgrade possible at all — `kaylan` was
running an older release throughout.

**What this buys, stated honestly: continuity, not authority.** The registry is
itself unauthenticated — anything that can reach NATS can write a public key
under any id. A signature therefore proves *the same key keeps being used under
the same name*, and makes a key swap visible. It says nothing about who or what
holds that key — a rebuilt session inherits it from disk and signs
indistinguishably — and it does not prove that name was ever entitled to exist.
Continuity of key, not continuity of self. For a mesh whose realistic failure is a project
misaddressing itself, continuity is the property that matters. Anything
stronger requires an authenticated registry, which is a different and much
larger design — and if a real boundary ever appears, NATS **Auth Callout**
(2.10+) is the fallback K named.

**Private keys** live under `CROSSCHAT_KEY_DIR`, defaulting to
`~/.crosschat/keys/<project_id>.key`, written owner-only where the platform
supports it. Deliberately outside any project directory: a private key that
lives in a repo eventually gets committed.

Verified live 2026-08-10: a genuine message returned `verified`; an envelope
published straight to the subject, bypassing `crosschat send` and claiming to
be `velasari` under an impostor key, returned `bad-signature` and was still
delivered.

## Project ID — canonical definition (closes C3)

**Revision note (2026-08-10, K's call):** the slugified directory name was
originally the *only* rule. That made a project's address a function of where
it happened to sit on disk — renaming a folder silently renamed the brain, and
two brains could never share a directory name anywhere on the machine. With a
fleet of specialised instances in prospect, K asked for an explicit id
("I would say register with id"). The path rule survives as a fallback.

`project_id` is resolved by `resolve_project_id()` in the following
precedence, and nowhere else:

1. **Explicit** — `crosschat register <path> --id <id>`.
2. **Environment** — `CROSSCHAT_PROJECT_ID`.
3. **Path** — the slugified project directory name (e.g. `C:\Projects\velhari`
   → `velhari`), unchanged from the original rule.

Whatever the source, the value is normalized identically: lowercased, runs of
non-alphanumeric characters collapsed to single hyphens, leading/trailing
hyphens stripped. An explicitly supplied id is **not** trusted verbatim — it
still has to be a legal NATS subject token and a legal durable-consumer name.
An id that normalizes to nothing raises rather than registering something
unaddressable.

**Why the environment variable exists** (it is not redundant with the flag):
the plugin's SessionStart hook is generic and identical for every project, so
it cannot carry a per-project `--id`. A project's own `.claude/settings.json`
can set `CROSSCHAT_PROJECT_ID` in its `env` block, and the same unmodified
hook then registers it under that address. Flag for humans at a terminal,
environment for unattended registration.

`crosschat register` remains the **single authority** — `crosschat-send` and
`crosschat-monitor` always take `project_id` as an argument or read it from
the registry/envelope, and never recompute it from a path.

## Infra setup — `crosschat-init` (closes dryrun-2 C2)

**Revision note (2026-07-25, post dryrun-design-2 C2)**: stream/bucket
creation ownership was left as an unresolved "decide before starting"
fork in task.md, inconsistent with the KV-ownership call already made
below. K's correction: this isn't part of *any* project's registration
— it's a one-time (idempotent) setup of the shared cross-chat
infrastructure itself, invokable from any Claude session, decoupled
from any specific project.

- New skill: **`crosschat-init`** — not tied to a project. Ensures the
  `CROSSCHAT` JetStream stream and the `crosschat-registry` KV bucket
  exist, using the exact idempotent pattern `nats_bridge.py:60-69` uses
  for its own streams (`try: add_stream(...) / kv_add(...) except
  BadRequestError: pass`). Entirely client-side (skill script using
  `nats.py` directly) — same rationale as the KV-ownership call below,
  and **not** routed through `nats_bridge.py` (that module is Velhari
  MCP server-internal plumbing for the server's own `WORKERS`/`AUDIT`/
  `VH` streams; cross-chat infra has nothing to do with it).
- Safe to run multiple times, from multiple projects, in any order —
  whichever session runs it first creates the stream/bucket; every
  subsequent run is a no-op.
- `crosschat-register` (below) assumes this infra already exists; it
  does not create it. If `crosschat-register` finds the stream/bucket
  missing, it errors clearly ("run `crosschat-init` first") rather than
  silently creating it — keeps the "one-time infra setup" vs "per-project
  registration" concerns cleanly separated, per K's framing.

## Discovery — JetStream KV registry (closes W1)

- A JetStream **KV bucket** named `crosschat-registry` holds one entry
  per active project: key = `project_id`, value = JSON-encoded
  `{project_name, path, registered_at, last_heartbeat}`.
- **Bucket TTL**: 5 minutes (KV bucket-level TTL, entries auto-expire if
  not refreshed).
- **Heartbeat cadence**: `crosschat-monitor` is a single persistent
  process for the session's life (see below), so it refreshes
  `last_heartbeat` on a simple **internal 60-second timer** running
  alongside its message-wait loop — comfortably under the 5-minute
  bucket TTL. A session that dies (its `crosschat-monitor` process ends)
  stops ticking the timer and falls out of the registry within 5
  minutes — no manual cleanup needed.
- `crosschat-register` writes the initial entry on session start;
  `crosschat-monitor` owns the periodic heartbeat refresh for the rest
  of the session's life.
- Discovery (which projects are active) = read the KV bucket, not a
  subject-search — plain NATS has no built-in subject enumeration for
  this. Exposed via `crosschat list` (`list_active_projects` in
  `crosschat.py`), which prints one `CROSSCHAT_PROJECT <json>` line per
  live entry, or `CROSSCHAT_NO_PROJECTS` if the registry is empty.
- KV access is client-side, inside the CLI (using `nats.py` directly),
  **not** routed through `nats_bridge.py` — the skills are Claude Code
  session hooks, not Velhari server components. `nats_bridge` today only
  manages JetStream streams for the MCP server itself; it has no
  KV-bucket code and none is added to it for this feature (closes W2).

## Explicit removal — `crosschat remove` (added 2026-07-26)

The TTL-based expiry above is the only *required* cleanup mechanism (no
manual step needed for a session ending normally). But an explicit
`crosschat remove <project_id>` command (`deregister_project` in
`crosschat.py`) was added as an operational convenience — deleting a
registry entry immediately rather than waiting out the 5-minute TTL,
useful for tearing down scratch/test registrations. Idempotent: removing
an already-absent entry is not an error. This does not change the
lifecycle model (US-6) — it's an operator escape hatch alongside it, not
a replacement for it.

## Multi-session-per-project

Not a v1 concern (K: "we don't worry about that"). If it becomes one,
NATS queue groups are the mechanical answer — same queue-group name per
project across listeners means NATS load-balances delivery to exactly
one, no lock/PID-check needed. Deferred, not designed further now.

## Skills

### `crosschat-register`

- Fires from the project's session-start hook.
- Assumes `crosschat-init` has already been run at least once (by any
  session) — errors clearly if the `CROSSCHAT` stream or
  `crosschat-registry` bucket don't exist, rather than creating them
  itself.
- Registers this project's initial entry in the `crosschat-registry` KV
  bucket (`project_name`, `path`, `registered_at`, `last_heartbeat`).
- Starts `crosschat-monitor` once, as a single long-running background
  process, watched via the Monitor tool (not relaunched — see
  `crosschat-monitor` below).

### `crosschat-monitor`

**Revision note (2026-07-25, post dryrun-design-2 C1 + K's follow-up)**:
`nats_wait.py`'s one-shot, exit-to-wake pattern was designed for a
subject that holds exactly one message ever (a worker's single
completion event). `project.{id}` accumulates many messages over a
session's life, so copying that pattern verbatim either replays stale
messages forever (ephemeral consumer — dryrun-2 C1) or, even with a
durable consumer, forces an awkward relaunch-per-message cycle that
exists only to work around the exit-to-wake constraint. Corrected model:

- **One persistent, long-running process**, started **once** by
  `crosschat-register` at session start — not relaunched per message,
  not per-cycle. Lives for the duration of the session.
- Subscribes to `project.{own_id}` on the `CROSSCHAT` JetStream stream
  using a **durable consumer** (name `crosschat-{project_id}`,
  `ack_policy=AckPolicy.EXPLICIT`) — closes dryrun-2 C1: the delivery
  position is tracked server-side by JetStream, independent of this
  process, so it always resumes at the next-undelivered message,
  never replays and never skips.
- Loops internally: on each message, prints one line to stdout, calls
  `msg.ack()`, and continues waiting for the next one. Never exits on
  its own (only on session end / explicit stop).
- **Wake mechanism**: watched via the **Monitor tool** (this harness's
  native capability to stream events from a background process, where
  each stdout line becomes its own notification to the live session —
  no process exit required, unlike the worker-tracker's exit-based
  wake). This is a better fit here than the `nats_wait.py` pattern
  precisely because our channel is multi-message and the tracker's
  isn't.
- **Wake contract**: each stdout line is `CROSSCHAT_MESSAGE <json>`
  (envelope: `source_project_id`, `text`, `sent_at`). No `TIMEOUT` line
  — there's no per-cycle timeout anymore, the subscription simply waits
  indefinitely for the next message. Steady-state disconnects (already
  connected, connection blips) are handled by `nats.py`'s own reconnect
  (`allow_reconnect=True, max_reconnect_attempts=-1`, matching
  `nats_bridge.py`'s connect config at lines 34-39) rather than a bespoke
  backoff — closes dryrun-2 W1, since there's no relaunch loop left to
  hot-loop in the first place.
- **Initial connection failure** (closes dryrun-3 W1): distinct from the
  steady-state case above — this is `nats.connect()` failing before any
  connection has ever been established (NATS unreachable at the moment
  `crosschat-monitor` first launches). `allow_reconnect` only governs an
  already-established connection, so it doesn't cover this path.
  `crosschat-monitor` uses the same retry-with-backoff pattern as
  `nats_bridge.py`'s `_connect()` (lines 30-51: bounded retry loop with
  `connect_timeout`) for the initial connect. If it still can't connect
  after that, it prints a clear failure line (surfaced to the live
  session via the Monitor tool, same as any other line) and exits — the
  session then knows its listener never started, rather than the process
  hanging or crashing silently with no listener for the rest of the
  session's life.
- **KV heartbeat**: a simple internal timer inside the same loop,
  ticking every 60 seconds independently of message arrival (back to the
  original, simpler cadence from dryrun-1 — the 240s relaunch-derived
  compromise is no longer needed once the process doesn't exit between
  messages).
- The Claude session, once it has composed its answer, calls
  `crosschat-send` itself, addressed to `source_project_id` — that *is*
  the reply. `crosschat-monitor` does not publish anything back on the
  session's behalf.

### Listener lifecycle (closes requirement AC-6.1)

A session's cross-chat listener is **always-on for the duration of the
session** — started once by `crosschat-register` at session start via
`crosschat-monitor`, kept alive as a single long-running background
process for the session's lifetime, and stopped only when the session
itself ends (nothing relaunches it after that, and its durable consumer
simply waits server-side, unread, until a future session for this
project starts a new `crosschat-monitor` against the same durable
consumer name). This is a stated decision, not an inference from the
mechanics — resolves the open question in requirement.md AC-6.1.

### `crosschat-send`

- One-shot, **fire-and-forget only** — no request/reply, no blocking
  wait, no timeout. Given a destination `project_id` and a message text,
  publishes an envelope to `project.{dest_id}` on the `CROSSCHAT` stream
  via `nc.publish(...)` (JetStream-backed, so delivery survives a
  transient listener reconnect) and returns immediately.
- Used identically for an initial message and for a reply — the only
  difference is which `project_id` the caller passes as destination.
- Checks the KV registry first and surfaces a clear error if the
  destination project isn't currently registered/active, rather than
  publishing into a void.

## CLI — separation of concerns (revised 2026-07-26, K's correction)

**Revision note**: the original plan gave each skill its own standalone
`scripts/*.py` file (portability pattern copied from `nats_wait.py`). K
flagged this as a separation-of-concerns violation: Velhari is a Python
package, so the argv-parsing/dispatch logic belongs in the package as a
proper CLI, not duplicated four ways across skill directories where it's
untestable from a terminal and untested by pytest. Corrected:

- One console script, `crosschat` (registered in `pyproject.toml`,
  same pattern as the existing `velhari`/`velhari-service` scripts),
  backed by `src/crosschat/cli.py` — four subcommands: `init`,
  `register`, `send`, `monitor`. Each subcommand is a thin argparse
  wrapper around the corresponding function in `src/crosschat/core.py`
  (unchanged) — `crosschat_cli.py` owns only argv parsing and the
  stdout wake-contract strings.
- The four skills no longer have their own `scripts/` directories. Each
  SKILL.md documents invoking `crosschat <subcommand> ...`
  directly — the skill is documentation for the CLI, not a separate
  implementation of it.
- Testable from a plain terminal with no skill/Claude Code involvement:
  `crosschat init`, `crosschat send <a> <b> "text"`, etc.
  Unit-tested in `tests/test_crosschat_cli.py` (argv parsing + dispatch +
  wake-contract output, all fakes — no live NATS needed for these tests).
- All subcommands take **arguments**, not hardcoded config: NATS server
  URL, project ID, message content. No project-specific values baked
  into the CLI itself.

## Message envelope (minimum shape)

```json
{
  "source_project_id": "...",
  "dest_project_id": "...",
  "text": "...",
  "sent_at": "..."
}
```

Reply routing doesn't need `in_reply_to`/correlation fields in the
envelope — a reply is routed purely by the sending session reading
`source_project_id` off the message it received and passing it back to
`crosschat-send` as the new destination.

## Verification plan for US-7 (closes dryrun-1 W4)

Cost-comparison evidence (AC-7.1, AC-7.2 as of the requirement.md
2026-07-25 renumbering) is a measurement task, not a design element, but
needs a concrete plan so it isn't forgotten post-implementation:

- Pick one real workflow currently done via Agent-tool sub-agent fan-out
  where the actual need is a single question-and-answer exchange with
  another session's context (not genuine parallel work).
- Run it once as a sub-agent call, once as a cross-chat exchange (two
  live sessions, same machine).
- Capture: total tokens billed across both sessions (not just the
  sender), wall-clock time, and whether the second session was otherwise
  idle or doing its own concurrent work (AC-7.2 — a cross-chat exchange
  against an otherwise-idle session is the fair comparison; against a
  session that would've billed anyway, cross-chat is even cheaper).
- Record the comparison in this spec folder (e.g.
  `cost-comparison.md`) once implementation lands. This is a task.md
  item, not a v1 architectural blocker.

## Deferred / not designed here

- Multi-session-per-project dedup (queue groups, if/when it's needed).
- Message envelope versioning, auth/trust between projects (anyone who
  can reach the NATS server can publish to any project channel today —
  fine for a single-machine, single-user setup; revisit if that changes).

---

**Status**: design revised 2026-07-25 (K + V co-think), second pass, to
address dryrun-design-2 findings (C1, C2, W1, W2) — durable JetStream
consumer + persistent long-running monitor process (closes C1, W1),
`crosschat-init` as a separate one-time infra-setup skill (closes C2),
explicit listener-lifecycle statement (closes W2). Given the scope this
now covers (four skills, two NATS primitives, a discovery layer), the
requirement is being rewritten into more granular user stories before
task.md is updated to match. Next step: revised requirement.md, then
task.md, then another `/dryrun-design` pass.
