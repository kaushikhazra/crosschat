# Session Cross-Talk via NATS — Tasks

## `crosschat-init` skill (US-1)

- [x] Implementer writes the `crosschat-init` logic: idempotently creates
      the `CROSSCHAT` JetStream stream (`subjects=["project.>"]`,
      `retention=limits`, `max_age=1*86400`, `storage=file`) and the
      `crosschat-registry` KV bucket (5-minute bucket TTL), using the
      same try/`add_stream`/except-`BadRequestError` pattern as
      `nats_bridge.py:60-69`. Entirely client-side via `nats.py` — not
      routed through `nats_bridge.py`. Core logic in
      `src/crosschat/core.py::ensure_infra`, exposed via
      `crosschat init` (`src/crosschat/cli.py`) — see
      "CLI consolidation" below.
      _requirement: AC-1.1_
- [x] Implementer confirms re-running `crosschat-init` against existing
      infra is a no-op (catches the already-exists error, no duplicate
      resources, no error surfaced to the caller). Covered by
      `tests/test_crosschat.py::TestEnsureInfra` (fakes NATS
      "already exists" errors, not a live server).
      _requirement: AC-1.2_
- [x] Implementer writes `crosschat-init`'s SKILL.md, stating explicitly
      that it is infra-only — it does not register a project or start
      any listener.
      _requirement: AC-1.3_

## `crosschat-register` skill (US-2)

- [x] Implementer writes the `crosschat-register` skill script: derives
      `project_id` as the slugified project directory name (sole
      derivation point — no other script recomputes it), writes the
      initial KV entry (`project_name`, `path`, `registered_at`,
      `last_heartbeat`) to `crosschat-registry`. Core logic in
      `src/crosschat/core.py::slugify_project_id`/`register_project`,
      tested in `TestSlugifyProjectId`/`TestRegisterProject`.
      _requirement: AC-2.1, AC-2.4_
- [x] Implementer adds the pre-flight check: if the `CROSSCHAT` stream or
      `crosschat-registry` bucket don't exist, fail with a clear error
      message naming `crosschat-init` as the fix — do not create the
      infra from this skill. `InfraNotReadyError`, tested in
      `TestRegisterProject::test_raises_when_infra_missing`.
      _requirement: AC-2.2_
- [x] **Implementer** wires `crosschat-register` into this repo's
      session-start hook — **repo-level** `.claude/settings.json`
      `SessionStart` hook (K's decision, per CLAUDE.md L5), not
      user-level. The hook runs `crosschat register` directly (a
      plain NATS call, no Claude involvement needed) and surfaces its
      result as `additionalContext`, instructing the live session to
      then launch `crosschat monitor` as a background process
      watched via the Monitor tool — starting the monitor itself requires
      Claude-side action the hook cannot perform directly. Verified live
      (pipe-tested against a real SessionStart payload, real
      registration succeeded: `project_id=velhari`) both before and after
      the CLI consolidation below.
      _requirement: AC-4.5, AC-6.1_

## `crosschat-send` skill (US-3, US-5)

- [x] Implementer writes the one-shot fire-and-forget send script:
      publishes the envelope (`source_project_id`, `dest_project_id`,
      `text`, `sent_at`) to `project.{dest_id}` on `CROSSCHAT` via
      `nc.publish`, returns immediately — no wait, no timeout, anywhere
      in this script. `src/crosschat/core.py::build_envelope`/
      `send_message`, tested in `TestBuildEnvelope`/`TestSendMessage`.
      _requirement: AC-3.1, AC-3.3_
- [x] Implementer adds the pre-publish `crosschat-registry` check,
      surfacing a clear error if the destination `project_id` is absent
      or its entry has expired, rather than publishing into a void.
      `DestinationNotRegisteredError`, tested in
      `TestSendMessage::test_raises_when_destination_not_registered`.
      _requirement: AC-3.2, AC-6.3_
- [x] Implementer confirms `crosschat-send` requires no separate "reply
      mode" — the same script/call replies to a message by passing the
      received `source_project_id` as the new `dest_project_id`.
      Documented in `crosschat-send`'s SKILL.md "Replying" section
      (closes US-5's "no special-cased reply path").
      _requirement: AC-3.4, AC-5.1_

## `crosschat-monitor` skill (US-4)

- [x] Implementer writes the persistent listener logic (now exposed via
      `crosschat monitor` — see "CLI consolidation" below):
      subscribes to `project.{own_id}` on `CROSSCHAT` using a **durable** JetStream
      consumer (`consumer_name=f"crosschat-{project_id}"`,
      `ack_policy=AckPolicy.EXPLICIT`), loops indefinitely — does not
      exit after a single message. `src/crosschat/core.py::monitor_loop`
      + `monitor_consumer_config`, tested in `TestMonitorLoop`/
      `TestMonitorConsumerConfig` against fakes. <!-- ⚠ fakes prove the
      code passes durable=consumer_name and consumes messages in the
      order the fake returns them; they cannot prove real JetStream's
      server-side delivery-cursor persistence across process restarts —
      that needs a live-NATS integration run (see Verification below). -->
      _requirement: AC-4.1_
- [x] Implementer implements the per-message behavior: print
      `CROSSCHAT_MESSAGE <json>` to stdout, `msg.ack()`, continue waiting
      for the next message. Tested in
      `TestMonitorLoop::test_emits_and_acks_each_message` and
      `test_delivers_messages_in_order_no_replay`.
      _requirement: AC-4.2_
- [x] Implementer adds the internal 60-second heartbeat timer running
      alongside the message-wait loop, refreshing this project's
      `crosschat-registry` `last_heartbeat` independent of message
      arrival. Tested in
      `TestMonitorLoop::test_refreshes_heartbeat_when_due`/
      `test_does_not_refresh_heartbeat_before_interval`.
      _requirement: AC-4.4_
- [x] Implementer wires `crosschat-monitor`'s script to print with
      `flush=True` so the Monitor tool can surface each stdout line as a
      notification (documented in SKILL.md "Wake contract"); confirms no
      relaunching occurs between messages (the loop is internal to
      `monitor_loop`, the script itself never exits on message receipt).
      _requirement: AC-4.3_
- [x] Implementer sets the NATS connection's own reconnect behavior
      (`allow_reconnect=True, max_reconnect_attempts=-1`, matching
      `nats_bridge.py:34-39`) so transient disconnects are handled by
      `nats.py` itself, not a bespoke relaunch/backoff loop.
      `connect_with_retry` in `src/crosschat/core.py`.
      _requirement: AC-6.4_
- [x] Implementer gives `crosschat-monitor`'s *initial* `nats.connect()`
      a bounded retry-with-backoff loop (same shape as `nats_bridge.py`'s
      `_connect()`, lines 30-51) distinct from the steady-state
      `allow_reconnect` behavior above. On exhausted retries, prints a
      clear failure line and exits, rather than hanging or crashing
      silently with no listener for the rest of the session. Tested in
      `TestConnectWithRetry`.
      _requirement: dryrun-design-3 W1_

## CLI consolidation (2026-07-26, K's separation-of-concerns correction)

The original pass gave each skill its own standalone `scripts/*.py` file
(copying `nats_wait.py`'s portability pattern). K flagged this as a
separation-of-concerns violation — Velhari is a Python package, so argv
parsing/dispatch belongs in the package as a real CLI, not duplicated four
ways across skill directories where it's untestable from a terminal.

- [x] **Implementer** adds `src/crosschat/cli.py`: one console
      script, `crosschat`, with `init`/`register`/`send`/`monitor`
      subcommands, each a thin argparse wrapper around the corresponding
      (unchanged) function in `src/crosschat/core.py`. Registered in
      `pyproject.toml` `[project.scripts]` alongside the existing
      `velhari`/`velhari-service` entries.
      _requirement: dryrun-design-3-adjacent — K's separation-of-concerns
      correction, 2026-07-26_
- [x] **Implementer** deletes the four `.claude/skills/crosschat-*/scripts/`
      directories — superseded by the CLI.
- [x] **Implementer** updates all four SKILL.md files to document
      `crosschat <subcommand> ...` instead of a per-skill script
      path, and updates `.claude/settings.json`'s `SessionStart` hook to
      call `crosschat register` instead of a script path.
- [x] **Implementer** writes `tests/test_crosschat_cli.py` (19 tests, 92%
      coverage on `crosschat_cli.py`): argv parsing for all 4 subcommands,
      dispatch routing, and each handler's wake-contract stdout output
      (`CROSSCHAT_INIT_OK`, `CROSSCHAT_REGISTERED`, `CROSSCHAT_SENT`,
      `CROSSCHAT_SEND_FAIL`, `CROSSCHAT_INIT_REQUIRED`,
      `NATS_CONNECT_FAIL`), all against fakes.
- [x] **Implementer** verifies `crosschat` is installed and
      runnable from a plain terminal after `pip install -e .`
      (`crosschat init` → `CROSSCHAT_INIT_OK` against the live
      local NATS server).

## Discovery & removal commands (2026-07-26, K's request)

Manual command-line testing surfaced a gap: no way to list registered
projects or force-remove one without waiting out the 5-minute TTL.
`list_active_projects` already existed in `crosschat.py` (US-2's
discovery mechanism) but was never exposed via the CLI; removal didn't
exist at all.

- [x] **Implementer** adds `deregister_project` to `src/crosschat/core.py`
      — idempotent immediate KV-entry removal, an operator convenience on
      top of (not a replacement for) the TTL-based lifecycle (US-6).
      Tested in `TestDeregisterProject` (4 tests).
- [x] **Implementer** adds `crosschat list` (prints one
      `CROSSCHAT_PROJECT <json>` line per live registry entry, or
      `CROSSCHAT_NO_PROJECTS`) and `crosschat remove <project_id>`
      (prints `CROSSCHAT_REMOVED <project_id>`) to `crosschat_cli.py`.
      Tested in `test_crosschat_cli.py` (8 new tests: parsing, dispatch,
      wake-contract output for both commands).
- [x] Verified live against the real NATS server: registered a scratch
      project, `list` showed it, `remove` deleted it, `list` afterward
      showed `CROSSCHAT_NO_PROJECTS`.
      _requirement: US-2 AC-2.3 (discovery), operational convenience
      alongside US-6_

## Verification

Unit-level correctness (durable-consumer wiring, fire-and-forget send,
heartbeat cadence, idempotent infra, initial-connect retry) is covered by
`tests/test_crosschat.py` (43 tests, 98% line coverage on
`src/crosschat/core.py`) using fakes for the NATS surface. Additionally,
a live smoke test ran against a real local NATS server this pass (not
via two actual Claude Code sessions — direct calls into
`src/crosschat/core.py` simulating both sides), test entries/consumers
cleaned up afterward:

- [x] `crosschat-init` → real `add_stream`/`create_key_value`, idempotent
      against a live server (AC-1.1, AC-1.2).
- [x] Two projects registered (`register_project`) and discoverable.
- [x] `crosschat-send` published two messages fire-and-forget to a live
      JetStream stream, with no wait/timeout (AC-3.1, AC-3.3).
- [x] `monitor_loop` delivered **both** messages in order against a real
      durable consumer — the core dryrun-2 C1 regression check. After a
      full reconnect (new `nc`, new `monitor_loop` call, same durable
      consumer name), it did **not** replay the two already-acked
      messages — confirms JetStream's server-side delivery-cursor
      persistence, not just the fake's in-memory queue order (AC-4.1).
- [x] Reply flow: B replied to A via the same `send_message` call
      addressed to `source_project_id`; A's monitor received it via the
      same path as any other message (AC-5.1, AC-5.2).
- [x] `crosschat-send` correctly raised `DestinationNotRegisteredError`
      for a project with no registry entry, without publishing (AC-3.2).

The remaining items need two genuinely separate live Claude Code sessions
or a real multi-minute wait window — not simulable from a single script —
so per K's direction they've moved to the cross-feature
**`.claude/manual-verification.md`** bucket, run together once all
features slated for a release are otherwise done, rather than blocking
this spec individually:

- [x] **US-1–US-5 end-to-end through two genuinely separate live sessions
      (AC-4.3, the live wake path) — VERIFIED 2026-08-10 19:32 IST.** Three
      second-brain instances were brought online in their own directories
      (`richard`, `keylan`, `zedd` under `C:/Projects/claude-sandbox/`),
      each registering itself and running its own listener. Velasari sent
      to `keylan`; `keylan` — a separate process with its own persona and
      context, not a simulation — replied 62 seconds later, and the reply
      surfaced on Velasari's Monitor as a `CROSSCHAT_MESSAGE` notification
      rather than a poll. This is the item no single-process smoke test
      could stand in for, open since 2026-07-25.
- [ ] The 3 still-open items in `.claude/manual-verification.md`
      § session-cross-chat: late-reply survival with a real elapsed gap
      (AC-5.3), listener-lifecycle TTL expiry (AC-6.2, AC-6.3), and US-7's
      cost comparison (AC-7.1, AC-7.2). The fleet now standing makes all
      three runnable for the first time.

---

**Status**: implemented 2026-07-25, refactored + extended 2026-07-26 on
`feature/session-cross-chat`. Core logic in `src/crosschat/core.py`
(47 unit tests, 98% coverage), exposed as a proper console script
`crosschat` (`src/crosschat/cli.py`, 28 tests, 94%
coverage — 75 total) per K's separation-of-concerns correction — the four
skills are now documentation for that CLI, not separate implementations
of it. Six subcommands: `init`, `register`, `send`, `monitor`, `list`,
`remove` (the last two added after manual CLI testing surfaced the
missing discovery/cleanup gap). Repo-level `SessionStart` hook wired in
`.claude/settings.json` (pipe-tested live). A full manual command-line
walkthrough (init → register → send → monitor → durable-consumer-restart
→ reply → unregistered-destination rejection → list/remove) passed
against the real local NATS server. The only remaining work is the
manual-verification bucket above (items needing two genuinely separate
live Claude Code sessions or a multi-minute real wait) — everything
else is done.
