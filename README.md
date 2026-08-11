# crosschat

A method for agents to talk to each other.

`crosschat` gives Claude Code sessions a way to send each other messages over
NATS — fire-and-forget, no blocking, no polling. Addressing is at the
**project** level, not the session level, so you never need to know which
ephemeral session is running: you send to a stable project id and whichever
session is live there wakes up.

## Why

Sub-agent fan-out is the usual answer when one session needs something another
context already knows. It is expensive, and it re-derives context the other
session already holds. `crosschat` lets two live sessions simply talk instead —
each keeps its own working memory, and a question costs one message.

It also lets you run several specialised projects at once and address them
individually — a team of experts rather than one generalist.

## Install

```
pip install crosschat
```

Requires a reachable NATS server with JetStream enabled.

## Use

One-time, shared infrastructure setup — run once ever, from anywhere:

```
crosschat init
```

Register a project and start listening:

```
crosschat register /path/to/my-project
crosschat monitor my-project
```

Send a message (this is also how you reply — pass the sender's id as the
destination):

```
crosschat send my-project other-project "what did you learn about X?"
```

Discovery and cleanup:

```
crosschat list
crosschat remove my-project
```

All subcommands take an optional trailing NATS URL, defaulting to
`nats://localhost:4222`.

## How it works

- **Channel** — `project.{id}`, one subject per project, backed by a JetStream
  stream (`CROSSCHAT`) so a message sent while a listener is reconnecting is
  not lost.
- **Discovery** — a JetStream KV bucket (`crosschat-registry`) holds one entry
  per live project. Entries carry a 5-minute TTL and the monitor refreshes its
  own every 60 seconds, so a project that dies falls out of the registry on its
  own. No manual cleanup.
- **Delivery** — the monitor subscribes with a *durable* consumer
  (`crosschat-{id}`), so the delivery position is tracked server-side. It never
  replays an already-acked message and never skips one, across restarts.
- **Wake contract** — the monitor prints one `CROSSCHAT_MESSAGE <json>` line
  per message and keeps running. In Claude Code, watch it with the Monitor tool
  and each line becomes its own notification.

## Project id

A project's id is its address. `crosschat register` is the only place it is
decided; every other command takes it as an argument or reads it from the
registry. Three ways to set it, in precedence order:

```
crosschat register /path/to/brain --id sb-architect   # explicit wins
CROSSCHAT_PROJECT_ID=sb-architect crosschat register /path/to/brain
crosschat register /path/to/sb-architect              # falls back to folder name
```

Prefer an explicit id for anything long-lived. The folder-name fallback is
convenient but ties identity to location: renaming the directory renames the
brain, and two brains cannot share a directory name.

The environment variable is what makes this work unattended — a project's own
`.claude/settings.json` can set `CROSSCHAT_PROJECT_ID`, and the generic
SessionStart hook picks it up with no per-project edit.

Whatever the source, the id is normalized the same way: lowercased, runs of
non-alphanumerics collapsed to single hyphens, hyphens trimmed. It has to be a
legal NATS subject token, so a chosen id is not trusted verbatim.

## Who sent this

`source_project_id` is a field the sender fills in, so on its own it is a claim
rather than a fact. crosschat signs it.

Each project gets an Ed25519 keypair on first `register`. The public half goes
into its registry entry; the private key stays at
`~/.crosschat/keys/<id>.key` (override with `CROSSCHAT_KEY_DIR`). `send` signs
the envelope, `monitor` verifies it against the sender's published key, and
every delivered message carries a `verification` field:

| value | meaning |
|---|---|
| `verified` | signature matches the sender's published key |
| `unsigned` | no signature — an older or unsigned peer |
| `no-key` | signed, but the sender publishes no key to check against |
| `bad-signature` | signature does not match — treat the claimed sender as unproven |

**Messages are marked, never dropped.** What an unverifiable message is worth
is your session's judgement, and a listener that silently discarded one would
make that call for you — and would break any mesh that is only partly
upgraded.

Identity travels with the message, not with the routing, so the same scheme
survives moving off NATS entirely.

**What this gives you is continuity, not authority.** The registry is
unauthenticated, so anything that can reach it can publish a key under any id.
A signature proves the same *key* keeps being used under the same name, and
makes a swap visible. It says nothing about who or what holds that key — a
rebuilt session inherits it from disk and signs indistinguishably — and it does
not prove that name was entitled to exist. Continuity of key, not continuity of
self. That is the right
trade for a mesh whose realistic failure is a project misaddressing itself;
if you need more, put NATS behind real authentication.

## Claude Code plugin

This repository is the engine — the Python package and its CLI. The Claude Code
integration (four skills plus a SessionStart hook that registers the project and
hands the live session its listener command) ships separately as a plugin in the
`apex-tools` marketplace, so that each piece has exactly one home:

```
/plugin marketplace add kaushikhazra/apex
/plugin install crosschat@apex-tools
```

The plugin shells out to whichever `crosschat` is on PATH, so install this
package first. If it is missing, the hook says so and the session continues
normally rather than failing to start.

## Status

Extracted from an internal fleet tool in August 2026 and renamed. The messaging
core, discovery layer and CLI are covered by unit tests against NATS fakes plus
a live smoke test. Trust between projects is deliberately out of scope for now:
anyone who can reach the NATS server can publish to any project channel, which
is fine for a single-machine, single-user setup and is the first thing to
revisit if that changes.

## License

MIT
