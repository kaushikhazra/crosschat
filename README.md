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

The project id is the slugified project directory name — `C:\Projects\velhari`
becomes `velhari`. `crosschat register` is the only place this is derived;
every other command takes the id as an argument or reads it from the registry.

**Your folder naming is your addressing scheme.** Unzip a brain into
`sb-architect/` and its address is `sb-architect`.

## Status

Extracted from an internal fleet tool in August 2026 and renamed. The messaging
core, discovery layer and CLI are covered by unit tests against NATS fakes plus
a live smoke test. Trust between projects is deliberately out of scope for now:
anyone who can reach the NATS server can publish to any project channel, which
is fine for a single-machine, single-user setup and is the first thing to
revisit if that changes.

## License

MIT
