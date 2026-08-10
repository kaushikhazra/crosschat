"""CLI entrypoint for session cross-talk: `crosschat <command> ...`.

Thin argparse wrapper around crosschat.core — the actual logic lives
there and is what's unit-tested; this module only owns argv parsing and
the stdout wake-contract strings each Claude Code skill depends on. See
.claude/specs/session-cross-chat/design.md and the crosschat-{init,
register,send,monitor} skills, which just document invoking this CLI.
"""

from __future__ import annotations

import argparse
import asyncio
import json

from crosschat.core import (
    DestinationNotRegisteredError,
    InfraNotReadyError,
    REGISTRY_BUCKET,
    build_envelope,
    connect_with_retry,
    deregister_project,
    ensure_infra,
    list_active_projects,
    monitor_loop,
    register_project,
    send_message,
    slugify_project_id,
)

DEFAULT_NATS_URL = "nats://localhost:4222"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="crosschat")
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init", help="One-time, idempotent cross-talk infra setup")
    p_init.add_argument("nats_url", nargs="?", default=DEFAULT_NATS_URL)

    p_register = sub.add_parser("register", help="Register this project for cross-talk")
    p_register.add_argument("project_path")
    p_register.add_argument("nats_url", nargs="?", default=DEFAULT_NATS_URL)

    p_send = sub.add_parser("send", help="Fire-and-forget send (also used for replies)")
    p_send.add_argument("source_project_id")
    p_send.add_argument("dest_project_id")
    p_send.add_argument("text")
    p_send.add_argument("nats_url", nargs="?", default=DEFAULT_NATS_URL)

    p_monitor = sub.add_parser("monitor", help="Persistent listener for this project")
    p_monitor.add_argument("project_id")
    p_monitor.add_argument("nats_url", nargs="?", default=DEFAULT_NATS_URL)

    p_list = sub.add_parser(
        "list", help="List currently registered (non-expired) projects"
    )
    p_list.add_argument("nats_url", nargs="?", default=DEFAULT_NATS_URL)

    p_remove = sub.add_parser(
        "remove",
        help="Remove a project's registry entry immediately (bypasses TTL wait)",
    )
    p_remove.add_argument("project_id")
    p_remove.add_argument("nats_url", nargs="?", default=DEFAULT_NATS_URL)

    return parser


async def _run_init(nats_url: str) -> None:
    try:
        nc = await connect_with_retry(nats_url)
    except ConnectionError as e:
        print(f"NATS_CONNECT_FAIL {e}")
        return
    try:
        await ensure_infra(nc.jetstream())
        print("CROSSCHAT_INIT_OK")
    finally:
        await nc.drain()


async def _run_register(project_path: str, nats_url: str) -> None:
    project_id = slugify_project_id(project_path)
    project_name = project_path.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]
    try:
        nc = await connect_with_retry(nats_url)
    except ConnectionError as e:
        print(f"NATS_CONNECT_FAIL {e}")
        return
    try:
        js = nc.jetstream()
        try:
            await register_project(js, project_id, project_name, project_path)
        except InfraNotReadyError as e:
            print(f"CROSSCHAT_INIT_REQUIRED {e}")
            return
        print(f"CROSSCHAT_REGISTERED {project_id}")
    finally:
        await nc.drain()


async def _run_send(
    source_project_id: str, dest_project_id: str, text: str, nats_url: str
) -> None:
    try:
        nc = await connect_with_retry(nats_url)
    except ConnectionError as e:
        print(f"NATS_CONNECT_FAIL {e}")
        return
    try:
        js = nc.jetstream()
        envelope = build_envelope(source_project_id, dest_project_id, text)
        try:
            await send_message(js, dest_project_id, envelope)
        except DestinationNotRegisteredError as e:
            print(f"CROSSCHAT_SEND_FAIL {e}")
            return
        print(f"CROSSCHAT_SENT {dest_project_id}")
    finally:
        await nc.drain()


async def _run_monitor(project_id: str, nats_url: str) -> None:
    try:
        nc = await connect_with_retry(nats_url)
    except ConnectionError as e:
        print(f"NATS_CONNECT_FAIL {e}", flush=True)
        return
    try:
        js = nc.jetstream()
        kv = await js.key_value(REGISTRY_BUCKET)
        await monitor_loop(
            js, kv, project_id, emit=lambda line: print(line, flush=True)
        )
    finally:
        await nc.drain()


async def _run_list(nats_url: str) -> None:
    try:
        nc = await connect_with_retry(nats_url)
    except ConnectionError as e:
        print(f"NATS_CONNECT_FAIL {e}")
        return
    try:
        js = nc.jetstream()
        kv = await js.key_value(REGISTRY_BUCKET)
        projects = await list_active_projects(kv)
        if not projects:
            print("CROSSCHAT_NO_PROJECTS")
            return
        for project in projects:
            print("CROSSCHAT_PROJECT " + json.dumps(project))
    finally:
        await nc.drain()


async def _run_remove(project_id: str, nats_url: str) -> None:
    try:
        nc = await connect_with_retry(nats_url)
    except ConnectionError as e:
        print(f"NATS_CONNECT_FAIL {e}")
        return
    try:
        js = nc.jetstream()
        kv = await js.key_value(REGISTRY_BUCKET)
        await deregister_project(kv, project_id)
        print(f"CROSSCHAT_REMOVED {project_id}")
    finally:
        await nc.drain()


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "init":
        asyncio.run(_run_init(args.nats_url))
    elif args.command == "register":
        asyncio.run(_run_register(args.project_path, args.nats_url))
    elif args.command == "send":
        asyncio.run(
            _run_send(
                args.source_project_id, args.dest_project_id, args.text, args.nats_url
            )
        )
    elif args.command == "monitor":
        asyncio.run(_run_monitor(args.project_id, args.nats_url))
    elif args.command == "list":
        asyncio.run(_run_list(args.nats_url))
    elif args.command == "remove":
        asyncio.run(_run_remove(args.project_id, args.nats_url))


if __name__ == "__main__":
    main()
