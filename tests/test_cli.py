"""Tests for crosschat.cli (the `crosschat` console script).

Exercises argv parsing and dispatch by invoking main() with monkeypatched
async handlers — this is the CLI surface that used to be duplicated across
four separate .claude/skills/*/scripts/*.py files; consolidating it here
makes it testable directly, which those duplicated scripts never were.
"""

from __future__ import annotations

import asyncio
import sys

import pytest

from crosschat import cli as crosstalk_cli


class TestBuildParser:
    def test_init_defaults_nats_url(self):
        args = crosstalk_cli.build_parser().parse_args(["init"])
        assert args.command == "init"
        assert args.nats_url == crosstalk_cli.DEFAULT_NATS_URL

    def test_init_accepts_custom_nats_url(self):
        args = crosstalk_cli.build_parser().parse_args(["init", "nats://other:4222"])
        assert args.nats_url == "nats://other:4222"

    def test_register_requires_project_path(self):
        args = crosstalk_cli.build_parser().parse_args(["register", "/some/path"])
        assert args.command == "register"
        assert args.project_path == "/some/path"
        assert args.nats_url == crosstalk_cli.DEFAULT_NATS_URL

    def test_send_requires_three_positional_args(self):
        args = crosstalk_cli.build_parser().parse_args(["send", "a", "b", "hello"])
        assert (args.source_project_id, args.dest_project_id, args.text) == (
            "a",
            "b",
            "hello",
        )

    def test_monitor_requires_project_id(self):
        args = crosstalk_cli.build_parser().parse_args(["monitor", "velhari"])
        assert args.project_id == "velhari"

    def test_list_defaults_nats_url(self):
        args = crosstalk_cli.build_parser().parse_args(["list"])
        assert args.command == "list"
        assert args.nats_url == crosstalk_cli.DEFAULT_NATS_URL

    def test_remove_requires_project_id(self):
        args = crosstalk_cli.build_parser().parse_args(["remove", "velhari"])
        assert args.command == "remove"
        assert args.project_id == "velhari"
        assert args.nats_url == crosstalk_cli.DEFAULT_NATS_URL

    def test_no_command_raises_systemexit(self):
        with pytest.raises(SystemExit):
            crosstalk_cli.build_parser().parse_args([])

    def test_unknown_command_raises_systemexit(self):
        with pytest.raises(SystemExit):
            crosstalk_cli.build_parser().parse_args(["bogus"])


class TestMainDispatch:
    """main() must route each subcommand to its handler with the right args."""

    def test_init_dispatches_to_run_init(self, monkeypatch):
        called = {}

        async def fake_run_init(nats_url):
            called["nats_url"] = nats_url

        monkeypatch.setattr(crosstalk_cli, "_run_init", fake_run_init)
        monkeypatch.setattr(sys, "argv", ["crosschat", "init", "nats://x:1"])
        crosstalk_cli.main()
        assert called == {"nats_url": "nats://x:1"}

    def test_register_dispatches_to_run_register(self, monkeypatch):
        called = {}

        async def fake_run_register(project_path, nats_url, project_id):
            called["args"] = (project_path, nats_url, project_id)

        monkeypatch.setattr(crosstalk_cli, "_run_register", fake_run_register)
        monkeypatch.setattr(sys, "argv", ["crosschat", "register", "/p"])
        crosstalk_cli.main()
        assert called["args"] == ("/p", crosstalk_cli.DEFAULT_NATS_URL, None)

    def test_register_passes_explicit_id_through(self, monkeypatch):
        called = {}

        async def fake_run_register(project_path, nats_url, project_id):
            called["args"] = (project_path, nats_url, project_id)

        monkeypatch.setattr(crosstalk_cli, "_run_register", fake_run_register)
        monkeypatch.setattr(
            sys, "argv", ["crosschat", "register", "/p", "--id", "sb-architect"]
        )
        crosstalk_cli.main()
        assert called["args"] == ("/p", crosstalk_cli.DEFAULT_NATS_URL, "sb-architect")

    def test_send_dispatches_to_run_send(self, monkeypatch):
        called = {}

        async def fake_run_send(source, dest, text, nats_url):
            called["args"] = (source, dest, text, nats_url)

        monkeypatch.setattr(crosstalk_cli, "_run_send", fake_run_send)
        monkeypatch.setattr(sys, "argv", ["crosschat", "send", "a", "b", "hi"])
        crosstalk_cli.main()
        assert called["args"] == ("a", "b", "hi", crosstalk_cli.DEFAULT_NATS_URL)

    def test_monitor_dispatches_to_run_monitor(self, monkeypatch):
        called = {}

        async def fake_run_monitor(project_id, nats_url):
            called["args"] = (project_id, nats_url)

        monkeypatch.setattr(crosstalk_cli, "_run_monitor", fake_run_monitor)
        monkeypatch.setattr(sys, "argv", ["crosschat", "monitor", "velhari"])
        crosstalk_cli.main()
        assert called["args"] == ("velhari", crosstalk_cli.DEFAULT_NATS_URL)

    def test_list_dispatches_to_run_list(self, monkeypatch):
        called = {}

        async def fake_run_list(nats_url):
            called["nats_url"] = nats_url

        monkeypatch.setattr(crosstalk_cli, "_run_list", fake_run_list)
        monkeypatch.setattr(sys, "argv", ["crosschat", "list"])
        crosstalk_cli.main()
        assert called == {"nats_url": crosstalk_cli.DEFAULT_NATS_URL}

    def test_remove_dispatches_to_run_remove(self, monkeypatch):
        called = {}

        async def fake_run_remove(project_id, nats_url):
            called["args"] = (project_id, nats_url)

        monkeypatch.setattr(crosstalk_cli, "_run_remove", fake_run_remove)
        monkeypatch.setattr(sys, "argv", ["crosschat", "remove", "velhari"])
        crosstalk_cli.main()
        assert called["args"] == ("velhari", crosstalk_cli.DEFAULT_NATS_URL)


class TestRunHandlersPrintWakeContract:
    """Each _run_* handler must print the exact wake-contract strings the
    skills' SKILL.md docs promise, given fakes for the underlying crosstalk
    calls (connect/jetstream/etc.) — no live NATS involved."""

    @pytest.mark.asyncio
    async def test_run_init_prints_connect_fail(self, monkeypatch, capsys):
        async def fake_connect_with_retry(url):
            raise ConnectionError("nope")

        monkeypatch.setattr(
            crosstalk_cli, "connect_with_retry", fake_connect_with_retry
        )
        await crosstalk_cli._run_init("nats://x")
        assert "NATS_CONNECT_FAIL" in capsys.readouterr().out

    @pytest.mark.asyncio
    async def test_run_init_prints_ok(self, monkeypatch, capsys):
        class _FakeNC:
            def jetstream(self):
                return "js"

            async def drain(self):
                pass

        async def fake_connect_with_retry(url):
            return _FakeNC()

        async def fake_ensure_infra(js):
            assert js == "js"

        monkeypatch.setattr(
            crosstalk_cli, "connect_with_retry", fake_connect_with_retry
        )
        monkeypatch.setattr(crosstalk_cli, "ensure_infra", fake_ensure_infra)
        await crosstalk_cli._run_init("nats://x")
        assert capsys.readouterr().out.strip() == "CROSSCHAT_INIT_OK"

    @pytest.mark.asyncio
    async def test_run_register_prints_registered(self, monkeypatch, capsys):
        class _FakeNC:
            def jetstream(self):
                return "js"

            async def drain(self):
                pass

        async def fake_connect_with_retry(url):
            return _FakeNC()

        async def fake_register_project(js, project_id, project_name, path):
            assert project_id == "velhari"

        monkeypatch.setattr(
            crosstalk_cli, "connect_with_retry", fake_connect_with_retry
        )
        monkeypatch.setattr(crosstalk_cli, "register_project", fake_register_project)
        await crosstalk_cli._run_register(r"C:\Projects\velhari", "nats://x")
        assert capsys.readouterr().out.strip() == "CROSSCHAT_REGISTERED velhari"

    @pytest.mark.asyncio
    async def test_run_register_prints_init_required(self, monkeypatch, capsys):
        from crosschat.core import InfraNotReadyError

        class _FakeNC:
            def jetstream(self):
                return "js"

            async def drain(self):
                pass

        async def fake_connect_with_retry(url):
            return _FakeNC()

        async def fake_register_project(js, project_id, project_name, path):
            raise InfraNotReadyError("run crosschat-init first")

        monkeypatch.setattr(
            crosstalk_cli, "connect_with_retry", fake_connect_with_retry
        )
        monkeypatch.setattr(crosstalk_cli, "register_project", fake_register_project)
        await crosstalk_cli._run_register(r"C:\Projects\velhari", "nats://x")
        assert "CROSSCHAT_INIT_REQUIRED" in capsys.readouterr().out

    @pytest.mark.asyncio
    async def test_run_send_prints_sent(self, monkeypatch, capsys):
        class _FakeNC:
            def jetstream(self):
                return "js"

            async def drain(self):
                pass

        async def fake_connect_with_retry(url):
            return _FakeNC()

        async def fake_send_message(js, dest_project_id, envelope):
            assert dest_project_id == "b"

        monkeypatch.setattr(
            crosstalk_cli, "connect_with_retry", fake_connect_with_retry
        )
        monkeypatch.setattr(crosstalk_cli, "send_message", fake_send_message)
        await crosstalk_cli._run_send("a", "b", "hi", "nats://x")
        assert capsys.readouterr().out.strip() == "CROSSCHAT_SENT b"

    @pytest.mark.asyncio
    async def test_run_send_prints_send_fail(self, monkeypatch, capsys):
        from crosschat.core import DestinationNotRegisteredError

        class _FakeNC:
            def jetstream(self):
                return "js"

            async def drain(self):
                pass

        async def fake_connect_with_retry(url):
            return _FakeNC()

        async def fake_send_message(js, dest_project_id, envelope):
            raise DestinationNotRegisteredError("not registered")

        monkeypatch.setattr(
            crosstalk_cli, "connect_with_retry", fake_connect_with_retry
        )
        monkeypatch.setattr(crosstalk_cli, "send_message", fake_send_message)
        await crosstalk_cli._run_send("a", "ghost", "hi", "nats://x")
        assert "CROSSCHAT_SEND_FAIL" in capsys.readouterr().out

    @pytest.mark.asyncio
    async def test_run_monitor_never_gives_up_connecting(self, monkeypatch):
        # issue #3: a listener must outlast a NATS host that is only asleep.
        seen = {}

        async def fake_connect_with_retry(url, **kwargs):
            seen.update(kwargs)
            raise asyncio.CancelledError()

        monkeypatch.setattr(
            crosstalk_cli, "connect_with_retry", fake_connect_with_retry
        )
        with pytest.raises(asyncio.CancelledError):
            await crosstalk_cli._run_monitor("velhari", "nats://x")
        assert seen["timeout"] is None
        assert {"error_cb", "disconnected_cb", "reconnected_cb"} <= set(seen)

    @pytest.mark.asyncio
    async def test_run_monitor_survives_a_failing_drain(self, monkeypatch):
        class _FakeJS:
            async def key_value(self, bucket):
                return "kv"

        class _FakeNC:
            def jetstream(self):
                return _FakeJS()

            async def drain(self):
                raise RuntimeError("ConnectionReconnectingError")

        async def fake_connect_with_retry(url, **kwargs):
            return _FakeNC()

        async def fake_monitor_loop(js, kv, project_id, emit, on_ready=None):
            pass

        monkeypatch.setattr(
            crosstalk_cli, "connect_with_retry", fake_connect_with_retry
        )
        monkeypatch.setattr(crosstalk_cli, "monitor_loop", fake_monitor_loop)
        await crosstalk_cli._run_monitor("velhari", "nats://x")  # must not raise

    @pytest.mark.asyncio
    async def test_run_monitor_invokes_monitor_loop(self, monkeypatch):
        class _FakeJS:
            async def key_value(self, bucket):
                assert bucket == crosstalk_cli.REGISTRY_BUCKET
                return "kv"

        class _FakeNC:
            def jetstream(self):
                return _FakeJS()

            async def drain(self):
                pass

        called = {}

        async def fake_connect_with_retry(url, **kwargs):
            return _FakeNC()

        async def fake_monitor_loop(js, kv, project_id, emit, on_ready=None):
            called["args"] = (kv, project_id)
            emit("CROSSCHAT_MESSAGE {}")

        monkeypatch.setattr(
            crosstalk_cli, "connect_with_retry", fake_connect_with_retry
        )
        monkeypatch.setattr(crosstalk_cli, "monitor_loop", fake_monitor_loop)
        await crosstalk_cli._run_monitor("velhari", "nats://x")
        assert called["args"] == ("kv", "velhari")

    @pytest.mark.asyncio
    async def test_run_list_prints_connect_fail(self, monkeypatch, capsys):
        async def fake_connect_with_retry(url):
            raise ConnectionError("down")

        monkeypatch.setattr(
            crosstalk_cli, "connect_with_retry", fake_connect_with_retry
        )
        await crosstalk_cli._run_list("nats://x")
        assert "NATS_CONNECT_FAIL" in capsys.readouterr().out

    @pytest.mark.asyncio
    async def test_run_list_prints_no_projects_when_empty(self, monkeypatch, capsys):
        class _FakeJS:
            async def key_value(self, bucket):
                return "kv"

        class _FakeNC:
            def jetstream(self):
                return _FakeJS()

            async def drain(self):
                pass

        async def fake_connect_with_retry(url):
            return _FakeNC()

        async def fake_list_active_projects(kv):
            return []

        monkeypatch.setattr(
            crosstalk_cli, "connect_with_retry", fake_connect_with_retry
        )
        monkeypatch.setattr(
            crosstalk_cli, "list_active_projects", fake_list_active_projects
        )
        await crosstalk_cli._run_list("nats://x")
        assert capsys.readouterr().out.strip() == "CROSSCHAT_NO_PROJECTS"

    @pytest.mark.asyncio
    async def test_run_list_prints_one_line_per_project(self, monkeypatch, capsys):
        class _FakeJS:
            async def key_value(self, bucket):
                return "kv"

        class _FakeNC:
            def jetstream(self):
                return _FakeJS()

            async def drain(self):
                pass

        async def fake_connect_with_retry(url):
            return _FakeNC()

        projects = [
            {"project_id": "velhari", "project_name": "Velhari"},
            {"project_id": "velasari", "project_name": "Velasari"},
        ]

        async def fake_list_active_projects(kv):
            return projects

        monkeypatch.setattr(
            crosstalk_cli, "connect_with_retry", fake_connect_with_retry
        )
        monkeypatch.setattr(
            crosstalk_cli, "list_active_projects", fake_list_active_projects
        )
        await crosstalk_cli._run_list("nats://x")
        lines = capsys.readouterr().out.strip().splitlines()
        assert len(lines) == 2
        assert all(line.startswith("CROSSCHAT_PROJECT ") for line in lines)
        import json

        assert json.loads(lines[0][len("CROSSCHAT_PROJECT ") :]) == projects[0]

    @pytest.mark.asyncio
    async def test_run_remove_prints_connect_fail(self, monkeypatch, capsys):
        async def fake_connect_with_retry(url):
            raise ConnectionError("down")

        monkeypatch.setattr(
            crosstalk_cli, "connect_with_retry", fake_connect_with_retry
        )
        await crosstalk_cli._run_remove("velhari", "nats://x")
        assert "NATS_CONNECT_FAIL" in capsys.readouterr().out

    @pytest.mark.asyncio
    async def test_run_remove_prints_removed(self, monkeypatch, capsys):
        class _FakeJS:
            async def key_value(self, bucket):
                return "kv"

        class _FakeNC:
            def jetstream(self):
                return _FakeJS()

            async def drain(self):
                pass

        called = {}

        async def fake_connect_with_retry(url):
            return _FakeNC()

        async def fake_deregister_project(kv, project_id):
            called["project_id"] = project_id

        monkeypatch.setattr(
            crosstalk_cli, "connect_with_retry", fake_connect_with_retry
        )
        monkeypatch.setattr(
            crosstalk_cli, "deregister_project", fake_deregister_project
        )
        await crosstalk_cli._run_remove("velhari", "nats://x")
        assert called["project_id"] == "velhari"
        assert capsys.readouterr().out.strip() == "CROSSCHAT_REMOVED velhari"
