from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest

from acp_transport import AcpClient, AcpRequestTimeout
from control_plane import ControlPlaneError
from zcode_protocol import ProtocolError
from opencode_acp_backend import OpenCodeBackend, OpenCodeRuntime
from resource_leases import ResourceLeaseStore


HERE = os.path.dirname(os.path.abspath(__file__))
FAKE_OPENCODE = os.path.join(HERE, "fixtures", "fake_opencode_acp.py")


def wait_until(predicate, timeout=10.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError("condition was not met within %.1fs" % timeout)


class OpenCodeAcpBackendTest(unittest.TestCase):
    def test_same_persistent_session_cannot_run_on_another_policy_peer(self):
        backend, clients, _calls, _polls = self.make_backend()
        cp = self.control_plane(backend)
        first = cp.start(self.args('slow:owner', workspaceAccess='shared'))
        snapshot = self.wait_result(cp,first['runId'],'slow:owner')
        second = cp.start(self.args('other', workspaceAccess='shared',
                                    threadId=snapshot['threadId'],toolDenylist=['read']))
        rejected = self.wait_terminal(cp,second['runId'])
        self.assertEqual('failed',rejected['status'])
        self.assertIn('session is active',rejected['error'])
        self.assertEqual(2,len(clients))
        self.assertNotIn(cp.snapshot(first['runId'])['status'],('failed','completed','cancelled'))
        cp.cancel(first['runId'])
        self.wait_terminal(cp,first['runId'])

    def test_session_startup_timeout_reaps_shared_peer_and_existing_work(self):
        backend, clients, _calls, _polls = self.make_backend()
        cp = self.control_plane(backend)
        first = cp.start(self.args('slow:owner',cwd=self.dir_a))
        self.wait_result(cp,first['runId'],'slow:owner')
        peer = cp._runs[first['runId']].runtime.peer
        request = peer.request
        def timeout_new(method, params, timeout=30):
            if method == 'session/new':
                raise AcpRequestTimeout('session/new timed out')
            return request(method,params,timeout=timeout)
        peer.request = timeout_new
        second = cp.start(self.args('new',cwd=self.dir_b))
        self.assertEqual('failed',self.wait_terminal(cp,second['runId'])['status'])
        self.assertIsNotNone(clients[0].process.poll())
        self.assertEqual('failed',self.wait_terminal(cp,first['runId'])['status'])

    def test_concurrent_close_waits_for_confirmed_shutdown(self):
        backend, _clients, _calls, _polls = self.make_backend()
        self.addCleanup(backend.close)
        events = []
        runtime = OpenCodeRuntime(backend,self.args('slow:close'),events.append,lambda _e:None)
        runtime.start('slow:close')
        wait_until(lambda:any('slow:close' in (e.get('result') or '') for e in events))
        entered, release, second_done = threading.Event(), threading.Event(), threading.Event()
        request = runtime.peer.request
        def delayed_close(method, params, timeout=30):
            if method == 'session/close':
                entered.set();release.wait(3)
            return request(method,params,timeout=timeout)
        runtime.peer.request = delayed_close
        first = threading.Thread(target=runtime.close)
        second = threading.Thread(target=lambda:(runtime.close(),second_done.set()))
        first.start();self.assertTrue(entered.wait(2));second.start()
        try:
            self.assertFalse(second_done.wait(.1))
        finally:
            release.set();first.join(4);second.join(4)
        self.assertTrue(second_done.is_set())

    def test_duplicate_resume_cleanup_cannot_close_the_existing_owner(self):
        backend, clients, _calls, _polls = self.make_backend()
        self.addCleanup(backend.close)
        events = []
        first = OpenCodeRuntime(backend, self.args('slow:first'), events.append, lambda _e: None)
        first.start('slow:first')
        self.addCleanup(first.close)
        wait_until(lambda: any('slow:first' in (e.get('result') or '') for e in events))
        rejected = OpenCodeRuntime(backend, self.args('second', threadId=first.session_id), lambda _e: None, lambda _e: None)
        with self.assertRaises(ControlPlaneError) as caught:
            rejected.start('second')
        self.assertEqual('session_active', caught.exception.code)
        rejected.close()
        self.assertIs(first, first.peer._owners[first.session_id])
        self.assertIsNone(clients[0].process.poll())
        self.assertFalse(any(e.get('type') == 'settled' for e in events))
        first.cancel()
        wait_until(lambda: any(e.get('type') == 'settled' for e in events))

    def test_failed_session_close_reaps_peer_before_releasing_all_owner_leases(self):
        backend, clients, _calls, _polls = self.make_backend()
        cp = self.control_plane(backend)
        runs = [cp.start(self.args('slow:'+directory, cwd=directory)) for directory in (self.dir_a,self.dir_b)]
        for run in runs:
            self.wait_result(cp,run['runId'],'slow:')
        runtime = cp._runs[runs[0]['runId']].runtime
        peer = runtime.peer
        request = peer.request
        def broken_close(method, params, timeout=30):
            if method == 'session/close':
                raise ProtocolError('close unconfirmed')
            return request(method,params,timeout=timeout)
        peer.request = broken_close
        with self.assertRaises(ProtocolError):
            runtime.close()
        self.assertIsNotNone(clients[0].process.poll())
        for run in runs:
            snapshot = self.wait_terminal(cp,run['runId'])
            self.assertEqual('failed',snapshot['status'])
            self.assertFalse(snapshot['resourceLease']['acquired'])

    def test_adopt_session_searches_pages_instead_of_truncating_to_list_limit(self):
        backend = OpenCodeBackend(binary='/fake/opencode')
        calls = []
        class Peer:
            def request(self, method, params):
                calls.append(dict(params))
                if not params.get('cursor'):
                    return {'sessions':[{'sessionId':'new','cwd':'/new'}],'nextCursor':'next'}
                return {'sessions':[{'sessionId':'older','cwd':'/older'}]}
        backend.peer = lambda _args: Peer()
        result = backend.list_sessions({'adoptThreadId':'older','limit':1})
        self.assertEqual('older',result[0]['threadId'])
        self.assertEqual([{}, {'cursor':'next'}],calls)

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="opencode-acp-test-")
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.dir_a = os.path.join(self.root, "a")
        self.dir_b = os.path.join(self.root, "b")
        os.makedirs(self.dir_a)
        os.makedirs(self.dir_b)
        self.lease_db = os.path.join(self.root, "leases.sqlite")
        self.old_lease_db = os.environ.get("AGENT_MCP_LEASE_DB")
        os.environ["AGENT_MCP_LEASE_DB"] = self.lease_db
        self.addCleanup(self.restore_env, "AGENT_MCP_LEASE_DB", self.old_lease_db)

    @staticmethod
    def restore_env(name, value):
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value

    def make_backend(self):
        clients = []
        calls = []
        polls = []

        def factory(command, *, cwd, env=None, client_capabilities=None,
                    request_handler=None, on_event=None, on_disconnect=None,
                    logger=None):
            holder = {}

            def disconnect(error):
                process = holder["client"].process
                holder["poll"] = process.poll() if process else None
                if on_disconnect:
                    on_disconnect(error)

            client = AcpClient(
                [sys.executable, FAKE_OPENCODE],
                cwd=cwd,
                env=env,
                client_capabilities=client_capabilities,
                request_handler=request_handler,
                on_event=on_event,
                on_disconnect=disconnect,
                logger=logger,
            )
            holder["client"] = client
            clients.append(client)
            polls.append(holder)
            calls.append({"command": command, "cwd": cwd, "env": dict(env or {})})
            return client

        backend = OpenCodeBackend(
            binary="/fake/opencode",
            client_factory=factory,
            process_cwd=self.root,
        )
        return backend, clients, calls, polls

    def control_plane(self, backend):
        control_plane = backend.control_plane()
        self.addCleanup(self.close_control_plane, control_plane)
        return control_plane

    @staticmethod
    def close_control_plane(control_plane):
        try:
            control_plane.close()
        except Exception:  # noqa: BLE001
            pass

    def args(self, prompt, **extra):
        value = {"prompt": prompt, "cwd": self.root, "timeout": 30}
        value.update(extra)
        return value

    def wait_terminal(self, control_plane, run_id):
        def done():
            snapshot = control_plane.snapshot(run_id)
            return snapshot if snapshot["status"] in {
                "completed", "failed", "cancelled", "timed_out", "closed"} else None

        return wait_until(done)

    def wait_result(self, control_plane, run_id, fragment):
        def seen():
            snapshot = control_plane.snapshot(run_id)
            return snapshot if fragment in (snapshot.get("result") or "") else None

        return wait_until(seen)

    def test_identical_policy_shares_one_peer_for_concurrent_sessions(self):
        backend, clients, _calls, _polls = self.make_backend()
        control_plane = self.control_plane(backend)
        first = control_plane.start(self.args("delay:0.4:alpha", cwd=self.dir_a))
        second = control_plane.start(self.args("delay:0.4:beta", cwd=self.dir_b))
        one = self.wait_terminal(control_plane, first["runId"])
        two = self.wait_terminal(control_plane, second["runId"])

        self.assertEqual("completed", one["status"])
        self.assertEqual("completed", two["status"])
        self.assertEqual(1, len(clients))
        self.assertNotEqual(one["threadId"], two["threadId"])
        short_one, short_two = one["threadId"][:8], two["threadId"][:8]
        self.assertIn("S[%s] delay:0.4:alpha" % short_one, one["result"])
        self.assertIn("S[%s] delay:0.4:beta" % short_two, two["result"])
        self.assertNotIn(short_two, one["result"])
        self.assertNotIn(short_one, two["result"])

    def test_different_policy_variants_use_separate_peers(self):
        backend, clients, _calls, _polls = self.make_backend()
        self.addCleanup(backend.close)
        shared = backend.peer({"workspaceAccess": "shared"})
        denied = backend.peer({"workspaceAccess": "exclusive",
                               "toolDenylist": ["bash"]})
        shared_again = backend.peer({"workspaceAccess": "shared"})
        self.assertIs(shared, shared_again)
        self.assertIsNot(shared, denied)
        self.assertEqual(2, len(clients))

    def test_closing_one_run_does_not_kill_another_run(self):
        backend, clients, _calls, _polls = self.make_backend()
        control_plane = self.control_plane(backend)
        slow = control_plane.start(self.args("slow:first", cwd=self.dir_a))
        other = control_plane.start(self.args("delay:0.5:second", cwd=self.dir_b))
        self.wait_result(control_plane, slow["runId"], "slow:first")

        control_plane.close_run(slow["runId"])
        snapshot = self.wait_terminal(control_plane, other["runId"])
        self.assertEqual("closed", control_plane.snapshot(slow["runId"])["status"])
        self.assertEqual("completed", snapshot["status"])
        self.assertIn("delay:0.5:second", snapshot["result"])
        self.assertEqual(1, len(clients))
        self.assertIsNone(clients[0].process.poll())

    def test_resumed_session_replaces_old_owner_listener(self):
        backend, _clients, _calls, _polls = self.make_backend()
        control_plane = self.control_plane(backend)
        first = control_plane.start(self.args("hello"))
        one = self.wait_terminal(control_plane, first["runId"])
        runtime_one = control_plane._get(first["runId"]).runtime
        self.assertTrue(runtime_one._closed)

        peer = next(iter(backend._peers.values()))
        # A late update for the closed session must not be buffered for the
        # next owner that reuses the same native session ID.
        peer.request("test/emit", {
            "sessionId": one["threadId"],
            "update": {"sessionUpdate": "agent_message_chunk",
                       "content": {"type": "text", "text": "STALE-AFTER-CLOSE"}},
        })

        second = control_plane.start(self.args("slow:again", threadId=one["threadId"]))
        two = self.wait_result(control_plane, second["runId"], "slow:again")
        runtime_two = control_plane._get(second["runId"]).runtime
        self.assertEqual(one["threadId"], two["threadId"])
        self.assertIn("S[%s] slow:again" % one["threadId"][:8], two["result"])
        self.assertNotIn("STALE-AFTER-CLOSE", two["result"])
        self.assertIn("hello", runtime_one._result)
        self.assertIs(peer._owners[one["threadId"]], runtime_two)
        self.assertNotIn(runtime_one, peer._owners.values())

        control_plane.cancel(second["runId"])
        settled = self.wait_terminal(control_plane, second["runId"])
        self.assertEqual("cancelled", settled["status"])

    def test_exact_model_value_preserves_provider_slash(self):
        backend, _clients, _calls, _polls = self.make_backend()
        control_plane = self.control_plane(backend)
        run = control_plane.start(self.args(
            "hello", model={"providerId": "acme/proxy", "modelId": "model-x"}))
        snapshot = self.wait_terminal(control_plane, run["runId"])
        self.assertEqual("completed", snapshot["status"])
        self.assertEqual({"providerId": "acme/proxy", "modelId": "model-x"},
                         snapshot["model"]["model"])

    def test_unknown_model_fails_without_fallback(self):
        backend, _clients, _calls, _polls = self.make_backend()
        control_plane = self.control_plane(backend)
        run = control_plane.start(self.args(
            "hello", model={"providerId": "unknown", "modelId": "missing"}))
        snapshot = self.wait_terminal(control_plane, run["runId"])
        self.assertEqual("failed", snapshot["status"])
        self.assertIn("not a unique advertised choice", snapshot["error"])
        self.assertNotIn("deepseek-v4-flash", json.dumps(snapshot["model"]))

    def test_effort_levels_use_advertised_thought_level(self):
        backend, _clients, _calls, _polls = self.make_backend()
        self.addCleanup(backend.close)
        for level in ("low", "none", "default"):
            with self.subTest(level=level):
                runtime = backend.create_runtime(
                    {"cwd": self.root, "thoughtLevel": level},
                    lambda _event: None,
                    lambda _message: None,
                )
                state = runtime.start(None)
                self.assertTrue(state["threadId"])
                option = next(item for item in runtime.options
                              if item["id"] == "thought_level")
                self.assertEqual(level, option["currentValue"])

    def test_guide_is_queued_until_the_current_prompt_finishes(self):
        backend, _clients, _calls, _polls = self.make_backend()
        control_plane = self.control_plane(backend)
        run = control_plane.start(self.args("hold:first"))
        self.wait_result(control_plane, run["runId"], "hold:first")
        control_plane.control(run["runId"], "guide", prompt="second")

        snapshot = self.wait_terminal(control_plane, run["runId"])
        self.assertEqual("completed", snapshot["status"])
        self.assertLess(snapshot["result"].index("hold:first"),
                        snapshot["result"].index("second"))
        self.assertEqual(2, snapshot["result"].count("done"))

    def test_interrupt_cancels_then_runs_queued_guidance(self):
        backend, _clients, _calls, _polls = self.make_backend()
        control_plane = self.control_plane(backend)
        run = control_plane.start(self.args("slow:first"))
        self.wait_result(control_plane, run["runId"], "slow:first")
        control_plane.control(run["runId"], "interrupt", prompt="second")

        snapshot = self.wait_terminal(control_plane, run["runId"])
        self.assertEqual("completed", snapshot["status"])
        self.assertIn("second done", snapshot["result"])
        self.assertEqual(1, snapshot["result"].count("done"))

    def test_cancel_settles_the_run_as_cancelled(self):
        backend, _clients, _calls, _polls = self.make_backend()
        control_plane = self.control_plane(backend)
        run = control_plane.start(self.args("slow:cancel-me"))
        self.wait_result(control_plane, run["runId"], "slow:cancel-me")
        control_plane.cancel(run["runId"])

        snapshot = self.wait_terminal(control_plane, run["runId"])
        self.assertEqual("cancelled", snapshot["status"])
        self.assertIn("slow:cancel-me", snapshot["result"])

    def test_usage_accumulates_across_prompts_and_cost_is_context_only(self):
        backend, _clients, _calls, _polls = self.make_backend()
        control_plane = self.control_plane(backend)
        run = control_plane.start(self.args("hold:one"))
        self.wait_result(control_plane, run["runId"], "hold:one")
        control_plane.control(run["runId"], "guide", prompt="two")

        snapshot = self.wait_terminal(control_plane, run["runId"])
        self.assertEqual("completed", snapshot["status"])
        self.assertEqual(30, snapshot["usage"]["inputTokens"])
        self.assertEqual(12, snapshot["usage"]["outputTokens"])
        self.assertEqual(42, snapshot["usage"]["totalTokens"])
        self.assertEqual(6, snapshot["usage"]["reasoningTokens"])
        self.assertEqual(4, snapshot["usage"]["cacheReadTokens"])
        self.assertNotIn("cost", snapshot["usage"])
        self.assertNotIn("sessionCost", snapshot["usage"])
        self.assertEqual(77, snapshot["context"]["used"])
        self.assertEqual(1000, snapshot["context"]["window"])
        self.assertEqual(2.5, snapshot["context"]["sessionCost"])

        observed = control_plane.observe(run["runId"], refresh=False, max_events=30)
        updates = [event for event in observed["events"]
                   if event["type"] == "usage.updated"]
        self.assertGreaterEqual(len(updates), 2)

    def test_child_session_messages_do_not_overwrite_root_result(self):
        backend, _clients, _calls, _polls = self.make_backend()
        control_plane = self.control_plane(backend)
        run = control_plane.start(self.args("child"))
        snapshot = self.wait_terminal(control_plane, run["runId"])
        self.assertEqual("completed", snapshot["status"])
        self.assertIn("child done", snapshot["result"])
        self.assertNotIn("CHILD-SESSION-TEXT", snapshot["result"])
        self.assertNotIn("GRANDCHILD-SESSION-TEXT", snapshot["result"])
        observed = control_plane.observe(run["runId"], refresh=False, max_events=30)
        children = [event for event in observed["events"]
                    if event["type"] == "native.child-session"]
        self.assertEqual(2, len(children))

    def test_list_and_resume_work_across_control_planes(self):
        backend_one, _clients, _calls, _polls = self.make_backend()
        control_one = self.control_plane(backend_one)
        run = control_one.start(self.args("persist-me"))
        first = self.wait_terminal(control_one, run["runId"])
        self.assertEqual("completed", first["status"])
        control_one.close()

        backend_two, _clients_two, _calls_two, _polls_two = self.make_backend()
        control_two = self.control_plane(backend_two)
        sessions = backend_two.list_sessions({"cwd": self.root})
        self.assertIn(first["threadId"],
                      {item["threadId"] for item in sessions})
        resumed = control_two.start(self.args("resumed", threadId=first["threadId"]))
        second = self.wait_terminal(control_two, resumed["runId"])
        self.assertEqual("completed", second["status"])
        self.assertEqual(first["threadId"], second["threadId"])
        self.assertIn("resumed", second["result"])

    def test_fork_uses_latest_checkpoint_and_rejects_anchors(self):
        backend, _clients, _calls, _polls = self.make_backend()
        control_plane = self.control_plane(backend)
        run = control_plane.start(self.args("hello"))
        parent = self.wait_terminal(control_plane, run["runId"])
        child = control_plane.branch(run["runId"])
        self.assertEqual(parent["threadId"], child["parentThreadId"])
        self.assertNotEqual(parent["threadId"], child["threadId"])

        for kwargs in (
            {"target_kind": "message", "target_id": "message-1"},
            {"target_kind": "latestCheckpoint", "target_id": "checkpoint-1"},
            {"target_kind": "latestCheckpoint", "turn_index": 0},
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ControlPlaneError) as caught:
                    control_plane.branch(run["runId"], **kwargs)
                self.assertEqual("unsupported_capability", caught.exception.code)

    def test_compact_reacquires_lease_and_reports_resource_busy(self):
        backend, _clients, _calls, _polls = self.make_backend()
        control_plane = self.control_plane(backend)
        run = control_plane.start(self.args("compact-me"))
        self.wait_terminal(control_plane, run["runId"])

        compacted = control_plane.context(run["runId"], action="compact")
        self.assertTrue(compacted["compacted"])

        lease = ResourceLeaseStore(self.lease_db)
        acquired = lease.try_acquire(
            "other-task", {os.path.realpath(self.root): "exclusive"})
        self.assertTrue(acquired["acquired"])
        try:
            with self.assertRaises(ControlPlaneError) as caught:
                control_plane.context(run["runId"], action="compact")
            self.assertEqual("resource_busy", caught.exception.code)
        finally:
            lease.release("other-task")
            lease.close()

    def test_eof_reaps_peer_before_failing_every_owner(self):
        backend, clients, _calls, polls = self.make_backend()
        self.addCleanup(backend.close)
        runtimes = []
        events = []
        disconnects = []

        for directory in (self.dir_a, self.dir_b):
            runtime_events = []
            events.append(runtime_events)

            def disconnect(error, seen=disconnects):
                seen.append(error)

            runtime = backend.create_runtime(
                {"cwd": directory, "timeout": 30},
                runtime_events.append,
                disconnect,
            )
            runtime.start("slow:eof-" + os.path.basename(directory))
            runtimes.append(runtime)

        wait_until(lambda: all(
            any("slow:eof-" in (event.get("result") or "")
                for event in runtime_events)
            for runtime_events in events))
        client = runtimes[0].peer.client
        client.notify("test/exit", {})
        wait_until(lambda: len(disconnects) == 2)
        self.assertTrue(all(holder["poll"] is not None for holder in polls))
        wait_until(lambda: all(
            any(event.get("type") == "settled" for event in runtime_events)
            for runtime_events in events))
        for runtime_events in events:
            settled = [event for event in runtime_events
                       if event.get("type") == "settled"]
            self.assertEqual("failed", settled[-1]["status"])
            self.assertIn("transport closed", settled[-1]["error"])
        self.assertIsNotNone(clients[0].process.poll())
        self.assertEqual(1, len(clients))

    def test_shared_and_denylist_policy_env_is_hard_deny_structure(self):
        backend, _clients, calls, _polls = self.make_backend()
        self.addCleanup(backend.close)
        old_config = os.environ.get("OPENCODE_CONFIG_CONTENT")
        os.environ["OPENCODE_CONFIG_CONTENT"] = json.dumps(
            {"policies": [], "permissions": [], "custom": {"kept": True}})
        try:
            backend.peer({"workspaceAccess": "shared"})
            backend.peer({"workspaceAccess": "exclusive",
                          "toolDenylist": ["bash", "patch"]})
        finally:
            self.restore_env("OPENCODE_CONFIG_CONTENT", old_config)

        shared = json.loads(calls[0]["env"]["OPENCODE_CONFIG_CONTENT"])
        denied = json.loads(calls[1]["env"]["OPENCODE_CONFIG_CONTENT"])
        shared_rules = {(item["action"], item["resource"], item["effect"])
                        for item in shared["policies"]}
        denied_rules = {(item["action"], item["resource"], item["effect"])
                        for item in denied["policies"]}
        for rule in (
            ("tool.use", "edit:*", "deny"),
            ("tool.use", "shell:*", "deny"),
            ("tool.use", "subagent:*", "deny"),
            ("tool.use", "*", "deny"),
            ("tool.use", "read:*", "allow"),
            ("tool.use", "glob:*", "allow"),
            ("tool.use", "grep:*", "allow"),
        ):
            self.assertIn(rule, shared_rules)
        # Caller restrictions stay in force after the broad shared deny/allow block.
        self.assertEqual(2, sum(
            1 for item in shared["policies"]
            if item == {"action": "tool.use", "resource": "edit:*", "effect": "deny"}))
        self.assertEqual(2, sum(
            1 for item in shared["policies"]
            if item == {"action": "tool.use", "resource": "shell:*", "effect": "deny"}))
        self.assertEqual({
            ("tool.use", "edit:*", "deny"),
            ("tool.use", "shell:*", "deny"),
        }, denied_rules)
        for config in (shared, denied):
            self.assertTrue(config["custom"]["kept"])
            self.assertIn({"action": "edit", "resource": "*", "effect": "ask"},
                          config["permissions"])

    def test_allowlist_unknown_denylist_and_mixed_resources_are_rejected(self):
        backend = OpenCodeBackend(binary="/fake/opencode")
        with self.assertRaises(ControlPlaneError) as allowlist:
            backend.validate({"toolAllowlist": ["bash"]})
        self.assertEqual("unsupported_capability", allowlist.exception.code)
        with self.assertRaises(ControlPlaneError) as unknown:
            backend.validate({"toolDenylist": ["not-a-real-action"]})
        self.assertEqual("unsupported_capability", unknown.exception.code)
        backend.validate({"toolDenylist": ["bash", "patch"]})
        extra = os.path.join(self.root, "extra")
        os.makedirs(extra)
        with self.assertRaises(ControlPlaneError) as mixed:
            backend.validate({
                "workspaceAccess": "exclusive",
                "resources": [{"key": extra, "mode": "shared"}],
            })
        self.assertEqual("unsupported_capability", mixed.exception.code)

    def test_permission_paths_are_structural_not_titles(self):
        backend = OpenCodeBackend(binary="/fake/opencode")
        runtime = OpenCodeRuntime(backend, {"cwd": self.root},
                                  lambda _event: None, lambda _error: None)
        inside = os.path.join(self.root, "inside.txt")

        self.assertFalse(runtime.allow_permission({"title": "/etc/passwd"}))
        self.assertTrue(runtime.allow_permission({
            "title": "/etc/passwd", "locations": [{"path": inside}]}))
        self.assertTrue(runtime.allow_permission({
            "title": "ignored", "rawInput": {"path": "inside.txt"}}))
        self.assertFalse(runtime.allow_permission({
            "rawInput": {"filePath": "../outside.txt"}}))
        self.assertTrue(runtime.allow_permission({
            "content": [{"type": "diff", "path": inside}]}))
        self.assertTrue(runtime.allow_permission({
            "rawInput": {"path": inside}}))
        self.assertFalse(runtime.allow_permission({
            "title": "./ignored-title-only"}))

        shared = OpenCodeRuntime(
            backend, {"cwd": self.root, "workspaceAccess": "shared"},
            lambda _event: None, lambda _error: None)
        self.assertFalse(shared.allow_permission({
            "name": "read", "kind": "edit", "locations": [{"path": inside}]}))
        denied = OpenCodeRuntime(
            backend, {"cwd": self.root, "toolDenylist": ["bash"]},
            lambda _event: None, lambda _error: None)
        self.assertFalse(denied.allow_permission({
            "name": "bash", "locations": [{"path": inside}]}))


if __name__ == "__main__":
    unittest.main()
