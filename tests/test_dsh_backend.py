from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest

from agent_control_plane import AdapterControlPlane
from control_plane import ControlPlaneError
from dsh_backend import DshAcpClient, DshBackend, DshRuntime
from resource_leases import ResourceLeaseStore
from zcode_protocol import ProtocolError


HERE = os.path.dirname(os.path.abspath(__file__))
FAKE_DSH = os.path.join(HERE, "fixtures", "fake_dsh_acp.py")
DEFAULT_SESSION = "00000000-0000-4000-8000-000000000001"


def wait_until(predicate, timeout=10.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError("condition was not met within %.1fs" % timeout)


class DshAcpTransportTest(unittest.TestCase):
    def test_malformed_peer_is_reaped_before_disconnect_releases_lease(self):
        disconnected = []
        client = self.make_client(on_disconnect=lambda _error: disconnected.append(client.process.poll()))
        try:
            client.start()
            with self.assertRaises(ProtocolError):
                client.request('test/malformed', timeout=15)
            wait_until(lambda: disconnected, timeout=2)
            self.assertIsNotNone(disconnected[0])
        finally:
            client.close()

    def make_client(self, mode="default", on_event=None, on_disconnect=None):
        return DshAcpClient(
            [sys.executable, FAKE_DSH, mode],
            cwd=HERE,
            on_event=on_event,
            on_disconnect=on_disconnect,
        )

    def test_strict_lf_transport_preserves_u2028(self):
        client = self.make_client()
        try:
            self.assertEqual(1, client.start()["protocolVersion"])
            result = client.request("echo", {"value": "left\u2028right"})
            self.assertEqual("left\u2028right", result["value"])
        finally:
            client.close()

    def test_multiple_concurrent_requests_match_responses_by_id(self):
        client = self.make_client()
        responses = {}
        errors = []
        try:
            client.start()

            def call(index):
                try:
                    responses[index] = client.request("echo", {"index": index})
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)

            threads = [threading.Thread(target=call, args=(index,)) for index in range(12)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(10)
            self.assertFalse(any(thread.is_alive() for thread in threads))
            self.assertEqual([], errors)
            self.assertEqual(set(range(12)), set(responses))
            for index, response in responses.items():
                self.assertEqual(index, response["index"])
        finally:
            client.close()

    def test_reverse_permission_request_is_cancelled_without_leaking_content(self):
        events = []
        client = self.make_client("reverse", on_event=events.append)
        try:
            client.start()
            result = client.request(
                "session/prompt",
                {"sessionId": DEFAULT_SESSION,
                 "prompt": [{"type": "text", "text": "TOP-SECRET prompt"}]},
                timeout=10,
            )
            replies = [event for event in events
                       if event.get("method") == "test/reverse_reply"]
            permission = next(item for item in replies
                              if item["params"]["name"] == "permission")
            reply = permission["params"]["reply"]
            self.assertEqual("cancelled", reply["result"]["outcome"]["outcome"])
            self.assertNotIn("TOP-SECRET", json.dumps(reply, ensure_ascii=False))
            declined = [event for event in events
                        if event.get("method") == "bridge/permission_declined"]
            self.assertEqual(1, len(declined))
            self.assertNotIn("TOP-SECRET", json.dumps(replies, ensure_ascii=False))
            self.assertEqual("end_turn", result["stopReason"])
        finally:
            client.close()

    def test_unknown_reverse_request_returns_method_not_found(self):
        events = []
        client = self.make_client("reverse_unknown", on_event=events.append)
        try:
            client.start()
            client.request(
                "session/prompt",
                {"sessionId": DEFAULT_SESSION,
                 "prompt": [{"type": "text", "text": "hello"}]},
                timeout=10,
            )
            reply = next(item["params"]["reply"] for item in events
                         if item.get("method") == "test/reverse_reply"
                         and item["params"]["name"] == "unknown")
            self.assertEqual(-32601, reply["error"]["code"])
            self.assertIn("Unsupported ACP client request", reply["error"]["message"])
        finally:
            client.close()

    def test_eof_wakes_pending_request_and_reports_disconnect(self):
        disconnects = []
        client = self.make_client(on_disconnect=disconnects.append)
        try:
            client.start()
            started = time.monotonic()
            with self.assertRaises(ProtocolError) as raised:
                client.request("test/exit", timeout=10)
            self.assertLess(time.monotonic() - started, 3)
            self.assertIn("transport closed", str(raised.exception))
            self.assertEqual({}, client._pending)
            self.assertTrue(disconnects)
        finally:
            client.close()

    def test_close_releases_process_and_is_idempotent(self):
        client = self.make_client()
        client.start()
        process = client.process
        client.close()
        self.assertIsNotNone(process.poll())
        self.assertEqual({}, client._pending)
        client.close()
        with self.assertRaises(ProtocolError):
            client.request("echo", {"value": "after-close"}, timeout=1)


class DshBackendTest(unittest.TestCase):
    def test_prompt_error_and_cleanup_error_still_publish_one_terminal(self):
        events = []
        class BrokenClient:
            closed = False
            def request(self, *_args, **_kwargs):
                raise ProtocolError('broken peer')
            def close(self):
                self.closed = True
        runtime = DshRuntime(DshBackend(binary='/fake/dsh'), {}, events.append, lambda _error: None)
        runtime.client = BrokenClient()
        runtime.thread_id = DEFAULT_SESSION
        runtime._prompt('hello')
        terminal = [e for e in events if e['type']=='settled']
        self.assertEqual(1,len(terminal))
        self.assertEqual('failed',terminal[0]['status'])
        self.assertTrue(runtime.client.closed)

    def test_latest_answer_survives_progress_messages_and_long_output(self):
        runtime = DshRuntime(DshBackend(binary='/fake/dsh'), {}, lambda _e: None, lambda _e: None)
        runtime.thread_id = DEFAULT_SESSION
        def update(mid, text):
            runtime._on_event({'method':'session/update','params':{'sessionId':DEFAULT_SESSION,'update':{
                'sessionUpdate':'agent_message_chunk','messageId':mid,'content':{'type':'text','text':text}}}})
        update('progress','x'*15000)
        self.assertTrue(runtime._result.startswith('[Earlier response text truncated]'))
        update('final','FINAL_ANSWER')
        self.assertEqual('FINAL_ANSWER',runtime._result)

    def test_interrupt_before_session_ready_does_not_enqueue_or_send(self):
        runtime = DshRuntime(DshBackend(binary='/fake/dsh'), {}, lambda _e: None, lambda _e: None)
        with self.assertRaises(ControlPlaneError) as caught:
            runtime.guide('new task',interrupt=True)
        self.assertEqual('session_not_ready',caught.exception.code)
        self.assertEqual([],runtime._pending_guide)

    def test_public_control_plane_closes_and_rejects_unsupported_args_before_launch(self):
        backend, _log, clients = self.make_backend()
        control_plane = backend.control_plane()
        try:
            with self.assertRaises(ControlPlaneError) as caught:
                control_plane.start(self.args('hello', workspaceAccess='shared'))
            self.assertEqual('unsupported_capability', caught.exception.code)
            self.assertEqual([], clients)
        finally:
            control_plane.close()

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="dsh-backend-test-")
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.sessions = os.path.join(self.root, "sessions")
        os.makedirs(self.sessions)
        self._counter = 0

    def make_backend(self, mode="default"):
        self._counter += 1
        log = os.path.join(self.root, "prompts-%d.log" % self._counter)
        clients = []

        def factory(command, *, cwd, on_event=None, on_disconnect=None, logger=None):
            client = DshAcpClient(
                [sys.executable, FAKE_DSH, mode, log],
                cwd=cwd,
                on_event=on_event,
                on_disconnect=on_disconnect,
                logger=logger,
            )
            clients.append(client)
            return client

        backend = DshBackend(binary="/fake/dsh", profile="acp",
                             session_dir=self.sessions, client_factory=factory)
        return backend, log, clients

    def control_plane(self, backend):
        self._counter += 1
        lease = ResourceLeaseStore(os.path.join(self.root, "leases-%d.sqlite" % self._counter))
        control_plane = AdapterControlPlane(backend, lease_store=lease)
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

    def track_run(self, control_plane, run_id):
        def cleanup():
            try:
                control_plane.close_run(run_id)
            except Exception:  # noqa: BLE001
                pass

        self.addCleanup(cleanup)

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

    @staticmethod
    def prompt_log(path):
        if not os.path.exists(path):
            return []
        with open(path, encoding="utf-8") as handle:
            return [line.rstrip("\n") for line in handle if line.strip()]

    def test_runtime_completes_with_text_reasoning_tool_and_context_events(self):
        backend, _log, clients = self.make_backend()
        control_plane = self.control_plane(backend)
        run = control_plane.start(self.args("hello"))
        self.track_run(control_plane, run["runId"])

        snapshot = self.wait_terminal(control_plane, run["runId"])
        self.assertEqual("completed", snapshot["status"])
        self.assertEqual("echo:hello done", snapshot["result"])
        self.assertEqual({"used": 42, "window": 1000}, snapshot["context"])
        self.assertEqual(1, snapshot["counts"]["toolCalls"])
        self.assertEqual([], snapshot["activeTools"])

        observed = control_plane.observe(run["runId"], refresh=False, max_events=30)
        kinds = {event["type"] for event in observed["events"]}
        for required in (
            "run.started", "agent.accepted", "agent.prompt-started", "message.updated",
            "reasoning.started", "reasoning.ended", "tool.started", "tool.ended",
            "context.updated", "settled",
        ):
            self.assertIn(required, kinds)
        self.assertTrue(clients)
        self.assertTrue(all(client.process.poll() is not None for client in clients))

    def test_model_is_selected_exactly_without_provider_fallback(self):
        backend, _log, _clients = self.make_backend()
        control_plane = self.control_plane(backend)
        run = control_plane.start(self.args(
            "hello", model={"providerId": "deepseek-account", "modelId": "deepseek-flash"}))
        self.track_run(control_plane, run["runId"])

        snapshot = self.wait_terminal(control_plane, run["runId"])
        self.assertEqual("completed", snapshot["status"])
        self.assertEqual(
            {"providerId": "deepseek-account", "modelId": "deepseek-flash"},
            snapshot["model"]["model"],
        )

    def test_unknown_model_fails_instead_of_falling_back(self):
        backend, _log, _clients = self.make_backend()
        control_plane = self.control_plane(backend)
        run = control_plane.start(self.args(
            "hello", model={"providerId": "unknown-provider", "modelId": "unknown-model"}))
        self.track_run(control_plane, run["runId"])

        snapshot = self.wait_terminal(control_plane, run["runId"])
        self.assertEqual("failed", snapshot["status"])
        self.assertIn("not a unique advertised choice", snapshot["error"])
        self.assertNotIn("deepseek-flash", json.dumps(snapshot["model"]))

    def test_reasoning_effort_is_configured_from_the_live_option_ladder(self):
        backend, _log, _clients = self.make_backend()
        for level in ("off", "low", "high", "max"):
            with self.subTest(level=level):
                runtime = backend.create_runtime(
                    {"cwd": self.root, "thoughtLevel": level},
                    lambda _event: None,
                    lambda _message: None,
                )
                try:
                    state = runtime.start(None)
                    self.assertEqual(
                        {"providerId": "deepseek-account", "modelId": "deepseek-flash"},
                        state["model"]["model"],
                    )
                    option = next(item for item in runtime.options
                                  if item["id"] == "reasoning_effort")
                    self.assertEqual(level, option["currentValue"])
                finally:
                    runtime.close()

    def test_resume_uses_the_existing_thread_id(self):
        backend, _log, _clients = self.make_backend()
        thread_id = "00000000-0000-4000-8000-0000000000aa"
        runtime = backend.create_runtime(
            {"cwd": self.root, "threadId": thread_id},
            lambda _event: None,
            lambda _message: None,
        )
        try:
            state = runtime.start(None)
            self.assertEqual(thread_id, state["threadId"])
            path = os.path.join(self.sessions, thread_id + ".json")
            with open(path, encoding="utf-8") as handle:
                remembered = json.load(handle)
            self.assertEqual(thread_id, remembered["threadId"])
            self.assertEqual(os.path.realpath(self.root), remembered["cwd"])
        finally:
            runtime.close()

    def test_guide_is_queued_until_the_current_prompt_finishes(self):
        backend, log, _clients = self.make_backend("delayed")
        control_plane = self.control_plane(backend)
        run = control_plane.start(self.args("first"))
        self.track_run(control_plane, run["runId"])

        self.wait_result(control_plane, run["runId"], "echo:first")
        control_plane.control(run["runId"], "guide", prompt="second")
        snapshot = self.wait_terminal(control_plane, run["runId"])
        self.assertEqual("completed", snapshot["status"])
        self.assertIn("echo:first", snapshot["result"])
        self.assertIn("echo:second", snapshot["result"])
        self.assertEqual(["first", "second"], self.prompt_log(log))

    def test_interrupt_cancels_current_prompt_then_runs_queued_guidance(self):
        backend, log, _clients = self.make_backend("slow_first")
        control_plane = self.control_plane(backend)
        run = control_plane.start(self.args("first"))
        self.track_run(control_plane, run["runId"])

        self.wait_result(control_plane, run["runId"], "echo:first")
        control_plane.control(run["runId"], "interrupt", prompt="second")
        snapshot = self.wait_terminal(control_plane, run["runId"])
        self.assertEqual("completed", snapshot["status"])
        self.assertIn("echo:second", snapshot["result"])
        self.assertNotIn("echo:first done", snapshot["result"])
        self.assertEqual(1, snapshot["result"].count("done"))
        self.assertEqual(["first", "second"], self.prompt_log(log))

    def test_cancel_settles_the_run_as_cancelled(self):
        backend, _log, _clients = self.make_backend("slow")
        control_plane = self.control_plane(backend)
        run = control_plane.start(self.args("cancel-me"))
        self.track_run(control_plane, run["runId"])

        self.wait_result(control_plane, run["runId"], "echo:cancel-me")
        control_plane.cancel(run["runId"])
        snapshot = self.wait_terminal(control_plane, run["runId"])
        self.assertEqual("cancelled", snapshot["status"])
        self.assertEqual("echo:cancel-me", snapshot["result"])

    def test_shared_workspace_is_rejected(self):
        backend, _log, _clients = self.make_backend()
        control_plane = self.control_plane(backend)
        run = control_plane.start({**self.args("hello"), "workspaceAccess": "shared"})
        self.track_run(control_plane, run["runId"])
        snapshot = self.wait_terminal(control_plane, run["runId"])
        self.assertEqual("failed", snapshot["status"])
        self.assertIn("read-only workspace", snapshot["error"])

    def test_tool_allow_and_deny_lists_are_rejected(self):
        backend, _log, _clients = self.make_backend()
        control_plane = self.control_plane(backend)
        for key in ("toolAllowlist", "toolDenylist"):
            with self.subTest(key=key):
                run = control_plane.start({**self.args("hello"), key: ["bash"]})
                self.track_run(control_plane, run["runId"])
                snapshot = self.wait_terminal(control_plane, run["runId"])
                self.assertEqual("failed", snapshot["status"])
                self.assertIn("allow/deny", snapshot["error"])

    def test_additional_absolute_directory_is_rejected(self):
        backend, _log, _clients = self.make_backend()
        extra = os.path.join(self.root, "extra")
        os.makedirs(extra)
        control_plane = self.control_plane(backend)
        run = control_plane.start({**self.args("hello"),
                                   "resources": [{"key": extra, "mode": "exclusive"}]})
        self.track_run(control_plane, run["runId"])
        snapshot = self.wait_terminal(control_plane, run["runId"])
        self.assertEqual("failed", snapshot["status"])
        self.assertIn("one workspace only", snapshot["error"])

    def test_branch_and_compact_are_explicitly_rejected(self):
        backend, _log, _clients = self.make_backend()
        control_plane = self.control_plane(backend)
        run = control_plane.start(self.args("hello"))
        self.track_run(control_plane, run["runId"])
        self.assertEqual("completed",
                         self.wait_terminal(control_plane, run["runId"])["status"])

        with self.assertRaises(ControlPlaneError) as branch:
            control_plane.branch(run["runId"])
        self.assertEqual("unsupported_capability", branch.exception.code)
        self.assertIn("branching", branch.exception.message)

        with self.assertRaises(ControlPlaneError) as compact:
            control_plane.context(run["runId"], action="compact")
        self.assertEqual("unsupported_capability", compact.exception.code)
        self.assertIn("compaction", compact.exception.message)


if __name__ == "__main__":
    unittest.main()
