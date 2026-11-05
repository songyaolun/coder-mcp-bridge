from __future__ import annotations

import json
import os
import sys
import unittest

from acp_transport import AcpClient

HERE = os.path.dirname(os.path.abspath(__file__))
FAKE_DSH = os.path.join(HERE, "fixtures", "fake_dsh_acp.py")
DEFAULT_SESSION = "00000000-0000-4000-8000-000000000001"

INIT_FAKE = r'''
import json
import os
import sys

def send(message):
    sys.stdout.write(json.dumps(message) + "\n")
    sys.stdout.flush()

for raw in sys.stdin:
    message = json.loads(raw)
    if message.get("method") == "initialize":
        params = message.get("params") or {}
        send({"jsonrpc": "2.0", "id": message["id"], "result": {
            "protocolVersion": 1,
            "clientCapabilities": params.get("clientCapabilities"),
            "environment": os.environ.get("ACP_TRANSPORT_TEST_ENV"),
        }})
    elif "id" in message:
        send({"jsonrpc": "2.0", "id": message["id"], "result": {}})
'''


class AcpTransportOptionsTest(unittest.TestCase):
    def make_inline_client(self, **kwargs):
        return AcpClient([sys.executable, "-c", INIT_FAKE], cwd=HERE, **kwargs)

    def test_environment_is_passed_to_the_child_process(self):
        client = self.make_inline_client(
            env={**os.environ, "ACP_TRANSPORT_TEST_ENV": "env-visible"})
        try:
            result = client.start()
            self.assertEqual("env-visible", result["environment"])
        finally:
            client.close()

    def test_initialize_uses_supplied_client_capabilities(self):
        capabilities = {"fs": {"readTextFile": True}, "terminal": False}
        client = self.make_inline_client(client_capabilities=capabilities)
        try:
            result = client.start()
            self.assertEqual(capabilities, result["clientCapabilities"])
        finally:
            client.close()

    def test_initialize_defaults_to_empty_client_capabilities(self):
        client = self.make_inline_client()
        try:
            result = client.start()
            self.assertEqual({}, result["clientCapabilities"])
        finally:
            client.close()


class AcpTransportReverseHandlerTest(unittest.TestCase):
    def make_client(self, mode, request_handler, on_event=None):
        return AcpClient(
            [sys.executable, FAKE_DSH, mode],
            cwd=HERE,
            on_event=on_event,
            request_handler=request_handler,
        )

    @staticmethod
    def prompt(client):
        return client.request(
            "session/prompt",
            {"sessionId": DEFAULT_SESSION,
             "prompt": [{"type": "text", "text": "handler test"}]},
            timeout=10,
        )

    @staticmethod
    def replies(events):
        return {
            event["params"]["name"]: event["params"]["reply"]
            for event in events
            if event.get("method") == "test/reverse_reply"
        }

    def test_custom_handler_answers_every_reverse_request(self):
        events = []
        calls = []

        def handler(method, params):
            calls.append((method, dict(params)))
            if method == "session/request_permission":
                return {"outcome": {"outcome": "selected", "optionId": "allow"}}
            return {"accepted": True}

        client = self.make_client("reverse", handler, events.append)
        try:
            client.start()
            self.assertEqual("end_turn", self.prompt(client)["stopReason"])
            replies = self.replies(events)
            self.assertEqual(
                {"outcome": {"outcome": "selected", "optionId": "allow"}},
                replies["permission"]["result"],
            )
            self.assertEqual({"accepted": True}, replies["unknown"]["result"])
            self.assertEqual(
                ["session/request_permission", "session/unknown_reverse"],
                [method for method, _params in calls],
            )
            self.assertNotIn(
                "bridge/permission_declined",
                [event.get("method") for event in events],
            )
        finally:
            client.close()

    def test_handler_not_implemented_returns_method_not_found(self):
        def handler(method, _params):
            raise NotImplementedError(method)

        events = []
        client = self.make_client("reverse_unknown", handler, events.append)
        try:
            client.start()
            self.prompt(client)
            reply = self.replies(events)["unknown"]
            self.assertEqual(-32601, reply["error"]["code"])
            self.assertIn("Unsupported ACP client request", reply["error"]["message"])
        finally:
            client.close()

    def test_handler_exception_returns_internal_error_without_echoing_params(self):
        def handler(_method, params):
            raise RuntimeError("failed for " + str(params.get("secret")))

        events = []
        client = self.make_client("reverse_unknown", handler, events.append)
        try:
            client.start()
            self.prompt(client)
            reply = self.replies(events)["unknown"]
            self.assertEqual(-32603, reply["error"]["code"])
            self.assertNotIn("TOP-SECRET", json.dumps(reply, ensure_ascii=False))
        finally:
            client.close()


if __name__ == "__main__":
    unittest.main()
