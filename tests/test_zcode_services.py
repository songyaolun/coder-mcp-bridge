"""Services transport contracts, without account credentials or model calls."""

import io
import threading

import pytest

from zcode_protocol import ProtocolError
from zcode_services_rpc import decode, encode, _exact
from zcode_services_protocol import ZCodeServicesProtocolClient, create_zcode_protocol


class FakeRPC:
    def __init__(self):
        self.calls = []
        self.listeners = {}
        self.disposed = []
        self.closed = False

    def call(self, channel, method, params=None, timeout=30):
        self.calls.append((channel, method, params))
        if method == "helloConversationV4":
            return {"clientMode": "desktop-continuous"}
        if method in {"createSession", "resumeSession", "readSession"}:
            return {"session": {"sessionId": "session-1"}, "settings": {}, "messages": []}
        if method in {"readSessionEvents", "listSessions"}:
            return []
        if method == "sendConversationCommandV4":
            return {"status": "accepted"}
        if method == "subscribeConversationV4":
            return {"ack": {"subscriptionId": "subscription-1"}}
        return {}

    def listen(self, channel, method, params, callback):
        key = len(self.listeners) + 1
        self.listeners[key] = callback
        return key

    def unlisten(self, key):
        self.disposed.append(key)

    def close(self):
        self.closed = True


@pytest.fixture
def client():
    rpc = FakeRPC()
    instance = ZCodeServicesProtocolClient(rpc=rpc)
    yield instance
    instance.close()


def test_channel_wire_golden_and_utf8():
    # Official ChannelClient header uses Array, VLQ Int and UTF-8 String tags.
    golden = b'\x04\x04\x06d\x06\x01\x01\x0bzcode-agent\x01\x0breadSession'
    assert encode([100, 1, "zcode-agent", "readSession"]) == golden
    assert decode(io.BytesIO(golden)) == [100, 1, "zcode-agent", "readSession"]
    assert decode(io.BytesIO(b'\x01\x06\xe4\xb8\xad\xe6\x96\x87')) == "中文"


def test_fragmented_reads_and_invalid_codec_fail_closed():
    class Fragmented(io.BytesIO):
        def read(self, size=-1):
            return super().read(min(size, 1))
    assert _exact(Fragmented(b"abcdef"), 6) == b"abcdef"
    with pytest.raises(EOFError):
        _exact(Fragmented(b"abc"), 4)
    with pytest.raises(ProtocolError):
        decode(io.BytesIO(b"\xff"))
    with pytest.raises(ProtocolError):
        decode(io.BytesIO(b"\x06\x80\x80\x80\x80\x80"))


def test_create_uses_official_account_and_has_no_personal_provider_mutation(client):
    selection = {"providerId": "account:bigmodel-individual-coding-plan",
                 "modelId": "GLM-5.3-Flash", "options": {"reasoningLevel": "low"}}
    result = client.request("session/create", {
        "workspace": {"workspacePath": "/workspace"}, "model": selection,
        "thoughtLevel": "low", "toolDenylist": ["Bash"],
    })
    assert result["session"]["sessionId"] == "session-1"
    _, method, params = client.rpc.calls[-1]
    assert method == "createSession"
    assert params["workspacePath"] == params["workspaceKey"] == "/workspace"
    assert params["model"] == selection
    assert params["toolDenylist"] == ["Bash"]
    assert "workspace" not in params
    assert all(name != "provider/updateAccountConfig" for _, name, _ in client.rpc.calls)


def test_unknown_session_never_falls_back_to_another_workspace(client):
    with pytest.raises(ProtocolError, match="explicit workspace"):
        client.request("session/read", {"sessionId": "unknown"})


def test_reasoning_selection_is_complete_without_control_plane_enrichment(client):
    selection = {"providerId": "account:bigmodel-individual-coding-plan",
                 "modelId": "GLM-5.3-Flash"}
    client.request("session/create", {
        "workspace": {"workspacePath": "/workspace"},
        "model": selection, "thoughtLevel": "low",
    })
    assert client.rpc.calls[-1][2]["model"]["options"] == {"reasoningLevel": "low"}
    client.request("session/setModel", {"sessionId": "session-1", "model": selection})
    assert client.rpc.calls[-1][2]["model"]["options"] == {"reasoningLevel": "low"}
    assert "options" not in selection


def test_subscribe_replay_replaces_old_subscription(client):
    client.request("session/create", {"workspace": {"workspacePath": "/workspace"}})
    client.request("session/subscribe", {"sessionId": "session-1", "afterSeq": 12})
    client.request("session/subscribe", {"sessionId": "session-1", "afterSeq": 15})
    assert client.rpc.disposed == [1]
    replay = [params for _, method, params in client.rpc.calls if method == "readSessionEvents"]
    assert [params["afterSeq"] for params in replay] == [12, 15]


def test_permission_policy_is_preserved_and_defaults_to_deny(client):
    client._workspaces["session-1"] = "/workspace"
    client.on_server_request = lambda *_: {"decision": "deny"}
    client._interaction("session-1", {"type": "permission.request", "request": {
        "sessionId": "session-1", "requestId": "request-1", "toolName": "Bash",
    }})
    params = client.rpc.calls[-1][2]
    assert params["envelope"]["type"] == "resolveInteraction"
    assert params["envelope"]["payload"] == {
        "interactionId": "request-1", "answer": {"optionId": "deny"},
    }


def test_allow_uses_official_v4_allow_once_option(client):
    client._workspaces["session-1"] = "/workspace"
    client.on_server_request = lambda *_: {"decision": "allow"}
    client._interaction("session-1", {"type": "permission.request", "request": {
        "sessionId": "session-1", "requestId": "request-1",
    }})
    assert client.rpc.calls[-1][2]["envelope"]["payload"]["answer"] == {"optionId": "allowOnce"}


def test_message_tail_uses_compatible_snapshot_facade(client):
    client._workspaces["session-1"] = "/workspace"
    result = client.request("session/messages", {"sessionId": "session-1", "limit": 6})
    assert "messages" in result
    assert client.rpc.calls[-1][1] == "readSession"
    assert client.rpc.calls[-1][2]["messageLimit"] == 6


def test_services_capabilities_do_not_claim_checkpoint_branching(monkeypatch):
    from backend_manager import ZCodeBackend
    monkeypatch.setenv("ZCODE_PROTOCOL_TRANSPORT", "services")
    backend = ZCodeBackend(lambda: ("/node", "/agent"))
    assert backend.capabilities["branch"] is False
    assert backend.probe()["version"] == "services-runtime"
    monkeypatch.setenv("ZCODE_PROTOCOL_TRANSPORT", "direct")
    assert ZCodeBackend(lambda: ("/node", "/agent")).capabilities["branch"] is True


def test_notifications_arrive_in_order_without_blocking_rpc_reader(client):
    seen = []
    done = threading.Event()
    def on_notification(message):
        seen.append(message)
        if len(seen) == 2:
            done.set()
    client.on_notification = on_notification
    client._enqueue_event("session-1", {"type": "session.event", "event": {
        "sessionId": "session-1", "seq": 1,
    }})
    client._enqueue_event("session-1", {"type": "state.updated", "notification": {
        "sessionId": "session-1", "revision": 2,
    }})
    assert done.wait(2)
    assert [event["method"] for event in seen] == ["session/event", "state.updated"]


def test_unrecognized_transport_fails_instead_of_silent_fallback(monkeypatch):
    monkeypatch.setenv("ZCODE_PROTOCOL_TRANSPORT", "invalid")
    with pytest.raises(ProtocolError, match="Unknown"):
        create_zcode_protocol()
