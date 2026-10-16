"""Bridge session facade backed by official ZCode account-aware Services RPC.

The Services Server owns account selection, credential refresh and model request
authentication. This adapter never reconstructs account providers or keys.
"""

from __future__ import annotations

import os
import queue
import threading
import time
import uuid

from zcode_protocol import ProtocolError
from zcode_services_rpc import ServicesRPC


class ZCodeServicesProtocolClient:
    requires_workspace_on_resume = True
    METHODS = {
        "session/create": "createSession", "session/resume": "resumeSession",
        "session/read": "readSession", "session/setModel": "setModel",
        "session/setThoughtLevel": "setThoughtLevel", "session/setMode": "setMode",
        "session/send": "sendPrompt", "session/usage": "getTaskTokenUsage",
        "session/subagents": "listSessionSubagents", "session/compact": "compactSession",
        "session/goal": "goalSession", "session/events": "readSessionEvents",
    }

    def __init__(self, _binary=None, _bundle=None, *, on_notification=None,
                 on_disconnect=None, on_server_request=None, logger=None, rpc=None):
        self.on_notification = on_notification
        self.on_disconnect = on_disconnect
        self.on_server_request = on_server_request
        self.logger = logger or (lambda _message: None)
        runtime = os.environ.get("ZCODE_SERVICES_RUNTIME", "")
        node = os.environ.get("ZCODE_SERVICES_NODE", "")
        if rpc is None:
            for path in (runtime, node):
                if not os.path.isabs(path):
                    raise ProtocolError("Services transport requires absolute ZCODE_SERVICES_RUNTIME and ZCODE_SERVICES_NODE")
            for path in (node, os.path.join(runtime, "server", "remote", "zcode-server.cjs"),
                         os.path.join(runtime, "agent", "zcode.cjs")):
                if not os.path.isfile(path):
                    raise ProtocolError("ZCode Services runtime file is missing")
        self.rpc = rpc or ServicesRPC(node, runtime, self._disconnected)
        self._lock = threading.RLock()
        self._initialized = False
        self._closed = False
        self._workspaces = {}
        self._thought_levels = {}
        self._subscriptions = {}
        self._telemetry = {}
        self._v4_subscriptions = {}
        self._events = queue.Queue(maxsize=4096)
        self._worker = threading.Thread(target=self._consume_events, daemon=True)
        self._worker.start()

    def _initialize(self):
        with self._lock:
            if self._closed:
                raise ProtocolError("Services protocol is closed")
            if self._initialized:
                return
            hello = self.rpc.call("zcode-agent", "helloConversationV4")
            if hello.get("clientMode") != "desktop-continuous":
                raise ProtocolError("Services transport requires desktop-continuous mode")
            self.rpc.call("zcode-agent", "initializeConversationV4", {
                "kind": "clientHello", "protocolVersion": 3,
                "clientId": "coder-mcp-bridge", "appVersion": "0.1.0",
            })
            self.rpc.call("zcode-agent", "syncAppRuntimePreferences", {
                "askUserQuestionAutoResolutionEnabled": False,
            })
            self._initialized = True

    @property
    def process_id(self):
        return self.rpc.process_id

    def _target(self, params):
        workspace = params.get("workspace")
        session_id = params.get("sessionId")
        if isinstance(workspace, dict):
            path = workspace.get("workspacePath")
        elif session_id:
            path = self._workspaces.get(session_id)
        else:
            path = None
        if not path:
            raise ProtocolError("Services session requires an explicit workspace or a known session")
        return {"workspacePath": path, "workspaceKey": path}

    def request(self, method, params=None, timeout=30):
        self._initialize()
        params = dict(params or {})
        if method == "session/list":
            if not params.get("workspace"):
                raise ProtocolError("Services session recovery requires workspace")
            target = self._target(params)
            result = self.rpc.call("zcode-agent", "listSessions", {
                **target, **{k: v for k, v in params.items() if k != "workspace"},
            }, timeout=timeout)
            for item in result:
                self._workspaces[item["sessionId"]] = target["workspacePath"]
            return {"sessions": result}
        target = self._target(params)
        native = {**target, **{k: v for k, v in params.items() if k != "workspace"}}
        session_id = params.get("sessionId")
        thought = params.get("thoughtLevel") or self._thought_levels.get(session_id)
        if isinstance(native.get("model"), dict) and thought:
            selection = dict(native["model"])
            options = dict(selection.get("options") or {})
            options.setdefault("reasoningLevel", thought)
            native["model"] = {**selection, "options": options}
        if method == "session/subscribe":
            session_id = params["sessionId"]
            old = self._subscriptions.pop(session_id, None)
            if old is not None:
                self.rpc.unlisten(old)
            self._subscriptions[session_id] = self.rpc.listen(
                "zcode-agent", "onDynamicSessionEvent", native,
                lambda event: self._enqueue_event(session_id, event),
            )
            path = target["workspacePath"]
            if path not in self._telemetry:
                self._telemetry[path] = self.rpc.listen(
                    "zcode-agent", "onDynamicConversationTelemetryFact", target,
                    lambda fact: self._enqueue_event(fact.get("sessionId"), {
                        "type": "telemetry", "fact": fact,
                    }),
                )
            if session_id not in self._v4_subscriptions:
                opened = self.rpc.call("zcode-agent", "subscribeConversationV4", {
                    **target, "sessionId": session_id, "visibility": "foreground",
                }, timeout=timeout)
                self._v4_subscriptions[session_id] = opened["ack"]["subscriptionId"]
            snapshot = self.rpc.call("zcode-agent", "readSession", native, timeout=timeout)
            events = self.rpc.call("zcode-agent", "readSessionEvents", {
                **target, "sessionId": session_id, "afterSeq": params.get("afterSeq", 0),
            }, timeout=timeout)
            return {"snapshot": snapshot, "events": events}
        if method == "session/close":
            v4_subscription = self._v4_subscriptions.pop(params["sessionId"], None)
            if v4_subscription:
                self.rpc.call("zcode-agent", "unsubscribeConversationV4", {
                    **target, "subscriptionId": v4_subscription,
                }, timeout=timeout)
            result = self.rpc.call("zcode-agent", "closeSession", native, timeout=timeout)
            subscription = self._subscriptions.pop(params["sessionId"], None)
            if subscription is not None:
                self.rpc.unlisten(subscription)
            return {"closed": result}
        if method in {"session/stop", "session/cancelBackgroundTask"}:
            action = "stop" if method == "session/stop" else "cancelBackgroundWork"
            payload = {} if action == "stop" else {"workId": params["taskId"]}
            if action == "stop":
                self._command(target, params["sessionId"], "setAutoDrain", {"autoDrain": False}, timeout)
            self._command(target, params["sessionId"], action, payload, timeout)
            return {"cancelled": True} if action != "stop" else {"stopped": True}
        if method == "session/fork":
            raise ProtocolError("Checkpoint branching is not exposed by the Services transport", code=-32601)
        if method == "session/messages":
            # Services 3.14.3 readSessionMessages validates legacy message IDs
            # against a newer schema. readSession owns the compatible snapshot
            # projection and supplies the same bounded message tail.
            return self.rpc.call("zcode-agent", "readSession", {
                **target, "sessionId": params["sessionId"],
                "messageLimit": params.get("limit", 6),
            }, timeout=timeout)
        native_method = self.METHODS.get(method)
        if not native_method:
            raise ProtocolError("Unsupported Services bridge method: " + method, code=-32601)
        result = self.rpc.call("zcode-agent", native_method, native, timeout=timeout)
        if method in {"session/create", "session/resume"}:
            session_id = result["session"]["sessionId"]
            self._workspaces[session_id] = target["workspacePath"]
        if session_id and params.get("thoughtLevel"):
            self._thought_levels[session_id] = params["thoughtLevel"]
        return result

    def _command(self, target, session_id, action, payload, timeout=30):
        result = self.rpc.call("zcode-agent", "sendConversationCommandV4", {
            **target, "envelope": {
                "commandId": str(uuid.uuid4()), "clientId": "coder-mcp-bridge",
                "sessionId": session_id, "type": action, "payload": payload,
                "issuedAt": int(time.time() * 1000),
            },
        }, timeout=timeout)
        if result.get("status") not in {"accepted", "applied", "duplicate", "noop"}:
            raise ProtocolError("ZCode command was not accepted: " + str(result.get("reasonCode") or result.get("status")))
        return result

    def _enqueue_event(self, session_id, event):
        try:
            self._events.put_nowait((session_id, event))
        except queue.Full:
            self._disconnected("Services event queue exceeded its limit")

    def _consume_events(self):
        while not self._closed:
            try:
                item = self._events.get(timeout=0.5)
            except queue.Empty:
                continue
            if item is None:
                return
            session_id, event = item
            try:
                kind = event.get("type")
                if kind in {"permission.request", "userInput.request"}:
                    self._interaction(session_id, event)
                elif self.on_notification:
                    if kind == "telemetry":
                        self.on_notification({"method": "telemetry", "params": event["fact"]})
                    elif kind == "session.event":
                        self.on_notification({"method": "session/event", "params": event["event"]})
                    elif kind == "state.updated":
                        self.on_notification({"method": "state.updated", "params": event["notification"]})
                    elif kind == "snapshot":
                        self.on_notification({"method": "state.updated", "params": {
                            "sessionId": session_id, "patch": event["snapshot"],
                        }})
            except Exception:
                # Fail closed, without raw model/auth payloads in logs.
                self._disconnected("Services session event handling failed")

    def _interaction(self, session_id, event):
        request = event["request"]
        permission = event["type"] == "permission.request"
        method = "interaction/requestPermission" if permission else "interaction/requestUserInput"
        try:
            response = self.on_server_request(method, request) if self.on_server_request else {}
        except Exception:
            response = {}
        if permission:
            decision = response.get("decision", "deny")
            answer = {"optionId": "allowOnce" if decision == "allow" else "deny"}
        else:
            answer = {"action": response.get("action", "cancel")}
            if isinstance(response.get("content"), dict):
                answer["content"] = response["content"]
        self._command(self._target({"sessionId": session_id}), session_id, "resolveInteraction", {
            "interactionId": request["requestId"], "answer": answer,
        })

    def _disconnected(self, reason):
        with self._lock:
            self._initialized = False
            self._subscriptions.clear()
            self._telemetry.clear()
            self._v4_subscriptions.clear()
        if not self._closed and self.on_disconnect:
            self.on_disconnect(reason)

    def close(self):
        self._closed = True
        self.rpc.close()
        try:
            self._events.put_nowait(None)
        except queue.Full:
            pass
        if threading.current_thread() is not self._worker:
            self._worker.join(timeout=1)


def create_zcode_protocol(*args, **kwargs):
    transport = os.environ.get("ZCODE_PROTOCOL_TRANSPORT", "direct")
    if transport == "services":
        return ZCodeServicesProtocolClient(*args, **kwargs)
    if transport != "direct":
        raise ProtocolError("Unknown ZCODE_PROTOCOL_TRANSPORT")
    from zcode_protocol import ZCodeProtocolClient
    return ZCodeProtocolClient(*args, **kwargs)
