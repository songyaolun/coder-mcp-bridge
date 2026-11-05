#!/usr/bin/env python3
"""Real subprocess fake for the DeepSeek Harness ACP v1 NDJSON transport.

Modes (argv[1]): default, delayed, slow, slow_first, reverse, reverse_unknown.
argv[2], when present, is a path where received prompt texts are appended.
No credentials, network or real DSH process are involved.
"""
import json
import os
import sys
import threading
import time

MODE = sys.argv[1] if len(sys.argv) > 1 else "default"
LOG = sys.argv[2] if len(sys.argv) > 2 else None
DEFAULT_SESSION = "00000000-0000-4000-8000-000000000001"

OUT_LOCK = threading.Lock()
STATE_LOCK = threading.Lock()
WAIT_LOCK = threading.Lock()
WAITERS = {}
PROMPT_LOCK = threading.Lock()
ACTIVE_CANCEL = None
PROMPT_COUNT = 0

MODEL_CHOICES = [
    {"value": json.dumps(["deepseek-account", "deepseek-flash"]),
     "name": "DeepSeek Flash (account)"},
    {"value": json.dumps(["deepseek-account", "deepseek-v4-pro"]),
     "name": "DeepSeek V4 Pro (account)"},
    {"value": json.dumps(["dsh-catalog", "deepseek-flash"]),
     "name": "DeepSeek Flash (catalog)"},
]
REASONING_CHOICES = [
    {"value": level, "name": level} for level in ("off", "low", "high", "max")
]
STATE = {"sessionId": DEFAULT_SESSION, "model": MODEL_CHOICES[0]["value"],
         "reasoning": "low"}


def append_log(value):
    if LOG:
        with OUT_LOCK:
            with open(LOG, "a", encoding="utf-8") as handle:
                handle.write(value + "\n")
                handle.flush()


def send(message):
    data = (json.dumps(message, ensure_ascii=False) + "\n").encode("utf-8")
    with OUT_LOCK:
        try:
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()
        except (BrokenPipeError, OSError):
            os._exit(0)


def respond(request_id, result):
    send({"jsonrpc": "2.0", "id": request_id, "result": result})


def respond_error(request_id, code, message):
    send({"jsonrpc": "2.0", "id": request_id,
          "error": {"code": code, "message": message}})


def config_options():
    return [
        {"id": "model", "category": "model", "currentValue": STATE["model"],
         "options": MODEL_CHOICES},
        {"id": "reasoning_effort", "category": "reasoning_effort",
         "currentValue": STATE["reasoning"], "options": REASONING_CHOICES},
    ]


def notification(session_id, update):
    send({"jsonrpc": "2.0", "method": "session/update",
          "params": {"sessionId": session_id, "update": update}})


def reverse_request(request_id, method, params):
    waiter = {"event": threading.Event(), "reply": None}
    with WAIT_LOCK:
        WAITERS[request_id] = waiter
    send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
    if not waiter["event"].wait(5):
        return {"error": {"code": -1, "message": "client did not answer"}}
    with WAIT_LOCK:
        WAITERS.pop(request_id, None)
    return waiter["reply"] or {"error": {"code": -1, "message": "no reply"}}


def run_prompt(request_id, params, index, cancel_event):
    session_id = params.get("sessionId") or STATE["sessionId"]
    chunks = params.get("prompt") or []
    text = "".join(chunk.get("text", "") for chunk in chunks
                   if isinstance(chunk, dict) and chunk.get("type") == "text")
    notification(session_id, {"sessionUpdate": "agent_message_chunk",
                              "content": {"type": "text", "text": "echo:" + text}})

    if MODE in ("reverse", "reverse_unknown") and index == 1:
        if MODE == "reverse":
            reply = reverse_request(
                "reverse-permission-1", "session/request_permission",
                {"sessionId": session_id,
                 "toolCall": {"toolCallId": "perm-1",
                              "title": "TOP-SECRET escalation request"},
                 "options": [{"optionId": "allow", "kind": "allow_once"},
                             {"optionId": "deny", "kind": "reject_once"}]},
            )
            send({"jsonrpc": "2.0", "method": "test/reverse_reply",
                  "params": {"name": "permission", "reply": reply}})
        reply = reverse_request("reverse-unknown-1", "session/unknown_reverse",
                                {"secret": "TOP-SECRET unknown payload"})
        send({"jsonrpc": "2.0", "method": "test/reverse_reply",
              "params": {"name": "unknown", "reply": reply}})

    if MODE == "delayed" and index == 1:
        time.sleep(0.45)
    elif MODE == "slow" or (MODE == "slow_first" and index == 1):
        cancel_event.wait(10)
        respond(request_id, {"stopReason": "cancelled"})
        return

    notification(session_id, {"sessionUpdate": "agent_thought_chunk",
                              "content": {"type": "text", "text": "thinking"}})
    notification(session_id, {"sessionUpdate": "tool_call",
                              "toolCallId": "tool-1", "title": "bash",
                              "kind": "execute", "status": "in_progress"})
    notification(session_id, {"sessionUpdate": "tool_call_update",
                              "toolCallId": "tool-1", "title": "bash",
                              "status": "completed"})
    notification(session_id, {"sessionUpdate": "usage_update",
                              "used": 42, "size": 1000})
    notification(session_id, {"sessionUpdate": "agent_message_chunk",
                              "content": {"type": "text", "text": " done"}})
    respond(request_id, {"stopReason": "end_turn"})


def handle_request(message):
    global ACTIVE_CANCEL, PROMPT_COUNT
    method, request_id = message.get("method"), message.get("id")
    params = message.get("params") or {}
    if method == "initialize":
        respond(request_id, {"protocolVersion": 1, "agentCapabilities": {}})
    elif method == "echo":
        respond(request_id, params)
    elif method in ("session/new", "session/resume"):
        if method == "session/resume":
            STATE["sessionId"] = params.get("sessionId") or DEFAULT_SESSION
        respond(request_id, {"sessionId": STATE["sessionId"],
                             "configOptions": config_options()})
    elif method == "session/set_config_option":
        config_id, value = params.get("configId"), params.get("value")
        if config_id == "model":
            STATE["model"] = value
        elif config_id == "reasoning_effort":
            STATE["reasoning"] = value
        respond(request_id, {"configOptions": config_options()})
    elif method == "session/close":
        respond(request_id, {})
    elif method == "session/prompt":
        chunks = params.get("prompt") or []
        text = "".join(chunk.get("text", "") for chunk in chunks
                       if isinstance(chunk, dict) and chunk.get("type") == "text")
        append_log(text)
        cancel_event = threading.Event()
        with PROMPT_LOCK:
            PROMPT_COUNT += 1
            index = PROMPT_COUNT
            ACTIVE_CANCEL = cancel_event
        threading.Thread(target=run_prompt,
                         args=(request_id, params, index, cancel_event),
                         daemon=True).start()
    elif method == "test/exit":
        os._exit(0)
    elif method == "test/malformed":
        sys.stdout.buffer.write(b'not-json\n')
        sys.stdout.buffer.flush()
        time.sleep(60)  # Remain alive until the bridge terminates the malformed peer.
    else:
        respond_error(request_id, -32601, "method not found: %s" % method)


def handle_notification(message):
    if message.get("method") == "session/cancel":
        with PROMPT_LOCK:
            cancel_event = ACTIVE_CANCEL
        if cancel_event:
            cancel_event.set()


def handle_response(message):
    with WAIT_LOCK:
        waiter = WAITERS.pop(message.get("id"), None)
    if waiter:
        waiter["reply"] = message
        waiter["event"].set()


def main():
    for raw in sys.stdin.buffer:
        if not raw.endswith(b"\n"):
            continue
        try:
            message = json.loads(raw)
        except ValueError:
            continue
        if "method" in message and "id" in message:
            handle_request(message)
        elif "method" in message:
            handle_notification(message)
        else:
            handle_response(message)


if __name__ == "__main__":
    main()
