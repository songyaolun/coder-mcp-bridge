#!/usr/bin/env python3
"""Real subprocess fake for the OpenCode ACP v1 NDJSON transport.

It keeps a real multi-session dictionary, persists session metadata in the
workspace so a second bridge/control plane can list and resume it, and answers
prompts on per-session worker threads so cancel/close notifications stay live.
No credentials, network or real OpenCode process are involved.
"""
import json
import os
import sys
import threading
import time
import uuid

OUT_LOCK = threading.Lock()
STATE_LOCK = threading.RLock()
WAIT_LOCK = threading.Lock()
WAITERS = {}
SESSIONS = {}
PROMPT_LOCK = threading.Lock()
PROMPT_COUNT = 0
HISTORY_DIR = ".fake-opencode-sessions"

MODEL_CHOICES = [
    {"value": "opencode-catalog/deepseek-v4-flash", "name": "OpenCode Catalog Flash"},
    {"value": "acme/proxy/model-x", "name": "Acme proxy model x"},
    {"value": "acme/proxy/model-y", "name": "Acme proxy model y"},
]
THOUGHT_CHOICES = [
    {"value": level, "name": level} for level in ("low", "none", "default")
]
USAGES = [
    {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15,
     "thoughtTokens": 2, "cachedReadTokens": 3},
    {"inputTokens": 20, "outputTokens": 7, "totalTokens": 27,
     "thoughtTokens": 4, "cachedReadTokens": 1},
]


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


def config_options(session):
    return [
        {"id": "model", "category": "model", "currentValue": session["model"],
         "options": MODEL_CHOICES},
        {"id": "thought_level", "category": "thought_level",
         "currentValue": session["effort"], "options": THOUGHT_CHOICES},
    ]


def history_path(cwd, session_id):
    return os.path.join(cwd, HISTORY_DIR, session_id + ".json")


def persist(session):
    directory = os.path.join(session["cwd"], HISTORY_DIR)
    os.makedirs(directory, exist_ok=True)
    path = history_path(session["cwd"], session["id"])
    temporary = path + ".tmp"
    serializable = {key: value for key, value in session.items() if key != "cancel"}
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(serializable, handle, ensure_ascii=False, sort_keys=True)
    os.replace(temporary, path)


def load_session(cwd, session_id):
    path = history_path(cwd, session_id)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def new_session(cwd, title, model=None, effort="low"):
    session_id = str(uuid.uuid4())
    session = {
        "id": session_id,
        "cwd": cwd,
        "title": title,
        "model": model or MODEL_CHOICES[0]["value"],
        "effort": effort,
        "commands": ["compact", "init"],
        "updatedAt": int(time.time() * 1000),
        "closed": False,
    }
    with STATE_LOCK:
        SESSIONS[session_id] = session
    persist(session)
    return session


def active_session(session_id, cwd):
    with STATE_LOCK:
        session = SESSIONS.get(session_id)
    if session:
        return session
    session = load_session(cwd, session_id)
    if session:
        with STATE_LOCK:
            SESSIONS[session_id] = session
    return session


def prompt_text(params):
    chunks = params.get("prompt") or []
    return "".join(chunk.get("text", "") for chunk in chunks
                   if isinstance(chunk, dict) and chunk.get("type") == "text")


def run_prompt(request_id, params, session, count, cancel_event):
    session_id = session["id"]
    text = prompt_text(params)
    short = session_id[:8]
    if text == "/compact":
        notification(session_id, {"sessionUpdate": "compaction_update",
                                  "status": "running", "compactionId": "compact-1"})
        notification(session_id, {"sessionUpdate": "compaction_summary_chunk",
                                  "content": {"type": "text", "text": "summary"}})
        notification(session_id, {"sessionUpdate": "compaction_update",
                                  "status": "completed", "compactionId": "compact-1"})
        respond(request_id, {"stopReason": "end_turn", "usage": USAGES[(count - 1) % 2]})
        return

    notification(session_id, {"sessionUpdate": "agent_message_chunk",
                              "content": {"type": "text",
                                          "text": "S[%s] %s" % (short, text)}})
    if "child" in text:
        notification(session_id, {
            "sessionUpdate": "agent_message_chunk",
            "_meta": {"opencode/child-session": {"id": "child-1", "depth": 1}},
            "content": {"type": "text", "text": "CHILD-SESSION-TEXT"},
        })
        notification(session_id, {
            "sessionUpdate": "agent_message_chunk",
            "_meta": {"opencode/child-session": {"id": "child-2", "depth": 2}},
            "content": {"type": "text", "text": "GRANDCHILD-SESSION-TEXT"},
        })

    if text.startswith("slow:"):
        cancel_event.wait(10)
        respond(request_id, {"stopReason": "cancelled"})
        return
    if text.startswith("hold:"):
        time.sleep(0.6)
    elif text.startswith("delay:"):
        try:
            time.sleep(float(text.split(":", 2)[1]))
        except (ValueError, IndexError):
            time.sleep(0.3)

    notification(session_id, {"sessionUpdate": "agent_thought_chunk",
                              "content": {"type": "text", "text": "thinking"}})
    tool_id = "tool-%d" % count
    notification(session_id, {"sessionUpdate": "tool_call", "toolCallId": tool_id,
                              "title": "bash", "kind": "execute",
                              "status": "in_progress"})
    notification(session_id, {"sessionUpdate": "tool_call_update",
                              "toolCallId": tool_id, "title": "bash",
                              "status": "completed"})
    notification(session_id, {"sessionUpdate": "usage_update", "used": 77,
                               "size": 1000, "cost": round(count * 1.25, 2)})
    notification(session_id, {"sessionUpdate": "agent_message_chunk",
                              "content": {"type": "text", "text": " done"}})
    respond(request_id, {"stopReason": "end_turn", "usage": USAGES[(count - 1) % 2]})


def handle_request(message):
    global PROMPT_COUNT
    method, request_id = message.get("method"), message.get("id")
    params = message.get("params") or {}
    cwd = os.path.realpath(params.get("cwd") or os.getcwd())
    if method == "initialize":
        respond(request_id, {
            "protocolVersion": 1,
            "agentCapabilities": {"sessionCapabilities": {"fork": {}}},
        })
    elif method == "session/new":
        session = new_session(cwd, "Session %d" % (len(SESSIONS) + 1))
        notification(session["id"], {
            "sessionUpdate": "available_commands_update",
            "availableCommands": [{"name": "compact"}, {"name": "init"}],
        })
        respond(request_id, {"sessionId": session["id"],
                             "configOptions": config_options(session)})
    elif method == "session/resume":
        session = active_session(params.get("sessionId") or "", cwd)
        if not session:
            respond_error(request_id, -32001, "session not found")
            return
        notification(session["id"], {
            "sessionUpdate": "available_commands_update",
            "availableCommands": [{"name": name} for name in session["commands"]],
        })
        respond(request_id, {"sessionId": session["id"],
                             "configOptions": config_options(session)})
    elif method == "session/set_config_option":
        session = active_session(params.get("sessionId") or "", cwd)
        if not session:
            respond_error(request_id, -32001, "session not found")
            return
        config_id, value = params.get("configId"), params.get("value")
        if config_id == "model":
            if value not in {choice["value"] for choice in MODEL_CHOICES}:
                respond_error(request_id, -32602, "unknown model choice")
                return
            session["model"] = value
        elif config_id == "thought_level":
            if value not in {choice["value"] for choice in THOUGHT_CHOICES}:
                respond_error(request_id, -32602, "unknown thought level")
                return
            session["effort"] = value
        else:
            respond_error(request_id, -32602, "unknown config option")
            return
        session["updatedAt"] = int(time.time() * 1000)
        persist(session)
        notification(session["id"], {
            "sessionUpdate": "config_option_update",
            "configOptions": config_options(session),
        })
        respond(request_id, {"configOptions": config_options(session)})
    elif method == "session/close":
        session_id = params.get("sessionId") or ""
        with STATE_LOCK:
            session = SESSIONS.pop(session_id, None)
            cancel_event = session.get("cancel") if session else None
        if session:
            session["closed"] = True
            session["cancel"] = None
            session["updatedAt"] = int(time.time() * 1000)
            persist(session)
        if cancel_event:
            cancel_event.set()
        respond(request_id, {})
    elif method == "session/list":
        found = {}
        directory = os.path.join(cwd, HISTORY_DIR)
        if os.path.isdir(directory):
            for name in os.listdir(directory):
                if not name.endswith(".json"):
                    continue
                try:
                    with open(os.path.join(directory, name), encoding="utf-8") as handle:
                        session = json.load(handle)
                except (OSError, ValueError):
                    continue
                if session.get("cwd") == cwd:
                    found[session["id"]] = session
        with STATE_LOCK:
            for session in SESSIONS.values():
                if session["cwd"] == cwd:
                    found[session["id"]] = session
        items = sorted(found.values(), key=lambda item: item["updatedAt"], reverse=True)
        respond(request_id, {"sessions": [
            {"sessionId": item["id"], "cwd": item["cwd"], "title": item["title"],
             "updatedAt": item["updatedAt"]} for item in items]})
    elif method == "session/fork":
        parent = active_session(params.get("sessionId") or "", cwd)
        if not parent:
            respond_error(request_id, -32001, "session not found")
            return
        child = new_session(cwd, parent["title"] + " (fork)",
                            model=parent["model"], effort=parent["effort"])
        child["commands"] = parent["commands"]
        persist(child)
        respond(request_id, {"sessionId": child["id"]})
    elif method == "session/prompt":
        session = active_session(params.get("sessionId") or "", cwd)
        if not session:
            respond_error(request_id, -32001, "session not found")
            return
        cancel_event = threading.Event()
        with STATE_LOCK:
            session["cancel"] = cancel_event
            session["updatedAt"] = int(time.time() * 1000)
        with PROMPT_LOCK:
            PROMPT_COUNT += 1
            count = PROMPT_COUNT
        threading.Thread(target=run_prompt,
                         args=(request_id, params, session, count, cancel_event),
                         daemon=True).start()
    elif method == "session/request_permission":
        reply = reverse_request(request_id, "session/request_permission",
                                {"sessionId": params.get("sessionId"),
                                 "toolCall": params.get("toolCall") or {},
                                 "options": params.get("options") or []})
        send({"jsonrpc": "2.0", "method": "test/reverse_reply",
              "params": {"name": "permission", "reply": reply}})
        respond(request_id, {})
    elif method == "test/emit":
        notification(params.get("sessionId") or "",
                     params.get("update") or {})
        respond(request_id, {})
    elif method == "test/malformed":
        sys.stdout.buffer.write(b"not-json\n")
        sys.stdout.buffer.flush()
        time.sleep(60)
    else:
        respond_error(request_id, -32601, "method not found: %s" % method)


def handle_notification(message):
    method = message.get("method")
    if method == "session/cancel":
        with STATE_LOCK:
            session = SESSIONS.get((message.get("params") or {}).get("sessionId"))
            cancel_event = session.get("cancel") if session else None
        if cancel_event:
            cancel_event.set()
    elif method == "test/exit":
        os._exit(0)


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
