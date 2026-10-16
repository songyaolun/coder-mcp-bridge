"""Opt-in real-account MCP E2E; ordinary test runs make no model requests."""

import json
import os
import time
from pathlib import Path

import pytest

from test_mcp import McpClient


pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_ZCODE_ACCOUNT_E2E") != "1",
    reason="Requires explicit opt-in and a logged-in ZCode Coding Plan account",
)


def wait(client, state):
    deadline = time.monotonic() + 180
    while state["status"] not in {"completed", "failed", "cancelled", "timed_out"}:
        assert time.monotonic() < deadline, "Account smoke exceeded deadline"
        state = client.tool("agent-wait", {
            "runId": state["runId"], "afterRevision": state["revision"],
            "timeoutMs": 10000,
        }, timeout=45)
    assert state["status"] == "completed", state.get("error")
    return state


def start_client():
    client = McpClient()
    client.request("initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
                                   "clientInfo": {"name": "account-e2e", "version": "1"}})
    return client


def test_real_account_mcp_and_cold_resume(tmp_path, monkeypatch):
    monkeypatch.setenv("ZCODE_PROTOCOL_TRANSPORT", "services")
    config = Path.home() / ".zcode/v2/provider_config.json"
    before = config.read_bytes() if config.exists() else None
    client = start_client()
    state = None
    try:
        available = client.tool("agent-config", {"action": "list"})
        assert available["selectedBackend"] == "zcode"
        assert available["capabilities"]["branch"] is False
        state = wait(client, client.tool("agent-start", {
            "cwd": str(tmp_path), "prompt": "Reply exactly BRIDGE_ACCOUNT_OK. Do not use tools.",
            "model": {"providerId": "account:bigmodel-individual-coding-plan",
                      "modelId": "GLM-5.3-Flash"},
            "thoughtLevel": "low", "timeout": 150,
        }))
        assert "BRIDGE_ACCOUNT_OK" in state["result"]
        assert state["usage"]["totalTokens"] > 0
        assert state["usage"]["modelRequests"] >= 1
        assert state["model"]["model"]["providerId"].startswith("account:")
        thread_id = state["threadId"]
    finally:
        client.close()
    client = start_client()
    try:
        resumed = wait(client, client.tool("agent-start", {
            "cwd": str(tmp_path), "threadId": thread_id,
            "prompt": "Reply exactly RESUME_OK. Do not use tools.",
            "model": {"providerId": "account:bigmodel-individual-coding-plan",
                      "modelId": "GLM-5.3-Flash"},
            "thoughtLevel": "low", "timeout": 150,
        }))
        assert "RESUME_OK" in resumed["result"]
        assert resumed["usage"]["totalTokens"] > 0
        assert resumed["usage"]["modelRequests"] >= 1
        assert resumed["sessionUsage"]["modelRequests"] >= 2
        client.tool("agent-close", {"runId": resumed["runId"]})
        evidence = {"provider": resumed["model"]["model"], "status": resumed["status"],
                    "usage": resumed["usage"], "sessionUsage": resumed["sessionUsage"],
                    "coldResume": True, "personalConfigUnchanged": True}
        (tmp_path / "account-e2e-evidence.json").write_text(json.dumps(evidence, indent=2))
    finally:
        client.close()
        assert (config.read_bytes() if config.exists() else None) == before
