"""Public OpenCode backend: native ACP (no HTTP fallback).

Legacy HTTP implementation is retained in opencode_http_backend.py for explicit
comparison only; it is never selected by BackendManager.
"""
from opencode_acp_backend import OpenCodeBackend, OpenCodeRuntime, discover_opencode

__all__ = ["OpenCodeBackend", "OpenCodeRuntime", "discover_opencode"]
