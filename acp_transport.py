"""Shared ACP v1 NDJSON transport used by adapter-specific ACP backends.

The transport owns one child process: responses, notifications and reverse
requests all arrive on stdout. It performs no adapter-specific policy work.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import threading
import uuid

from agent_control_plane import bounded
from zcode_protocol import ProtocolError


class AcpRequestTimeout(ProtocolError):
    """A native operation has no confirmed result; callers must clean up ownership."""


class AcpClient:
    """One ACP peer; responses, notifications and reverse requests share stdout."""

    def __init__(self, command, *, cwd, on_event=None, on_disconnect=None, logger=None,
                 env=None, client_capabilities=None, request_handler=None):
        self.command, self.cwd = list(command), cwd
        self.on_event = on_event or (lambda _event: None)
        self.on_disconnect = on_disconnect or (lambda _error: None)
        self.logger = logger or (lambda _message: None)
        self.env = env
        self.client_capabilities = client_capabilities
        self.request_handler = request_handler
        self.process = None
        self._lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._pending = {}
        self._closed = False

    def start(self):
        kwargs = {'start_new_session': True} if os.name != 'nt' else {}
        self.process = subprocess.Popen(self.command, cwd=self.cwd, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                        bufsize=0, env=self.env, **kwargs)
        threading.Thread(target=self._read, daemon=True).start()
        threading.Thread(target=self._drain_stderr, daemon=True).start()
        capabilities = self.client_capabilities if self.client_capabilities is not None else {}
        return self.request('initialize', {
            'protocolVersion': 1, 'clientCapabilities': capabilities}, timeout=30)

    def _send(self, message):
        data = (json.dumps(message, ensure_ascii=False) + '\n').encode('utf-8')
        with self._write_lock:
            if not self.process or self.process.poll() is not None or self._closed:
                raise ProtocolError('ACP transport is closed')
            self.process.stdin.write(data)
            self.process.stdin.flush()

    def request(self, method, params=None, timeout=30):
        rid = uuid.uuid4().hex
        entry = {'event': threading.Event(), 'response': None}
        with self._lock:
            self._pending[rid] = entry
        try:
            self._send({'jsonrpc': '2.0', 'id': rid, 'method': method, 'params': params or {}})
            if not entry['event'].wait(timeout):
                try:
                    self.notify('$/cancel_request', {'requestId': rid})
                except (ProtocolError, OSError):
                    pass
                raise AcpRequestTimeout('ACP request timed out: ' + method)
            response = entry['response']
            if response.get('error'):
                # Never echo the request, which may contain proprietary prompt content.
                raise ProtocolError('ACP %s: %s' % (method, bounded(response['error'].get('message'), 800)))
            return response.get('result') or {}
        finally:
            with self._lock:
                self._pending.pop(rid, None)

    def notify(self, method, params):
        self._send({'jsonrpc': '2.0', 'method': method, 'params': params})

    def _handle(self, message):
        if 'method' in message:
            if 'id' in message:
                if self.request_handler is not None:
                    self._handle_reverse_request(message)
                elif message['method'] == 'session/request_permission':
                    # Native policy already handles normal workspace tools. Never approve
                    # an escalation or interactive question headlessly on the user's behalf.
                    result = {'outcome': {'outcome': 'cancelled'}}
                    self._send({'jsonrpc': '2.0', 'id': message['id'], 'result': result})
                    self.on_event({'method': 'bridge/permission_declined', 'params': {}})
                else:
                    self._send({'jsonrpc': '2.0', 'id': message['id'], 'error': {
                        'code': -32601, 'message': 'Unsupported ACP client request'}})
            else:
                self.on_event(message)
        elif 'id' in message:
            with self._lock:
                entry = self._pending.get(message['id'])
                if entry:
                    entry['response'] = message
                    entry['event'].set()

    def _handle_reverse_request(self, message):
        method = message['method']
        params = message.get('params') or {}
        try:
            result = self.request_handler(method, params)
        except (NotImplementedError, LookupError):
            self._send({'jsonrpc': '2.0', 'id': message['id'], 'error': {
                'code': -32601, 'message': 'Unsupported ACP client request'}})
        except Exception:  # noqa: BLE001
            # Handler errors may quote request params; never echo their text.
            self._send({'jsonrpc': '2.0', 'id': message['id'], 'error': {
                'code': -32603, 'message': 'ACP client request handler failed'}})
        else:
            if result is None:
                self._send({'jsonrpc': '2.0', 'id': message['id'], 'error': {
                    'code': -32601, 'message': 'Unsupported ACP client request'}})
            else:
                self._send({'jsonrpc': '2.0', 'id': message['id'], 'result': result})

    def _read(self):
        # Binary readline splits LF only; U+2028/U+2029 inside JSON strings are valid.
        try:
            while True:
                raw = self.process.stdout.readline(8 * 1024 * 1024 + 1)
                if not raw:
                    break
                if len(raw) > 8 * 1024 * 1024 or not raw.endswith(b'\n'):
                    raise ProtocolError('ACP invalid or oversized NDJSON frame')
                self._handle(json.loads(raw))
        except Exception as exc:  # noqa: BLE001
            self.logger('ACP reader failed: %s' % bounded(exc, 300))
        finally:
            unexpected = not self._closed
            if unexpected:
                # Do not release the workspace lease while a malformed peer can
                # still execute tools. Terminate/reap it before publishing loss.
                self.close()
            with self._lock:
                for entry in self._pending.values():
                    entry['response'] = {'error': {'message': 'ACP transport closed'}}
                    entry['event'].set()
            if unexpected:
                self.on_disconnect('ACP process exited unexpectedly')

    def _drain_stderr(self):
        # Drain without logging arbitrary profile output: it may contain secrets.
        try:
            while self.process and self.process.stderr.read(4096):
                pass
        except (OSError, ValueError):
            pass  # Stream closed by the owning cleanup path.

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
        process = self.process
        if not process:
            return
        try:
            process.stdin.close()
            process.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            try:
                if os.name != 'nt':
                    os.killpg(process.pid, signal.SIGTERM)
                else:
                    process.terminate()
                process.wait(timeout=5)
            except (OSError, subprocess.TimeoutExpired):
                try:
                    if os.name != 'nt':
                        os.killpg(process.pid, signal.SIGKILL)
                    else:
                        process.kill()
                    process.wait(timeout=5)
                except (OSError, subprocess.TimeoutExpired):
                    pass
        finally:
            for stream in (process.stdout, process.stderr):
                if stream:
                    stream.close()
