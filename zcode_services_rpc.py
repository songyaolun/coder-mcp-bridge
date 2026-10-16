"""Bounded stdio RPC transport for the official ZCode Services Server.

Wire format follows zai-org/ZCode packages/rpc (Apache-2.0): a 13-byte
SocketProtocol header and typed/VLQ ChannelClient header/body. No credentials
are read or logged here; account authentication stays inside Services Server.
"""

from __future__ import annotations

import io
import json
import os
import struct
import subprocess
import threading

from zcode_protocol import ProtocolError

MAX_FRAME = 64 * 1024 * 1024


def _vlq(value):
    result = bytearray()
    value &= 0xffffffff
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def encode(value):
    if value is None:
        return b"\x00"
    if isinstance(value, str):
        raw = value.encode("utf-8")
        return b"\x01" + _vlq(len(raw)) + raw
    if isinstance(value, list):
        return b"\x04" + _vlq(len(value)) + b"".join(map(encode, value))
    if isinstance(value, int) and not isinstance(value, bool) and 0 <= value < 2**31:
        return b"\x06" + _vlq(value)
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return b"\x05" + _vlq(len(raw)) + raw


def _exact(stream, count):
    chunks = bytearray()
    while len(chunks) < count:
        part = stream.read(count - len(chunks))
        if not part:
            raise EOFError("ZCode Services Server stream closed")
        chunks.extend(part)
    return bytes(chunks)


def _read_vlq(stream):
    result = 0
    for shift in range(0, 35, 7):
        value = _exact(stream, 1)[0]
        result |= (value & 127) << shift
        if not value & 128:
            return result
    raise ProtocolError("Invalid RPC integer")


def decode(stream, depth=0):
    if depth > 64:
        raise ProtocolError("RPC nesting limit exceeded")
    tag = _exact(stream, 1)[0]
    if tag == 0:
        return None
    if tag == 6:
        return _read_vlq(stream)
    if tag not in {1, 2, 3, 4, 5}:
        raise ProtocolError("Unknown RPC value type")
    length = _read_vlq(stream)
    if length > MAX_FRAME:
        raise ProtocolError("RPC value limit exceeded")
    if tag == 4:
        return [decode(stream, depth + 1) for _ in range(length)]
    raw = _exact(stream, length)
    if tag == 1:
        return raw.decode("utf-8")
    if tag == 5:
        return json.loads(raw)
    return raw


class ServicesRPC:
    def __init__(self, node, runtime, on_disconnect=None):
        self.node, self.runtime = node, runtime
        self.on_disconnect = on_disconnect
        self._proc = None
        self._lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._ready = threading.Event()
        self._pending = {}
        self._listeners = {}
        self._next = 0
        self._closed = False
        self._error = None

    @property
    def process_id(self):
        return self._proc.pid if self._proc and self._proc.poll() is None else None

    def start(self):
        with self._lock:
            if self._closed:
                raise ProtocolError("Services RPC is closed")
            if self._proc is None:
                self._error = None
                self._ready.clear()
                env = dict(os.environ)
                env.pop("ELECTRON_RUN_AS_NODE", None)
                env.update({
                    "ZCODE_SERVICE_AUTHORITY_MODE": "standalone-server",
                    "ZCODE_AGENT_SERVER_COMMAND": self.node,
                    "ZCODE_AGENT_SERVER_ARGS_JSON": json.dumps([
                        os.path.join(self.runtime, "agent", "zcode.cjs"),
                        "app-server", "--stdio",
                    ]),
                })
                self._proc = subprocess.Popen(
                    [self.node, os.path.join(self.runtime, "server", "remote", "zcode-server.cjs")],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL, env=env, start_new_session=True,
                )
                threading.Thread(target=self._read, daemon=True).start()
        if not self._ready.wait(20):
            self.close()
            raise ProtocolError("Services Server handshake timed out")
        if self._error:
            raise ProtocolError(self._error)

    def _send(self, header, body=None):
        payload = encode(header) + encode(body)
        if len(payload) > MAX_FRAME:
            raise ProtocolError("RPC frame limit exceeded")
        with self._write_lock:
            self._proc.stdin.write(struct.pack(">BIII", 1, 0, 0, len(payload)) + payload)
            self._proc.stdin.flush()

    def _id(self):
        with self._lock:
            self._next += 1
            return self._next

    def call(self, channel, method, params=None, timeout=30):
        self.start()
        request_id = self._id()
        pending = {"event": threading.Event()}
        with self._lock:
            self._pending[request_id] = pending
        try:
            self._send([100, request_id, channel, method], [] if params is None else [params])
            if not pending["event"].wait(timeout):
                self._send([101, request_id])
                raise ProtocolError("Services RPC timed out: " + method)
            if "error" in pending:
                # Server stack/data can contain request material. Only its bounded
                # public message is allowed through the bridge error boundary.
                raise ProtocolError(str(pending["error"])[:800])
            return pending.get("result")
        finally:
            with self._lock:
                self._pending.pop(request_id, None)

    def listen(self, channel, method, params, callback):
        self.start()
        request_id = self._id()
        with self._lock:
            self._listeners[request_id] = callback
        self._send([102, request_id, channel, method], params)
        return request_id

    def unlisten(self, request_id):
        with self._lock:
            self._listeners.pop(request_id, None)
        if not self._closed:
            self._send([103, request_id])

    def _read(self):
        process = self._proc
        try:
            line = process.stdout.readline(65537)
            hello = json.loads(line)
            if (len(line) > 65536 or hello.get("type") != "zcode-hello"
                    or hello.get("pid") != process.pid):
                raise ProtocolError("Invalid Services Server identity")
            with self._write_lock:
                process.stdin.write(json.dumps({
                    "type": "zcode-hello-ack", "version": "0.1.0",
                    "clientId": "coder-mcp-bridge",
                }).encode() + b"\n")
                process.stdin.flush()
            while not self._closed:
                frame_type, _, _, size = struct.unpack(">BIII", _exact(process.stdout, 13))
                if size > MAX_FRAME:
                    raise ProtocolError("RPC frame limit exceeded")
                frame = io.BytesIO(_exact(process.stdout, size))
                if frame_type != 1:
                    continue
                header, body = decode(frame), decode(frame)
                if header[0] == 200:
                    self._ready.set()
                    continue
                request_id = header[1]
                with self._lock:
                    pending = self._pending.get(request_id)
                    listener = self._listeners.get(request_id)
                if header[0] == 204 and listener:
                    # Never invoke callers on the reader: permission handling can
                    # issue an RPC response and must not deadlock its own reader.
                    listener(body)
                elif header[0] in {202, 203} and listener:
                    raise ProtocolError("Services event subscription failed")
                elif pending:
                    if header[0] == 201:
                        pending["result"] = body
                    else:
                        pending["error"] = body.get("message", "Services RPC failed") if isinstance(body, dict) else "Services RPC failed"
                    pending["event"].set()
        except Exception:
            self._error = "ZCode Services Server connection closed"
        finally:
            self._ready.set()
            with self._lock:
                for pending in self._pending.values():
                    pending["error"] = self._error or "Services RPC closed"
                    pending["event"].set()
                if self._proc is process:
                    self._proc = None
            if process.poll() is None:
                try:
                    process.stdin.close()
                    process.wait(timeout=5)
                except (OSError, subprocess.TimeoutExpired):
                    process.terminate()
            if not self._closed and self.on_disconnect:
                self.on_disconnect(self._error)

    def close(self):
        with self._lock:
            self._closed = True
            process = self._proc
        if process:
            try:
                process.stdin.close()
                process.wait(timeout=10)
            except (OSError, subprocess.TimeoutExpired):
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
