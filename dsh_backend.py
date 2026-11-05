"""DeepSeek Harness adapter using the native ACP v1 NDJSON transport.

DSH owns its profile/credentials. No terminal scraping, session-file guessing,
provider fallback or private DSH API is used here.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import threading
import uuid

from acp_transport import AcpClient as DshAcpClient
from agent_control_plane import AdapterControlPlane, bounded
from control_plane import ControlPlaneError
from zcode_protocol import ProtocolError


def discover_dsh():
    binary = os.environ.get('DSH_BINARY') or shutil.which('dsh')
    if not binary or not os.path.isfile(binary):
        raise RuntimeError('DeepSeek Harness executable not found; install dsh or set DSH_BINARY')
    return os.path.realpath(binary)


def _choices(option):
    result = []
    for item in option.get('options') or []:
        if 'options' in item:
            result.extend(_choices(item))
        elif 'value' in item:
            result.append(item)
    return result


class DshRuntime:
    def __init__(self, backend, args, emit, disconnect):
        self.backend, self.args, self.emit, self.disconnect = backend, dict(args), emit, disconnect
        self.client = None
        self.thread_id = None
        self.options = []
        self._result = ''
        self._message_id = None
        self._context = {}
        self._cancelled = False
        self._closed = False
        self._pending_guide = []
        self._accepting_guidance = True
        self._lock = threading.RLock()

    def start(self, prompt):
        self.backend.validate(self.args)
        cwd = os.path.realpath(self.args.get('cwd') or os.getcwd())
        self.client = self.backend.client_factory(
            [self.backend.binary, '--profile', self.backend.profile], cwd=cwd,
            on_event=self._on_event, on_disconnect=self.disconnect, logger=self.backend.logger)
        self.client.start()
        method = 'session/resume' if self.args.get('threadId') else 'session/new'
        params = {'cwd': cwd, 'mcpServers': []}
        if self.args.get('threadId'):
            params['sessionId'] = self.args['threadId']
        response = self.client.request(method, params)
        self.thread_id = response.get('sessionId') or self.args.get('threadId')
        if not self.thread_id:
            raise ProtocolError('DSH ACP did not return a sessionId')
        self.options = response.get('configOptions') or []
        model = self.args.get('model')
        if model:
            self._select('model', model)
        if self.args.get('thoughtLevel'):
            self.set_thinking(self.args['thoughtLevel'])
        self.backend.remember(self.thread_id, cwd)
        state = {'threadId': self.thread_id, 'model': self._model()}
        if prompt is not None:
            threading.Thread(target=self._prompt, args=(prompt,), daemon=True).start()
        else:
            self.close()
        return state

    def _model(self):
        option = next((x for x in self.options if x.get('category') == 'model' or x.get('id') == 'model'), {})
        selected = option.get('currentValue')
        try:
            provider, model = json.loads(selected)
        except (TypeError, ValueError):
            provider, model = 'dsh-catalog', selected
        return {'model': {'modelId': model, 'providerId': provider}}

    def _select(self, category, value):
        option = next((x for x in self.options if x.get('category') == category or x.get('id') == category), None)
        if not option:
            raise ControlPlaneError('DSH does not advertise config option: ' + category, 'unsupported_capability')
        values = _choices(option)
        if isinstance(value, dict):
            # Public model objects identify a route. Match an opaque live choice, never
            # silently discard providerId or change to another provider's same model.
            provider, model = value.get('providerId'), value.get('modelId')
            candidates = {model} if provider == 'dsh-catalog' else {
                '%s/%s' % (provider, model), '%s:%s' % (provider, model)}
        else:
            candidates = {str(value)}
        matches = []
        for choice in values:
            matched = str(choice['value']) in candidates or str(choice.get('name')) in candidates
            if isinstance(value, dict):
                try:
                    matched |= json.loads(choice['value']) == [value.get('providerId'), value.get('modelId')]
                except (TypeError, ValueError):
                    pass
            if matched:
                matches.append(choice)
        if len(matches) != 1:
            raise ControlPlaneError('DSH config value is not a unique advertised choice', 'invalid_params')
        response = self.client.request('session/set_config_option', {
            'sessionId': self.thread_id, 'configId': option['id'], 'value': matches[0]['value']})
        self.options = response.get('configOptions') or self.options

    def set_thinking(self, level):
        category = next((x.get('id') for x in self.options if x.get('category') in ('thought_level', 'reasoning_effort')
                         or x.get('id') in ('thought_level', 'reasoning_effort')), 'reasoning_effort')
        self._select(category, level)

    def _prompt(self, prompt):
        status, error = 'failed', None
        try:
            while True:
                if self._cancelled:
                    response = {'stopReason': 'cancelled'}
                    cancelled = True
                    break
                # ACP admits a prompt, not an individual provider request. Do not
                # count one model request per prompt when a turn may use many steps.
                self.emit({'type': 'agent.prompt-started',
                           'model': {**self._model(), 'status': 'running'}, 'phase': 'agent'})
                response = self.client.request('session/prompt', {
                    'sessionId': self.thread_id, 'prompt': [{'type': 'text', 'text': prompt}]},
                    timeout=int(self.args.get('timeout') or 900) + 30)
                with self._lock:
                    if self._pending_guide and not self._cancelled:
                        prompt = self._pending_guide.pop(0)
                        continue
                    self._accepting_guidance = False
                    cancelled = self._cancelled
                    break
            reason = response.get('stopReason')
            status = 'cancelled' if cancelled or reason == 'cancelled' else (
                'completed' if reason == 'end_turn' else 'failed')
            if status == 'failed':
                error = 'DSH stop reason: %s' % reason
        except Exception as exc:  # noqa: BLE001
            status = 'cancelled' if self._cancelled else 'failed'
            error = bounded(exc)
        finally:
            # Even a failed session/close must not suppress the terminal event.
            try:
                self.close()
            except Exception as exc:  # noqa: BLE001
                error = error or bounded(exc)
                if status == 'completed':
                    status = 'failed'
            self.emit({'type': 'settled', 'status': status, 'result': self._result,
                       'threadId': self.thread_id, 'context': self._context, 'error': error})

    def _on_event(self, message):
        if message.get('method') == 'bridge/permission_declined':
            self.emit({'type': 'interaction.declined'})
            return
        params = message.get('params') or {}
        if message.get('method') != 'session/update' or (self.thread_id and params.get('sessionId') != self.thread_id):
            return
        update = params.get('update') or {}
        kind = update.get('sessionUpdate')
        if kind == 'agent_message_chunk':
            content = update.get('content') or {}
            if content.get('type') == 'text':
                message_id = update.get('messageId')
                if message_id is not None and message_id != self._message_id:
                    self._message_id, self._result = message_id, ''
                self._result += content.get('text', '')
                if len(self._result) > 12000:
                    marker = '[Earlier response text truncated]\n'
                    self._result = marker + self._result[-(12000-len(marker)):]
                self.emit({'type': 'message.updated', 'result': self._result})
        elif kind == 'agent_thought_chunk':
            self.emit({'type': 'reasoning.started'})
            self.emit({'type': 'reasoning.ended'})
        elif kind in ('tool_call', 'tool_call_update'):
            terminal = update.get('status') in ('completed', 'failed')
            if kind == 'tool_call' or terminal:
                self.emit({'type': 'tool.ended' if terminal else 'tool.started',
                           'toolCallId': update.get('toolCallId'), 'toolName': update.get('title') or update.get('kind')})
        elif kind == 'usage_update':
            self._context = {'used': update.get('used'), 'window': update.get('size')}
            self.emit({'type': 'context.updated', 'context': self._context})
        elif kind == 'config_option_update':
            self.options = update.get('configOptions') or self.options
            self.emit({'type': 'model.updated', 'model': self._model()})

    def guide(self, prompt, *, interrupt=False):
        with self._lock:
            if self._closed or not self._accepting_guidance:
                raise ControlPlaneError('DSH session is closed; resume using threadId', 'run_terminal')
            if interrupt and (not self.client or not self.thread_id):
                raise ControlPlaneError('DSH ACP session is not ready', 'session_not_ready')
            self._pending_guide.append(prompt)
        if interrupt:
            self.client.notify('session/cancel', {'sessionId': self.thread_id})

    def cancel(self):
        self._cancelled = True
        if self.client and self.thread_id:
            self.client.notify('session/cancel', {'sessionId': self.thread_id})

    def refresh(self):
        return None  # ACP semantic updates are authoritative; no polling API exists.

    def branch(self, **_kwargs):
        raise ControlPlaneError('DSH ACP does not support branching', 'unsupported_capability')

    def context(self, *, action='inspect', instructions=None):
        if action != 'inspect':
            raise ControlPlaneError('DSH ACP does not support compaction', 'unsupported_capability')
        return {'context': dict(self._context)}

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
        if self.client:
            try:
                if self.thread_id:
                    self.client.request('session/close', {'sessionId': self.thread_id}, timeout=10)
            finally:
                self.client.close()


class DshControlPlane(AdapterControlPlane):
    def _validate(self, args):
        super()._validate(args)
        self.backend.validate(args)


class DshBackend:
    name = 'dsh'
    capabilities = {
        'prompt': True, 'durableGoal': False, 'guidance': ['guide', 'interrupt', 'cancel', 'set-thinking'],
        'reasoningEvents': True, 'usage': 'context-only', 'branch': False, 'compact': False,
        'backgroundTasks': False, 'permissionProtocol': 'acp-request-permission-deny-escalation',
        'filesystemBoundary': 'native-profile-policy', 'shellBoundary': 'native-profile-policy',
        'toolPolicy': 'native-profile-policy',
    }

    def __init__(self, binary=None, *, profile=None, session_dir=None, client_factory=DshAcpClient, logger=None):
        self.binary = binary
        self.profile = profile or os.environ.get('DSH_PROFILE') or 'acp'
        self.session_dir = os.path.realpath(session_dir or os.environ.get('DSH_BRIDGE_SESSION_DIR')
                                           or os.path.expanduser('~/.cache/coder-mcp-bridge/dsh-sessions'))
        self.client_factory, self.logger = client_factory, logger or (lambda _message: None)

    def probe(self):
        try:
            self.binary = self.binary or discover_dsh()
            result = subprocess.run([self.binary, '--version'], capture_output=True, text=True, timeout=10)
            return {'available': result.returncode == 0, 'version': result.stdout.strip()[:100],
                    'binary': self.binary, 'profile': self.profile}
        except Exception as exc:  # noqa: BLE001
            return {'available': False, 'reason': bounded(exc, 500)}

    @staticmethod
    def validate(args):
        if args.get('workspaceAccess', 'exclusive') != 'exclusive':
            raise ControlPlaneError('DSH has no bridge-enforced read-only workspace policy', 'unsupported_capability')
        if args.get('toolAllowlist') or args.get('toolDenylist'):
            raise ControlPlaneError('DSH ACP cannot enforce tool allow/deny lists', 'unsupported_capability')
        cwd = os.path.realpath(args.get('cwd') or os.getcwd())
        if any(os.path.isabs(r.get('key', '')) and os.path.realpath(r['key']) != cwd for r in args.get('resources', [])):
            raise ControlPlaneError('DSH ACP supports one workspace only', 'unsupported_capability')

    def create_runtime(self, args, emit, disconnect):
        self.validate(args)
        self.binary = self.binary or discover_dsh()
        return DshRuntime(self, args, emit, disconnect)

    def remember(self, session_id, cwd):
        # Only index bridge-created/resumed sessions; native history stays with DSH.
        uuid.UUID(session_id)
        os.makedirs(self.session_dir, exist_ok=True)
        path = os.path.join(self.session_dir, session_id + '.json')
        temporary = path + '.' + uuid.uuid4().hex + '.tmp'
        with open(temporary, 'w', encoding='utf-8') as handle:
            json.dump({'threadId': session_id, 'cwd': cwd}, handle)
        os.replace(temporary, path)

    def list_sessions(self, args):
        import glob
        result = []
        paths = sorted(glob.glob(os.path.join(self.session_dir, '*.json')), key=os.path.getmtime, reverse=True)
        for path in paths:
            try:
                with open(path, encoding='utf-8') as handle:
                    item = json.load(handle)
                if args.get('cwd') and item.get('cwd') != os.path.realpath(args['cwd']):
                    continue
                result.append({**item, 'status': 'idle', 'updatedAtMs': int(os.path.getmtime(path)*1000)})
            except (OSError, ValueError):
                continue
            if len(result) >= min(max(int(args.get('limit', 20)), 1), 100):
                break
        return result

    def control_plane(self, *, max_concurrency=0, logger=None, terminal_outbox=None):
        options = {'max_concurrency': max_concurrency, 'logger': logger or self.logger}
        if terminal_outbox is not None:
            options['terminal_outbox'] = terminal_outbox
        return DshControlPlane(self, **options)

    def close(self):
        # Runtime ownership is per run; the control plane closes each child first.
        return None
