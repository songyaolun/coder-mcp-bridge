"""OpenCode native ACP adapter; peers are shared only by identical hard policies."""
from __future__ import annotations

from contextlib import contextmanager
import json
import os
import shutil
import subprocess
import threading
import uuid

from acp_transport import AcpClient, AcpRequestTimeout
from agent_control_plane import AdapterControlPlane, bounded, normalize_resources
from control_plane import ControlPlaneError
from resource_leases import ResourceLeaseStore
from zcode_protocol import ProtocolError


ACTION_ALIASES = {
    'write': 'edit', 'edit': 'edit', 'patch': 'edit', 'apply_patch': 'edit',
    'bash': 'shell', 'shell': 'shell', 'task': 'subagent', 'subagent': 'subagent',
    **{name: name for name in ('read', 'glob', 'grep', 'skill', 'question', 'webfetch', 'websearch')},
}


def discover_opencode():
    binary = os.environ.get('OPENCODE_BINARY') or shutil.which('opencode')
    if not binary or not os.path.isfile(binary):
        raise RuntimeError('OpenCode executable not found; install it or set OPENCODE_BINARY')
    return os.path.realpath(binary)


def choices(option):
    result = []
    for item in option.get('options') or []:
        if 'options' in item:
            result.extend(choices(item))
        elif 'value' in item:
            result.append(item)
    return result


class OpenCodePeer:
    def __init__(self, backend, env):
        self.backend, self.env = backend, env
        self.client = None
        self.initialized = {}
        self._lock = threading.RLock()
        self._owners = {}
        self._early = {}
        self._closed_sessions = set()
        self._dead = False

    def start(self):
        with self._lock:
            if self._dead:
                raise ProtocolError('OpenCode ACP peer has exited')
            if self.client:
                return
            self.client = self.backend.client_factory(
                [self.backend.binary, 'acp'], cwd=self.backend.process_cwd, env=self.env,
                client_capabilities={'session': {'compaction': {}}},
                request_handler=self._request_permission, on_event=self._on_event,
                on_disconnect=self._on_disconnect, logger=self.backend.logger)
            try:
                self.initialized = self.client.start()
            except Exception:
                self._dead = True
                self.client.close()
                raise

    def request(self, method, params, timeout=30):
        if self._dead:
            raise ProtocolError('OpenCode ACP peer has exited')
        return self.client.request(method, params, timeout=timeout)

    def register(self, session_id, runtime):
        with self._lock:
            self._closed_sessions.discard(session_id)
            previous = self._owners.get(session_id)
            if previous is not None and previous is not runtime:
                raise ControlPlaneError('OpenCode session is already managed', 'session_active')
            self._owners[session_id] = runtime
            early = self._early.pop(session_id, [])
        for event in early:
            runtime.on_update(event)

    def reserve(self, session_id, runtime):
        with self._lock:
            if session_id in self._owners:
                raise ControlPlaneError('OpenCode session is already managed', 'session_active')
            self._closed_sessions.discard(session_id)
            self._owners[session_id] = runtime

    def unregister(self, session_id, runtime):
        with self._lock:
            if self._owners.get(session_id) is runtime:
                self._owners.pop(session_id, None)
                self._early.pop(session_id, None)
                self._closed_sessions.add(session_id)

    def prepare_resume(self, session_id):
        with self._lock:
            self._closed_sessions.discard(session_id)

    def _on_event(self, message):
        if message.get('method') != 'session/update':
            return
        params = message.get('params') or {}
        sid, update = params.get('sessionId'), params.get('update') or {}
        with self._lock:
            owner = self._owners.get(sid)
            if owner is None and sid and sid not in self._closed_sessions:
                # OpenCode publishes commands/config before session/new returns its ID.
                if len(self._early) < 100:
                    self._early.setdefault(sid, []).append(update)
                    self._early[sid] = self._early[sid][-16:]
        if owner:
            owner.on_update(update)

    def _request_permission(self, method, params):
        if method == 'elicitation/create':
            return {'action': 'cancel'}
        if method != 'session/request_permission':
            raise NotImplementedError(method)
        with self._lock:
            owner = self._owners.get(params.get('sessionId'))
        allowed = owner is not None and owner.allow_permission(params.get('toolCall') or {})
        kind = 'allow_once' if allowed else 'reject_once'
        option = next((o.get('optionId') for o in params.get('options', []) if o.get('kind') == kind), None)
        if owner:
            owner.emit({'type': 'interaction.permission-approved' if allowed else 'interaction.permission-denied'})
        return {'outcome': {'outcome': 'selected', 'optionId': option} if option else {'outcome': 'cancelled'}}

    def _on_disconnect(self, error):
        with self._lock:
            self._dead = True
            owners = list(self._owners.values())
            self._owners.clear()
            self._early.clear()
        # AcpClient reaps the peer before this callback, so leases cannot be
        # released while a malformed/disconnected peer still executes tools.
        for owner in owners:
            owner.release_session_lock()
            owner.disconnect(error)

    def close(self):
        with self._lock:
            self._dead = True
        if self.client:
            self.client.close()

    def abort(self, error):
        # A failed session close leaves native work unconfirmed. Reap the peer
        # before notifying owners/releasing any of their workspace leases.
        self.close()
        self._on_disconnect(error)


class OpenCodeRuntime:
    def __init__(self, backend, args, emit, disconnect):
        self.backend, self.args, self.emit, self.disconnect = backend, dict(args), emit, disconnect
        self.cwd = os.path.realpath(args.get('cwd') or os.getcwd())
        self.session_id = args.get('threadId')
        self.peer = None
        self.options = []
        self._context, self._usage = {}, {}
        self._result, self._message_id = '', None
        self._tools = set()
        self._commands = set()
        self._cancelled = False
        self._closed = False
        self._owned = False
        self._accepting_guidance = True
        self._pending_guidance = []
        self._lock = threading.RLock()
        self._session_lease = None
        self._session_holder = None
        self._close_done = threading.Event()
        self._closing_thread = None

    def claim_session_lock(self):
        if self._session_lease is not None:
            return
        store = ResourceLeaseStore()
        holder = 'opencode-session-owner-' + uuid.uuid4().hex
        result = store.try_acquire(holder, {'opencode-session:' + self.session_id: 'exclusive'})
        if not result.get('acquired'):
            store.close()
            raise ControlPlaneError('OpenCode session is active in another run or bridge', 'session_active')
        self._session_lease, self._session_holder = store, holder

    def release_session_lock(self):
        with self._lock:
            store, holder = self._session_lease, self._session_holder
            self._session_lease, self._session_holder = None, None
        if store:
            store.release(holder)
            store.close()

    def session_params(self):
        roots = normalize_resources(self.cwd, self.args.get('workspaceAccess') or 'exclusive', self.args.get('resources'))
        additional = [r for r in roots if os.path.isabs(r) and r != self.cwd]
        return {'cwd': self.cwd, 'mcpServers': [], 'additionalDirectories': additional}

    def start(self, prompt):
        self.backend.validate(self.args)
        self.peer = self.backend.peer(self.args)
        params = self.session_params()
        method = 'session/resume' if self.session_id else 'session/new'
        if self.session_id:
            params['sessionId'] = self.session_id
            self.claim_session_lock()
            self.peer.reserve(self.session_id, self)
        try:
            response = self.peer.request(method, params)
        except AcpRequestTimeout:
            self.peer.abort('OpenCode ACP session startup timed out')
            raise
        self.session_id = response.get('sessionId') or self.session_id
        if not self.session_id:
            raise ProtocolError('OpenCode ACP did not return a session ID')
        self.claim_session_lock()
        self.options = response.get('configOptions') or []
        self.peer.register(self.session_id, self)
        self._owned = True
        if self.args.get('model'):
            model = self.args['model']
            self.select('model', '%s/%s' % (model['providerId'], model['modelId']))
        mode = self.args.get('mode')
        if mode in ('plan', 'build', 'edit'):
            self.select('mode', 'build' if mode == 'edit' else mode)
        if self.args.get('thoughtLevel'):
            self.set_thinking(self.args['thoughtLevel'])
        state = {'threadId': self.session_id, 'model': self.model_projection()}
        if prompt is not None:
            threading.Thread(target=self._prompt, args=(prompt,), daemon=True).start()
        else:
            self.close()
        return state

    def select(self, category, value):
        option = next((o for o in self.options if o.get('category') == category or o.get('id') == category), None)
        if not option:
            raise ControlPlaneError('OpenCode does not advertise config option ' + category, 'unsupported_capability')
        # Match the opaque value exactly. Provider IDs themselves may contain '/'.
        matches = [o for o in choices(option) if o.get('value') == value]
        if len(matches) != 1:
            raise ControlPlaneError('OpenCode config value is not a unique advertised choice', 'invalid_params')
        response = self.peer.request('session/set_config_option', {
            'sessionId': self.session_id, 'configId': option['id'], 'value': value})
        self.options = response.get('configOptions') or self.options

    def model_projection(self):
        option = next((o for o in self.options if o.get('category') == 'model'), {})
        requested = self.args.get('model')
        matches = requested and option.get('currentValue') == '%s/%s' % (requested.get('providerId'), requested.get('modelId'))
        model = requested if matches else {'providerId': 'opencode-catalog', 'modelId': option.get('currentValue')}
        return {'model': dict(model)}

    def set_thinking(self, level):
        if not self.peer or self._closed:
            raise ControlPlaneError('OpenCode session is not active', 'session_not_ready')
        self.select('thought_level', level)

    def _prompt(self, prompt):
        status, error = 'failed', None
        try:
            while True:
                if self._cancelled:
                    status = 'cancelled'
                    break
                self.emit({'type': 'agent.prompt-started', 'model': {**self.model_projection(), 'status': 'running'}})
                response = self.peer.request('session/prompt', {
                    'sessionId': self.session_id, 'prompt': [{'type': 'text', 'text': prompt}]},
                    timeout=int(self.args.get('timeout') or 900) + 30)
                self.add_usage(response.get('usage') or {})
                with self._lock:
                    if self._pending_guidance and not self._cancelled:
                        prompt = self._pending_guidance.pop(0)
                        continue
                    self._accepting_guidance = False
                    reason = response.get('stopReason')
                    status = 'cancelled' if self._cancelled or reason == 'cancelled' else (
                        'completed' if reason == 'end_turn' else 'failed')
                    if status == 'failed':
                        error = 'OpenCode stop reason: %s' % reason
                    break
        except Exception as exc:  # noqa: BLE001
            status = 'cancelled' if self._cancelled else 'failed'
            error = bounded(exc)
        finally:
            try:
                self.close()
            except Exception as exc:  # noqa: BLE001
                error = error or bounded(exc)
                if status == 'completed':
                    status = 'failed'
            self.emit({'type': 'settled', 'status': status, 'result': self._result,
                       'threadId': self.session_id, 'model': self.model_projection(),
                       'usage': dict(self._usage), 'context': dict(self._context), 'error': error})

    def add_usage(self, usage):
        fields = {'inputTokens': 'inputTokens', 'outputTokens': 'outputTokens', 'totalTokens': 'totalTokens',
                  'thoughtTokens': 'reasoningTokens', 'cachedReadTokens': 'cacheReadTokens',
                  'cachedWriteTokens': 'cacheWriteTokens'}
        for source, target in fields.items():
            if isinstance(usage.get(source), (int, float)):
                self._usage[target] = self._usage.get(target, 0) + int(usage[source])
        # This is root prompt/run usage, not historical session usage or child usage.
        self.emit({'type': 'usage.updated', 'usage': dict(self._usage)})

    def on_update(self, update):
        kind = update.get('sessionUpdate')
        child = (update.get('_meta') or {}).get('opencode/child-session')
        if child:
            self.emit({'type': 'native.child-session', 'detail': {'id': child.get('id'), 'depth': child.get('depth')}})
        if kind == 'agent_message_chunk' and not child:
            content = update.get('content') or {}
            if content.get('type') == 'text':
                mid = update.get('messageId')
                if mid is not None and mid != self._message_id:
                    self._message_id, self._result = mid, ''
                self._result += content.get('text', '')
                if len(self._result) > 12000:
                    prefix = '[Earlier response text truncated]\n'
                    self._result = prefix + self._result[-(12000-len(prefix)):]
                self.emit({'type': 'message.updated', 'result': self._result})
        elif kind == 'agent_thought_chunk':
            self.emit({'type': 'reasoning.started'})
            self.emit({'type': 'reasoning.ended'})
        elif kind in ('tool_call', 'tool_call_update'):
            tid = update.get('toolCallId')
            terminal = update.get('status') in ('completed', 'failed')
            if terminal:
                self._tools.discard(tid)
                self.emit({'type': 'tool.ended', 'toolCallId': tid})
            elif tid and tid not in self._tools:
                self._tools.add(tid)
                self.emit({'type': 'tool.started', 'toolCallId': tid,
                           'toolName': update.get('name') or update.get('title') or update.get('kind')})
        elif kind == 'config_option_update':
            self.options = update.get('configOptions') or self.options
            self.emit({'type': 'model.updated', 'model': self.model_projection()})
        elif kind == 'available_commands_update':
            self._commands = {x.get('name') for x in update.get('availableCommands', [])}
        elif kind == 'usage_update':
            self._context.update({'used': update.get('used'), 'window': update.get('size')})
            if update.get('cost'):
                self._context['sessionCost'] = update['cost']
            self.emit({'type': 'context.updated', 'context': dict(self._context)})
        elif kind in ('compaction_update', 'compaction_summary_chunk'):
            self.emit({'type': 'context.compaction', 'detail': {'status': update.get('status'),
                                                              'compactionId': update.get('compactionId')}})
        elif kind == 'session_info_update':
            meta = update.get('_meta') or {}
            if meta.get('opencode/retry'):
                self.emit({'type': 'model.retrying', 'phase': 'retrying'})

    def allow_permission(self, tool):
        name = tool.get('name') or ''
        kind = tool.get('kind')
        action = ACTION_ALIASES.get(name)
        blocked = self.backend.blocked_actions(self.args)
        if action in blocked or (kind in ('edit', 'delete', 'execute') and
                                 self.args.get('workspaceAccess') == 'shared'):
            return False
        paths = [x.get('path') for x in tool.get('locations', []) if isinstance(x, dict)]
        raw = tool.get('rawInput') or {}
        if isinstance(raw, dict):
            paths.extend(raw.get(k) for k in ('path', 'filePath', 'file_path', 'cwd') if raw.get(k))
        for item in tool.get('content') or []:
            if isinstance(item, dict) and item.get('type') == 'diff':
                paths.append(item.get('path'))
        paths = [p for p in paths if isinstance(p, str)]
        roots = normalize_resources(self.cwd, self.args.get('workspaceAccess') or 'exclusive', self.args.get('resources'))
        roots = [r for r in roots if os.path.isabs(r)]
        def allowed(path):
            path = os.path.realpath(path if os.path.isabs(path) else os.path.join(self.cwd, path))
            return any(os.path.commonpath([path, root]) == root for root in roots)
        return bool(paths) and all(allowed(path) for path in paths)

    def guide(self, prompt, *, interrupt=False):
        with self._lock:
            if self._closed or not self._accepting_guidance:
                raise ControlPlaneError('OpenCode run is terminal; resume using threadId', 'run_terminal')
            if interrupt and (not self.peer or not self.session_id):
                raise ControlPlaneError('OpenCode session is not ready', 'session_not_ready')
            self._pending_guidance.append(prompt)
        if interrupt:
            self.peer.client.notify('session/cancel', {'sessionId': self.session_id})

    def cancel(self):
        self._cancelled = True
        if self.peer and self.session_id and not self._closed:
            self.peer.client.notify('session/cancel', {'sessionId': self.session_id})

    def refresh(self):
        return None  # Native responses and notifications are authoritative.

    @contextmanager
    def maintenance(self):
        # Terminal runs have released their leases. Reacquire before any native
        # operation capable of compacting/continuing a saved session.
        leases = ResourceLeaseStore()
        holder = 'acp-maintenance-' + uuid.uuid4().hex
        resources = normalize_resources(self.cwd, 'exclusive', self.args.get('resources'))
        acquired = leases.try_acquire(holder, resources).get('acquired')
        if not acquired:
            leases.close()
            raise ControlPlaneError('Workspace is busy; retry after its active run', 'resource_busy')
        try:
            self.claim_session_lock()
            yield
        finally:
            self.release_session_lock()
            leases.release(holder)
            leases.close()

    def branch(self, *, target_kind='latestCheckpoint', target_id=None, turn_index=None):
        if target_kind != 'latestCheckpoint' or target_id or turn_index is not None:
            raise ControlPlaneError('OpenCode ACP supports latest conversation fork only, not message/checkpoint anchors',
                                    'unsupported_capability')
        with self.maintenance():
            peer = self.backend.peer(self.args)
            caps = peer.initialized.get('agentCapabilities', {}).get('sessionCapabilities', {})
            if 'fork' not in caps:
                raise ControlPlaneError('OpenCode ACP does not advertise fork', 'unsupported_capability')
            result = peer.request('session/fork', {**self.session_params(), 'sessionId': self.session_id})
            sid = result.get('sessionId')
            if not sid:
                raise ProtocolError('OpenCode ACP fork did not return sessionId')
            peer.request('session/close', {'sessionId': sid})
            return {'threadId': sid, 'parentThreadId': self.session_id}

    def context(self, *, action='inspect', instructions=None):
        if action == 'inspect':
            return {'context': dict(self._context), 'usage': dict(self._usage),
                    'usageScope': 'current-run/root-prompts; sessionCost is cumulative',
                    'unreportedCounters': ['modelRequests']}
        if action != 'compact' or instructions:
            raise ControlPlaneError('ACP compaction supports no custom instructions', 'unsupported_capability')
        with self.maintenance():
            peer = self.backend.peer(self.args)
            peer.reserve(self.session_id, self)
            attached = False
            try:
                try:
                    response = peer.request('session/resume', {**self.session_params(), 'sessionId': self.session_id})
                except AcpRequestTimeout:
                    peer.abort('OpenCode ACP session resume timed out')
                    raise
                attached = True
                peer.register(self.session_id, self)
                self.options = response.get('configOptions') or self.options
                if 'compact' not in self._commands:
                    raise ControlPlaneError('OpenCode does not advertise compact', 'unsupported_capability')
                result = peer.request('session/prompt', {'sessionId': self.session_id,
                                      'prompt': [{'type': 'text', 'text': '/compact'}]}, timeout=180)
                if result.get('stopReason') != 'end_turn':
                    raise ProtocolError('OpenCode compaction did not complete')
                return {'context': dict(self._context), 'compacted': True}
            finally:
                try:
                    if attached:
                        try:
                            peer.request('session/close', {'sessionId': self.session_id})
                        except Exception:
                            peer.abort('OpenCode ACP could not confirm session shutdown')
                            raise
                finally:
                    peer.unregister(self.session_id, self)

    def close(self):
        with self._lock:
            if self._closed:
                already_closing = True
            else:
                already_closing = False
                self._closed = True
                self._closing_thread = threading.get_ident()
                self._accepting_guidance = False
        if already_closing:
            if self._closing_thread != threading.get_ident() and not self._close_done.wait(35):
                raise ProtocolError('OpenCode session shutdown is still pending')
            return
        try:
            if self.peer and self.session_id:
                try:
                    if self._owned and not self.peer._dead:
                        try:
                            self.peer.request('session/close', {'sessionId': self.session_id}, timeout=15)
                        except Exception:
                            self.peer.abort('OpenCode ACP could not confirm session shutdown')
                            raise
                finally:
                    self._owned = False
                    self.peer.unregister(self.session_id, self)
        finally:
            self.release_session_lock()
            self._close_done.set()


class OpenCodeControlPlane(AdapterControlPlane):
    def _validate(self, args):
        super()._validate(args)
        self.backend.validate(args)


class OpenCodeBackend:
    name = 'opencode'

    def __init__(self, binary=None, *, logger=None, client_factory=AcpClient, process_cwd=None):
        self.binary = binary
        self.logger = logger or (lambda _message: None)
        self.client_factory = client_factory
        self.process_cwd = os.path.realpath(process_cwd or os.path.dirname(__file__))
        self._lock = threading.RLock()
        self._peers = {}

    @property
    def capabilities(self):
        return {'prompt': True, 'durableGoal': False, 'guidance': ['guide', 'interrupt', 'cancel', 'set-thinking'],
                'reasoningEvents': True, 'usage': 'reported-root-turn-tokens', 'branch': True,
                'branchTargets': ['latestCheckpoint'], 'compact': True, 'backgroundTasks': False,
                'transport': 'acp-v1', 'permissionProtocol': 'acp-request-permission',
                'filesystemBoundary': 'native-profile-policy', 'shellBoundary': 'native-profile-policy; denied-in-shared',
                'toolPolicy': 'native-hard-deny-known-actions'}

    @staticmethod
    def blocked_actions(args):
        blocked = {'edit', 'shell', 'subagent'} if args.get('workspaceAccess') == 'shared' else set()
        blocked.update(ACTION_ALIASES[name] for name in args.get('toolDenylist') or [])
        return blocked

    @staticmethod
    def validate(args):
        if args.get('toolAllowlist'):
            raise ControlPlaneError('OpenCode ACP cannot enforce a closed tool allowlist', 'unsupported_capability')
        unknown = set(args.get('toolDenylist') or []) - ACTION_ALIASES.keys()
        if unknown:
            raise ControlPlaneError('Unknown OpenCode permission action in toolDenylist', 'unsupported_capability')
        mode = args.get('workspaceAccess') or 'exclusive'
        if any((r.get('mode') or 'exclusive') != mode for r in args.get('resources') or [] if os.path.isabs(r.get('key', ''))):
            raise ControlPlaneError('OpenCode ACP does not support mixed writable/read-only directory grants',
                                    'unsupported_capability')

    def peer(self, args):
        self.validate(args)
        try:
            config = json.loads(os.environ.get('OPENCODE_CONFIG_CONTENT') or '{}')
            if not isinstance(config, dict):
                raise ValueError('not an object')
            policies = config.setdefault('policies', [])
            permissions = config.setdefault('permissions', [])
            if not isinstance(policies, list) or not isinstance(permissions, list):
                raise ValueError('invalid policy arrays')
        except (ValueError, TypeError):
            raise ControlPlaneError('OPENCODE_CONFIG_CONTENT must contain valid v2 config policy arrays', 'invalid_params')
        for action in sorted(self.blocked_actions(args)):
            policies.append({'action': 'tool.use', 'resource': action + ':*', 'effect': 'deny'})
        if args.get('workspaceAccess') == 'shared':
            original_denies = [dict(p) for p in policies if p.get('effect') == 'deny']
            policies.append({'action': 'tool.use', 'resource': '*', 'effect': 'deny'})
            policies.extend({'action': 'tool.use', 'resource': action + ':*', 'effect': 'allow'}
                            for action in ('read', 'glob', 'grep'))
            # Retain stronger caller restrictions even when lifting our broad deny.
            policies.extend(original_denies)
        # Defense in depth, not a sandbox: saved native Allow always approvals may
        # bypass soft ask rules. Hard denies above remain authoritative.
        permissions.append({'action': 'edit', 'resource': '*', 'effect': 'ask'})
        encoded = json.dumps(config, sort_keys=True, separators=(',', ':'))
        with self._lock:
            peer = self._peers.get(encoded)
            if peer is None or peer._dead:
                env = dict(os.environ)
                env['OPENCODE_CONFIG_CONTENT'] = encoded
                self.binary = self.binary or discover_opencode()
                peer = OpenCodePeer(self, env)
                self._peers[encoded] = peer
                peer.start()
            return peer

    def probe(self):
        try:
            self.binary = self.binary or discover_opencode()
            result = subprocess.run([self.binary, '--version'], capture_output=True, text=True, timeout=10)
            return {'available': result.returncode == 0, 'version': result.stdout.strip()[:100],
                    'binary': self.binary, 'transport': 'acp-v1'}
        except Exception as exc:  # noqa: BLE001
            return {'available': False, 'reason': bounded(exc, 500)}

    def create_runtime(self, args, emit, disconnect):
        self.validate(args)
        return OpenCodeRuntime(self, args, emit, disconnect)

    def list_sessions(self, args):
        peer = self.peer(args)
        target = args.get('adoptThreadId')
        directory = args.get('cwd') or args.get('workspace')
        params = {'cwd': os.path.realpath(directory or os.getcwd())} if directory or not target else {}
        limit = min(max(int(args.get('limit', 20)), 1), 100)
        seen = set()
        for _ in range(100):
            result = peer.request('session/list', params)
            rows = [{'threadId': x.get('sessionId'), 'cwd': x.get('cwd'), 'title': x.get('title'),
                     'status': 'idle', 'updatedAt': x.get('updatedAt')}
                    for x in result.get('sessions', [])]
            if not target:
                return rows[:limit]
            found = next((row for row in rows if row['threadId'] == target), None)
            if found:
                return [found]
            cursor = result.get('nextCursor')
            if not cursor:
                return []
            if cursor in seen:
                raise ProtocolError('OpenCode session/list repeated its pagination cursor')
            seen.add(cursor)
            params['cursor'] = cursor
        raise ControlPlaneError('OpenCode session lookup exceeded 100 pages', 'pagination_limit')

    def close(self):
        with self._lock:
            peers, self._peers = list(self._peers.values()), {}
        for peer in peers:
            peer.close()

    def control_plane(self, *, max_concurrency=0, logger=None, terminal_outbox=None):
        options = {'max_concurrency': max_concurrency, 'logger': logger or self.logger}
        if terminal_outbox is not None:
            options['terminal_outbox'] = terminal_outbox
        return OpenCodeControlPlane(self, **options)
