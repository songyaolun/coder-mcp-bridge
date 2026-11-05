# OpenCode 原生 ACP 后端

`backend=opencode` 默认执行 `opencode acp`，与 dsh 共用 `acp_transport.py`。MCP 工具名和调用方式保持不变，不再启动HTTP服务、订阅SSE或轮询idle。

```json
{"action":"set","backend":"opencode"}
```

```json
{
  "cwd":"/absolute/project",
  "prompt":"执行任务并验证结果。",
  "model":{"providerId":"your-provider","modelId":"your-model"},
  "thoughtLevel":"low"
}
```

provider/model组合必须匹配ACP实时模型目录。provider本身可包含斜线，Bridge不会随意拆分，也不自动换路由。effort按模型实时选项验证，不能假定所有模型都有low/high。

## 进程和会话

- 同一权限策略复用一个ACP进程，不同shared/denylist配置使用不同peer。文件路径授权由会话参数与原生profile管理。
- 每个run拥有自己的session与事件监听器。正常终态只关闭session，保留peer供其他run使用；退出Bridge时回收所有peer。
- 原生prompt响应决定终态。guide排队执行，interrupt取消当前prompt后执行引导，cancel仅取消对应session。
- 持久session增加跨Bridge进程的逻辑独占租约，防止不同policy peer同时恢复同一session。失败的resume不会关闭其他owner的会话。
- session关闭无法确认或session启动超时时，先终止对应peer再发布失败/释放租约；同peer的其他任务也会失败，之后可从持久session恢复。不会为了保住peer而留下不受控的工具任务。
- fork和compact对终态会话重新获取工作区与session租约，忙时返回resource_busy；不会在别的任务使用工作区时偷偷压缩旧会话。

## 功能

- `agent-recover`按cwd列出原生session；指定adoptThreadId时分页查找，不受普通列表默认20项截断。100页的查找边界会显式返回pagination_limit。
- `agent-start`带threadId续接持久会话；已验证关闭整个Bridge进程后重连续接。
- `agent-branch`支持latestCheckpoint，含义为最新已保存的会话历史；不是文件系统checkpoint。指定message/checkpoint锚点或turnIndex暂时明确拒绝，避免悄悄分叉错位置。
- `agent-context compact`使用原生`/compact`及压缩事件；自定义压缩instructions暂不支持。
- root文本按messageId更新，保留最终回答；子会话文本不覆盖根回答，工具事件保留原生ID。
- 用量按本轮run的root prompt响应累加input/output/total、reasoning和cache字段。原生session累计费用单独放context.sessionCost，不重复加到每轮Token；子会话用量不冒充根会话用量。ACP未提供的精确modelRequests仍为未报告，不按一次prompt伪造一次模型调用。

## 权限边界

只读shared使用进程级原生`tool.use`硬策略：先拒绝全部工具，再仅开放read/glob/grep，保留调用者更严格的deny。shell、subagent及未知MCP工具不会因为同进程里有可写任务而获得权限。不同策略分peer，不改用户的全局/项目配置。

known toolDenylist映射到原生action的硬deny：write/edit/patch→edit，bash/shell→shell，task→subagent，其余已核对action为read/glob/grep/skill/question/webfetch/websearch。未知名称、closed allowlist和同一任务混合可写/只读额外目录明确返回unsupported_capability。

exclusive沿用native-profile-policy。Bridge对出现的权限请求仅根据结构化路径批准一次，不读取title猜路径，不批准always。**原生既有Allow always可能绕过后续ask**，因此不声称exclusive文件路径或shell被Bridge强制沙箱隔离；调用者声明的路径不是OS级sandbox。这个限制已通过临时session恢复负例验证。

## 升级与验证

重连/重启`vibe_bridge`使已启动的Python进程加载新适配层。默认后端仍由原配置决定，本次不会自动将全部任务切到OpenCode。

```sh
.venv/bin/python -m pytest -q
.venv/bin/python server.py --probe
```

真实账户/MCP验收（会调用模型，测试脚本使用本机配置的magpie/deepseek + deepseek-flash路由）：

```sh
.venv/bin/python -m tests.opencode_acp_live_smoke
```

旧HTTP实现保留在`opencode_http_backend.py`供显式对照；默认注册路径不导入或选择它，没有自动fallback。旧启动参数在本机2.0.30已失效，详情见[迁移评估](opencode-acp-assessment.md)。
