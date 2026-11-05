# DeepSeek Harness（dsh）后端

Bridge 使用 `dsh --profile acp` 的原生 ACP v1 JSON-RPC，直接复用 dsh 已配置的账户、模型和持久会话，不复制凭据，不经过 ZCode/Pi。

## 使用

安装并配置 dsh 后，重启或重新连接 MCP server `vibe_bridge`，让已启动的 Python 进程加载新增后端。无需改 MCP command，也不需要改变默认后端。

```json
{"action":"list"}
```

确认 `availableBackends.dsh.available=true` 后，调用 `agent-config`：

```json
{"action":"set","backend":"dsh"}
```

之后用 `agent-start`：

```json
{
  "cwd":"/absolute/project",
  "prompt":"阅读项目说明，执行指定任务并验证结果。",
  "model":{"providerId":"deepseek-account","modelId":"deepseek-flash"},
  "thoughtLevel":"low",
  "timeout":900
}
```

模型必须是当前 ACP 配置目录中的精确 provider/model 组合，以上账户模型仅在本机实测可用；其他机器请使用其已配置路由。省略 `model` 沿用 dsh profile 的默认路由，不会自动切换到其他后端。ACP 的模型选项是字符串化 JSON 二元组，Bridge 根据实时选项匹配。思考档位也按实时选项验证，不假设所有模型都支持同一组值。

继续使用统一 `agent-wait`（传最新 `afterRevision`）、`agent-observe`、`agent-control` 和 `agent-close`。运行不会因切换后端而重新绑定。

## 生命周期与能力

- 一个运行一个 ACP 子进程，各工作区由已有跨进程资源租约调度。终态前关闭原生 session、刷新持久数据并退出进程，然后释放租约。
- `guide` 排到当前 prompt 后；`interrupt` 发原生取消，再执行引导；`cancel` 取消运行。取消和超时沿用统一终态事件。
- `threadId` 为原生持久 session UUID。`agent-start` 携带该 ID 和原工作区会通过 `session/resume` 续接，不重放旧输出。
- `agent-recover` 列出 Bridge 自己登记的 dsh 会话（不是全部原生历史），默认索引在 `~/.cache/coder-mcp-bridge/dsh-sessions`。会话内容仍由 dsh 保存。
- 文本、已提交思考片段、工具开始/结束和上下文 `used/size` 映射为统一事件。ACP 不提供逐 token 流或精确 input/output/cache Token；`usage=context-only`，未获取的 Token 字段为零不代表免费。
- Branch、compact、durable goal、后台任务投影目前不支持。请求明确返回不支持，不伪造结果。

## 权限

首版只接受 exclusive 单工作区。shared 只读模式、额外绝对目录、tool allowlist/denylist 均拒绝，避免以提示词冒充强制隔离。普通工具由当前 dsh profile 的原生策略管理；Bridge 不声称能替换用户 profile 或约束所有读取路径。ACP 请求额外审批时，Bridge 自动拒绝，不自动批准升级。

可选环境变量：`DSH_BINARY` 指定可执行文件，`DSH_PROFILE` 指定包含 ACP 的原生 profile（默认 `acp`），`DSH_BRIDGE_SESSION_DIR` 指定 Bridge 会话索引目录。设置 `AGENT_MCP_DEFAULT_BACKEND=dsh` 可让新连接默认使用 dsh；本次实现不修改现有默认配置。

## 验证

```sh
.venv/bin/python -m unittest tests.test_dsh_backend tests.test_backend_manager -v
.venv/bin/python server.py --probe
```

显式运行真实账户/工具验证（会调用模型）：

```sh
.venv/bin/python -m tests.dsh_live_smoke
```

2026-11-05 验收：全仓 149 项测试通过、1 项跳过、6 项子测试通过。23 项 dsh 测试覆盖 NDJSON/并发响应、模型精确选择、权限请求拒绝、恢复、guide/interrupt/cancel、异常清理与输出保留。真实 MCP 验证通过两个独立工作区并发写文件、关闭后续接读取，以及运行取消；已用 dsh 本身完成测试代码执行任务。

协议依据：安装包 `@deepseek-ai/dsh-acp` / `dsh-acp-app` 的 README 与 ACP 实现，以及 [官方协议文档](https://github.com/deepseek-ai/deepseek-harness/blob/master/packages/acp/acp/README.md)。安装版本 0.2.1-rc.5 已实测新建、精确账户模型选择、思考配置及真实 prompt。
