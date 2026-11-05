# dsh 接入状态

2026-11-05 用户要求为当前 vibe_bridge 支持 DeepSeek Harness（dsh）。已完成增量接入，保留原有未提交修改，不改默认后端或凭据。

- [x] 安装版本0.2.1-rc.5原生ACP协议及模型目录核对；参考官方文档和本机实际协议。
- [x] 新增dsh_backend.py，注册BackendManager与MCP backend枚举。
- [x] 生命周期：异步启动、上下文/文本/工具事件、精确模型选择、思考配置、guide/interrupt/cancel、原生resume及关闭。
- [x] 能力真实披露：无精确Token用量、分叉/压缩/goal；拒绝无法强制的shared/工具列表/额外目录；自动拒绝权限升级。
- [x] dsh本身派发完成测试实现；主代理复核并修正异常清理、读帧故障先回收进程、保留最后回答与启动期interrupt。
- [x] 149 passed / 1 skipped / 6 subtests passed；真实MCP并发写文件、跨进程续接和取消均通过。
- [x] 使用与验证说明 docs/dsh-backend.md；可重复真实验证 tests/dsh_live_smoke.py。

当前对话的MCP Python进程启动于修改前，不会热加载模块。用户重新连接或重启vibe_bridge后，`agent-config {"action":"list"}` 会出现dsh；然后set backend=dsh即可。本机MCP入口仍为 /Users/yolen/coder-mcp-bridge/server.py，无需改command。

临时完整验收日志：/tmp/vibe-dsh-validation/pytest-final.log、live-final.log、delegate.log。项目中不保存凭据。

OpenCode ACP评估完成（本机2.0.30）：核心任务/并发/模型/思考/resume/fork/compact/cancel及reasoning/cache-read用量已实测；建议公共ACP传输＋OpenCode共享peer。旧HTTP启动参数 --no-mdns 已失效，不能当作可用兜底。权限负例及指定消息边界等仍是迁移门槛。报告 docs/opencode-acp-assessment.md；本轮未改运行实现。

迁移已实施：OpenCode默认使用ACP；公共acp_transport.py与dsh复用。按权限策略共用peer，跨进程session锁、恢复分页及关闭异常回收已补齐。真实MCP并发/跨进程续接/fork/compact/shared只读负例/denylist/cancel通过。只提交本轮ACP相关修改，保留原有outbox等未提交开发内容。
