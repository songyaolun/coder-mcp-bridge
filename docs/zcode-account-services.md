# ZCode 内置账号通道

适用：包含本文件的 bridge revision。核对的官方源码为
`zai-org/ZCode@29628c9acdb81b703bbd4080c207a0e7ce5e276e`（Server 3.14.3）。

直接 app-server 不装配内置账号来源。选择 services 后，Bridge 启动官方 Services
Server，由它完成 Account Provider Config 同步、请求期账号鉴权和凭据刷新。
Bridge 只转发会话及权限协议，不解密、复制或持久化账号凭据，不创建 Personal Provider。
普通自定义 provider 的旧 direct 通道保持默认行为。

## 配置

解压完整的官方 integrated CLI distribution，使用普通 Node.js 24.14.0+；运行包需
包含 `server/remote/zcode-server.cjs`、`agent/zcode.cjs`、配置及运行依赖。不要只复制
两个 JS 文件或使用 Electron 代替 Node。发行包构建见官方 `scripts/build-zcode.mjs`。
安装、登录和更新与 bridge 源码独立。

```toml
[mcp_servers.vibe_bridge.env]
AGENT_MCP_DEFAULT_BACKEND = "zcode"
ZCODE_PROTOCOL_TRANSPORT = "services"
ZCODE_SERVICES_RUNTIME = "/absolute/path/to/extracted/zcode"
ZCODE_SERVICES_NODE = "/absolute/path/to/node"
```

重连 MCP 后，agent-config 的 ZCode probe 版本标记应为 services-runtime。账号资格与
可用模型以官方已登录状态和模型目录为准；不输出凭据。本机调用示例：

```json
{
  "cwd": "/absolute/path/to/workspace",
  "prompt": "Reply exactly OK. Do not use tools.",
  "model": {
    "providerId": "account:bigmodel-individual-coding-plan",
    "modelId": "GLM-5.3-Flash"
  },
  "thoughtLevel": "low"
}
```

恢复会话必须提供 workspace：agent-recover 使用 workspace 字段；agent-start 冷恢复
使用 cwd 和 threadId。工具权限由 Bridge 声明资源/headless 策略决定，账号鉴权由官方
服务处理。用量来自官方累计查询，恢复前基线用于计算本轮增量。

Services facade 不暴露 Bridge 的 checkpoint/turn 分叉契约；branch 能力明确为 false，
调用会报不支持，不将 V4 行分叉伪装成 checkpoint 分叉。direct 通道的分叉能力不变。

## E2E 测试计划

- **Revision**：本文件所在 bridge commit；依赖源码为上述完整 ZCode SHA。版本号相同
  不替代源码/发行包身份；本机运行包由该 SHA 构建，运行时二进制不提交。
- **前置条件**：运行包和普通 Node 路径正确；已有 BigModel 个人 Coding Plan 登录且
  官方目录可发现指定模型。不为测试创建套餐、购买服务或重建 API Key provider。
- **离线必跑**：`uv run --no-project --with pytest python -m pytest tests/test_zcode_services.py tests/test_backend_manager.py tests/test_control_plane.py tests/test_mcp_contract.py tests/test_windows_streams.py -q`。
  核对分片/错误帧、账号选择、订阅替换、权限拒绝及直连回归。
- **显式授权真实链路**：配置运行时环境，设置 RUN_ZCODE_ACCOUNT_E2E=1 后运行
  `uv run --no-project --with pytest python -m pytest tests/test_zcode_account_e2e.py -q`。
  此阶段消耗账号额度并留下原生会话；默认测试跳过，不发送模型请求。
- **真实流程**：新 MCP stdio 进程 → 检查 capabilities → 内置账号回显 → completed、
  非空结果及正 Token 用量 → 关闭 MCP → 新进程用相同 threadId/cwd 冷恢复再回显 →
  检查本轮增量、累计请求数 → 关闭会话。
- **跨端一致性**：测试前后 Personal Provider 配置字节相同；Paseo 列表仍只有内置
  账号条目。无网页、桌面或手机 UI 改动，浏览器布局验收 not applicable。
- **失败关闭**：缺运行时、未知 transport/workspace、无账号资格、订阅失败、错误帧
  必须报错；不静默切回旧 provider，不恢复全局 API Key 配置。
- **外部副作用**：离线无模型调用；真实测试只发送最小回显并使用临时 workspace。
  无发布、通知、交易和调度注册；不将登录信息写入证据或 Git。
- **证据**：pytest 结果和真实测试临时目录中的 account-e2e-evidence.json，仅含模型
  标识、终态、用量、冷恢复结论。会话正文、凭据和本机配置不提交。
- **通过标准**：离线通过；真实两轮 completed 且结果符合预期，account providerId
  不变，Token/本轮请求数为正，冷恢复后累计请求数至少 2，Personal Provider 未改变。

## Wire contract 来源

官方 packages/rpc/src 下的 protocol.ts、serialization.ts、channelClient.ts、
proxy-channel.ts 定义 stdio Channel RPC。Python 实现沿用同一帧格式，不新增依赖。
账号配置和鉴权核对官方 zcodeAgentService.ts、accountProviderRequestAuthService.ts。
