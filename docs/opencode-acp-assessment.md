# OpenCode ACP 迁移评估

后续状态：迁移已实施，当前默认适配为ACP；使用与边界见 [OpenCode ACP后端](opencode-acp-backend.md)。下文保留迁移前评估证据。

评估日期：2026-11-05。对象为本机 OpenCode **2.0.30**、当前 `opencode_backend.py` HTTP/SSE 实现及新增 dsh ACP 实现。本轮未修改运行通道、生产配置或凭据；模型/文件验证只在临时工作区进行。

## 结论

建议让 OpenCode 以 **ACP 为默认主通道**，与 dsh 共用传输层和可复用的控制流程。当前没有证据表明核心任务生命周期需要 HTTP 兜底。暂时保留旧 HTTP 代码供回滚和对照；它在本机2.0.30上尚不可用，不能称为已验证兜底。

“分叉、压缩、详细用量需要 HTTP”这一理由不成立：完整会话分叉、压缩和 reasoning/cache-read 用量已通过本机 ACP 验证。剩余迁移门槛主要是权限策略、指定消息边界分叉以及长期任务的计数/历史口径。

## 本机实测

沿用 `DshAcpClient` 的 NDJSON 传输类，命令改为 `opencode acp`；无需新依赖。验证脚本没有借用 DshRuntime 的会话/模型逻辑。

| 项目 | 结果 | 说明 |
|---|---|---|
| initialize | 通过 | v1；本机报告OpenCode2.0.30，支持list/resume/close/delete/fork/additionalDirectories |
| 模型精确选择 | 通过 | `magpie/deepseek/deepseek-flash`；使用实时目录，不自动换路由 |
| 思考档位 | 通过 | config ID=`effort`、category=`thought_level`；该模型有none/low/high/max/default，实测low |
| 多会话并发 | 通过 | 同一ACP进程、两个独立工作区并发，各自写出正确proof.txt |
| 原生list/close/resume | 通过 | 保存会话；close后resume并读取原文件；本轮为同进程重附着，跨进程读取历史仍列迁移验收 |
| 完整会话fork | 通过 | 有消息时返回新session；空会话分叉被明确拒绝；未验证指定message/checkpoint边界 |
| 压缩 | 通过 | `/compact`产生in_progress/completed及summary chunks，随后正常回答COMPACT_CONTINUE_OK |
| cancel | 通过 | 活跃prompt收到原生cancel后，以stopReason=cancelled结束 |
| 事件 | 通过 | 文本、thought、tool_call、tool_call_update、config/command、上下文/费用、compaction |
| 用量 | 通过但有口径门槛 | response含inputTokens/outputTokens/totalTokens/thoughtTokens/cachedReadTokens；update含上下文used/size及session累计USD费用 |
| 权限回调 | 未覆盖现有策略 | 临时目录write成功，但没有出现request_permission；只拒绝回调不能保证只读或denylist |
| session/delete | 通过 | 内存快照测试清理自建空会话时使用，未删除既有用户会话 |

实际一个prompt响应示例：inputTokens=15351、outputTokens=99、thoughtTokens=22、cachedReadTokens=14592、totalTokens=30064；不能套用“input+output就是total”的假设。没有在本样本看到cache-write字段，不应把缺失字段当成已验证零值；modelRequests精确步数和子会话用量归属也需单独映射。

## 当前HTTP/SSE实现的问题

真实调用 `OpenCodeServer.start()` 返回 `OpenCode server failed to start: no listen address`。直接执行其启动命令后，CLI明确返回：

```text
Unrecognized flag: --no-mdns in command opencode serve
```

因此当前 `probe.available=true` 只代表找到二进制并能读取版本，不代表当前HTTP适配端到端可用。移除此参数后的API兼容性没有验证；代码还使用未加前缀的旧session路径，不能假设只删参数就全部恢复。旧单测使用fake服务，也不等价于本机2.x兼容验证。

现有 `_monitor()` 每250ms读取状态，通过连续两次idle及已见busy/等待至少0.6s判断结束，并同时维护SSE重连。ACP直接等待原生`session/prompt`响应，可移除这部分启发式和重复状态通道；这是主要收益，不是单纯统一命令外观。

## 共用范围和必须保留的差异

```text
统一外层：资源租约、队列、run/wait/control、超时、终态outbox
  ├─ ACP transport：请求ID、NDJSON、notification、reverse request、退出清理
  │    ├─ DSH dialect + 当前每run独立peer
  │    └─ OpenCode dialect + Backend持有共享peer、按sessionId分发
  └─ Pi原生RPC（保持）
```

公共层可抽取stdio JSON-RPC、进程回收、事件分发、prompt/guide/interrupt/cancel流程。不要让OpenCode直接继承DshRuntime：

- 模型值：dsh为字符串化JSON二元组；OpenCode为目录提供的`provider/model[/variant]`，provider自身也可包含`/`，不能随意按第一个斜线拆分。
- effort：dsh为reasoning_effort；OpenCode为effort，二者按实时option验证。
- 会话：dsh UUID与Bridge索引；OpenCode ses_*与原生list分页。不能套用UUID校验或Bridge私有索引。
- 进程：OpenCode一个ACP进程可管理多个目录/会话；关闭run只关session，不能连带关闭其他run的peer。
- 用量、子会话元数据、压缩、fork、权限需要OpenCode自己的映射。

本机一次空会话快照：同一peer承载4会话，进程树RSS求和约**1815 MiB**；4个peer各1会话约**4519 MiB**。包括当前配置加载的MCP/插件子进程、共享页可能重复计数，不是PSS或通用性能基准；初始化约0.2s也不代表完整任务启动延迟。结果足以说明不能盲目照搬“一run一进程”，不用于断言ACP本身比HTTP更快或更省内存。

## 迁移门槛与HTTP保留原则

1. 提取公共ACP传输，保留DSH现有回归；OpenCode按initialize能力动态开放功能。
2. OpenCode使用共享peer和session路由，测试同/不同目录并发、退出、EOF、单run取消不能伤及别的run。
3. 权限先做负例：shared不得写、denylist工具不得执行、工作区外写必须拒绝；如果无法可靠注入策略，相关组合先明确不支持。additionalDirectories只是额外目录授权，plan模式也不能未经验证当作任意read-only/denylist替代。
4. 验证指定消息边界fork、跨进程resume、压缩后resume和外部会话分页；不把全量会话fork冒充任意checkpoint分叉。
5. 对账长任务、多prompt、reasoning/cache、压缩及子会话usage。明确本轮增量与session累计费用的区别，不能把累计费用重复累加。
6. 通过后将ACP设为OpenCode主通道。HTTP仅为实测ACP确实缺失的操作保留，并独立适配/验证v2；不提前维护双通道，也不静默从ACP失败回退HTTP。

## 证据

- [OpenCode官方v2 ACP协议文档](https://opencode.ai/v2/docs/cli/acp/)；并用Context7查阅`/anomalyco/opencode/v2.0.28`作邻近版本对照，本机2.0.30行为优先。
- 现有实现：`opencode_backend.py`、`dsh_backend.py`、`backend_manager.py`、`agent_control_plane.py`。
- 临时探针与完整日志：`/tmp/vibe-opencode-acp-assessment/`，包含probe.py/log、live.py/log、live-events.json、memory.py/log、summary.json。日志只涉及临时工作区和公开路由标识，不含凭据。
- GitHub精确版本源码树读取遇到API限流，故没有对未核验的消息边界/权限扩展作肯定承诺。
