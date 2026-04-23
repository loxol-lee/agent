# V0 目录与文件拆分（Step 2）

目标：把《V0模块+接口+协议》里的分层落到“可实现的工程骨架”，确保依赖单向、可观测/可回放默认开启，后续扩展不推倒重来。

---

## 1. 总原则（必须遵守）

### 1.1 单向依赖（禁止反向耦合）
- Connector → Orchestrator
- Orchestrator → Model Layer / Tool Runtime / Storage / Observability
- Model Layer → Providers（各厂商/本地模型）
- Tool Runtime → Tools（具体工具实现）
- Storage / Observability → 不依赖上层

### 1.2 文件职责单一
- 接口与协议：只放“类型/结构/约束”，不放具体实现
- 实现：放在 `impl/` 或 `providers/` / `tools/` 下
- 入口层：只做鉴权/限流/协议适配/流式输出，不做编排与工具执行

### 1.3 默认可观测/可回放
- 每次请求都必须落 trace + spans + replay_events（token 可合并）
- 严禁任何 secret 落盘

---

## 2. 推荐目录结构（V0）

> 说明：这是“规划的结构”，你后续实现时按这个创建即可。  
> 技术栈先按“后端 + Web 前端”拆开；后端语言后续可选 Python/Node/Go，这里用中性命名。

```text
agunt/
  V0模块+接口+协议
  V0目录与文件拆分.md
  V0默认工具清单.txt

  apps/
    web-chat/                       # Web 前端（UI，V0必须）
      README.md                     # 可选：仅说明如何本地启动（后续再写也行）
      src/
        index.html
        app.tsx|app.js
        api.ts                      # SSE/WS 客户端、请求封装
        types.ts                    # StreamEvent 等前端类型（与协议一致）
        modules/
          conversation/             # 会话列表、搜索、批量操作、回收站
          attachments/              # 上传、状态展示、附件引用
          run-status/               # processing/retrying/failed/done/cancelled
          settings/                 # 工具开关、风格开关、记忆开关
      public/

    server/                         # 后端服务（HTTP + SSE/WS）
      src/
        main.(py|ts|go)             # 进程入口：启动 HTTP、路由、依赖注入
        config.(py|ts|go)           # 仅加载环境变量/配置引用（不含明文 secret）

        connectors/
          web/
            routes.(py|ts|go)       # /chat、/stream、/health 等路由
            auth.(py|ts|go)         # V0: 本机/内网模式；公网模式预留
            rate_limit.(py|ts|go)   # V0: 简单限流（防刷）

        core/
          contracts/                # 协议与接口（只定义，不实现）
            chat.(py|ts|go)         # ChatRequest/ChatResponse/StreamEvent/Attachment/Usage
            model.(py|ts|go)        # ModelChatRequest/ModelStreamEvent/AssistantMessage
            tools.(py|ts|go)        # ToolDefinition/ToolCall/ToolResult/ToolContext/ToolPolicy
            observability.(py|ts|go)# Trace/Span/ReplayEvent 结构 + Writer 接口
            storage.(py|ts|go)      # Storage 接口：messages/traces/spans/tool_runs/memories

          orchestrator/
            agent_runtime.(py|ts|go)# Agent loop 主逻辑（只依赖 contracts）
            context_builder.(py|ts|go)# 上下文拼装策略（最近 N 轮、system 指令、预算）
            budgets.(py|ts|go)      # max_steps/time_budget/tool_call_limit 默认值与配置读取
            stop_reason.(py|ts|go)  # stop_reason 枚举（固定集合）

          model_layer/
            router.(py|ts|go)       # model_alias → provider/model 映射、policy: primary_then_fallback
            client.(py|ts|go)       # ModelLayer 接口实现入口（统一 chat/stream）
            retry.(py|ts|go)        # 重试/退避/超时封装
            limits.(py|ts|go)       # 并发/限流（semaphore/rpm/tpm）
            providers/
              siliconflow.(py|ts|go)# OpenAI 兼容 provider（硅基流动）
              local_stub.(py|ts|go) # V0 可选：本地模型占位（后续替换）

          tool_runtime/
            registry.(py|ts|go)     # 工具注册与 schema 输出
            executor.(py|ts|go)     # 工具执行（校验/超时/错误封装/审计）
            policy.(py|ts|go)       # allowlist/denylist/risk 判定（固定顺序）
            redact.(py|ts|go)       # 脱敏与摘要工具（供 tool_runs/replay_events 使用）
            tools/
              time_now.(py|ts|go)
              remember.(py|ts|go)
              recall.(py|ts|go)
              calc.(py|ts|go)
              echo.(py|ts|go)       # 默认关闭，仅开发启用

          infra/
            storage/
              sqlite_store.(py|ts|go)# SQLite 实现：messages/traces/spans/tool_runs/replay_events
              schema.sql            # 建表（可选：也可在代码里迁移）
            observability/
              tracer.(py|ts|go)     # span 生成、trace 生命周期管理
              writers/
                sqlite_writer.(py|ts|go)
                jsonl_writer.(py|ts|go) # 可选，便于快速调试
```

---

## 3. 文件清单与职责（关键文件）

### 3.1 协议/接口（contracts/）
- `core/contracts/chat.*`
  - 定义：ChatRequest/ChatResponse/StreamEvent/Attachment/Usage
  - 约束：字段语义与 3.4 payload 规则保持一致
- `core/contracts/model.*`
  - 定义：Message/AssistantMessage/ModelChatRequest/ModelChatResponse/ModelStreamEvent
  - 约束：tools/stream 的统一语义
- `core/contracts/tools.*`
  - 定义：ToolDefinition/ToolCall/ToolResult/ToolContext/ToolPolicy + error.code 枚举
- `core/contracts/observability.*`
  - 定义：Trace/Span/ReplayEvent 的字段结构（不含落地实现）
- `core/contracts/storage.*`
  - 定义：Storage 接口（读写 messages/traces/spans/tool_runs/replay_events/memories）

### 3.2 Orchestrator（core/orchestrator/）
- `agent_runtime.*`
  - 实现：5.1 的标准执行序列（初始化→上下文→loop→工具→done→落盘）
  - 产出：StreamEvent（token/tool_call/tool_result/error/done）
  - 强制：写 orchestrator.span + 触发 model.span/tool.span
- `context_builder.*`
  - 实现：最近 N 轮策略、system 指令、tools schema 选择（仅 low 风险）
- `budgets.*`
  - 实现：默认预算与可配置读取（max_steps/tool_call_limit/time_budget_ms/tool_timeout_ms）
- `stop_reason.*`
  - 实现：stop_reason 固定枚举（用于 trace 与 done 事件）

### 3.3 Model Layer（core/model_layer/）
- `router.*`
  - 实现：model_alias 映射、policy（primary_then_fallback）
- `client.*`
  - 实现：统一 chat/stream；屏蔽 provider 差异
- `providers/siliconflow.*`
  - 实现：OpenAI 兼容 chat.completions；工具调用与流式适配
- `retry.*` + `limits.*`
  - 实现：超时/重试/并发/限流治理
- 强制：每次调用产出 model.span（provider/model/latency/retry/usage/error）

### 3.4 Tool Runtime（core/tool_runtime/）
- `registry.*`
  - 实现：读取工具清单，输出 tools schema 给模型
- `policy.*`
  - 实现：固定顺序判定（未注册→denylist→allowlist→risk→schema→执行）
- `executor.*`
  - 实现：参数校验、超时、错误封装、产出 tool_result、写 tool.span、落 tool_runs
- `redact.*`
  - 实现：脱敏与摘要（命中 key/token/secret/... → "***"）

### 3.5 Storage（infra/storage/）
- `sqlite_store.*`
  - 实现：7.1~7.7 定义的表与索引
  - 强制：大对象摘要策略、脱敏策略
- `schema.sql`（可选）
  - 用于明确建表与索引（便于非代码同学理解 DB 里有什么）

### 3.6 Connector（apps/server/src/connectors/web/）
- `routes.*`
  - 实现：/chat（提交消息）、/stream（SSE/WS 流式）、/health
- `auth.*`
  - V0：本机/内网；公网模式预留（口令/令牌）
- `rate_limit.*`
  - V0：简单限流（避免成本失控）
- 约束：不做 agent loop，不直接调用 model，不执行工具

---

## 4. 依赖检查（自检规则）

- `apps/server/src/connectors/**` 不能 import `core/model_layer/providers/**`、`core/tool_runtime/tools/**`
- `core/orchestrator/**` 只能 import `core/contracts/**` + `core/model_layer/client`（接口）+ `core/tool_runtime/executor`（接口）+ `infra/*`（实现通过依赖注入）
- `core/model_layer/providers/**` 不得 import `core/orchestrator/**` 或任何 connector
- `infra/**` 不得 import `connectors/**` 或 `orchestrator/**`

---

## 5. Step 3（填代码实现）前的准备清单

- 确定后端语言：Python / Node / Go（三选一）
- 确定前端通信：SSE（优先，简单）或 WebSocket（复杂但灵活）
- 确定 SQLite 落地：是否需要 `schema.sql` 或代码迁移
- 配置约定：model_alias 配置文件格式（json/yaml）或纯环境变量

---

## 6. 产品功能验收清单（V0）

- 会话管理：新建、重命名、搜索、批量归档、删除、恢复、清空全部。
- 删除语义：必须区分 archived / deleted / purged，并在 UI 给出可恢复说明。
- 输入输出：支持停止生成、失败重试、重新生成、代码块复制、长文本折叠。
- 附件能力：支持上传，展示文件名/大小/hash/状态；对话中可引用附件。
- 状态可见：processing/retrying/failed/done/cancelled 必须对用户可见。
- 会话设置：工具开关、记忆开关、回答风格开关可直接配置。
- 安全基线：本机模式可免鉴权；公网模式必须开启 API Key。

---

## 7. 迭代优先级与执行清单（P0/P1/P2）

### 7.1 P0（先闭环）
- 任务1：模型 usage 全链路打通（采集/透传/落盘/回放可见）。
  - 文件：contracts/model, model_layer/providers/siliconflow, model_layer/client, orchestrator/agent_runtime, infra/storage/schema.sql, infra/storage/sqlite_store
  - 验收：每次模型调用可查 usage；失败可定位 stage 与错误码。
- 任务2：工具错误码与用户提示统一（NOT_ALLOWED/ARGS_INVALID/TIMEOUT/RUNTIME_ERROR）。
  - 文件：contracts/tools, tool_runtime/policy, tool_runtime/executor, connectors/web/routes
  - 验收：错误提示一致，回放与前端语义一致。
- 任务3：memory_forget 落地（skills + 存储 + 回放）。
  - 文件：tool_runtime/tools/memory_forget（新增）, tool_runtime/registry, contracts/storage, infra/storage/sqlite_store
  - 验收：可按 id 删除记忆；recall 不再返回；tool_runs/replay 可追踪。
- 任务4：记忆治理 API（list/delete/toggle）。
  - 文件：connectors/web/routes, contracts/storage, infra/storage/sqlite_store
  - 验收：可查看/删除记忆，可关闭记忆写入。
- 任务5：协议字段最小对齐（request_id/stop_reason/error_code/done payload）。
  - 文件：contracts/chat, orchestrator/stop_reason, connectors/web/routes
  - 验收：流式 done/error 字段稳定且无同义重复。

### 7.2 P1（再增强）
- 任务6：附件最小可用（上传 + 元数据 + 引用）。
- 任务7：会话搜索与批量操作。
- 任务8：运行状态可见化（processing/retrying/failed/done/cancelled）。
- 任务9：会话级工具开关（关闭后强制 tool_choice=none）。

### 7.3 P2（后规模化）
- 任务10：目录拆分落地（web-chat 独立，后端仅 API/stream）。
- 任务11：可观测指标接口（成功率/时延/工具成功率/限流触发率）。
- 任务12：安全与多用户预备（公网鉴权、审计、权限分层）。

### 7.4 推荐执行顺序
- 第一轮：任务1 → 任务3 → 任务2 → 任务4 → 任务5
- 第二轮：任务6 → 任务8 → 任务7 → 任务9
- 第三轮：任务10 → 任务11 → 任务12