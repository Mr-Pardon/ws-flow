---
description: ws-flow 的模块组成、消息路径与任务生命周期。
---

# 架构模型

ws-flow 以共享任务内核组织双向通信。Client 和 Server 提供业务入口，协议适配器解释消息，中间件管线处理公共逻辑，调度器安排消息执行。各模块通过明确的接口协作，应用可以从默认配置开始，按需要扩展。

## 模块组成

<div class="design-flow" aria-label="ws-flow 模块组成">
  <div><strong>业务入口</strong><br>Client · Server · Session · Sender</div>
  <div aria-hidden="true">↕</div>
  <div><strong>共享任务内核</strong><br>通知分发 · 请求关联 · 流生命周期 · 取消与清理</div>
  <div aria-hidden="true">↕</div>
  <div><strong>消息处理</strong><br>ProtocolAdapter · Pipeline · Scheduler</div>
  <div aria-hidden="true">↕</div>
  <div><strong>连接接入</strong><br>Codec · Browser WebSocket · Node ws</div>
</div>

| 模块 | 职责 |
| --- | --- |
| `client` | 管理单条连接、环境接入、编解码与重连，提供订阅和任务 API |
| `server` | 接入连接，管理 Session、房间、目标选择和公共处理器 |
| `runtime-core` | 编排消息处理，维护请求关联、订阅分发、流状态与任务回收 |
| `envelope` | 定义配套协议的消息结构与构造工具 |
| `pipeline` | 组合中间件，将插件注册到消息处理的各个阶段 |
| `scheduler` | 控制消息帧的排队、顺序和并发 |
| `consumer` | 提供队列、异步消费与消费生命周期 |

业务应用安装需要的 Client 或 Server，其余模块由包依赖带入。编写自定义协议或调度器时，可以直接使用对应模块的公开接口。

## 业务入口

Client 面向当前连接。Server 通过 Session 操作一个客户端，通过 Sender 选择一组客户端。两端共享相同的通信语义：

| 通信方式 | 发起 | 接收 | 结果 |
| --- | --- | --- | --- |
| 通知 | `send` / `emit` | `subscribe` / `on` | 普通消息进入各自匹配的订阅 |
| 单结果请求 | `request` | `handleRequest` | 一个 Promise |
| 请求结果流 | `requestStream` | `handleStream` | 可取消的异步迭代任务 |
| 主动数据流 | `stream` | `onStream` | 有开始、分片和终态的独立数据流 |

响应与取消等控制消息由内核关联到所属任务。普通订阅只接收通知，多个订阅可以独立筛选、缓冲和消费同一条消息。

## 消息处理路径

接收方向：

```text
Socket 收到数据
  → Codec 解码
  → ProtocolAdapter 解释消息
  → 校验与入站调度
  → receive 中间件
  → 校验任务身份
  → 通知分发 / 请求处理 / 结果关联 / 流与取消处理
```

发送方向：

```text
业务调用
  → 创建消息，按需登记请求或流
  → 出站调度与 send 中间件
  → 校验任务身份
  → ProtocolAdapter 映射消息结构
  → Codec 编码与传输准入
  → Socket 发送
```

请求在发送前登记，响应到达后即可匹配对应调用。插件可以转换业务数据，任务所属连接、请求标识、流标识及结果模式由内核维护。

## 扩展接口

**Codec 处理传输表示。** 它在字符串、二进制和业务对象之间转换。Client 与 Server 默认使用 JSON；具体编解码方式由宿主配置。

**ProtocolAdapter 解释消息语义。** Client 默认采用 generic 模式，保留第三方服务的消息结构。连接 ws-flow Server 时配置 `protocol: 'ws-flow'`，即可使用配套事件与任务语义。适配已有服务时，按对端协议声明事件、请求、流与取消能力。

**插件组合公共处理。** 中间件分别进入收发消息、事件和流的处理管线。日志、校验与字段转换可以集中封装并复用，业务处理器负责具体输入和结果。

**调度器安排帧执行。** 默认同一连接内有序处理，不同连接可以并行。Server 支持注入调度器，自定义排队与执行策略。

配置方式和示例见[按需扩展](/guide/extensions)。Server 使用标准 ws-flow 协议；通用协议适配由 Client 或直接使用 RuntimeCore 的自定义宿主提供。

## 任务生命周期

每个请求和流都属于一条具体连接。任务由响应、正常结束、超时、取消、失败或断连进入终态；终态只结算一次，并清除关联记录、定时器与监听。迟到结果不会重新激活已结束的任务。

请求处理器、通知回调和流消费者可以等待异步业务，不占住整条连接的帧处理队列。中间件参与帧处理，应完成当前消息的处理后继续传递，避免在其中等待同一连接的未来响应。

取消通过 `AbortSignal` 传递。内核结束本地等待并尽力通知远端，业务将信号传给支持取消的操作，或在工作步骤中主动检查。已经完成的外部操作由业务自身管理一致性。

## 连接与资源

Client 重连创建新的连接周期，旧连接上的任务和通知订阅结束。请求处理器注册随 Client 实例保留；订阅由应用在新连接上重新建立。重连恢复连接，不自动重发业务操作。

队列、传输字节、活动任务和执行期限分别管理不同资源。它们可独立配置，超限或超时会明确失败并回收对应资源。逻辑任务额度约束内核登记的任务，业务代码仍需配合取消。

连接的持有与释放见[生命周期](/guide/lifecycle)，配置项与默认值见[资源与期限](/reference/resources)，业务接入从[快速开始](/guide/quick-start)进入。

<style scoped>
.design-flow {
  display: grid;
  gap: 0.6rem;
  padding: 1.5rem;
  border: 1px solid var(--vp-c-divider);
  border-radius: 16px;
  background: var(--vp-c-bg-soft);
  text-align: center;
  line-height: 1.7;
}
.design-flow [aria-hidden] {
  color: var(--vp-c-brand-1);
  font-weight: 700;
}
</style>
