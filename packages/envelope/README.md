# @ws-flow/envelope

标准 ws-flow 消息类型与构建器。提供普通消息及主动流帧的构建，不负责 JSON 编解码、请求表、流状态机或 socket。

## 安装

```sh
npm install @ws-flow/envelope@^1
```

Client 与 Server 会自动构造所需消息。直接使用本包适合编写自定义消息构建器或扩展模块。

## 消息模型

| 字段 | 含义 |
| --- | --- |
| id | 普通消息标识，不等于 requestId |
| event / data | 业务事件与数据 |
| meta / priority | 扩展信息与调度提示 |
| requestId / kind / resultMode | 请求关联、控制类别与结果模式 |
| streamId / streamState | 流身份及 start/chunk/end/error |
| error | 错误内容，最终序列化由协议适配层负责 |

BaseWSEnvelope 描述标准结构，WSEnvelope<Events> 将事件名映射到对应数据类型。类型本身不做运行时校验，完整协议校验在 runtime-core/protocol.ts。

## 使用与扩展

```ts
import { createEnvelopeBuilder } from '@ws-flow/envelope'

const builder = createEnvelopeBuilder<{ notice: { value: number } }>()
const message = builder.createMessage({
  event: 'notice',
  data: { value: 1 }
})

const start = builder.createStreamStart({ event: 'notice' })
const chunk = builder.createStreamChunk({
  streamId: start.streamId,
  data: { value: 2 }
})

console.log(message, start, chunk)
```

createEnvelopeBuilder 接受 Partial<EnvelopeBuilderLike> 覆盖，或按顺序应用 EnvelopeBuilderPlugin[]。EnvelopeBuilder 包装具体实现并提供类型重载，默认 builder 内维护消息/流计数器，ID 不是跨进程全局唯一，也不是业务幂等键。

创建流时应由同一运行时维持 ID 唯一性，后续帧复用 start 的 streamId。自定义 builder 不应改变已分配的任务归属，不能通过更换结构替代 ProtocolAdapter。请求 ID 与请求/响应编码由内核任务层负责。

## 结构归一化与维护

`src/normalize.ts` 导出 isEnvelope、normalizeSendInput、normalizeReceiveInput。它们以 event 或 streamId 字段判断结构，并将其他输入包装为 data；这类结构归一化不是完整协议校验。请求和流语义由 RuntimeCore 的 ProtocolAdapter 解释，自定义第三方数据应通过适配器接入。

- `src/types.ts`：消息字段、构建输入、泛型和插件契约。
- `src/factory.ts`：默认形状、计数器和插件应用。
- `src/envelope.ts`：类型封装与委托。
- `src/index.ts`：公共出口。

标准格式变化需要同时检查 wsFlowAdapter、请求关联、流状态校验及两个宿主；不要只增加可选字段就认为协议能力已实现。验证入口为仓库根 `pnpm run check-types`、`pnpm test` 和 `pnpm run verify-packages`，重点关注 tests/protocol 与请求/流生命周期测试。

接入方式见[扩展指南](../../docs/manual/guide/extensions.md)，模块协作见[系统架构](../../docs/manual/design/index.md)。
