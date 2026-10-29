# @ws-flow/consumer

将 start/chunk/end/error 转成回调或异步迭代的内存消费器。它是 ReceiveStream 的底层队列，不知道连接、请求 ID 或远端取消协议。

## 安装

```sh
npm install @ws-flow/consumer@^1
```

业务应用通常直接消费 Client 或 Server 提供的流。直接使用本包适合将自定义分片事件转换为异步迭代。

## 模型与状态

StreamConsumer 接收 emit(StreamEvent)。start 仅通知状态，chunk 交给等待中的 next 或进入队列，end 正常结束，error 失败。首次终态生效，后续事件忽略。

正常 end 保留已排队数据供迭代取完；error 清空队列并拒绝等待中的 next；迭代 return 清空队列并结束。onClose 在终态时触发一次，宿主借此清理流记录或发送取消。

## 使用

```ts
import { StreamConsumer } from '@ws-flow/consumer'

const consumer = new StreamConsumer<number>({ capacity: 16 })
consumer.emit({ state: 'start' })
consumer.emit({ state: 'chunk', data: 1 })
consumer.emit({ state: 'chunk', data: 2 })
consumer.emit({ state: 'end' })

for await (const value of consumer) {
  console.log(value)
}
```

| API | 行为 |
| --- | --- |
| emit / stop | 投递事件/发出终态 |
| next / return / Symbol.asyncIterator | 迭代消费及提前退出 |
| onData | 回调消费，返回移除函数 |
| onState | 观察状态事件，返回移除函数，不补发已发生的事件 |
| isEnded / bufferedSize | 终态与队列长度 |

onData 和迭代消费互斥，不能把同一个 consumer 同时当两套消费者使用。需要独立分发应在上层创建不同订阅。回调模式不等待每个异步回调完成，不提供慢回调背压；异步错误发生时会在尚未终止的流上尝试失败结算。

## 资源与维护约束

capacity 默认 128，必须为正整数，按数据项数而非字节计算。缓冲溢出使流失败并抛错，不静默丢数据。多个并发 next 会形成等待者队列，不应无界提交 next 调用。

实现集中在 `src/consumer.ts`，出口在 `src/index.ts`。修改时必须保留终态幂等、等待者结算、缓冲释放、回调/迭代互斥和 onClose 清理。onClose 是同步扩展钩子，宿主应避免抛出异常。

仓库根执行 `pnpm test`；tests/receive-lifecycle、send-lifecycle、request-stream 覆盖集成的缓冲与终态行为。新增底层行为应直接验证正常结束排空、失败丢弃、提前 return 和溢出，再执行 `pnpm run check-types` 与 `pnpm run verify-packages`。

业务流的使用见[流式任务](../../docs/manual/guide/streams.md)，消费容量见[资源与期限](../../docs/manual/reference/resources.md)。
