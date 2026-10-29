# @ws-flow/scheduler

收发任务调度策略包。调度 WSTask.run，不解释 Envelope，不关联请求，不管理网络重传。

## 安装

```sh
npm install @ws-flow/scheduler@^1
```

Client 与 Server 默认保持同连接帧的处理顺序。直接依赖本包适合自定义宿主调度，或复用调度策略执行自有任务。

## 策略与顺序

| 类型 | 行为 |
| --- | --- |
| ImmediateScheduler | 立即执行，可并发 |
| KeyedSerialScheduler | 相同 key 串行，不同 key 并行；无 key 立即执行 |
| SessionScheduler | 以 task.peerId 为 key |
| PriorityScheduler | 全局串行，待执行任务按较大 priority 优先，同优先级 FIFO |
| ConcurrencyScheduler | 最多 limit 个任务同时执行，其余排队 |
| ComposableScheduler | 从外到内组合策略，每层通过替换 run 进入下一层 |
| SchedulerRuntime | 保存和切换入站/出站调度器，缺省为 ImmediateScheduler |

PriorityScheduler 不抢占已执行任务，高优先级持续进入可能使低优先级饥饿。ComposableScheduler 的外层占用会持续到内层完成，因此组合顺序影响吞吐。不要对同一个连接的有序帧随意启用并发/优先级重排。

## 使用

```ts
import { SessionScheduler } from '@ws-flow/scheduler'

const scheduler = new SessionScheduler()
const order: number[] = []

await Promise.all([
  scheduler.schedule({
    peerId: 'peer-1',
    run: async () => { order.push(1) }
  }),
  scheduler.schedule({
    peerId: 'peer-1',
    run: async () => { order.push(2) }
  })
])

console.log(order) // [1, 2]
```

Server/Client 默认注入独立的入站与出站 SessionScheduler；裸 RuntimeCore 没有这个默认策略。peerId 应区分连接周期，否则不同 Peer 可能被串在同一队列。

## 生命周期与维护

`src/types.ts` 定义 WSTask/WSScheduler；`src/schedulers.ts` 实现策略；`src/runtime.ts` 管理调度器引用。schedule 的 Promise 必须反映该 task 的执行结果，一个失败不能卡住后续队列，任务不得重复执行。

调度器只等待短帧处理；等待完整流结束会让后续 chunk 无法进入。请求与流处理器脱离帧调度的职责在 runtime-core。

排队策略构造器支持可选 QueueLimits：maxQueued 限制本实例等待总数，maxQueuedPerPeer 限制同 peerId 等待数，均为正安全整数，未配置维度无限额。KeyedSerialScheduler 的限额放在 getKey 后，ConcurrencyScheduler 的限额放在并发数后；SessionScheduler/PriorityScheduler 直接接受限额对象。queuedCount 只统计未开始工作，超限抛 QueueCapacityError（code/scope/limit/peerId）。

WSTask.signal 在排队时取消会物理移除任务并释放计数；已开始工作需要自行响应 signal，不能提前释放并发槽位而破坏执行顺序。没有统一 dispose API；换调度器不会迁移已有队列，宿主需通过任务 signal 取消旧任务。ConcurrencyScheduler 的并发数必须为正安全整数；PriorityScheduler 拒绝非有限数值 priority，缺省为 0。限额不等于字节背压或全部运行任务上限。

根目录 `pnpm test` 包含 tests/scheduler.test.mjs，验证并发上限、同 key 顺序、跨 key 并行、失败后继续、非法参数、优先级 FIFO、组合顺序及替换行为；这不等于持续压力验证。修改策略后执行严格类型与安装包验证。

宿主配置见[扩展指南](../../docs/manual/guide/extensions.md)，调度与业务生命周期的关系见[系统架构](../../docs/manual/design/index.md)。
