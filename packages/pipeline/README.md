# @ws-flow/pipeline

中间件组合与插件注册基础包。不依赖 socket、请求表或具体消息格式。

## 安装

```sh
npm install @ws-flow/pipeline@^1
```

业务应用可通过 Client 或 Server 的 `use()` 注册插件；直接使用本包适合组合自己的中间件执行链。

## 模型

Middleware 是 `(ctx, next) => void | Promise<void>`。composeMiddleware 将其组合为执行函数，支持调用下游前后处理；同一执行链重复 next 会被拒绝。不调用 next 只停止该中间件链，是否取消发送/请求由宿主 context 的控制方法决定。

PipelineRegistry 保存六类中间件：receive、receiveEvent、receiveStream、send、sendEvent、sendStream。插件只是这些列表的模块化集合，use 按顺序展开注册。事件匹配由 runtime-core 的 Dispatcher 执行，本包只保存 matcher 与中间件。

## 使用

```ts
import { composeMiddleware, PipelineRegistry } from '@ws-flow/pipeline'

type Context = { value: number }
const registry = new PipelineRegistry<Context, Context, Context>()

registry.use({
  receive: async (ctx, next) => {
    ctx.value += 1
    await next()
  }
})

const run = composeMiddleware(registry.receiveMiddlewares)
const context = { value: 1 }
await run(context)
console.log(context.value) // 2
```

异步中间件应 await 或 return next，否则上游无法等待下游完成，异常也可能脱离调用链。串行帧中间件应短小，不要等待同一连接的未来响应；将长业务放到请求处理器或订阅回调。

## 维护约束

- `src/compose.ts`：执行顺序、同步异常转 Promise、重复 next 检查。
- `src/registry.ts`：按注册顺序追加，支持单项/数组/插件。
- `src/types.ts`：三种 Context 泛型及 matcher 类型；无运行时业务校验。

Registry 的 getter 返回实际数组，不是快照；运行期间直接修改会影响后续分发。插件采用追加注册，不提供卸载、命名依赖排序或热替换协议；闭包资源由使用者管理。请求 ID 与 socket 状态归属连接及任务模块。

在仓库根运行 `pnpm run check-types` 与 `pnpm test`；tests/pipeline.test.mjs 独立验证异步嵌套、短路、异常传播、重复 next 及六类管线注册顺序，协议和生命周期测试覆盖宿主集成。改变组合语义应补相应回归场景，再跑 `pnpm run verify-packages`。

在宿主中使用插件见[扩展指南](../../docs/manual/guide/extensions.md)，模块职责见[系统架构](../../docs/manual/design/index.md)。
