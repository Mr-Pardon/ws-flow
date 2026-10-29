# 请求与响应

需要一次操作的结果时，直接 `await client.request(...)`。对方用 `handleRequest` 注册处理器，返回的值就是你的结果。

请求标识、响应匹配、等待超时和任务清理由内核处理，业务不需要再去完整消息流里寻找响应。

## 像调用函数一样取得结果

下面约定 `double` 接收一个数字，并返回它的两倍。先启动 `server.ts`，再运行 `client.ts`。

::: code-group

```ts [server.ts]
import { WSServer } from '@ws-flow/server'

const server = WSServer.create({ port: 8080 })

server.handleRequest<number, number>('double', ctx => {
  return ctx.data * 2
})
```

```ts [client.ts]
import { WSClient } from '@ws-flow/client/node'

const client = new WSClient({ protocol: 'ws-flow' })
await client.connect('ws://127.0.0.1:8080')

const result = await client.request<number>('double', 21)
console.log(result) // 42
```

:::

`ctx.data` 是请求输入。处理器可以返回普通值，也可以返回 Promise；调用方的写法相同。

这两个示例是持续运行的应用，连接可用于后续调用。应用退出时调用 `client.dispose()` 和 `server.close()`。

::: tip 类型约定与输入校验
上例用类型参数表达双方约定。TypeScript 不会校验收到的网络数据；面向真实用户的接口还应检查输入，本文后面给出了做法。
:::

## 为等待设置期限

请求默认最多等待 30 秒。单次操作可以用 `timeoutMs` 调整期限：

```ts
import { WSClient } from '@ws-flow/client/node'

export async function double(client: WSClient, value: number) {
  return client.request<number>('double', value, { timeoutMs: 5_000 })
}
```

这里传入已连接的 Client。期限从登记请求开始计算，包含排队、发送、远端执行和结果返回；超时会拒绝 Promise。`timeoutMs: null` 可以关闭总期限，长期任务仍应有明确的结束条件。

## 让耗时任务响应取消

请求方传入 `signal`，处理方通过 `ctx.signal` 接收取消通知。下面用一个可取消的等待模拟耗时业务：

::: code-group

```ts [server.ts]
import { setTimeout as delay } from 'node:timers/promises'
import { WSServer } from '@ws-flow/server'

const server = WSServer.create({ port: 8080 })

server.handleRequest('slow-work', async ctx => {
  await delay(5_000, undefined, { signal: ctx.signal })
  return { complete: true }
})
```

```ts [client.ts]
import { WSClient } from '@ws-flow/client/browser'

const client = new WSClient({ protocol: 'ws-flow' })
const controller = new AbortController()
await client.connect('ws://127.0.0.1:8080')

// 取消按钮或页面卸载时调用 controller.abort()
try {
  const result = await client.request('slow-work', null, {
    signal: controller.signal
  })
  console.log(result)
} catch (error) {
  if (controller.signal.aborted) console.log('已取消')
  else throw error
}
```

:::

如果业务 API 支持 `AbortSignal`，像上例一样把 `ctx.signal` 传下去；否则，在重要步骤前检查 `ctx.signal.throwIfAborted()`，并自行释放业务资源。

取消会停止本地等待，并尽力通知远端。它不会自动回滚远端已经完成的业务操作。上面的处理器可以替换成数据库查询、文件操作等支持取消的业务。

## 校验输入，返回可识别的错误

下面把输入校验和处理器注册放在一个函数中。移除第一个例子的 `server.handleRequest(...)` 注册，改为调用 `registerDouble(server)`，就能拒绝不符合约定的输入：

```ts
import { WSServer, RequestError } from '@ws-flow/server'

export function registerDouble(server: WSServer) {
  return server.handleRequest<unknown, number>('double', ctx => {
    if (typeof ctx.data !== 'number' || !Number.isFinite(ctx.data)) {
      throw new RequestError('INVALID_INPUT', '请输入有限数字')
    }

    return ctx.data * 2
  })
}
```

调用方捕获 `RequestError` 后，可以通过 `error.code === 'INVALID_INPUT'` 显示对应提示。普通异常也会让请求失败；更完整的接收方式见[错误处理](/reference/errors)。

::: details 常见失败与处理器约束

| 情况 | 常见结果 |
| --- | --- |
| 远端没有对应处理器 | `RequestError`，`code: 'METHOD_NOT_FOUND'` |
| 单结果与流结果模式不匹配 | `RequestError`，`code: 'MODE_MISMATCH'` |
| 请求方总期限到达 | `RequestError`，`code: 'TIMEOUT'` |
| 处理器抛出普通异常 | 请求方收到 `RequestError`，`code: 'HANDLER_ERROR'` |
| 处理器抛出 `RequestError` | 请求方保留它的 `code` 与 `message` |
| 主动取消、断连、发送失败 | Promise 拒绝，原因可能是其他错误类型 |

并非所有拒绝都是 `RequestError`。业务调用仍应 `await` 并处理异常，异常观察器不能替代结果 Promise 的错误处理。

`handleRequest` 返回注销函数。同一作用域、同一事件只能注册一个请求处理器；`handleRequest` 与 `handleStream` 共享这个事件注册空间。

输入和结果都应符合当前 codec 的格式约定。默认 JSON 不能传输任意 JavaScript 对象，例如函数、循环引用或 BigInt。

已发出的 ws-flow 请求会尽力通知远端取消；尚在本地排队、没有开始提交传输的请求只清理本地任务。关闭连接也会结束相应任务。

执行方还可以配置独立的入站执行期限，见[资源配置](/reference/resources)。普通使用无需预先设置这些选项。

:::

## Server 也可以发起请求

把方向反过来，使用方式依然相同：Client 注册 `handleRequest`，Server 通过 `session.request` 调用。

需要同时查询多个客户端时，`Sender.requestAll` 会保留每个 Session 的成功或失败结果。见[服务端使用](/guide/server)和[双向通信案例](/examples/duplex)。
