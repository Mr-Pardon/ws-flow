# 连接与生命周期

把连接当作应用持有的资源：需要通信时连接，不再使用时释放。Client 管理一条连接；Server 用 Session 表示每个接入的客户端，并在断连时清理它的任务。

## 连接后使用，用完后释放

`await client.connect()` 成功后，就可以发送消息和发起任务。下面的短脚本查询服务状态，无论连接或请求是否成功，最后都会释放 Client。远端需要注册 `health` 请求处理器。

```ts
import { WSClient } from '@ws-flow/client/node'

const client = new WSClient({ protocol: 'ws-flow' })

try {
  await client.connect('ws://localhost:8080')
  const health = await client.request('health', null)
  console.log('服务状态：', health)
} finally {
  client.dispose()
}
```

**页面或长期运行的应用应继续持有连接。** 在页面组件卸载或应用退出时调用 `client.dispose()`，不要在每次发送后销毁它。尤其是通知，发送完成不代表对端已经处理完毕。

| 你的目的 | 使用方法 | Client 能否继续使用 |
| --- | --- | --- |
| 暂时断开，稍后重新连接 | `close()` | 可以再次 `connect()` |
| 结束这个 Client 的使用 | `dispose()` | 不可复用 |
| 将外部 socket 交还给调用方 | `detach()` | 可以再次连接或接管 |

`dispose()` 会一起释放 Client 持有的订阅、处理器和观察器。仅想停止某一项监听时，调用注册函数返回的注销函数即可。

## 让应用在断线后重新连接

自动重连默认关闭。启用后，Client 负责重新建立连接，应用负责恢复自己的业务订阅。

下面的页面在每次连接成功时重新监听 `status` 通知：

```ts
import { WSClient } from '@ws-flow/client/browser'

const client = new WSClient({
  protocol: 'ws-flow',
  reconnect: { delayMs: 1_000, maxAttempts: 5 }
})

client.onIssue(error => console.error('连接或消息处理异常：', error))

client.onState(state => {
  if (state !== 'open' || client.state !== 'open') return

  client.on('status', status => {
    console.log('最新状态：', status)
  })
})

await client.connect('ws://localhost:8080')
```

旧连接上的普通订阅会在断连时结束，所以这里每次进入 `open` 都重新注册。应用退出时调用 `client.dispose()`，即可关闭连接并停止重连。

如果远端还要求发送 `subscribe` 之类的业务命令，也应在每次新的 `open` 阶段重新发送，并处理发送失败。

::: details 重连时保留什么，结束什么？

- 旧连接的未完成请求会拒绝，流会终止，普通订阅会结束；这些任务不会自动重放。
- Client 的 `handleRequest`、`handleStream` 和 `onStream` 注册跨连接保留，不需要重复注册。
- 重连使用固定间隔，成功连接后重置尝试次数。主动 `close()`、`detach()` 或 `dispose()` 会停止重连。
- Server 的全局处理器和订阅继续服务其他连接；Session 的局部处理器、订阅及房间成员关系随连接清理。
- 旧连接迟到的异步解码结果不会进入新连接。

`onState` 异步通知状态，不能依赖它一定先于服务端的首条消息运行。必须收到首次连接后的首条消息时，在 `connect()` 前订阅；重连后通过业务握手，让对端在订阅恢复后再开始推送。

业务是否重试、如何避免重复执行，由应用决定。例如支付或提交订单，不应仅因没有收到响应就直接重放。

:::

## 让长任务响应取消

连接断开、调用方取消等情况会让任务失效。处理器通过 `ctx.signal` 获知这一变化，把它传给支持取消的操作即可。

例如，服务端向内部 HTTP 服务读取报表时，可以把取消信号继续传下去：

```ts
import { WSServer } from '@ws-flow/server'

const server = WSServer.create({ port: 8080 })

server.handleRequest<string, string>('reportText', async ctx => {
  const reportId = encodeURIComponent(ctx.data)
  const response = await fetch(`http://localhost:3000/reports/${reportId}`, {
    signal: ctx.signal
  })

  if (!response.ok) throw new Error('读取报表失败')
  return response.text()
})
```

这里假设内部报表服务运行在 `localhost:3000`。调用方取消任务后，`fetch` 也能停止等待；服务退出时关闭 `server`。

取消不能强行中断任意 Promise、同步循环或已经完成的数据库写入。流生成器需要释放文件、游标等资源时，使用 `try/finally`；需要一致性的副作用仍由业务事务负责。完整例子见[可取消的进度任务](/examples/cancellable-task)。

::: details 连接状态与接管连接

`state` 读取当前状态；`onState` 观察之后的变化。主要状态为 `idle`、`connecting`、`open`、`closed`、`reconnecting` 和 `disposed`。

`connectTimeoutMs` 默认 10 秒，包含异步 socket factory 和握手过程。连接失败会使 `connect()` 拒绝。

`WSClient.takeover({ socket, send }, options)` 可以接管已经打开的连接。`detach()` 只解除本库监听并清理任务，保留外部 socket；连接过程中不能调用它。

自定义 `send` 必须准确反映发送完成或失败；Node.js 的自定义传输通常需要包装 `ws.send` callback。默认环境工厂已经处理这一差别。

任务处理器的信号属于整个任务；流钩子的信号属于相应流；普通收发中间件的信号只针对那一次分发，不能替代长任务的取消信号。

:::

有关连接与任务错误的区分，继续阅读[错误处理](/reference/errors)。
