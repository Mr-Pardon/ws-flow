---
description: 用三个可运行的小案例理解事件筛选、进度任务与双向请求。
---

# 把通信模型用起来

通过事件筛选、可取消的进度任务和双向查询，将通信 API 接入具体业务。每个案例提供关键步骤、运行命令与完整源码。

所有案例都在本机启动服务，运行完成后自动关闭；无需外部账号、在线服务或额外配置。

| 想解决的问题 | 案例 | 主要 API |
| --- | --- | --- |
| 接入已有服务，只处理关心的事件 | [独立 Client 与事件筛选](/examples/generic-client) | `send`、`subscribe` |
| 发起耗时任务，看进度并随时停止 | [可取消的进度任务](/examples/cancellable-task) | `requestStream`、`handleStream`、`signal` |
| 两端互相查询，Server 汇总 Client 状态 | [双向请求与结果收集](/examples/duplex) | `request`、`handleRequest`、`requestAll` |

## 运行方式

在项目仓库根目录使用 Node.js 22 或更高版本和 pnpm 10.11.0：

```sh
pnpm install --frozen-lockfile
pnpm run build
node examples/generic-client.mjs
node examples/cancellable-task.mjs
node examples/duplex.mjs
```

每个案例独立分配本机空闲端口，运行后自动释放连接。完整源码可在对应页面展开。

## 阅读顺序

只使用 Client 接入现有服务，从第一个案例开始。需要完整的请求与流语义，先读[快速开始](/guide/quick-start)，再运行后两个案例。

接入应用时可以复用已有连接，把 `console.log` 换成界面更新或数据处理，并将连接的清理绑定到页面卸载或应用退出。

::: tip 带回自己的项目
仓库示例使用相对路径导入构建产物，便于直接运行。应用项目通过包名导入，环境选择与最小代码见[快速开始](/guide/quick-start)。
:::
