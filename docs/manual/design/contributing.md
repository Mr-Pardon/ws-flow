---
description: 本地运行、验证和维护 ws-flow 的起点。
---

# 参与维护

欢迎从一个具体用法或可复现的问题开始。维护目标是保持现有通信模型清晰、可靠，让新增行为有明确的归属和清理路径。

## 本地环境

仓库使用 ESM、Node.js 22 或更高版本与 pnpm 10.11.0。

```sh
pnpm install --frozen-lockfile
pnpm run check-types
pnpm run format:check
pnpm test
pnpm run verify-packages
```

| 命令 | 检查什么 |
| --- | --- |
| `check-types` | 源码和声明的严格类型 |
| `format:check` | 换行、缩进与方法间隔 |
| `test` | 构建后的任务、连接、异常与清理行为 |
| `verify-packages` | 仓库外安装实际 tarball，验证元数据、类型、README 与用户手册示例和同组行为 |
| `verify-browser` | 浏览器构建及可用环境中的真实浏览器通信 |

浏览器验证需要 Edge/Chrome，或通过 `WS_FLOW_BROWSER` 指定可执行文件。脚本明确记录实际握手结果；缺少浏览器时的跳过不表示已经通过真实浏览器验证。打包验证不执行 npm 发布。

三个[可运行案例](/examples/)帮助确认关键用法。在修改客户端入口、协议或生命周期后，应检查受影响案例是否仍然成立。

## 到哪里修改

| 变化 | 优先位置 |
| --- | --- |
| 请求结果关联、取消、超时、流状态 | `packages/runtime-core/src/core/` |
| 协议解释与能力声明 | `packages/runtime-core/src/protocol.ts` |
| 浏览器 / Node socket 入口、连接状态 | `packages/client/src/` |
| Server 接入、Session、房间与筛选 | `packages/server/src/core/` |
| 中间件组合 | `packages/pipeline/src/` |
| 消息调度与等待队列 | `packages/scheduler/src/` |
| 用户教程与手册页面 | `docs/manual/` |

同一个任务规则应在共享内核实现，并验证 Client 与 Server 两个方向。浏览器共享代码不能导入 Node `ws`；外部 socket 或服务器的接管也必须保留清晰的所有权。

## 修改的判断标准

修复时先写清楚触发条件和用户可见行为，再增加能够复现问题的回归。涉及任务或连接状态时，除正常完成外，还应覆盖此次修改影响的失败、取消或断连路径。

生命周期变更特别关注：是否只结算一次，是否移除监听和定时器，取消后迟到结果是否被隔离，以及连接关闭后本库持有的资源是否回收。

纯文案和格式修改无需增加行为测试。运行 `pnpm run format` 整理代码；不要手动修改 `dist` 等生成文件。

公共 API 变更同时更新用户手册与对应包 README。手册回答“用户如何做”，包 README 解释“模块如何维护”，尽量让可运行示例成为二者共享的事实来源。

## 问题反馈与变更说明

反馈问题时提供运行环境、使用入口、最小复现、预期结果与实际结果。移除连接地址中的凭证和消息中的业务敏感数据。

变更说明应让未读过讨论的人理解问题、最终行为、验证结果与兼容影响。新协议、新调度抽象或跨连接恢复等能力先说明实际场景与必要性，日常修复尽量在现有边界内完成。

仓库根的 `CONTRIBUTING.md` 提供同样的开发入口，`LICENSE` 记录 MIT 许可证；各包 README 提供进一步的源码导航。
