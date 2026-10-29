# 发布操作

本页供维护者执行交付，用户用法以 [用户手册](manual/guide/index.md) 为准。

## 发布清单

| 包 | 版本 | 发布顺序 |
| --- | --- | --- |
| @ws-flow/consumer | 1.0.0 | 1 |
| @ws-flow/envelope | 1.0.0 | 1 |
| @ws-flow/pipeline | 1.0.0 | 1 |
| @ws-flow/scheduler | 1.0.0 | 1 |
| @ws-flow/runtime-core | 1.0.0 | 2 |
| @ws-flow/server | 1.0.0 | 3 |
| @ws-flow/client | 2.0.0 | 3 |

包配置使用正式版本和 public 访问级别。根 workspace 保持 private。打包通过 prepack 构建自身；先运行根 build，以保证依赖声明已生成。pnpm pack/publish 将 workspace 引用替换为实际包版本。

## 仓库与归属

源代码位于 [Mr-Pardon/ws-flow](https://github.com/Mr-Pardon/ws-flow)，现有 `Mr-Pardon/ws-flow-client` 保留其 Client 1.x 发行记录。各包的 repository、homepage、bugs 指向新仓库，发布前核对这些链接与文档站地址。

本地文件准备不等于远端已经交付。发布操作需确认 GitHub 仓库、npm 账号及 @ws-flow scope 权限。不要根据公共查询的 404 判断包名可用或账号拥有发布权。

## 本地验收

```sh
pnpm install --frozen-lockfile
pnpm run check-types
pnpm run format:check
pnpm test
pnpm run verify-packages
pnpm run docs:build
pnpm run example:duplex
pnpm run example:generic
pnpm run example:cancel
```

verify-packages 在仓库外安装七个实际 tarball，验证正式版本、公开配置、LICENSE、元数据、依赖版本、公开类型、README/手册示例和行为测试。安装覆盖使用本次 tarball，仍需在实际发布后验证注册表依赖可获取。

浏览器验收有三种入口：默认自动启动本机浏览器；`pnpm run verify-packages --browser-manual` 打印本地验证地址，由已打开的浏览器访问并完成同组原生 WebSocket 断言；`--browser-bundle-only` 仅验证浏览器构建，不能作为实际握手通过的证据。单独运行浏览器验证时，对应参数为 `pnpm run verify-browser --manual` / `--bundle-only`。

文档检查覆盖首页、快速开始、架构模型、搜索、代码组和折叠说明；部署到项目路径时设置 `WS_FLOW_DOCS_BASE=/ws-flow/` 再构建并检查路径。

## 远端交付

1. 核对版本与发布清单，填写可用的仓库元数据，推送 GitHub 并确认 CI 结果。
2. 使用最终发布源按表中顺序执行各包的 pnpm publish；每个依赖层公开后确认对应版本可读取，再发布依赖它的包。
3. 从全新项目安装明确版本的 Client/Server，使用 Node 和浏览器公开入口运行消费示例，不使用 workspace 链接或 overrides。
4. 在 GitHub Pages 部署文档站，核对首页安装命令、导航、搜索与静态资源。
5. 为实际交付填写 CHANGELOG 的发布日期与版本记录，并将旧 Client 仓库入口引导到新仓库的升级说明。

以上远端动作以真实返回结果为准；不要将本地打包验证记录为 npm 发布成功。
