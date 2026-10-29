# 参与 ws-flow 开发

项目使用 ESM、Node.js 22 或更高版本与 pnpm 10.11.0。自动化验证基线为 Node.js 22。

## 本地运行与验证

```sh
pnpm install --frozen-lockfile
pnpm run check-types
pnpm run format:check
pnpm test
pnpm run verify-packages
pnpm run docs:build
```

运行 `pnpm run docs:dev` 阅读并编辑用户手册。三个通信案例可分别通过 `pnpm run example:duplex`、`pnpm run example:generic`、`pnpm run example:cancel` 执行。

`verify-packages` 在仓库外安装七个实际 tarball，检查许可证、包元数据、依赖、公共类型、README 与用户手册示例和同组行为测试，再验证浏览器产物。内部依赖通过 overrides 指向同一次构建的 tarball，确保检查对象是完整发布物；脚本不执行发布。

真实浏览器验证需要本机 Edge/Chrome，或设置 `WS_FLOW_BROWSER` 指向浏览器可执行文件。缺少浏览器时会明确记录跳过握手；跳过不表示完成真实通信验证。浏览器构建检查不引入 Node `ws`，实际握手覆盖双向请求、流和资源错误后的行为。临时验证目录保留以便排查问题。

不使用自动启动时，可运行 `pnpm run verify-packages --browser-manual`，在浏览器中打开输出的本地地址完成通信断言；`--browser-bundle-only` 仅检查浏览器构建，不验证握手。单独运行 `verify-browser` 时，对应参数为 `--manual` 和 `--bundle-only`。

## 模块职责

- 请求、订阅、流状态和任务清理归属 runtime-core，两端共享同一语义。
- 环境 socket、编解码与连接管理归属 client/server，浏览器入口不能引入 Node `ws`。
- 协议解释和编码消息，插件组合处理规则，调度器安排帧的执行顺序；各模块不另建请求表。
- 修复逻辑错误时补能复现该错误的回归，覆盖相关异常、取消或断连路径；纯文案和格式修改无需新增行为测试。
- 保持简洁换行和方法间空行，提交前运行 `pnpm run format`。不要手动修改生成的 `dist` 或验证产物。

新增公共能力应明确具体使用场景、模块归属、资源所有权和兼容影响。系统职责见[系统架构](docs/manual/design/index.md)，各包 README 提供源码导航与维护约束。

## 提交说明

Issue 请附 Node/浏览器版本、使用入口、最小复现、预期与实际行为；移除消息中的凭证和业务敏感信息。PR 说明解决的问题、用户可见变化、验证结果和兼容影响。公开 API 变化同步更新对应包 README 与用户手册。

## 维护用户手册

`docs/manual` 是用户手册的唯一内容源，`docs/.vitepress` 保存导航和主题。使用 `pnpm run docs:dev` 本地编辑，`pnpm run docs:build` 检查静态构建和页面链接，`pnpm run docs:preview` 预览构建产物。

用户文档直接说明产品行为、使用方法与边界。架构文档解释模块如何协作，避免把开发过程、阶段定位或历史决策写入使用路径。版本升级的兼容要求集中在迁移说明与变更记录中。

TypeScript 代码块应能独立编译，包含必需的 import 和变量声明。`verify-packages` 将各包 README 和手册中的这些代码块放到仓库外，使用本次打包的公共入口严格编译。纯签名或伪代码使用 text 代码块；完整案例直接引用 examples 中实际运行的源文件。

指南先展示一个业务动作，再逐步加入错误处理、取消和配置。两端代码分开展示，说明运行顺序；持续连接示例在文字中交代清理时机。清理与重连的完整用法集中在生命周期章节，少见边界放在后段或折叠说明中。

案例页可以摘录实际 JavaScript 源码中的关键步骤，注明沿用的变量，并保留完整源码引用和运行命令。摘录不能冒充可独立执行的程序，TypeScript 也不能改成普通文本来绕过检查。

站点默认根路径。部署到 GitHub Pages 项目路径时，设置 `WS_FLOW_DOCS_BASE=/ws-flow/` 后构建，并按实际仓库名调整路径。

## 许可证

本项目使用 [MIT 许可证](LICENSE)。

## 发布

正式版本、依赖顺序、发布物验收与远端交付步骤见[发布操作](docs/RELEASING.md)。本地构建与安装验证不代表 npm 或文档站已发布，远端交付需以实际操作结果确认。
