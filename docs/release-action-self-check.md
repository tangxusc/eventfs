# Release Action 设计自检

本文记录 EventFS 四平台 release 构建的方案依据、边界与恢复方式。实现入口为
`.github/workflows/release.yml`。

## 需求覆盖

| 需求 | 设计 |
|---|---|
| Linux x86_64 / ARM64 | `ubuntu-24.04` 与 `ubuntu-24.04-arm` 原生 runner |
| macOS x86_64 / ARM64 | `macos-15-intel` 与 `macos-15` 原生 runner |
| 固定工具链 | Rust 1.88.0、Cargo.lock、protoc 35.1 |
| 发布门禁 | 每个平台执行 `cargo test --workspace --locked` 后才编译 release |
| 可执行性 | 每个平台执行 `--help`，并用 `file` 核对文件格式与架构 |
| Linux FUSE | 仅 Linux 资产包含 `eventfs-fuse` 和对应示例配置 |
| 完整性 | 四个压缩包按文件名排序生成并自校验 `SHA256SUMS` |
| 触发边界 | 仅 `workflow_dispatch`，不响应 branch/tag push |
| 手动验收 | 汇总产物为保留 30 天的 Actions artifact，不创建 GitHub Release |

## 方案选择

采用四个原生 runner，而不是在 x86 runner 上通过 `cross` 或 `cargo-zigbuild` 交叉
编译。项目依赖 `ring`、`zstd-sys` 和 Linux `fuser` 等原生构建链；原生 runner 能在
目标架构直接运行测试和 release 二进制，减少 C 工具链、链接器及模拟执行差异。

代价是四个平台都运行 workspace 默认测试，尤其会消耗更多 macOS runner 分钟。
这是发布可靠性优先的显式选择。GitHub 官方 runner-images 清单已确认上述四个标签
分别提供 x86_64 与 ARM64 环境。

## 资产与版本边界

每个平台生成一个 `eventfs-sha-<短提交号>-<target>.tar.gz`。压缩包内带 README 和
适用的示例配置，避免二进制与基本运行说明分离。该提交标识与
`[workspace.package].version` 相互独立，`eventstored --version` 与 `esctl --version`
仍以编译时 Cargo 版本为准。

## 权限与失败隔离

workflow 只有 `contents: read`。矩阵构建与资产汇总不能修改仓库；不存在发布 job，
也不申请 `contents: write`、`id-token`、`attestations` 或其他写权限。

四个平台任一测试、编译、冒烟或打包失败，汇总 job 不会执行。汇总 job 要求恰好四个
压缩包，并在上传前执行 `sha256sum --check`。上传中断时重新运行 workflow 会创建新的
run artifact；旧 run 保持不变，便于比较与追溯。

## 已知边界与回滚

- 默认测试包含仓库中的单元、属性/模糊和进程内 e2e；17 项 `#[ignore]` 环境型测试不
  自动启用，其中真 FUSE 挂载需要 `/dev/fuse` 与 `fusermount3`。
- workflow 不重新统计覆盖率；本轮全功能验收按 `docs/full-validation-self-check.md`
  独立重算行与分支覆盖率，并要求两者均不低于 80%。
- SHA256 用于下载完整性核对，不提供发布者身份签名或 GitHub artifact attestation。
- workflow 回滚只需还原对应 YAML；已生成的 Actions artifact 按 30 天保留策略到期，
  无需修改 GitHub Release。
