# 覆盖率达标实施设计

## 目标与范围

以整个 Cargo workspace 为验收范围，使用 `cargo +nightly llvm-cov` 的行覆盖率与
分支覆盖率均达到或超过 80%。默认测试、已有属性/模糊测试以及全部标记为
`#[ignore]` 的真实多进程测试均属于验收集。

## 统计流程

为避免普通 Cargo 构建覆盖带 instrumentation 的产物，覆盖率命令必须串行执行：

1. `cargo +nightly llvm-cov clean --workspace` 清理旧 profile 和 coverage 产物；
2. 运行 workspace 默认测试并保留 profile；
3. 以 `--no-clean` 分别运行 es-server 和 esctl 的 ignored 多进程测试；
4. 合并 profile 后生成带 `--branch` 的全工作区报告。

报告低于门槛时，先以文件和未覆盖行定位，再补测试；不通过排除生产文件、降低
统计范围或用无行为断言的测试抬高数值。

## 实施方案比较

### 方案 A：全工作区测试补齐（已选）

为所有报告中主要缺口补充确定性单元测试、属性测试和端到端测试。优点是与
`CLAUDE.md` 的验收范围一致，并能持续防止回归；代价是需要覆盖多个 crate 的错误
分支和失败恢复路径。

### 方案 B：仅统计本次改动 crate

只对 es-server、es-client 和 esctl 统计覆盖率。成本较低，但会掩盖其余 workspace
的未覆盖代码，违反全工作区验收约束，因此不采用。

## 测试策略

- 纯函数、编码和边界输入：使用单元与属性测试；
- 网络、重试、TLS、订阅、迁移与故障切换：使用进程内端到端测试；
- 选主、复制、进程重启：运行现有真实多进程 ignored 测试；
- 每个新增测试同时断言成功结果和关键失败分支，避免只提升行覆盖率。

## 设计自检

- 验收范围覆盖整个 workspace，而非单个 package；
- 分支统计显式启用 `--branch`；
- ignored 多进程测试在独立串行阶段纳入同一 profile；
- 测试优先覆盖生产行为，不修改业务语义以适配测试；
- README 与设计文档在达标后更新为实际统计命令和结果。

## 验收记录（2026-08-14）

以下命令在独立 worktree 串行执行；后两步通过 `EVENTSTORED_BIN` 复用带
instrumentation 的服务端二进制，确保真实多进程子进程也写入同一 profile：

```zsh
cargo +nightly llvm-cov clean --workspace
cargo +nightly llvm-cov --workspace --branch --no-report
EVENTSTORED_BIN="$PWD/target/llvm-cov-target/debug/eventstored" \
  cargo +nightly llvm-cov --package es-server --test multi_node_test \
  --branch --no-clean -- --ignored --test-threads=1
EVENTSTORED_BIN="$PWD/target/llvm-cov-target/debug/eventstored" \
  cargo +nightly llvm-cov --package es-ctl --test multi_node_test \
  --branch --no-clean -- --ignored --test-threads=1
cargo +nightly llvm-cov report --branch
```

结果：默认 workspace 测试、服务端 12 个真实多进程 E2E、CLI 2 个真实多进程
E2E 以及已有属性/模糊测试均通过。全 workspace 行覆盖率为 **92.61%**
（10,595/11,440），分支覆盖率为 **80.00%**（624/780），满足验收门槛。
