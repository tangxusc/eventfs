#!/usr/bin/env bash
# 3 节点 TLS 集群端到端性能压测。
#
# 流程：release 构建 → 生成自签证书 → 生成 N 节点配置 → 启动集群 →
#       等端口就绪 → 跑 perf_test 压测客户端 → 打印结果。
# 清理：trap EXIT 统一 kill 节点进程、删除工作目录（数据/证书/日志），
#       无论成功失败都执行。结果 JSON 写到 $REPO_ROOT/target/perf-results/
#       （trap 删除范围之外），压测中途失败也保留已完成规格的结果。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d /tmp/eventfs-perf.XXXXXX)"
NODE_PIDS=()
PORT_BASE=50051
NODE_N=3
OUT_DIR="$REPO_ROOT/target/perf-results"

cleanup() {
    # 退出码由 trap 'cleanup $?' EXIT 传入：trap 字符串里的 $? 在触发
    # 时才展开，能拿到真实退出码。不能在函数里用 `local rc` 后接
    # `rc=$?`——local 本身成功返回 0，会覆盖 $?（bash 3.2 下
    # `local rc=$?` 单行又有返回非零的问题，两者都不可靠）
    local rc=$1
    # 杀节点进程（按启动顺序逆序），容忍"已退出"
    local pid
    for pid in "${NODE_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in "${NODE_PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    rm -rf "$WORK"
    if [ "$rc" -ne 0 ]; then
        echo "✗ 压测失败（exit=$rc），已清理测试资源" >&2
    fi
    exit "$rc"
}
trap 'cleanup $?' EXIT

echo "==> 工作目录: $WORK"

# ---------- 1. release 构建（性能测试必须 release 优化） ----------
echo "==> 构建 eventstored 与 perf_test ..."
# 必须进仓库根再构建：cargo 从调用者 cwd 解析工作区，仓库外调用会
# 找不到 Cargo.toml
cd "$REPO_ROOT"
cargo build --release -p es-server --bin eventstored --example perf_test
# 用 cargo metadata 解析 target 目录，兼容 CARGO_TARGET_DIR 指向别处
# 的场景（硬编码 $REPO_ROOT/../target 时产物落别处、节点启动失败且
# 报误导性错误）。macOS 自带 python3，无需 jq。
TARGET_DIR="$(cargo metadata --format-version=1 --no-deps |
    python3 -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')"
BIN="$TARGET_DIR/release/eventstored"
PERF="$TARGET_DIR/release/examples/perf_test"

# ---------- 2. 生成自签证书（每节点独立，CA:FALSE；ca_file 拼接互信） ----------
CERT_DIR="$WORK/certs"
mkdir -p "$CERT_DIR"
for i in $(seq 1 "$NODE_N"); do
    openssl req -x509 -newkey rsa:2048 -nodes \
        -keyout "$CERT_DIR/node$i.key" -out "$CERT_DIR/node$i.crt" \
        -days 1 -subj "/CN=127.0.0.1" \
        -addext "subjectAltName=IP:127.0.0.1" \
        -addext "basicConstraints=critical,CA:FALSE" \
        >/dev/null 2>&1
done
# 互信包按 NODE_N 循环拼接（硬编码 node{1,2,3} 会在 NODE_N 变化时
# 漏掉新节点——TLS 握手失败被吞进日志，静默跑成少节点拓扑）
: > "$CERT_DIR/all.crt"
for i in $(seq 1 "$NODE_N"); do
    cat "$CERT_DIR/node$i.crt" >> "$CERT_DIR/all.crt"
done

# ---------- 3. 生成 N 节点配置（TLS + https peers 自动组建） ----------
for i in $(seq 1 "$NODE_N"); do
    port=$((PORT_BASE + i - 1))
    # peers 块按 NODE_N 动态生成（与上方证书拼接同源，配置不再写死 3 节点）
    peers_text=""
    for j in $(seq 1 "$NODE_N"); do
        p=$((PORT_BASE + j - 1))
        peers_text+="[[node.peers]]
id = $j
addr = \"https://127.0.0.1:$p\"
"
    done
    cat > "$WORK/node$i.toml" <<EOF
[node]
id = $i
listen_addr = "127.0.0.1:$port"

$peers_text
[tls]
cert_file = "$CERT_DIR/node$i.crt"
key_file = "$CERT_DIR/node$i.key"
ca_file = "$CERT_DIR/all.crt"

[storage]
data_dir = "$WORK/data/node$i"

# 三节点 rf=1：每节点主承载自己的分片（8 分片，环形）
[placement]
replication_factor = 1

[[placement.nodes]]
id = 1
primary = [0, 1, 2, 3, 4]
replica = []

[[placement.nodes]]
id = 2
primary = [5, 6, 7]
replica = []

[[placement.nodes]]
id = 3
primary = []
replica = []
EOF
done

# ---------- 4. 启动集群，轮询端口就绪 ----------
# 清理上次 SIGKILL（trap EXIT 不执行）残留的节点进程：孤儿进程持有
# 端口会让 nc 探测误判成功，压测连上旧证书/旧数据集群产生误导结果
if pkill -f "$BIN" 2>/dev/null; then
    echo "==> 已清理残留 eventstored 进程"
    sleep 1
fi
echo "==> 启动 $NODE_N 节点（TLS，端口 $PORT_BASE-$((PORT_BASE + NODE_N - 1))）..."
for i in $(seq 1 "$NODE_N"); do
    "$BIN" --config "$WORK/node$i.toml" >"$WORK/node$i.log" 2>&1 &
    NODE_PIDS+=($!)
done

# 端口就绪轮询（最多 30s）
for i in $(seq 1 "$NODE_N"); do
    port=$((PORT_BASE + i - 1))
    for _ in $(seq 1 60); do
        if nc -z 127.0.0.1 "$port" 2>/dev/null; then
            break
        fi
        sleep 0.5
    done
    if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
        echo "✗ 节点 $i 端口 $port 未就绪" >&2
        tail -20 "$WORK/node$i.log" >&2 || true
        exit 1
    fi
done
# 选举收敛不等固定时长：收敛耗时随机器负载波动（实测可超 7s），由
# perf_test 预热 append 的重试兜底，端口就绪后直接开跑
echo "==> 端口全部就绪（选举收敛由 perf_test 预热重试兜底）"

# ---------- 5. 跑压测 ----------
# 节点地址按 NODE_N 动态拼接
addrs=""
for j in $(seq 1 "$NODE_N"); do
    p=$((PORT_BASE + j - 1))
    [ -n "$addrs" ] && addrs+=","
    addrs+="https://127.0.0.1:$p"
done
# 结果落盘到 trap 删除范围之外（写 $WORK 里等于跑完即失），带时间戳
# 便于多轮对比；perf_test 每完成一个规格增量写盘，中途失败也能保留
mkdir -p "$OUT_DIR"
OUT="$OUT_DIR/result-$(date +%Y%m%d-%H%M%S).json"
# --output 由脚本管理（决定落盘路径与备份），透传参数不允许重复指定
for a in "$@"; do
    case "$a" in
        --output|--output=*)
            echo "✗ 透传参数含 --output：结果路径由脚本管理（$OUT_DIR/）" >&2
            exit 1
            ;;
    esac
done
echo "==> 开始压测（1KB / 10KB / 100KB 事件，全链路 写+读+订阅）..."
"$PERF" \
    --addrs "$addrs" \
    --ca "$CERT_DIR/all.crt" \
    --output "$OUT" \
    "$@"

# ---------- 6. 汇总 ----------
echo
echo "===== 结果 JSON ====="
cat "$OUT"
echo
echo "===== 结果已保存: $OUT ====="
echo "===== 压测完成，清理测试资源 ====="
