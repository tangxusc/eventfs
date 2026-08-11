//! openraft RaftNetwork 实现：通过 gRPC 做节点间通信。

use openraft::BasicNode;
use openraft::error::{
    InstallSnapshotError, NetworkError, PayloadTooLarge, RPCError, RaftError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use es_proto::eventstore::raft_rpc_client::RaftRpcClient;
use es_proto::limits::MAX_GRPC_MESSAGE_SIZE;
use es_proto::tls::{TlsClientConfig, apply_endpoint_tls};
use es_storage::TypeConfig;

/// 发送预算：8MB 上限减去 proto 信封 + gRPC 帧头 + 余量（1024 字节）。
///
/// bincode payload 超过该值即认为超限——留出的余量保证编码进
/// `RaftAppendEntriesRequest` 并套上 gRPC 帧后仍不超 8MB。
const APPEND_WIRE_BUDGET: usize = MAX_GRPC_MESSAGE_SIZE - 1024;

/// AppendEntries 批量超限时的拆分建议。
#[derive(Debug)]
enum SplitAdvice {
    /// 未超限，正常发送
    NoSplit,
    /// 按 hint（条数）拆小重试
    ShrinkTo(u64),
    /// 仅剩单条仍超限：openraft 无拆小路径，返回 Unreachable
    SingleEntryTooLarge,
}

/// 超限批量建议拆成的条数。
///
/// 优先按预算等比折算（按当前编码实际每条约字节数）：
/// `hint = budget × entries / payload_len`，超限时恒 < entries；
/// 折算出 ≥ entries 时（对端上限更小等场景）退化为「少一条」。
fn shrink_hint(entries: usize, payload_len: usize) -> u64 {
    let by_budget = (APPEND_WIRE_BUDGET as u64 * entries as u64) / payload_len as u64;
    by_budget.clamp(1, entries as u64 - 1)
}

/// 由 (条数, bincode payload 字节数) 计算拆分建议（发送前检查用）。
fn split_advice(entries: usize, payload_len: usize) -> SplitAdvice {
    if payload_len <= APPEND_WIRE_BUDGET {
        return SplitAdvice::NoSplit;
    }
    if entries <= 1 {
        // hint=1 会让 openraft 以单条无限紧循环重试（0.9.25 未实现
        // 「仅剩单条解释为 Unreachable」的文档语义），直接返回 Unreachable。
        return SplitAdvice::SingleEntryTooLarge;
    }
    SplitAdvice::ShrinkTo(shrink_hint(entries, payload_len))
}

/// 发送前检查：超限批量直接让 openraft 拆小重试。
///
/// 不把超限请求发上线路由被 gRPC 拒绝——openraft 对 Network 错误退避后
/// 重试同样大小的批量，复制永久停滞；PayloadTooLarge 则立即拆小重试。
fn pre_send_oversize_err<E: std::error::Error>(
    entries: usize,
    payload_len: usize,
) -> Option<RPCError<u64, BasicNode, E>> {
    match split_advice(entries, payload_len) {
        SplitAdvice::NoSplit => None,
        SplitAdvice::ShrinkTo(hint) => {
            Some(RPCError::PayloadTooLarge(PayloadTooLarge::new_entries_hint(hint)))
        }
        SplitAdvice::SingleEntryTooLarge => Some(RPCError::Unreachable(Unreachable::new(
            &std::io::Error::other("单条 AppendEntries 超过 8MB 消息上限"),
        ))),
    }
}

/// 接收侧兜底：对端拒绝超限消息（`Code::ResourceExhausted`）时的映射。
///
/// 正常路径发送前检查已拦截；此处覆盖估算误差、节点间上限配置不一致等
/// 场景。对端已拒绝且不知道其上限，用二分收缩：每轮 1 次 RPC（失败会刷新
/// hint，TTL=10 次计数在成功续传时才限制 hint 寿命），约 log2(条数) 轮收敛。
fn recv_oversize_err<E: std::error::Error>(
    entries: usize,
) -> RPCError<u64, BasicNode, E> {
    if entries <= 1 {
        RPCError::Unreachable(Unreachable::new(&std::io::Error::other(
            "单条 AppendEntries 超过对端消息上限",
        )))
    } else {
        RPCError::PayloadTooLarge(PayloadTooLarge::new_entries_hint(
            ((entries as u64) / 2).max(1),
        ))
    }
}

// 端点归一化规则定义在 es-proto（es-ctl 等客户端共用），此处 re-export 保持原路径 API：
// `es_raft::network::normalize_endpoint` 与 `es_raft::normalize_endpoint` 均仍可用。
pub use es_proto::endpoint::normalize_endpoint;

/// 某分片的网络工厂。
///
/// 每个分片各持一个实例并记住自己的 `shard_id`：`RaftNetworkFactory::new_client`
/// 只传 target 节点，不传分片，所以分片信息必须由工厂自身携带，
/// 否则对端无法知道该把消息投给哪个 Raft 实例。
#[derive(Clone)]
pub struct GrpcNetwork {
    shard_id: u64,
    /// 出站 Raft RPC 的客户端信任策略：目标地址为 https:// 时生效
    /// （缺省跳过校验，自签友好）；http 集群传 None。
    tls: Option<TlsClientConfig>,
}

impl GrpcNetwork {
    /// tls：节点间 Raft RPC 的 TLS 信任策略；明文集群传 None。
    pub fn new(shard_id: u64, tls: Option<TlsClientConfig>) -> Self {
        Self { shard_id, tls }
    }

    pub fn shard_id(&self) -> u64 {
        self.shard_id
    }
}

impl RaftNetworkFactory<TypeConfig> for GrpcNetwork {
    type Network = GrpcConnection;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        // 地址取自 openraft 保存的 BasicNode.addr —— 它随 add_learner/initialize
        // 写入 membership 日志并复制到各节点，因此无需另建 node_id → addr 映射表。
        GrpcConnection::new(self.shard_id, target, node.addr.clone(), self.tls.clone())
    }
}

/// 指向单个目标节点的连接。
pub struct GrpcConnection {
    shard_id: u64,
    target: u64,
    addr: String,
    /// 出站 TLS 信任策略（https 目标时生效）
    tls: Option<TlsClientConfig>,
    /// 惰性建立的 gRPC 通道。
    ///
    /// 用 `connect_lazy` 而非 `connect`：后者在对端未就绪时直接失败，
    /// 而集群启动阶段各节点上线有先后，选举期间必然出现互相未就绪。
    /// 惰性通道会在每次调用时按需重连，且 Channel 内部自带连接复用。
    client: Option<RaftRpcClient<tonic::transport::Channel>>,
}

impl GrpcConnection {
    /// 创建指向目标节点的连接（网络工厂内部使用；测试直接构造 stub 场景）
    pub fn new(shard_id: u64, target: u64, addr: String, tls: Option<TlsClientConfig>) -> Self {
        Self {
            shard_id,
            target,
            addr,
            tls,
            client: None,
        }
    }

    /// 取出（必要时建立）gRPC 客户端
    fn client<E>(
        &mut self,
    ) -> Result<RaftRpcClient<tonic::transport::Channel>, RPCError<u64, BasicNode, E>>
    where
        E: std::error::Error,
    {
        if let Some(c) = &self.client {
            return Ok(c.clone());
        }

        // BasicNode.addr 可能不带 scheme，补上 http:// 才是合法的 endpoint
        let uri = normalize_endpoint(&self.addr);

        let endpoint = tonic::transport::Endpoint::from_shared(uri.clone()).map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                "节点 {} 地址 {uri} 非法: {e}",
                self.target
            ))))
        })?;

        // https 目标按信任策略装配 TLS（缺省跳过校验）；http 原样
        let endpoint = apply_endpoint_tls(endpoint, self.tls.as_ref()).map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                "节点 {} TLS 配置失败（{uri}）: {e}",
                self.target
            ))))
        })?;

        let c = RaftRpcClient::new(endpoint.connect_lazy())
            // 与系统级 8MB 上限对齐：tonic 解码默认 4MB，不设置的话
            // 大响应（快照块、批量响应）在客户端就被拒。
            .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE);
        self.client = Some(c.clone());
        Ok(c)
    }
}

fn net_err<E>(ctx: &str, e: impl std::fmt::Display) -> RPCError<u64, BasicNode, E>
where
    E: std::error::Error,
{
    RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
        "{ctx}: {e}"
    ))))
}

impl RaftNetwork<TypeConfig> for GrpcConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let payload = bincode::serde::encode_to_vec(&req, bincode::config::standard())
            .map_err(|e| net_err("序列化 AppendEntries 请求", e))?;

        // 发送前拦截超限批量（编码已做，零额外成本）：让 openraft 立即拆小
        // 重试，而不是让超限请求被对端 gRPC 拒绝后按 Network 错误退避重试
        // 同样大小的批量（复制永久停滞）。
        if let Some(err) = pre_send_oversize_err(req.entries.len(), payload.len()) {
            return Err(err);
        }

        let mut client = self.client()?;
        let resp = client
            .append_entries(es_proto::eventstore::RaftAppendEntriesRequest {
                shard_id: self.shard_id,
                payload,
            })
            .await
            .map_err(|e| {
                // 兜底：估算误差或节点间上限配置不一致导致对端拒绝超限消息。
                // 映射为 PayloadTooLarge 让 openraft 拆小重试；其余错误原样包装。
                if e.code() == tonic::Code::ResourceExhausted {
                    recv_oversize_err(req.entries.len())
                } else {
                    net_err("AppendEntries RPC", e)
                }
            })?
            .into_inner();

        let out = bincode::serde::decode_from_slice(&resp.payload, bincode::config::standard())
            .map_err(|e| net_err("反序列化 AppendEntries 响应", e))?
            .0;
        Ok(out)
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let payload = bincode::serde::encode_to_vec(&req, bincode::config::standard())
            .map_err(|e| net_err("序列化 InstallSnapshot 请求", e))?;

        // 快照侧不做发送前检查：openraft 0.9.25 对快照块的 PayloadTooLarge
        // 直接放弃传输（无拆小路径），检查无意义——块大小由启动校验
        // （snapshot max_chunk_size ≤ 6MiB）保证不超限。
        let mut client = self.client()?;
        let resp = client
            .install_snapshot(es_proto::eventstore::RaftInstallSnapshotRequest {
                shard_id: self.shard_id,
                payload,
            })
            .await
            .map_err(|e| {
                // 兜底映射（正常不可达）：PayloadTooLarge 在快照传输里是明确的
                // 「块被拒」终止错误，比 Network 错误退避重试同样大小的块更好诊断。
                // 注:openraft 未公开快照动作的 PayloadTooLarge 构造器(new_bytes_hint
                // 是 pub(crate)),统一用 new_entries_hint(1) —— Chunked 只看变体不看
                // action,日志里显示 AppendEntries 是 openraft 的显示限制,功能无害。
                if e.code() == tonic::Code::ResourceExhausted {
                    RPCError::PayloadTooLarge(PayloadTooLarge::new_entries_hint(1))
                } else {
                    net_err("InstallSnapshot RPC", e)
                }
            })?
            .into_inner();

        let out = bincode::serde::decode_from_slice(&resp.payload, bincode::config::standard())
            .map_err(|e| net_err("反序列化 InstallSnapshot 响应", e))?
            .0;
        Ok(out)
    }

    async fn vote(
        &mut self,
        req: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let payload = bincode::serde::encode_to_vec(&req, bincode::config::standard())
            .map_err(|e| net_err("序列化 Vote 请求", e))?;

        let mut client = self.client()?;
        let resp = client
            .vote(es_proto::eventstore::RaftVoteRequest {
                shard_id: self.shard_id,
                payload,
            })
            .await
            .map_err(|e| net_err("Vote RPC", e))?
            .into_inner();

        let out = bincode::serde::decode_from_slice(&resp.payload, bincode::config::standard())
            .map_err(|e| net_err("反序列化 Vote 响应", e))?
            .0;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 预算内 → 正常发送
    #[test]
    fn split_advice_within_budget_no_split() {
        assert!(matches!(
            split_advice(10, APPEND_WIRE_BUDGET),
            SplitAdvice::NoSplit
        ));
        assert!(matches!(
            split_advice(10, APPEND_WIRE_BUDGET - 1),
            SplitAdvice::NoSplit
        ));
        // 心跳（无 entries）永远不拆分
        assert!(matches!(split_advice(0, 100), SplitAdvice::NoSplit));
    }

    /// 超限且多条的批量 → 等比拆小 hint，恒 ∈ (0, entries)
    #[test]
    fn split_advice_oversized_multi_entry_shrinks() {
        // 300 条编码后 3 倍于预算 → hint ≈ 100
        let entries = 300;
        match split_advice(entries, APPEND_WIRE_BUDGET * 3) {
            SplitAdvice::ShrinkTo(hint) => {
                assert!(hint > 0 && hint < entries as u64);
                // 等比折算：hint = budget×300/(3×budget) = 100
                assert_eq!(hint, 100);
            }
            other => panic!("应 ShrinkTo，实际 {other:?}"),
        }
        // 略微超限 → hint = entries - 1（clamp 上界）
        match split_advice(entries, APPEND_WIRE_BUDGET + 1) {
            SplitAdvice::ShrinkTo(hint) => assert_eq!(hint, entries as u64 - 1),
            other => panic!("应 ShrinkTo，实际 {other:?}"),
        }
        // 两倍预算 → hint = entries / 2
        match split_advice(entries, APPEND_WIRE_BUDGET * 2) {
            SplitAdvice::ShrinkTo(hint) => assert_eq!(hint, 150),
            other => panic!("应 ShrinkTo，实际 {other:?}"),
        }
    }

    /// 单条超限 → SingleEntryTooLarge（不构造 hint，规避 openraft debug_assert）
    #[test]
    fn split_advice_single_entry_oversized() {
        assert!(matches!(
            split_advice(1, APPEND_WIRE_BUDGET + 1),
            SplitAdvice::SingleEntryTooLarge
        ));
    }

    /// hint 公式边界：payload_len 极小（对端上限更小时）不 panic、hint 有界
    #[test]
    fn shrink_hint_always_bounded() {
        for entries in [2u64, 3, 10, 300] {
            for payload in [1usize, 1024, APPEND_WIRE_BUDGET, APPEND_WIRE_BUDGET * 10] {
                let hint = shrink_hint(entries as usize, payload);
                assert!(hint >= 1 && hint < entries, "hint={hint} entries={entries}");
            }
        }
    }

    /// 接收侧兜底:单条被拒 → Unreachable(避免 hint=1 让 openraft 死循环)
    #[test]
    fn recv_oversize_err_single_entry_unreachable() {
        assert!(matches!(
            recv_oversize_err::<std::io::Error>(1),
            RPCError::Unreachable(_)
        ));
    }

    /// 接收侧兜底:多条被拒 → 二分收缩 hint
    #[test]
    fn recv_oversize_err_multi_entry_bisect() {
        match recv_oversize_err::<std::io::Error>(8) {
            RPCError::PayloadTooLarge(p) => assert_eq!(p.entries_hint(), 4),
            other => panic!("应 PayloadTooLarge,实际 {other:?}"),
        }
    }

    /// 传输链不变量:服务端最大合法 append(单事件 ≤1MiB、批次编码 ≤7MiB)
    /// 转换领域模型后 bincode 不超发送预算。
    ///
    /// 该不变量依赖 bincode 默认变长整数编码(bincode 2 standard()):
    /// 若默认改为 fixint,逐事件膨胀 ~19B × 20 万条就会让合法请求超预算,
    /// 走 SingleEntryTooLarge → Unreachable → 复制停滞——正是本文件要防的故障。
    #[test]
    fn max_legal_append_bincode_within_budget() {
        use es_core::{ExpectedVersion, Hlc, NewEvent};
        use es_storage::EsRequest;

        // 6 条 × 1MiB:客户端估算 ≈ 6.3MiB 通过本地检查,服务端 encoded_len
        // ≈ 6.3MiB ≤ 7MiB 接受(7 条会超 7MiB 被拒,有效上限 6 条)
        let req = EsRequest::Append {
            stream_id: "s".to_string(),
            expected_version: ExpectedVersion::Any,
            events: vec![NewEvent {
                event_id: uuid::Uuid::new_v4(),
                event_type: "t".into(),
                data: vec![0u8; 1024 * 1024],
                metadata: vec![],
            }; 6],
            hlc: Hlc::now(),
        };
        let payload = bincode::serde::encode_to_vec(&req, bincode::config::standard())
            .expect("编码 EsRequest");
        assert!(
            payload.len() <= APPEND_WIRE_BUDGET,
            "最大合法 append bincode {} 字节超发送预算 {}",
            payload.len(),
            APPEND_WIRE_BUDGET
        );
    }
}
