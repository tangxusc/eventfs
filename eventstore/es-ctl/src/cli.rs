//! esctl 命令行参数定义（clap derive）。
//!
//! 本文件只做声明与纯解析（不触网、不读盘），可解析部分全部下沉为
//! 独立函数与类型，便于单元测试。

use std::path::PathBuf;
use std::str::FromStr;

use clap::{Args, Parser, Subcommand, ValueEnum};

/// 输出格式（-w）
#[derive(Clone, Copy, Debug, Default, ValueEnum, PartialEq)]
pub enum Format {
    /// 逐行可读文本
    #[default]
    Simple,
    /// 对齐表格
    Table,
    /// JSON（便于脚本解析）
    Json,
}

/// 集群成员：ID@ADDR（地址裸格式由连接层统一归一化补 http://）
#[derive(Clone, Debug, PartialEq)]
pub struct MemberArg {
    pub node_id: u64,
    pub addr: String,
}

impl FromStr for MemberArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (id, addr) = s
            .split_once('@')
            .ok_or_else(|| format!("非法成员 {s:?}：应为 ID@ADDR，如 2@127.0.0.1:50052"))?;
        let node_id = id
            .parse::<u64>()
            .map_err(|_| format!("非法成员 {s:?}：节点 ID 应为数字"))?;
        if addr.is_empty() {
            return Err(format!("非法成员 {s:?}：地址不能为空"));
        }
        Ok(Self {
            node_id,
            addr: addr.to_string(),
        })
    }
}

/// --shards 解析：拒绝 0。
///
/// 0 个 Shard 无法承载控制 catalog 或 Aggregate 分区，没有合法含义。
fn parse_shards(s: &str) -> Result<u64, String> {
    let v: u64 = s
        .parse()
        .map_err(|_| format!("非法分片数 {s:?}：应为数字"))?;
    if v == 0 {
        return Err("分片数必须 ≥ 1".into());
    }
    Ok(v)
}

/// 全局参数（必须位于子命令之前，同 etcdctl 约定）
#[derive(Args, Debug, Clone)]
pub struct GlobalArgs {
    /// 集群节点 gRPC 地址列表，逗号分隔（如
    /// http://127.0.0.1:50051,http://127.0.0.1:50052）；裸地址自动补 http://
    #[arg(long, value_delimiter = ',', default_value = "http://127.0.0.1:50051")]
    pub endpoints: Vec<String>,

    /// 建立连接的超时时间（秒）
    #[arg(long, default_value_t = 5)]
    pub dial_timeout: u64,

    /// 单次 RPC 请求的超时时间（秒），0 表示不设超时；watch 长连接不受影响
    #[arg(long, default_value_t = 10)]
    pub timeout: u64,

    /// 服务端证书校验用的 CA 文件（PEM，可含多张）；仅对 https 端点生效，
    /// 与 --insecure-skip-tls-verify 互斥
    #[arg(long, conflicts_with = "insecure_skip_tls_verify")]
    pub cacert: Option<PathBuf>,

    /// 跳过 https 端点证书校验（自签证书场景，默认行为）；仅对 https 端点生效
    #[arg(long)]
    pub insecure_skip_tls_verify: bool,

    /// 输出格式：simple / table / json
    #[arg(short, long, value_enum, default_value_t = Format::Simple)]
    pub write_out: Format,

    /// 分片总数；缺省时对各端点 ListShards 取并集，探测失败回退默认 8
    #[arg(long, value_parser = parse_shards)]
    pub shards: Option<u64>,
}

#[derive(Parser, Debug)]
#[command(name = "esctl")]
#[command(about = "EventFS AggregateStore 命令行工具（参照 etcdctl）")]
#[command(version)]
pub struct Cli {
    #[command(flatten)]
    pub global: GlobalArgs,

    #[command(subcommand)]
    pub command: Command,
}

/// 子命令树（两层：esctl <命令>；member 下再分 add/remove/list）
#[derive(Subcommand, Debug)]
pub enum Command {
    /// 初始化分片集群（把给定成员写入首条 membership 日志，只需在一个节点调用一次）
    Init(InitArgs),
    /// 集群成员管理（加 learner、提升/移除投票成员、列表）
    Member(MemberArgs),
    /// 各端点健康与分片归属视图
    Status(StatusArgs),
    /// 快照管理（离线操作数据目录）
    Snapshot(SnapshotArgs),
    /// 管理聚合类型、实例事件、状态与消费者组。
    Aggregate(AggregateArgs),
}

/// AggregateStore 命令（esctl aggregate <action>）。
#[derive(Args, Debug)]
pub struct AggregateArgs {
    #[command(subcommand)]
    pub action: AggregateAction,
}

/// AggregateStore 操作。
#[derive(Subcommand, Debug)]
pub enum AggregateAction {
    /// 查询 AggregateStore 协议能力。
    Capabilities,
    /// 注册、枚举或查询 AggregateType。
    Type(AggregateTypeCommandArgs),
    /// 追加一条实例事件。
    Append(AggregateAppendArgs),
    /// 从历史或当前时刻持续跟随事件。
    Follow(AggregateFollowArgs),
    /// 读取或覆盖聚合实例状态。
    State(AggregateStateArgs),
    /// 管理和消费聚合消费者组。
    Group(AggregateGroupArgs),
    /// 查询 catalog 状态。
    Status,
    /// 查看聚合类型的物理分区放置。
    Partitions(AggregateTypeArgs),
}

/// AggregateType catalog 子命令。
#[derive(Args, Debug)]
pub struct AggregateTypeCommandArgs {
    #[command(subcommand)]
    pub action: AggregateTypeAction,
}

/// AggregateType catalog 操作。
#[derive(Subcommand, Debug)]
pub enum AggregateTypeAction {
    /// 注册并激活一个 AggregateType。
    Register(AggregateTypeRegisterArgs),
    /// 枚举全部 AggregateType。
    List,
    /// 查询一个 AggregateType。
    Get(AggregateTypeArgs),
}

/// 聚合类型身份。
#[derive(Args, Debug, Clone)]
pub struct AggregateTypeArgs {
    /// 业务空间，例如 orders。
    pub business_space: String,
    /// 聚合类型，例如 order。
    pub aggregate_type: String,
}

/// 注册聚合类型参数。
#[derive(Args, Debug, Clone)]
pub struct AggregateTypeRegisterArgs {
    /// 业务空间，例如 orders。
    pub business_space: String,
    /// 聚合类型，例如 order。
    pub aggregate_type: String,
    /// operation UUID；缺省随机生成，手工重试时应复用。
    #[arg(long)]
    pub operation_id: Option<String>,
}

/// 聚合实例期望版本。
#[derive(Clone, Debug, PartialEq)]
pub enum ExpectedAggregateVersionArg {
    Any,
    NoAggregate,
    AggregateExists,
    Exact(u64),
}

impl FromStr for ExpectedAggregateVersionArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "any" => Ok(Self::Any),
            "no-aggregate" | "noaggregate" => Ok(Self::NoAggregate),
            "exists" => Ok(Self::AggregateExists),
            number => number.parse::<u64>().map(Self::Exact).map_err(|_| {
                format!("非法聚合期望版本 {value:?}：应为 any、no-aggregate、exists 或数字")
            }),
        }
    }
}

/// 追加聚合事件参数。
#[derive(Args, Debug)]
#[command(group(
    clap::ArgGroup::new("aggregate_data_source")
        .required(true)
        .args(["data", "data_file"])
))]
pub struct AggregateAppendArgs {
    pub business_space: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    #[arg(long)]
    pub event_type: String,
    /// JSON data。
    #[arg(long)]
    pub data: Option<String>,
    /// 从文件读取 JSON data。
    #[arg(long)]
    pub data_file: Option<PathBuf>,
    /// JSON metadata；缺省空对象。
    #[arg(long)]
    pub metadata: Option<String>,
    /// 事件 UUID；缺省随机生成。
    #[arg(long)]
    pub event_id: Option<String>,
    /// any / no-aggregate / exists / 精确数字。
    #[arg(long, value_parser = ExpectedAggregateVersionArg::from_str, default_value = "any")]
    pub expected_version: ExpectedAggregateVersionArg,
}

/// 持续跟随聚合事件参数。
#[derive(Args, Debug)]
pub struct AggregateFollowArgs {
    pub business_space: String,
    pub aggregate_type: String,
    /// 从当前各分区 head 开始；缺省从 Beginning。
    #[arg(long, conflicts_with = "cursor")]
    pub now: bool,
    /// 服务端 opaque cursor 的十六进制表示。
    #[arg(long, conflicts_with = "now")]
    pub cursor: Option<String>,
    /// 收到 caught_up 后退出。
    #[arg(long)]
    pub once: bool,
}

/// 聚合状态命令（esctl aggregate state <action>）。
#[derive(Args, Debug)]
pub struct AggregateStateArgs {
    #[command(subcommand)]
    pub action: AggregateStateAction,
}

/// 聚合状态操作。
#[derive(Subcommand, Debug)]
pub enum AggregateStateAction {
    /// 分页枚举存在状态的实例。
    List(AggregateStateListArgs),
    /// 读取一个实例的状态。
    Get(AggregateStateGetArgs),
    /// 以 revision CAS 覆盖一个实例的状态。
    Put(AggregateStatePutArgs),
}

#[derive(Args, Debug)]
pub struct AggregateStateListArgs {
    pub business_space: String,
    pub aggregate_type: String,
    #[arg(long, default_value_t = 100)]
    pub page_size: u32,
    /// 上一页 token 的十六进制表示。
    #[arg(long)]
    pub page_token: Option<String>,
}

#[derive(Args, Debug)]
pub struct AggregateStateGetArgs {
    pub business_space: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
}

#[derive(Args, Debug)]
#[command(group(
    clap::ArgGroup::new("aggregate_state_data_source")
        .required(true)
        .args(["data", "data_file"])
))]
pub struct AggregateStatePutArgs {
    pub business_space: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    /// JSON 状态。
    #[arg(long)]
    pub data: Option<String>,
    /// 从文件读取 JSON 状态。
    #[arg(long)]
    pub data_file: Option<PathBuf>,
    /// absent 或精确 revision。
    #[arg(long, default_value = "absent")]
    pub expected_revision: String,
}

/// 聚合消费者组命令（esctl aggregate group <action>）。
#[derive(Args, Debug)]
pub struct AggregateGroupArgs {
    /// 要执行的消费者组操作。
    #[command(subcommand)]
    pub action: AggregateGroupAction,
}

/// 聚合消费者组操作。
#[derive(Subcommand, Debug)]
pub enum AggregateGroupAction {
    /// 创建消费者组。
    Create(AggregateGroupCreateArgs),
    /// 以 revision CAS 更新设置或 reset 起点。
    Update(AggregateGroupUpdateArgs),
    /// 以 revision CAS 删除消费者组。
    Delete(AggregateGroupDeleteArgs),
    /// 枚举聚合类型下的消费者组。
    List(AggregateTypeArgs),
    /// 拉取一批待显式结算的 delivery。
    Fetch(AggregateGroupFetchArgs),
    /// Ack、Retry、Park 或 Skip 一条 delivery。
    Settle(AggregateGroupSettleArgs),
}

/// 可选的聚合消费者组设置。
#[derive(Args, Debug, Clone, Default)]
pub struct AggregateGroupSettingsArgs {
    /// 单消费者最大未确认数。
    #[arg(long)]
    pub max_unacked_per_consumer: Option<u32>,
    /// 整个组最大未确认数。
    #[arg(long)]
    pub max_unacked_per_group: Option<u32>,
    /// delivery 租约时长（毫秒）。
    #[arg(long)]
    pub ack_timeout_ms: Option<u64>,
    /// 自动重试次数上限。
    #[arg(long)]
    pub max_retries: Option<u32>,
    /// 最小重试退避（毫秒）。
    #[arg(long)]
    pub retry_min_ms: Option<u64>,
    /// 最大重试退避（毫秒）。
    #[arg(long)]
    pub retry_max_ms: Option<u64>,
}

/// 创建聚合消费者组参数。
#[derive(Args, Debug)]
pub struct AggregateGroupCreateArgs {
    pub business_space: String,
    pub aggregate_type: String,
    pub name: String,
    /// 从创建时的各分区 head 开始；缺省从 Beginning。
    #[arg(long)]
    pub now: bool,
    /// operation UUID；模糊重试必须复用。
    #[arg(long)]
    pub operation_id: Option<String>,
    #[command(flatten)]
    pub settings: AggregateGroupSettingsArgs,
}

/// 更新聚合消费者组参数。
#[derive(Args, Debug)]
pub struct AggregateGroupUpdateArgs {
    pub business_space: String,
    pub aggregate_type: String,
    pub name: String,
    /// 当前组 revision。
    #[arg(long)]
    pub expected_revision: u64,
    /// reset 到各分区位置 0。
    #[arg(long, conflicts_with = "reset_now")]
    pub reset_beginning: bool,
    /// reset 到当前各分区 head。
    #[arg(long, conflicts_with = "reset_beginning")]
    pub reset_now: bool,
    /// operation UUID；模糊重试必须复用。
    #[arg(long)]
    pub operation_id: Option<String>,
    #[command(flatten)]
    pub settings: AggregateGroupSettingsArgs,
}

/// 删除聚合消费者组参数。
#[derive(Args, Debug)]
pub struct AggregateGroupDeleteArgs {
    pub business_space: String,
    pub aggregate_type: String,
    pub name: String,
    /// 当前组 revision。
    #[arg(long)]
    pub expected_revision: u64,
    /// operation UUID；模糊重试必须复用。
    #[arg(long)]
    pub operation_id: Option<String>,
}

/// 拉取聚合消费者组参数。
#[derive(Args, Debug)]
pub struct AggregateGroupFetchArgs {
    pub business_space: String,
    pub aggregate_type: String,
    pub name: String,
    /// 消费成员 ID。
    #[arg(long)]
    pub consumer: String,
    /// 最大 delivery 数；0 使用服务端默认。
    #[arg(long, default_value_t = 100)]
    pub max_events: u32,
    /// 最大 payload 字节；0 使用服务端默认。
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    pub max_bytes: u64,
    /// 长轮询时间（毫秒）。
    #[arg(long, default_value_t = 15_000)]
    pub wait_ms: u64,
}

/// delivery 结算动作。
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum AggregateGroupSettlementActionArg {
    Ack,
    Retry,
    Park,
    Skip,
}

/// 结算一条聚合消费者组 delivery 的参数。
#[derive(Args, Debug)]
pub struct AggregateGroupSettleArgs {
    pub business_space: String,
    pub aggregate_type: String,
    pub name: String,
    /// 消费成员 ID。
    #[arg(long)]
    pub consumer: String,
    /// Fetch 返回的十六进制 opaque token。
    #[arg(long)]
    pub delivery: String,
    /// 结算动作。
    #[arg(long, value_enum)]
    pub action: AggregateGroupSettlementActionArg,
    /// Retry/Park 的诊断原因。
    #[arg(long, default_value = "")]
    pub reason: String,
}

/// 快照子命令（esctl snapshot <list|restore>）
#[derive(Args, Debug)]
pub struct SnapshotArgs {
    #[command(subcommand)]
    pub action: SnapshotAction,
}

#[derive(Subcommand, Debug)]
pub enum SnapshotAction {
    /// 列出数据目录中的快照文件（分片/快照点/压缩算法/大小）
    List(SnapshotListArgs),
    /// 离线恢复：把快照恢复到数据目录（需集群停机）
    Restore(SnapshotRestoreArgs),
}

/// `esctl snapshot list`
#[derive(Args, Debug)]
pub struct SnapshotListArgs {
    /// 数据目录（快照位于 {data_dir}/snapshots）
    pub data_dir: PathBuf,
    /// 快照目录；缺省 {data_dir}/snapshots。
    /// 服务端配置了 [snapshot].dir 自定义目录时须显式传入
    #[arg(long)]
    pub snapshot_dir: Option<PathBuf>,
}

/// `esctl snapshot restore`
#[derive(Args, Debug)]
pub struct SnapshotRestoreArgs {
    /// 数据目录（快照位于 {data_dir}/snapshots）
    pub data_dir: PathBuf,
    /// 快照文件路径（可由 esctl snapshot list 或直接拷贝获得）
    pub snapshot_file: PathBuf,
    /// 快照目录；缺省 {data_dir}/snapshots。
    /// 服务端配置了 [snapshot].dir 自定义目录时须显式传入
    #[arg(long)]
    pub snapshot_dir: Option<PathBuf>,
    /// 跳过停机确认提示
    #[arg(long)]
    pub yes: bool,
}

#[derive(Args, Debug)]
pub struct InitArgs {
    /// 目标分片号；与 --all-shards 互斥
    #[arg(long, conflicts_with = "all_shards")]
    pub shard: Option<u64>,

    /// 对全部分片执行相同初始化（多分片集群手动组建必需：每分片独立 Raft group）
    #[arg(long)]
    pub all_shards: bool,

    /// 初始成员 ID@ADDR（可重复）
    #[arg(long, required = true, value_parser = MemberArg::from_str)]
    pub member: Vec<MemberArg>,

    /// 跳过确认提示
    #[arg(long)]
    pub yes: bool,
}

#[derive(Args, Debug)]
pub struct MemberArgs {
    #[command(subcommand)]
    pub action: MemberAction,
}

#[derive(Subcommand, Debug)]
pub enum MemberAction {
    /// 添加成员：先加为 learner，默认追平后提升为投票成员
    Add(MemberAddArgs),
    /// 移除投票成员（降级为 learner 或彻底剔除；learner 本身无法移除，无对应 RPC）
    Remove(MemberRemoveArgs),
    /// 列出各分片的节点状态（RPC 不暴露地址，故无地址列）
    List(MemberListArgs),
}

#[derive(Args, Debug)]
pub struct MemberAddArgs {
    /// 目标分片号；与 --all-shards 互斥
    #[arg(long, conflicts_with = "all_shards")]
    pub shard: Option<u64>,

    /// 对全部分片执行相同操作（各分片 leader 可不同，逐个重新发现）
    #[arg(long)]
    pub all_shards: bool,

    /// 新成员 ID@ADDR
    #[arg(long, value_parser = MemberArg::from_str)]
    pub member: MemberArg,

    /// 不等待新成员追平日志
    #[arg(long)]
    pub no_blocking: bool,

    /// 只添加为 learner，不提升为投票成员
    #[arg(long)]
    pub learner_only: bool,
}

#[derive(Args, Debug)]
pub struct MemberRemoveArgs {
    /// 目标分片号；与 --all-shards 互斥
    #[arg(long, conflicts_with = "all_shards")]
    pub shard: Option<u64>,

    /// 对全部分片执行相同操作
    #[arg(long)]
    pub all_shards: bool,

    /// 被移除的节点 ID（须为当前投票成员）
    #[arg(long)]
    pub node_id: u64,

    /// 移除时降级为 learner 而非彻底剔除
    #[arg(long)]
    pub retain: bool,
}

#[derive(Args, Debug)]
pub struct MemberListArgs {}

#[derive(Args, Debug)]
pub struct StatusArgs {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn member_parse_id_and_addr() {
        let m = MemberArg::from_str("2@127.0.0.1:50052").expect("合法成员");
        assert_eq!(m.node_id, 2);
        assert_eq!(m.addr, "127.0.0.1:50052");
    }

    #[test]
    fn member_parse_rejects_invalid() {
        assert!(MemberArg::from_str("abc@x").is_err(), "ID 非数字");
        assert!(MemberArg::from_str("1").is_err(), "缺 @");
        assert!(MemberArg::from_str("1@").is_err(), "地址为空");
        assert!(MemberArg::from_str("@x").is_err(), "ID 为空");
    }

    #[test]
    fn aggregate_type_register_command_is_nested() {
        let cli =
            Cli::try_parse_from(["esctl", "aggregate", "type", "register", "orders", "order"])
                .expect("解析 AggregateType 注册命令");
        assert!(matches!(
            cli.command,
            Command::Aggregate(AggregateArgs {
                action: AggregateAction::Type(AggregateTypeCommandArgs {
                    action: AggregateTypeAction::Register(_)
                })
            })
        ));
    }
}
