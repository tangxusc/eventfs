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

/// 期望版本（乐观并发控制）：any / nostream / exists / 精确数字
#[derive(Clone, Debug, PartialEq)]
pub enum ExpectedVersionArg {
    /// 不校验
    Any,
    /// 要求流不存在
    NoStream,
    /// 要求流已存在
    StreamExists,
    /// 要求当前版本恰为该值
    Exact(u64),
}

impl FromStr for ExpectedVersionArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "any" => Ok(Self::Any),
            "nostream" => Ok(Self::NoStream),
            "exists" => Ok(Self::StreamExists),
            n => n
                .parse::<u64>()
                .map(Self::Exact)
                .map_err(|_| format!("非法期望版本 {s:?}：应为 any、nostream、exists 或数字")),
        }
    }
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

/// 逐分片读取游标："shard:pos,..."，如 "3:7,5:2"
pub fn parse_shard_positions(s: &str) -> Result<Vec<(u64, u64)>, String> {
    if s.trim().is_empty() {
        return Ok(vec![]);
    }
    s.split(',')
        .map(|item| {
            let (shard, pos) = item
                .trim()
                .split_once(':')
                .ok_or_else(|| format!("非法游标 {item:?}：应为 shard:pos，如 3:7"))?;
            let shard = shard
                .trim()
                .parse::<u64>()
                .map_err(|_| format!("非法游标 {item:?}：分片号应为数字"))?;
            let pos = pos
                .trim()
                .parse::<u64>()
                .map_err(|_| format!("非法游标 {item:?}：位置应为数字"))?;
            Ok((shard, pos))
        })
        .collect()
}

/// 分片号列表："0,1,3"
pub fn parse_shard_ids(s: &str) -> Result<Vec<u64>, String> {
    if s.trim().is_empty() {
        return Ok(vec![]);
    }
    s.split(',')
        .map(|item| {
            item.trim()
                .parse::<u64>()
                .map_err(|_| format!("非法分片号 {item:?}：应为数字"))
        })
        .collect()
}

/// 逐分片游标参数（clap 单参数解析整个字符串为列表；
/// 不能直接用 `Option<Vec<T>>`——clap 会把它当作可重复参数，逐个 downcast 失败）
#[derive(Clone, Debug, PartialEq)]
pub struct ShardPositions(pub Vec<(u64, u64)>);

impl FromStr for ShardPositions {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_shard_positions(s).map(Self)
    }
}

/// 分片号列表参数（同上：单参数解析整个字符串）
#[derive(Clone, Debug, PartialEq)]
pub struct ShardIds(pub Vec<u64>);

impl FromStr for ShardIds {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_shard_ids(s).map(Self)
    }
}

/// --shards 解析：拒绝 0。
///
/// `route(stream, 0)` 是取模除零（es-core::routing），会 panic 而不是报错；
/// 且服务端配置也拒绝 num_shards=0。0 分片没有任何合法含义。
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

    /// 分片总数；缺省时自动探测（GetRaftState 扫描 0..N），探测失败回退默认 8
    #[arg(long, value_parser = parse_shards)]
    pub shards: Option<u64>,
}

#[derive(Parser, Debug)]
#[command(name = "esctl")]
#[command(about = "EventStore 分布式事件存储命令行工具（参照 etcdctl）")]
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
    /// 追加事件到流（乐观并发，期望版本冲突时报错）
    Append(AppendArgs),
    /// 读取单个流的事件区间
    Read(ReadArgs),
    /// 跨分片读取 $all 流（非严格全序，按分片归并）
    #[command(name = "readall")]
    ReadAll(ReadAllArgs),
    /// 查询流元数据（当前版本、所在分片）
    Meta(MetaArgs),
    /// 显式创建流：服务端分配 shard（大致最少流），返回流归属
    #[command(name = "create-stream")]
    CreateStream(CreateStreamArgs),
    /// 查看/校准流路由表（stream → shard 归属）
    Route(RouteArgs),
    /// 在线迁移流到目标分片（流的数据处理不暂停；取代旧 reshard）
    Migrate(MigrateArgs),
    /// 订阅流事件：先追平历史（catch-up），追平后实时推送
    Watch(WatchArgs),
    /// 初始化分片集群（把给定成员写入首条 membership 日志，只需在一个节点调用一次）
    Init(InitArgs),
    /// 集群成员管理（加 learner、提升/移除投票成员、列表）
    Member(MemberArgs),
    /// 各端点健康与分片归属视图
    Status(StatusArgs),
    /// 快照管理（离线操作数据目录）
    Snapshot(SnapshotArgs),
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
#[command(group(
    clap::ArgGroup::new("data_src")
        .required(true)
        .args(["data", "data_file"])
))]
pub struct AppendArgs {
    /// 流 ID（聚合根标识）
    pub stream: String,

    /// 事件类型名
    #[arg(long)]
    pub event_type: String,

    /// 事件数据（字符串；允许空串）
    #[arg(long)]
    pub data: Option<String>,

    /// 事件数据文件（以文件原始字节作为 data）
    #[arg(long)]
    pub data_file: Option<PathBuf>,

    /// 事件元数据（字符串）
    #[arg(long, group = "meta_src")]
    pub metadata: Option<String>,

    /// 事件元数据文件
    #[arg(long, group = "meta_src")]
    pub metadata_file: Option<PathBuf>,

    /// 事件 ID（UUID）；缺省随机生成 v4
    #[arg(long)]
    pub event_id: Option<String>,

    /// 期望版本：any / nostream / exists / 数字
    #[arg(long, value_parser = ExpectedVersionArg::from_str, default_value = "any")]
    pub expected_version: ExpectedVersionArg,
}

#[derive(Args, Debug)]
pub struct ReadArgs {
    /// 流 ID
    pub stream: String,

    /// 起始版本（含）
    #[arg(long, default_value_t = 0)]
    pub from_version: u64,

    /// 最多读取条数；0 表示不限量（由服务端按流控上限截断）
    #[arg(long, default_value_t = 0)]
    pub max_count: u64,

    /// 反向读取（从最新往旧；未显式指定 --from-version 时从最新一条开始）
    #[arg(long)]
    pub backward: bool,
}

#[derive(Args, Debug)]
pub struct ReadAllArgs {
    /// 所有分片统一的起始 position（仅适合首页查询；各分片序列相互独立）
    #[arg(long, default_value_t = 0)]
    pub from_position: u64,

    /// 逐分片游标 "shard:pos,..."，非空时覆盖 --from-position 与 --shard-ids；
    /// 翻页必须用它（续读游标可从上一页输出中获取）
    #[arg(long, value_parser = ShardPositions::from_str)]
    pub from_positions: Option<ShardPositions>,

    /// 最多读取条数；0 表示不限量
    #[arg(long, default_value_t = 0)]
    pub max_count: u64,

    /// 反向读取
    #[arg(long)]
    pub backward: bool,

    /// 显式分片列表 "0,1,3"；缺省用 --shards / 自动探测的全部分片
    #[arg(long, value_parser = ShardIds::from_str)]
    pub shard_ids: Option<ShardIds>,
}

#[derive(Args, Debug)]
pub struct MetaArgs {
    /// 流 ID
    pub stream: String,
}

/// `esctl create-stream`
#[derive(Args, Debug)]
pub struct CreateStreamArgs {
    /// 流 ID
    pub stream: String,
}

/// `esctl route`
#[derive(Args, Debug)]
pub struct RouteArgs {
    /// 只显示路由表（默认动作）
    #[arg(long)]
    pub show: bool,

    /// 校准 per-shard 流计数（从路由表重建）
    #[arg(long)]
    pub recount: bool,

    /// 孤儿流检测：对比各分片实际存储的流与路由表，报告
    /// 「存储有但路由表无」（孤儿）与「路由表指向与存储不一致」（虚挂）
    #[arg(long, conflicts_with = "recount")]
    pub check: bool,
}

/// `esctl migrate`
#[derive(Args, Debug)]
pub struct MigrateArgs {
    /// 迁移单个流
    #[arg(long, conflicts_with = "shard")]
    pub stream: Option<String>,

    /// 迁移整个分片的全部流（逐流独立状态机，失败隔离）
    #[arg(long, conflicts_with = "stream")]
    pub shard: Option<u64>,

    /// 目标分片
    #[arg(long)]
    pub to: u64,

    /// 只报告迁移计划与版本差，不执行
    #[arg(long)]
    pub dry_run: bool,

    /// 排水收敛判定：源分片连续 N 次（间隔 2s）无新增即认为收敛
    #[arg(long, default_value_t = 2)]
    pub drain_quiet_rounds: u32,

    /// 排水超时（秒），超时退出（数据无害，可重跑）
    #[arg(long, default_value_t = 300)]
    pub drain_timeout_secs: u64,
}

#[derive(Args, Debug)]
pub struct WatchArgs {
    /// 订阅单个流（按 version 定位）
    pub stream: Option<String>,

    /// 订阅全部分片 $all（当前服务端仅支持分片 0，见 --shard）
    #[arg(long, conflicts_with = "stream")]
    pub all: bool,

    /// --all 时的目标分片（服务端限制：目前仅分片 0 有 $all 数据）
    #[arg(long, default_value_t = 0)]
    pub shard: u64,

    /// 起始位置（不含）：订阅流时按 version，订阅 all 时按 position
    #[arg(long, default_value_t = 0)]
    pub from_exclusive: u64,

    /// 从头开始（忽略 --from-exclusive）
    #[arg(long)]
    pub from_start: bool,

    /// 追平历史（收到 caught_up 信号）后立即退出，退出码 0（脚本/测试用）
    #[arg(long)]
    pub once: bool,
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
    fn expected_version_parse_four_variants() {
        assert_eq!(
            ExpectedVersionArg::from_str("any"),
            Ok(ExpectedVersionArg::Any)
        );
        assert_eq!(
            ExpectedVersionArg::from_str("nostream"),
            Ok(ExpectedVersionArg::NoStream)
        );
        assert_eq!(
            ExpectedVersionArg::from_str("exists"),
            Ok(ExpectedVersionArg::StreamExists)
        );
        assert_eq!(
            ExpectedVersionArg::from_str("42"),
            Ok(ExpectedVersionArg::Exact(42))
        );
    }

    #[test]
    fn expected_version_parse_rejects_invalid() {
        assert!(ExpectedVersionArg::from_str("foo").is_err());
        assert!(ExpectedVersionArg::from_str("-1").is_err());
        assert!(ExpectedVersionArg::from_str("").is_err());
    }

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
    fn positions_parse_multiple_shards() {
        assert_eq!(parse_shard_positions("3:7,5:2"), Ok(vec![(3, 7), (5, 2)]));
        assert_eq!(parse_shard_positions(" 3 : 7 "), Ok(vec![(3, 7)]));
        assert_eq!(parse_shard_positions(""), Ok(vec![]));
        assert!(parse_shard_positions("3:7,5").is_err(), "缺冒号");
        assert!(parse_shard_positions("x:7").is_err(), "分片非数字");
    }

    #[test]
    fn shard_ids_parse() {
        assert_eq!(parse_shard_ids("0,1,3"), Ok(vec![0, 1, 3]));
        assert_eq!(parse_shard_ids(""), Ok(vec![]));
        assert!(parse_shard_ids("0,x").is_err());
    }
}
