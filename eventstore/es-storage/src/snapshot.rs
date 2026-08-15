//! 快照独立文件：压缩、文件格式、目录管理与传输句柄。
//!
//! 设计要点（docs/snapshot.md）：
//! - 快照文件与业务数据（surrealkv tree）分离，存于 `{data_dir}/snapshots/`
//! - 文件头记录压缩算法，读取时自动识别；v1 拒绝未知版本
//! - build/install 全程流式读写文件，传输块来自文件而非内存
//! - 保留最近 `keep` 个快照（按 (term, index) 数值排序）

use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use openraft::{LogId, SnapshotMeta};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};

/// 快照文件魔数：文件头第一道校验
pub const SNAP_MAGIC: [u8; 4] = *b"ESNP";
/// 文件格式版本：读侧拒绝未知版本，写侧恒为 1
pub const SNAP_VERSION: u8 = 1;
/// 记录流尾哨兵（合法 key_len 不可能是 u64::MAX，见 for_each_record 说明）
const END_MARKER: u64 = u64::MAX;

/// 快照压缩算法。配置序列化为小写字符串：zstd / lz4 / none
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    /// zstd，压缩率最高（level 3，固定）
    #[default]
    Zstd,
    /// lz4（frame 格式），速度快、压缩率低于 zstd
    Lz4,
    /// 不压缩
    None,
}

impl Compression {
    /// 文件头压缩 tag
    pub fn tag(self) -> u8 {
        match self {
            Compression::None => 0,
            Compression::Zstd => 1,
            Compression::Lz4 => 2,
        }
    }

    /// 从文件头 tag 解析；未知 tag 报错（格式损坏或版本不兼容）
    pub fn from_tag(tag: u8) -> io::Result<Self> {
        match tag {
            0 => Ok(Compression::None),
            1 => Ok(Compression::Zstd),
            2 => Ok(Compression::Lz4),
            _ => Err(io::Error::other(format!("未知快照压缩 tag: {tag}"))),
        }
    }

    /// 人类可读名（esctl 输出用）
    pub fn display_name(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Zstd => "zstd",
            Compression::Lz4 => "lz4",
        }
    }

    /// 用本算法包裹文件构造压缩写入流。
    ///
    /// 调用方需先把头部与 meta 写入裸文件（或 seek 到 payload 起点）：
    /// 头部与 meta 是未压缩的，压缩帧只覆盖 payload 段。
    pub fn writer(self, f: File) -> io::Result<SnapshotWriter> {
        match self {
            Compression::None => Ok(SnapshotWriter::None(f)),
            Compression::Zstd => Ok(SnapshotWriter::Zstd(zstd::stream::write::Encoder::new(
                f, 3,
            )?)),
            Compression::Lz4 => Ok(SnapshotWriter::Lz4(lz4_flex::frame::FrameEncoder::new(f))),
        }
    }

    /// 用本算法包裹文件构造解压读取流。
    ///
    /// 调用方需先 `seek` 到 payload 起点（头部 + meta 段之后）：
    /// 头部与 meta 是未压缩的，压缩帧只覆盖 payload 段。
    pub fn reader(self, f: File) -> io::Result<SnapshotReader> {
        match self {
            Compression::None => Ok(SnapshotReader::None(f)),
            Compression::Zstd => Ok(SnapshotReader::Zstd(zstd::stream::read::Decoder::new(f)?)),
            Compression::Lz4 => Ok(SnapshotReader::Lz4(lz4_flex::frame::FrameDecoder::new(f))),
        }
    }
}

/// 压缩写入流。用 enum 而非 Box<dyn>：std::fs::File 是唯一 sink，具体类型便于收尾
pub enum SnapshotWriter {
    /// 未压缩
    None(File),
    /// zstd 帧
    Zstd(zstd::stream::write::Encoder<'static, File>),
    /// lz4 帧
    Lz4(lz4_flex::frame::FrameEncoder<File>),
}

impl SnapshotWriter {
    /// 刷出压缩帧尾，返回底层文件。
    ///
    /// 不调用则文件缺帧尾，读侧解压报错——这是截断检测的一部分。
    pub fn finish(self) -> io::Result<File> {
        match self {
            SnapshotWriter::None(f) => Ok(f),
            SnapshotWriter::Zstd(e) => e.finish(),
            SnapshotWriter::Lz4(e) => e.finish().map_err(io::Error::other),
        }
    }
}

impl Write for SnapshotWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            SnapshotWriter::None(f) => f.write(buf),
            SnapshotWriter::Zstd(e) => e.write(buf),
            SnapshotWriter::Lz4(e) => e.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            SnapshotWriter::None(f) => f.flush(),
            SnapshotWriter::Zstd(e) => e.flush(),
            SnapshotWriter::Lz4(e) => e.flush(),
        }
    }
}

/// 压缩读取流
pub enum SnapshotReader {
    /// 未压缩
    None(File),
    /// zstd 帧（zstd 0.13 内部用 BufReader 缓冲）
    Zstd(zstd::stream::read::Decoder<'static, std::io::BufReader<File>>),
    /// lz4 帧
    Lz4(lz4_flex::frame::FrameDecoder<File>),
}

impl Read for SnapshotReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            SnapshotReader::None(f) => f.read(buf),
            SnapshotReader::Zstd(d) => d.read(buf),
            SnapshotReader::Lz4(d) => d.read(buf),
        }
    }
}

/// 快照文件头（v1，32 字节定长部分 + meta）
#[derive(Debug, Clone, Copy)]
pub struct SnapshotHeader {
    /// 文件格式版本（SNAP_VERSION）
    pub version: u8,
    /// 压缩算法
    pub compression: Compression,
    /// 分片 ID（显式存储，不依赖解析 snapshot_id 字符串）
    pub shard_id: u64,
    /// meta 字节数（未压缩）
    pub meta_len: u64,
    /// 未压缩 payload 总字节数，读侧与实读字节数比对
    pub payload_len: u64,
}

const HEADER_LEN: usize = 32;

/// meta 段长度上限：快照 meta（serde_json 的 SnapshotMeta）实际仅几百字节，
/// 上限只防损坏头导致巨量分配 OOM
const MAX_META_LEN: u64 = 64 * 1024 * 1024;

/// 单条记录长度上限：key 是状态机 key（结构固定，实际 <1KB）；
/// value 是事件数据，允许较大但必须有界（防损坏文件触发巨量分配）
const MAX_RECORD_LEN: u64 = 1 << 30; // 1GB

/// 写文件头（32 字节定长 + serde_json meta）。
///
/// meta 未压缩：esctl list 只需读头部即可列出快照，无需解压 payload。
pub fn write_header<W: Write>(
    w: &mut W,
    h: &SnapshotHeader,
    meta: &SnapshotMeta<u64, openraft::BasicNode>,
) -> io::Result<()> {
    let meta_bytes =
        serde_json::to_vec(meta).map_err(|e| io::Error::other(format!("meta 序列化失败: {e}")))?;
    if meta_bytes.len() as u64 != h.meta_len {
        return Err(io::Error::other(format!(
            "meta_len 与序列化结果不符: 声明 {} 实际 {}",
            h.meta_len,
            meta_bytes.len()
        )));
    }
    let mut head = [0u8; HEADER_LEN];
    head[0..4].copy_from_slice(&SNAP_MAGIC);
    head[4] = h.version;
    head[5] = h.compression.tag();
    head[8..16].copy_from_slice(&h.shard_id.to_le_bytes());
    head[16..24].copy_from_slice(&h.meta_len.to_le_bytes());
    head[24..32].copy_from_slice(&h.payload_len.to_le_bytes());
    w.write_all(&head)?;
    w.write_all(&meta_bytes)
}

/// 从读取流读文件头 + meta。magic/version/长度不符即报错
pub fn read_header<R: Read>(
    r: &mut R,
) -> io::Result<(SnapshotHeader, SnapshotMeta<u64, openraft::BasicNode>)> {
    let mut head = [0u8; HEADER_LEN];
    r.read_exact(&mut head)?;
    if head[0..4] != SNAP_MAGIC {
        return Err(io::Error::other(
            "快照文件 magic 不符（不是快照文件或已损坏）",
        ));
    }
    let version = head[4];
    if version != SNAP_VERSION {
        return Err(io::Error::other(format!(
            "快照文件版本 {version} 不支持（当前支持 v{SNAP_VERSION}）"
        )));
    }
    let compression = Compression::from_tag(head[5])?;
    let shard_id = u64::from_le_bytes(head[8..16].try_into().unwrap());
    let meta_len = u64::from_le_bytes(head[16..24].try_into().unwrap());
    let payload_len = u64::from_le_bytes(head[24..32].try_into().unwrap());

    if meta_len > MAX_META_LEN {
        return Err(io::Error::other(format!(
            "快照文件头 meta_len 越界: {meta_len}（上限 {MAX_META_LEN}，文件可能损坏）"
        )));
    }
    // try_reserve 而非直接 vec![0u8; n]：分配失败返回 Err 而不是 allocator abort
    let mut meta_bytes = Vec::new();
    meta_bytes
        .try_reserve_exact(meta_len as usize)
        .map_err(|e| io::Error::other(format!("快照 meta 分配失败: {e}")))?;
    meta_bytes.resize(meta_len as usize, 0);
    r.read_exact(&mut meta_bytes)?;
    let meta: SnapshotMeta<u64, openraft::BasicNode> = serde_json::from_slice(&meta_bytes)
        .map_err(|e| io::Error::other(format!("快照 meta 反序列化失败: {e}")))?;
    Ok((
        SnapshotHeader {
            version,
            compression,
            shard_id,
            meta_len,
            payload_len,
        },
        meta,
    ))
}

/// 从内存字节解析文件头 + meta（esctl list 用，不碰 payload）。
///
/// `bytes` 需包含完整头部与 meta 段，payload 可以截断。
pub fn parse_header_bytes(
    bytes: &[u8],
) -> io::Result<(SnapshotHeader, SnapshotMeta<u64, openraft::BasicNode>)> {
    let mut cur = io::Cursor::new(bytes);
    read_header(&mut cur)
}

/// 打开快照文件，返回 (头部, meta, 定位到 payload 起点的解压流)。
///
/// install / restore 读快照内容的统一入口。调用方持有返回的 reader，
/// 从中读记录流（先读 u64 记录数，再 for_each_record）。
pub fn open_payload_reader(
    path: &Path,
) -> io::Result<(
    SnapshotHeader,
    SnapshotMeta<u64, openraft::BasicNode>,
    SnapshotReader,
)> {
    use std::io::Seek;
    let mut f = File::open(path)?;
    let (h, m) = read_header(&mut f)?;
    f.seek(std::io::SeekFrom::Start(HEADER_LEN as u64 + h.meta_len))?;
    let r = h.compression.reader(f)?;
    Ok((h, m, r))
}

/// 单条记录的未压缩字节数：key_len(8) + key + val_len(8) + val
pub fn record_len(key: &[u8], val: &[u8]) -> u64 {
    16 + key.len() as u64 + val.len() as u64
}

/// payload 未压缩总字节数（build 侧写头前精确计算）
pub fn payload_len_for(entries: &[(Vec<u8>, Vec<u8>)]) -> u64 {
    8 + entries.iter().map(|(k, v)| record_len(k, v)).sum::<u64>() + 8
}

/// 写一条记录：[key_len:u64 LE][key][val_len:u64 LE][val]
pub fn write_record<W: Write>(w: &mut W, key: &[u8], val: &[u8]) -> io::Result<()> {
    w.write_all(&(key.len() as u64).to_le_bytes())?;
    w.write_all(key)?;
    w.write_all(&(val.len() as u64).to_le_bytes())?;
    w.write_all(val)
}

/// 写记录流尾哨兵
pub fn write_end_marker<W: Write>(w: &mut W) -> io::Result<()> {
    w.write_all(&END_MARKER.to_le_bytes())
}

/// 逐条读记录并回调，返回实读到的 payload 字节数（供与 payload_len 比对）。
///
/// 校验链：记录数 = entry_count → end_marker 必须是 END_MARKER。
/// END_MARKER 取 u64::MAX：key_len 实际不可能为 u64::MAX（单条 key 超 16EiB），
/// 因此哨兵不会与真实记录混淆；读到意外值即判格式损坏。
pub fn for_each_record<R: Read>(
    r: &mut R,
    mut f: impl FnMut(Vec<u8>, Vec<u8>) -> io::Result<()>,
) -> io::Result<u64> {
    let mut read_bytes = 0u64;
    let read_u64 = |r: &mut R, read_bytes: &mut u64| -> io::Result<u64> {
        let mut b = [0u8; 8];
        r.read_exact(&mut b)?;
        *read_bytes += 8;
        Ok(u64::from_le_bytes(b))
    };
    // entry_count 由本函数内部读取：返回的实读字节数才与头部 payload_len（含
    // count 8 字节）口径一致
    let count = read_u64(r, &mut read_bytes)?;
    for _ in 0..count {
        let key_len = read_u64(r, &mut read_bytes)?;
        if key_len == END_MARKER {
            return Err(io::Error::other("快照记录数不匹配（提前遇到流尾哨兵）"));
        }
        if key_len > MAX_RECORD_LEN {
            return Err(io::Error::other(format!(
                "快照记录 key 长度越界: {key_len}（文件可能损坏）"
            )));
        }
        // try_reserve：分配失败返回 Err 而非 allocator abort
        let mut key = Vec::new();
        key.try_reserve_exact(key_len as usize)
            .map_err(|e| io::Error::other(format!("快照记录分配失败: {e}")))?;
        key.resize(key_len as usize, 0);
        r.read_exact(&mut key)?;
        read_bytes += key_len;
        let val_len = read_u64(r, &mut read_bytes)?;
        if val_len > MAX_RECORD_LEN {
            return Err(io::Error::other(format!(
                "快照记录 value 长度越界: {val_len}（文件可能损坏）"
            )));
        }
        let mut val = Vec::new();
        val.try_reserve_exact(val_len as usize)
            .map_err(|e| io::Error::other(format!("快照记录分配失败: {e}")))?;
        val.resize(val_len as usize, 0);
        r.read_exact(&mut val)?;
        read_bytes += val_len;
        f(key, val)?;
    }
    let marker = read_u64(r, &mut read_bytes)?;
    if marker != END_MARKER {
        return Err(io::Error::other(format!(
            "快照流尾哨兵缺失（记录数 {count} 与文件内容不符）"
        )));
    }
    Ok(read_bytes)
}

/// openraft 的快照数据类型：文件句柄 + 路径 + 临时标记。
///
/// - temp=true：incoming 临时文件（begin_receiving_snapshot 创建，install 成功后转正）
/// - temp=false：正式快照文件（build 产物，或已转正）
/// Drop 时若仍为 temp 则删除——传输中断、被新流替换时自动清理残留。
pub struct SnapshotFile {
    inner: tokio::fs::File,
    path: PathBuf,
    temp: bool,
}

impl SnapshotFile {
    /// 在 incoming 目录创建唯一临时文件（openraft 每次 begin 都新建，天然隔离并发流）
    pub async fn create_temp(incoming_dir: &Path) -> io::Result<Self> {
        let name = format!("{}.tmp", uuid::Uuid::new_v4());
        let path = incoming_dir.join(name);
        let inner = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;
        Ok(Self {
            inner,
            path,
            temp: true,
        })
    }

    /// 打开既有正式快照文件（build 产物 / get_current_snapshot 返回）
    pub async fn open(path: PathBuf) -> io::Result<Self> {
        let inner = tokio::fs::File::open(&path).await?;
        Ok(Self {
            inner,
            path,
            temp: false,
        })
    }

    /// 文件路径
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 是否临时文件
    pub fn is_temp(&self) -> bool {
        self.temp
    }

    /// 转正：rename 到正式名（同目录/同文件系统内原子）。
    ///
    /// 失败时自身仍为 temp，Drop 兜底清理——只损失文件缓存，不影响已提交数据。
    pub fn promote(&mut self, final_path: PathBuf) -> io::Result<()> {
        std::fs::rename(&self.path, &final_path)?;
        self.path = final_path;
        self.temp = false;
        Ok(())
    }

    /// 转 std 句柄重开（install 的同步段用；解压流是 std::io::Read）。
    ///
    /// 关闭 tokio 句柄（tokio File 无用户空间缓冲，写已直达内核），
    /// 返回 (std::fs::File, 路径, temp 标记)。调用方负责后续 Drop 语义。
    pub fn into_std_file(mut self) -> io::Result<(File, PathBuf, bool)> {
        let f = File::open(&self.path)?;
        let path = std::mem::take(&mut self.path);
        Ok((f, path, self.temp))
    }
}

impl Drop for SnapshotFile {
    fn drop(&mut self) {
        if self.temp {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl AsyncRead for SnapshotFile {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for SnapshotFile {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        // openraft Chunked 在传输完成后调用 shutdown()；tokio File 的 shutdown 是 no-op
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl AsyncSeek for SnapshotFile {
    fn start_seek(
        mut self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> io::Result<()> {
        std::pin::Pin::new(&mut self.inner).start_seek(position)
    }

    fn poll_complete(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<u64>> {
        std::pin::Pin::new(&mut self.inner).poll_complete(cx)
    }
}

/// 快照配置（EsStorage 构造时传入）
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// 快照目录（已解析；生产为 {data_dir}/snapshots，可被配置覆盖）
    pub dir: PathBuf,
    /// 压缩算法
    pub compression: Compression,
    /// 保留历史快照数（含最新），build/install 后清理超出部分
    pub keep: usize,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./snapshots"),
            compression: Compression::Zstd,
            keep: 3,
        }
    }
}

/// 快照目录中的一个文件（含解析结果；损坏文件 valid=false 不中断列表）
pub struct SnapshotEntry {
    /// 文件路径
    pub path: PathBuf,
    /// 解析出的文件头（损坏时为 None）
    pub header: Option<SnapshotHeader>,
    /// 文件头内的 meta（损坏时为 None）
    pub meta: Option<SnapshotMeta<u64, openraft::BasicNode>>,
    /// 文件字节数
    pub size: u64,
    /// 文件修改时间
    pub mtime: SystemTime,
    /// 头部解析是否成功
    pub valid: bool,
}

/// 单分片快照存储：目录布局、文件命名、枚举与保留清理。
#[derive(Clone)]
pub struct SnapshotStore {
    cfg: SnapshotConfig,
    shard_id: u64,
}

impl SnapshotStore {
    /// 创建快照存储。目录惰性创建（`ensure_dirs`）：只读场景
    /// （如 reshard 枚举流名）不应产生目录副作用。
    pub fn new(cfg: SnapshotConfig, shard_id: u64) -> io::Result<Self> {
        Ok(Self { cfg, shard_id })
    }

    /// 确保快照根目录与 incoming 子目录存在（build/接收快照前调用）
    pub fn ensure_dirs(&self) -> io::Result<()> {
        std::fs::create_dir_all(self.cfg.dir.join("incoming"))
    }

    /// 快照根目录
    pub fn dir(&self) -> &Path {
        &self.cfg.dir
    }

    /// 压缩算法（build 快照时使用）
    pub fn compression(&self) -> Compression {
        self.cfg.compression
    }

    /// 保留历史快照数
    pub fn keep(&self) -> usize {
        self.cfg.keep
    }

    /// 传输中临时文件目录
    pub fn incoming_dir(&self) -> PathBuf {
        self.cfg.dir.join("incoming")
    }

    /// 新临时文件路径（incoming/{uuid}.tmp）
    pub fn tmp_path(&self) -> PathBuf {
        self.incoming_dir()
            .join(format!("{}.tmp", uuid::Uuid::new_v4()))
    }

    /// 正式快照文件路径。
    ///
    /// 命名 `snap-{shard:08}-{term:020}-{index:020}.esnap`，固定宽度补零使
    /// 同分片内字典序 = (term, index) 数值序。空快照（last_applied=None）
    /// 用哨兵 term=0/index=0——真实 term 从 1 起，不冲突。
    pub fn final_path(&self, last_log_id: Option<LogId<u64>>) -> PathBuf {
        let (term, index) = match last_log_id {
            Some(l) => (l.leader_id.term, l.index),
            None => (0, 0),
        };
        self.cfg.dir.join(format!(
            "snap-{shard:08}-{term:020}-{index:020}.esnap",
            shard = self.shard_id,
            term = term,
            index = index
        ))
    }

    /// 枚举根目录全部快照文件并解析头部；损坏文件标记 valid=false 不中断。
    /// 目录不存在视为空（只读场景不产生副作用）。
    pub fn list_entries(&self) -> io::Result<Vec<SnapshotEntry>> {
        if !self.cfg.dir.is_dir() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for ent in std::fs::read_dir(&self.cfg.dir)? {
            let ent = ent?;
            let path = ent.path();
            if !path.is_file() || path.extension().and_then(|e| e.to_str()) != Some("esnap") {
                continue;
            }
            let meta = ent.metadata()?;
            let mut entry = SnapshotEntry {
                path: path.clone(),
                header: None,
                meta: None,
                size: meta.len(),
                mtime: meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                valid: false,
            };
            match std::fs::File::open(&path)
                .map_err(io::Error::from)
                .and_then(|mut f| read_header(&mut f))
            {
                Ok((header, meta)) => {
                    entry.header = Some(header);
                    entry.meta = Some(meta);
                    entry.valid = true;
                }
                Err(e) => {
                    tracing::warn!("快照文件 {} 头部解析失败: {e}", path.display());
                }
            }
            out.push(entry);
        }
        Ok(out)
    }

    /// 最新快照文件路径：按 (term, index) 数值最大者，**仅限本分片**。
    ///
    /// 生产布局全部分片共享同一快照目录（文件名带分片号），
    /// 不过滤会取到其它分片的快照（跨分片 meta 错配是数据级事故）。
    ///
    /// `applied` 非 None 时过滤领先于它的快照：restore/崩溃残留的「更新」文件
    /// 与状态机不一致，不能作为当前快照（否则 follower 装旧状态当新状态）。
    /// 传 None 表示不过滤（新节点启动恢复、esctl 等待快照等场景）。
    ///
    /// 损坏文件跳过并告警（快照是缓存，损坏只损失缓存与历史点，
    /// 不阻塞 openraft 启动与日志复制兜底）。
    pub fn latest(&self, applied: Option<LogId<u64>>) -> io::Result<Option<PathBuf>> {
        let mut best: Option<(u64, u64, PathBuf)> = None;
        for entry in self.list_entries()? {
            let Some(header) = entry.header else { continue };
            if header.shard_id != self.shard_id {
                continue; // 其它分片的快照
            }
            let Some(meta) = entry.meta.as_ref() else {
                continue;
            };
            // 领先于 applied 的快照跳过（restore/崩溃残留）。applied 为 None
            // 不过滤：启动恢复场景（如刚装快照的新节点）正需要返回快照。
            if let (Some(a), Some(m)) = (applied, meta.last_log_id) {
                if (m.leader_id.term, m.index) > (a.leader_id.term, a.index) {
                    tracing::warn!(
                        "跳过领先于 applied 的快照 {}（残留的更新文件，状态机不一致）",
                        entry.path.display()
                    );
                    continue;
                }
            }
            let (term, index) = match meta.last_log_id {
                Some(l) => (l.leader_id.term, l.index),
                None => (0, 0),
            };
            match &best {
                Some((bt, bi, _)) if (*bt, *bi) >= (term, index) => {}
                _ => best = Some((term, index, entry.path)),
            }
        }
        Ok(best.map(|(_, _, p)| p))
    }

    /// 保留最新 keep 个**本分片**快照（按 (term, index) 数值排序），删除其余。
    ///
    /// 返回被删除的路径（供日志）。损坏文件与其它分片的不参与排序，
    /// 也不在此删除（由 esctl snapshot list 标记，人工处理）。
    pub fn retain(&self, keep: usize) -> io::Result<Vec<PathBuf>> {
        if keep == 0 {
            return Err(io::Error::other("keep 必须 ≥ 1（keep=0 会删光全部快照）"));
        }
        let mut valid: Vec<(u64, u64, PathBuf)> = Vec::new();
        for entry in self.list_entries()? {
            let Some(header) = entry.header else { continue };
            if header.shard_id != self.shard_id {
                continue; // 其它分片的快照
            }
            let (term, index) = match entry.meta.as_ref().and_then(|m| m.last_log_id) {
                Some(l) => (l.leader_id.term, l.index),
                None => (0, 0),
            };
            valid.push((term, index, entry.path));
        }
        valid.sort_by_key(|(t, i, _)| (*t, *i));
        let mut removed = Vec::new();
        while valid.len() > keep {
            let (_, _, path) = valid.remove(0);
            std::fs::remove_file(&path)?;
            removed.push(path);
        }
        Ok(removed)
    }

    /// 清理 incoming 残留临时文件（启动时调用——此时无进行中的传输）。
    ///
    /// 返回删除的文件数。目录不存在视为空。
    pub fn cleanup_incoming(&self) -> io::Result<usize> {
        let incoming = self.incoming_dir();
        if !incoming.is_dir() {
            return Ok(0);
        }
        let mut n = 0;
        for ent in std::fs::read_dir(incoming)? {
            let ent = ent?;
            let path = ent.path();
            if path.is_file() && path.extension().and_then(|e| e.to_str()) == Some("tmp") {
                match std::fs::remove_file(&path) {
                    Ok(()) => n += 1,
                    Err(e) => tracing::warn!("清理残留临时文件 {} 失败: {e}", path.display()),
                }
            }
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{BasicNode, CommittedLeaderId, LogId, SnapshotMeta, StoredMembership};

    fn log_id(term: u64, index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(term, 0), index)
    }

    fn meta(term: u64, index: u64) -> SnapshotMeta<u64, BasicNode> {
        SnapshotMeta {
            last_log_id: Some(log_id(term, index)),
            last_membership: StoredMembership::new(None, Default::default()),
            snapshot_id: format!("0-{}-{}", term, index),
        }
    }

    fn sample_entries() -> Vec<(Vec<u8>, Vec<u8>)> {
        vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"vvv2".to_vec()),
            (Vec::new(), b"empty-key".to_vec()),
        ]
    }

    /// 把 entries 以指定压缩写成一个完整快照文件
    fn write_snap_file(
        path: &Path,
        c: Compression,
        term: u64,
        index: u64,
        entries: &[(Vec<u8>, Vec<u8>)],
    ) {
        let m = meta(term, index);
        let meta_len = serde_json::to_vec(&m).unwrap().len() as u64;
        // shard 从路径解析（文件名 snap-{shard:08}-...）：
        // 文件头分片必须与文件名一致，latest/retain 的分片过滤依赖它
        let shard: u64 = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_prefix("snap-"))
            .and_then(|n| n.split('-').next())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0); // 非规范名测试文件默认分片 0
        // 头部 + meta 必须未压缩：先裸文件写，再构造压缩器写 payload（与读侧对称）
        let mut f = File::create(path).unwrap();
        write_header(
            &mut f,
            &SnapshotHeader {
                version: SNAP_VERSION,
                compression: c,
                shard_id: shard,
                meta_len,
                payload_len: payload_len_for(entries),
            },
            &m,
        )
        .unwrap();
        let mut w = c.writer(f).unwrap();
        w.write_all(&(entries.len() as u64).to_le_bytes()).unwrap();
        for (k, v) in entries {
            write_record(&mut w, k, v).unwrap();
        }
        write_end_marker(&mut w).unwrap();
        w.finish().unwrap();
    }

    /// 从快照文件读回 (header, meta, entries)；自动校验 payload_len 与 end_marker
    fn read_snap_file(
        path: &Path,
    ) -> (
        SnapshotHeader,
        SnapshotMeta<u64, BasicNode>,
        Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        let (h, m, mut r) = open_payload_reader(path).unwrap();
        let mut entries = Vec::new();
        let read_bytes = for_each_record(&mut r, |k, v| {
            entries.push((k, v));
            Ok(())
        })
        .unwrap();
        assert_eq!(read_bytes, h.payload_len, "实读字节数须与头部声明一致");
        (h, m, entries)
    }

    #[test]
    fn compression_roundtrip_all_algorithms() {
        let dir = tempfile::tempdir().unwrap();
        for c in [Compression::Zstd, Compression::Lz4, Compression::None] {
            let path = dir.path().join(format!("{}.esnap", c.display_name()));
            let entries = sample_entries();
            write_snap_file(&path, c, 3, 100, &entries);
            let (h, m, got) = read_snap_file(&path);
            assert_eq!(h.compression, c);
            assert_eq!(m.last_log_id, Some(log_id(3, 100)));
            assert_eq!(got, entries, "压缩往返后记录须逐字节一致");
        }
        // 大数据集上压缩必须确实减小体积（zstd/lz4 对可压缩内容成立）
        for c in [Compression::Zstd, Compression::Lz4] {
            let path = dir.path().join(format!("big-{}.esnap", c.display_name()));
            let big: Vec<(Vec<u8>, Vec<u8>)> = (0..500u64)
                .map(|i| {
                    (
                        format!("stream-{i}").into_bytes(),
                        format!("payload-payload-payload-{i}").into_bytes(),
                    )
                })
                .collect();
            write_snap_file(&path, c, 3, 100, &big);
            let (h, _, got) = read_snap_file(&path);
            assert_eq!(got, big);
            let size = std::fs::metadata(&path).unwrap().len();
            assert!(
                size < h.payload_len,
                "压缩文件 {size} 应小于未压缩体积 {}",
                h.payload_len
            );
        }
    }

    #[test]
    fn compression_roundtrip_empty_entries() {
        let dir = tempfile::tempdir().unwrap();
        for c in [Compression::Zstd, Compression::Lz4, Compression::None] {
            let path = dir.path().join(format!("empty-{}.esnap", c.display_name()));
            write_snap_file(&path, c, 1, 0, &[]);
            let (_, _, got) = read_snap_file(&path);
            assert!(got.is_empty());
        }
    }

    #[test]
    fn compression_tag_roundtrip() {
        for c in [Compression::Zstd, Compression::Lz4, Compression::None] {
            assert_eq!(Compression::from_tag(c.tag()).unwrap(), c);
        }
        assert!(Compression::from_tag(99).is_err(), "未知 tag 必须报错");
    }

    #[test]
    fn header_rejects_bad_magic_and_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.esnap");
        write_snap_file(&path, Compression::None, 1, 1, &[]);
        // 破坏 magic
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] = b'X';
        assert!(parse_header_bytes(&bytes).is_err(), "magic 损坏必须报错");
        // 破坏 version
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[4] = 9;
        assert!(parse_header_bytes(&bytes).is_err(), "未知版本必须报错");
    }

    #[test]
    fn meta_len_overflow_rejected_without_alloc() {
        // 损坏头声明巨量 meta_len：必须被上限拦截，不能触发巨量分配
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("huge.esnap");
        write_snap_file(&path, Compression::None, 1, 1, &[]);
        let mut bytes = std::fs::read(&path).unwrap();
        // meta_len 字段在偏移 16..24，篡改为 2^63（旧检查放行、会 OOM abort）
        bytes[16..24].copy_from_slice(&(1u64 << 63).to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        let err = parse_header_bytes(&bytes).unwrap_err();
        assert!(
            err.to_string().contains("越界"),
            "meta_len 越界必须被拦截: {err}"
        );
    }

    #[test]
    fn truncated_payload_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trunc.esnap");
        // 手工写：header 完整，payload 只写一半就丢弃 writer（不调 finish()，
        // zstd Encoder 的 Drop 不会补帧尾）→ 帧被截断
        let m = meta(1, 1);
        let meta_len = serde_json::to_vec(&m).unwrap().len() as u64;
        let entries = sample_entries();
        {
            // 头部 + meta 先裸写，压缩器只覆盖 payload（与读侧对称）
            let mut f = File::create(&path).unwrap();
            write_header(
                &mut f,
                &SnapshotHeader {
                    version: SNAP_VERSION,
                    compression: Compression::Zstd,
                    shard_id: 0,
                    meta_len,
                    payload_len: payload_len_for(&entries),
                },
                &m,
            )
            .unwrap();
            let mut w = Compression::Zstd.writer(f).unwrap();
            w.write_all(&(entries.len() as u64).to_le_bytes()).unwrap();
            for (k, v) in &entries[..1] {
                write_record(&mut w, k, v).unwrap();
            }
            drop(w);
        }
        // 读侧：解压到文件尾必然失败（帧不完整）
        let (_, _, mut r) = open_payload_reader(&path).unwrap();
        let e = for_each_record(&mut r, |_, _| Ok(()));
        assert!(e.is_err(), "截断的快照必须被解压层或记录层拦截");
    }

    #[test]
    fn payload_len_mismatch_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mismatch.esnap");
        write_snap_file(&path, Compression::None, 1, 1, &sample_entries());
        // 篡改头部 payload_len（+10）：读侧与实读字节数比对必然失败
        let mut bytes = std::fs::read(&path).unwrap();
        let n = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        bytes[24..32].copy_from_slice(&(n + 10).to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        let (h, _, mut r) = open_payload_reader(&path).unwrap();
        let rb = for_each_record(&mut r, |_, _| Ok(())).unwrap();
        assert_ne!(rb, h.payload_len, "篡改后实读字节数必然与声明不符");
    }

    #[test]
    fn end_marker_missing_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nomarker.esnap");
        write_snap_file(&path, Compression::None, 1, 1, &sample_entries());
        // 把 entry_count 篡改为比实际大：真实记录读完后再读一条时，读到的
        // 是 end_marker（u64::MAX 作为 key_len）→ 提前遇哨兵报错
        let mut bytes = std::fs::read(&path).unwrap();
        let (h, _) = parse_header_bytes(&bytes).unwrap();
        let count_off = 32 + h.meta_len as usize;
        bytes[count_off..count_off + 8].copy_from_slice(&(999u64).to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        let (_, _, mut r) = open_payload_reader(&path).unwrap();
        let err = for_each_record(&mut r, |_, _| Ok(())).unwrap_err();
        assert!(
            err.to_string().contains("记录数") || err.to_string().contains("哨兵"),
            "记录数不符须报错，实际: {err}"
        );
    }

    #[tokio::test]
    async fn snapshot_file_temp_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let incoming = dir.path().join("incoming");
        std::fs::create_dir_all(&incoming).unwrap();

        // temp 文件 Drop 即删
        {
            let sf = SnapshotFile::create_temp(&incoming).await.unwrap();
            let p = sf.path().to_path_buf();
            assert!(p.exists());
            assert!(sf.is_temp());
            drop(sf);
            assert!(!p.exists(), "temp 文件 Drop 后必须被清理");
        }
        // promote 后 Drop 不删
        {
            let mut sf = SnapshotFile::create_temp(&incoming).await.unwrap();
            let final_path = dir.path().join("promoted.esnap");
            sf.promote(final_path.clone()).unwrap();
            assert!(!sf.is_temp());
            drop(sf);
            assert!(final_path.exists(), "转正文件不能被 Drop 清理");
        }
    }

    #[tokio::test]
    async fn snapshot_file_async_io_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let incoming = dir.path().join("incoming");
        std::fs::create_dir_all(&incoming).unwrap();
        let mut sf = SnapshotFile::create_temp(&incoming).await.unwrap();
        // AsyncWrite 写入 + AsyncSeek 回零 + AsyncRead 读回
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncSeekExt;
        use tokio::io::AsyncWriteExt;
        sf.write_all(b"hello snapshot").await.unwrap();
        sf.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let mut buf = Vec::new();
        sf.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"hello snapshot");

        // into_std_file 后 std 句柄能读到相同内容
        let path = sf.path().to_path_buf();
        let (mut f, p, temp) = sf.into_std_file().unwrap();
        assert_eq!(p, path);
        assert!(temp);
        use std::io::Read;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"hello snapshot");
    }

    #[test]
    fn store_naming_and_list() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(
            SnapshotConfig {
                dir: dir.path().to_path_buf(),
                compression: Compression::Zstd,
                keep: 3,
            },
            7,
        )
        .unwrap();
        store.ensure_dirs().unwrap();
        // 命名格式：snap-{shard:08}-{term:020}-{index:020}.esnap
        let p = store.final_path(Some(log_id(12, 34)));
        let name = p.file_name().unwrap().to_str().unwrap().to_string();
        assert!(name.starts_with("snap-00000007-"), "分片宽度补零: {name}");
        assert!(name.contains("-00000000000000000012-"), "term 补零: {name}");
        assert!(
            name.ends_with("-00000000000000000034.esnap"),
            "index 补零: {name}"
        );
        // 空快照哨兵 term=0/index=0
        let p0 = store.final_path(None);
        assert!(
            p0.to_str()
                .unwrap()
                .ends_with("-00000000000000000000-00000000000000000000.esnap")
        );

        // 写 3 个快照文件后 list 可见
        write_snap_file(
            &store.final_path(Some(log_id(1, 10))),
            Compression::None,
            1,
            10,
            &[],
        );
        write_snap_file(
            &store.final_path(Some(log_id(2, 20))),
            Compression::Lz4,
            2,
            20,
            &[],
        );
        let entries = store.list_entries().unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.valid));
        // list 不扫 incoming
        std::fs::write(store.incoming_dir().join("junk.tmp"), b"x").unwrap();
        assert_eq!(store.list_entries().unwrap().len(), 2);
    }

    #[test]
    fn store_latest_by_numeric_term_index() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(
            SnapshotConfig {
                dir: dir.path().to_path_buf(),
                compression: Compression::None,
                keep: 3,
            },
            0,
        )
        .unwrap();
        store.ensure_dirs().unwrap();
        // 字符串排序陷阱：term=9 vs term=10（"9" > "10" 字符串序但数值序相反）
        write_snap_file(
            &store.final_path(Some(log_id(10, 5))),
            Compression::None,
            10,
            5,
            &[],
        );
        write_snap_file(
            &store.final_path(Some(log_id(9, 100))),
            Compression::None,
            9,
            100,
            &[],
        );
        let latest = store.latest(None).unwrap().unwrap();
        assert!(
            latest.to_str().unwrap().contains("-00000000000000000010-"),
            "latest 必须按数值序取 (term=10)：{}",
            latest.display()
        );
        // 损坏文件被 latest 跳过
        let bad = store.final_path(Some(log_id(11, 1)));
        std::fs::write(&bad, b"not a snapshot").unwrap();
        let latest = store.latest(None).unwrap().unwrap();
        assert!(!latest.to_str().unwrap().contains("-00000000000000000011-"));
    }

    /// 恢复前扫描必须容忍缺失目录、无关文件和损坏快照，并过滤领先状态机的残留。
    #[test]
    fn store_scan_skips_invalid_entries_and_future_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_dir = dir.path().join("snapshots");
        let store = SnapshotStore::new(
            SnapshotConfig {
                dir: snapshot_dir,
                compression: Compression::None,
                keep: 2,
            },
            0,
        )
        .unwrap();

        assert!(store.list_entries().unwrap().is_empty(), "缺失目录应视为空");
        assert_eq!(
            store.cleanup_incoming().unwrap(),
            0,
            "缺失 incoming 不应报错"
        );

        store.ensure_dirs().unwrap();
        std::fs::create_dir(store.cfg.dir.join("nested")).unwrap();
        std::fs::write(store.cfg.dir.join("ignored.txt"), b"not a snapshot").unwrap();
        std::fs::write(store.cfg.dir.join("broken.esnap"), b"broken").unwrap();
        let entries = store.list_entries().unwrap();
        assert_eq!(entries.len(), 1, "只列出 .esnap 普通文件");
        assert!(!entries[0].valid, "损坏快照必须标记为无效而不是中断扫描");

        let applied = log_id(1, 10);
        let retained = store.final_path(Some(applied));
        let future = store.final_path(Some(log_id(2, 1)));
        write_snap_file(&retained, Compression::None, 1, 10, &[]);
        write_snap_file(&future, Compression::None, 2, 1, &[]);
        assert_eq!(
            store.latest(Some(applied)).unwrap(),
            Some(retained),
            "领先于已应用位置的残留快照不能参与恢复"
        );
    }

    #[test]
    fn store_retain_keeps_newest_n() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(
            SnapshotConfig {
                dir: dir.path().to_path_buf(),
                compression: Compression::None,
                keep: 2,
            },
            0,
        )
        .unwrap();
        store.ensure_dirs().unwrap();
        for (t, i) in [(1, 10), (1, 20), (2, 5), (3, 1)] {
            write_snap_file(
                &store.final_path(Some(log_id(t, i))),
                Compression::None,
                t,
                i,
                &[],
            );
        }
        let removed = store.retain(2).unwrap();
        assert_eq!(removed.len(), 2, "保留最新 2 个应删除 2 个");
        let remaining = store.list_entries().unwrap();
        assert_eq!(remaining.len(), 2);
        // 剩余必须是 (2,5) 与 (3,1)：按 (term,index) 数值序保留最大的两个，
        // (1,10) 与 (1,20) 被删（字符串序会错删 (2,5) 而留 (1,20)，数值序则不会）
        let names: Vec<String> = remaining
            .iter()
            .map(|e| e.path.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        for n in &names {
            assert!(
                n.contains("-00000000000000000002-") || n.contains("-00000000000000000003-"),
                "剩余 {n}"
            );
        }
        // retain(0) 拒绝
        assert!(store.retain(0).is_err());
    }

    #[test]
    fn store_cleanup_incoming_only_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(
            SnapshotConfig {
                dir: dir.path().to_path_buf(),
                compression: Compression::None,
                keep: 3,
            },
            0,
        )
        .unwrap();
        store.ensure_dirs().unwrap();
        std::fs::write(store.incoming_dir().join("a.tmp"), b"x").unwrap();
        std::fs::write(store.incoming_dir().join("b.tmp"), b"x").unwrap();
        // 非 .tmp 后缀不动
        std::fs::write(store.incoming_dir().join("keep.txt"), b"x").unwrap();
        assert_eq!(store.cleanup_incoming().unwrap(), 2);
        assert!(store.incoming_dir().join("keep.txt").exists());
    }

    #[test]
    fn store_latest_filters_by_shard() {
        // 生产布局：全部分片共享同一快照目录（文件名带分片号）
        let dir = tempfile::tempdir().unwrap();
        let store0 = SnapshotStore::new(
            SnapshotConfig {
                dir: dir.path().to_path_buf(),
                compression: Compression::None,
                keep: 3,
            },
            0,
        )
        .unwrap();
        let store1 = SnapshotStore::new(
            SnapshotConfig {
                dir: dir.path().to_path_buf(),
                compression: Compression::None,
                keep: 3,
            },
            1,
        )
        .unwrap();
        store0.ensure_dirs().unwrap();

        // 写分片 0（term=1,index=5）与分片 1（term=2,index=99，全局更大）的快照
        let (p0, p1) = (
            store0.final_path(Some(log_id(1, 5))),
            store1.final_path(Some(log_id(2, 99))),
        );
        write_snap_file(&p0, Compression::None, 1, 5, &[]);
        write_snap_file(&p1, Compression::None, 2, 99, &[]);

        // 分片 0 的 store 必须只看到分片 0 的快照（不能取全局最大）
        let latest0 = store0.latest(None).unwrap().unwrap();
        assert!(
            latest0
                .to_str()
                .unwrap()
                .contains("-00000000000000000005.esnap"),
            "分片 0 不应取到分片 1 的快照: {}",
            latest0.display()
        );
        let latest1 = store1.latest(None).unwrap().unwrap();
        assert!(
            latest1
                .to_str()
                .unwrap()
                .contains("-00000000000000000099.esnap")
        );

        // retain 同样只清理本分片
        let removed = store0.retain(1).unwrap();
        assert_eq!(removed.len(), 0, "分片 0 只有一个文件，无需清理");
        assert!(p1.exists(), "retain 不得删除其它分片的快照");
    }

    #[test]
    fn store_latest_skips_newer_than_applied() {
        // restore/崩溃残留的「更新」快照文件必须被 latest(applied) 过滤
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(
            SnapshotConfig {
                dir: dir.path().to_path_buf(),
                compression: Compression::None,
                keep: 3,
            },
            0,
        )
        .unwrap();
        store.ensure_dirs().unwrap();
        // 残留的更新文件 (term=5, index=100) 与正常文件 (term=1, index=4)
        write_snap_file(
            &store.final_path(Some(log_id(5, 100))),
            Compression::None,
            5,
            100,
            &[],
        );
        write_snap_file(
            &store.final_path(Some(log_id(1, 4))),
            Compression::None,
            1,
            4,
            &[],
        );

        // applied=(1,4) 时返回 (1,4) 而非残留的 (5,100)
        let latest = store.latest(Some(log_id(1, 4))).unwrap().unwrap();
        assert!(
            latest
                .to_str()
                .unwrap()
                .contains("-00000000000000000004.esnap"),
            "领先于 applied 的残留快照必须被跳过: {}",
            latest.display()
        );
        // applied=None（新节点）不过滤
        let latest = store.latest(None).unwrap().unwrap();
        assert!(
            latest
                .to_str()
                .unwrap()
                .contains("-00000000000000000100.esnap")
        );
    }
}

/// 离线恢复报告
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RestoreReport {
    /// 恢复到的分片
    pub shard_id: u64,
    /// 快照点 term（空快照为 0）
    pub term: u64,
    /// 快照点 index（空快照为 0）
    pub index: u64,
    /// 恢复的事件条数
    pub events: usize,
    /// 复制到快照目录后的文件路径
    pub snapshot_file: PathBuf,
}

/// 离线恢复：把快照文件恢复到数据目录中指定分片（**集群必须停机**）。
///
/// 语义：该分片回到快照时刻——清空 Raft 日志区与状态机区，装入快照内容，
/// 并把 `raft_last_purged` / `raft_committed` 写回快照点。重启后 openraft
/// 的日志基线与状态机一致（get_log_state 在日志为空时回落 last_purged），
/// 不重放、不报错；vote 保留使单节点直接恢复领导，多节点由 leader 复制
/// 快照点之后的日志或新快照。
///
/// 快照目录中该分片的旧快照文件全部清除，再复制恢复的快照为当前唯一快照
/// （残留更新的旧文件会让 get_current_snapshot 返回与状态机不一致的 meta）。
///
/// 事务原子：中途失败（含崩溃）数据目录原样无损。
pub async fn restore(
    tree: std::sync::Arc<surrealkv::Tree>,
    shard_id: u64,
    snapshot_file: &Path,
    snapshot_dir: &Path,
) -> es_core::Result<RestoreReport> {
    use crate::key;

    // 1. 校验快照文件（magic/version/压缩 tag/分片一致）
    let mut f = File::open(snapshot_file)
        .map_err(|e| es_core::Error::Storage(format!("打开快照文件失败: {e}")))?;
    let (header, meta) = read_header(&mut f)
        .map_err(|e| es_core::Error::Storage(format!("快照文件头部校验失败: {e}")))?;
    if header.shard_id != shard_id {
        return Err(es_core::Error::InvalidInput(format!(
            "快照属于分片 {}，不能恢复到分片 {shard_id}",
            header.shard_id
        )));
    }
    std::io::Seek::seek(&mut f, std::io::SeekFrom::Start(32 + header.meta_len))
        .map_err(|e| es_core::Error::Storage(format!("定位快照 payload 失败: {e}")))?;
    let mut reader = header
        .compression
        .reader(f)
        .map_err(|e| es_core::Error::Storage(format!("打开解压流失败: {e}")))?;

    // 2. 快照文件准备（事务前）：删除该分片旧快照文件，失败即中止——数据未动。
    //    只删本分片（生产布局共享目录），排除源文件本身（恢复目录内现有
    //    快照是常见用法：snapshot list 选中的历史点即规范名文件）。
    let (term, index) = match meta.last_log_id {
        Some(l) => (l.leader_id.term, l.index),
        None => (0, 0),
    };
    let store = SnapshotStore::new(
        SnapshotConfig {
            dir: snapshot_dir.to_path_buf(),
            compression: header.compression,
            keep: 1,
        },
        shard_id,
    )
    .map_err(|e| es_core::Error::Storage(format!("快照目录初始化失败: {e}")))?;
    store
        .ensure_dirs()
        .map_err(|e| es_core::Error::Storage(format!("建快照目录失败: {e}")))?;
    let final_path = store.final_path(meta.last_log_id);
    // 源与目标是否为同一文件（canonicalize 比较，防 fs::copy 的 O_TRUNC 截断同 inode）
    let same_file = match (
        std::fs::canonicalize(snapshot_file).ok(),
        std::fs::canonicalize(&final_path).ok(),
    ) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    };
    let src_canon = std::fs::canonicalize(snapshot_file).ok();
    for entry in store
        .list_entries()
        .map_err(|e| es_core::Error::Storage(format!("枚举快照失败: {e}")))?
    {
        // 只删本分片：损坏文件无法判断分片，保留（esctl list 标记）
        if entry.header.map(|h| h.shard_id) != Some(shard_id) {
            continue;
        }
        // 源文件不删（可能位于目录内任意位置：规范名或改名备份）
        let same_as_src = match (&src_canon, std::fs::canonicalize(&entry.path)) {
            (Some(a), Ok(b)) => a == &b,
            _ => false,
        };
        if same_as_src {
            continue;
        }
        std::fs::remove_file(&entry.path).map_err(|e| {
            es_core::Error::Storage(format!("删除旧快照 {} 失败: {e}", entry.path.display()))
        })?;
    }

    // 3. 清空该分片的 Raft 日志区与状态机区（含 vote/committed/last_purged）
    let raft_start = {
        let mut p = vec![0x01u8];
        p.extend_from_slice(&shard_id.to_be_bytes());
        p
    };
    let raft_end = key::successor(&raft_start)
        .ok_or_else(|| es_core::Error::Internal("raft 区前缀无后继".into()))?;
    let sm_start = {
        let mut p = vec![0x02u8];
        p.extend_from_slice(&shard_id.to_be_bytes());
        p
    };
    let sm_end = key::successor(&sm_start)
        .ok_or_else(|| es_core::Error::Internal("状态机区前缀无后继".into()))?;

    let mut txn = tree
        .begin()
        .map_err(|e| es_core::Error::Storage(format!("begin 失败: {e}")))?;
    // 迭代器与 delete 互斥借用，先收集 key 再删（同 EsStorage::collect_keys 模式）
    let collect_keys = |txn: &surrealkv::Transaction, start: Vec<u8>, end: Vec<u8>| {
        use surrealkv::LSMIterator;
        let mut keys = Vec::new();
        if start >= end {
            return Ok::<Vec<Vec<u8>>, es_core::Error>(keys);
        }
        let mut it = txn
            .range(start, end)
            .map_err(|e| es_core::Error::Storage(format!("range 失败: {e}")))?;
        it.seek_first()
            .map_err(|e| es_core::Error::Storage(format!("seek_first 失败: {e}")))?;
        while it.valid() {
            keys.push(it.key().user_key().to_vec());
            it.next()
                .map_err(|e| es_core::Error::Storage(format!("next 失败: {e}")))?;
        }
        Ok(keys)
    };
    let mut old_keys = collect_keys(&txn, raft_start, raft_end)?;
    old_keys.extend(collect_keys(&txn, sm_start, sm_end)?);
    // 保留 vote：选举状态与数据时间点无关。清掉后单节点重启无法恢复领导，
    // 而日志非空又拒绝重新 initialize——保留 vote 使节点以快照点直接恢复
    // 领导（etcd snapshot restore 同样要求恢复后重配，openraft 无此入口，
    // 保留 vote 是本实现的最小等价路径）。
    let vote_key = key::raft_vote(shard_id);
    old_keys.retain(|k| k != &vote_key);
    for k in old_keys {
        txn.delete(k)
            .map_err(|e| es_core::Error::Storage(format!("delete 失败: {e}")))?;
    }

    // 3. 流式读快照记录灌入（同 install_snapshot 的 txn 缓冲边界）
    let mut events = 0usize;
    let mut total = 0usize;
    let read_bytes = for_each_record(&mut reader, |k, v| {
        // 统计事件条数：key = [0x02][shard][0x01]... 的事件本体
        if k.len() >= 10 && k[0] == 0x02 && k[9] == 0x01 {
            events += 1;
        }
        total += 1;
        txn.set(k, v)
            .map_err(|e| std::io::Error::other(format!("set 失败: {e}")))?;
        Ok(())
    })
    .map_err(|e| es_core::Error::Storage(format!("读取快照记录失败: {e}")))?;
    if read_bytes != header.payload_len {
        return Err(es_core::Error::Storage(format!(
            "快照 payload 长度不符：实读 {read_bytes} vs 声明 {}",
            header.payload_len
        )));
    }

    // 4. 已应用状态（快照点）
    let applied = crate::state_machine::AppliedState {
        last_applied: meta.last_log_id,
        membership: meta.last_membership.clone(),
    };
    txn.set(
        key::sm_applied_state(shard_id),
        crate::encode::encode(&applied)
            .map_err(|e| es_core::Error::Serde(format!("applied 状态序列化失败: {e}")))?,
    )
    .map_err(|e| es_core::Error::Storage(format!("写 applied 失败: {e}")))?;

    // 5. 日志基线写回快照点（非空快照）：get_log_state 在日志为空时回落
    //    last_purged，committed 与 last_applied 三者一致，openraft 不重放
    if let Some(last_log_id) = meta.last_log_id {
        let val = crate::encode::encode(&Some(last_log_id))
            .map_err(|e| es_core::Error::Serde(format!("last_purged 序列化失败: {e}")))?;
        txn.set(key::raft_last_purged(shard_id), val.clone())
            .map_err(|e| es_core::Error::Storage(format!("写 last_purged 失败: {e}")))?;
        txn.set(key::raft_committed(shard_id), val)
            .map_err(|e| es_core::Error::Storage(format!("写 committed 失败: {e}")))?;
    }

    txn.commit()
        .await
        .map_err(|e| es_core::Error::Storage(format!("commit 失败: {e}")))?;
    drop(reader);

    // 6. 复制恢复的快照为当前快照（事务后：失败只损失文件缓存，SM 已就位）。
    //    源已在目标位置（规范名文件恢复）时跳过——fs::copy 以 O_TRUNC 打开
    //    目标会先截断同一 inode，导致快照文件被静默清空
    if !same_file {
        std::fs::copy(snapshot_file, &final_path)
            .map_err(|e| es_core::Error::Storage(format!("复制快照文件失败: {e}")))?;
    }

    Ok(RestoreReport {
        shard_id,
        term,
        index,
        events,
        snapshot_file: final_path,
    })
}
