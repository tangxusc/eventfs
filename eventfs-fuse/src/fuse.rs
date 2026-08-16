//! Linux `fuser` 适配层。

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsStr;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use es_proto::eventstore::AggregateGroupSettlementStatus;
use fuser::{
    Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo, InitFlags,
    KernelConfig, LockOwner, OpenAccMode, OpenFlags, PollEvents, PollFlags, PollNotifier,
    RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyPoll, ReplyWrite, Request, WriteFlags,
};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::backend::{AggregateType, BackendError, Capabilities, EventFsBackend};
use crate::codec::{self, EventEnvelope, SettlementEnvelope};
use crate::handle::{BeginError, BufferedWrite, StreamBuffer, WriteError};
use crate::path::{Node, PathError};

const TTL: Duration = Duration::ZERO;
const MAX_STREAM_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const STATE_DIRECTORY_PAGE_SIZE: u32 = 256;

/// 单次挂载的本地访问属性。
#[derive(Debug, Clone, Copy)]
pub struct MountIdentity {
    /// 挂载进程的有效用户 ID。
    pub uid: u32,
    /// 挂载进程的有效组 ID。
    pub gid: u32,
}

/// EventFS 的 Linux FUSE 文件系统。
pub struct EventFs<B: EventFsBackend> {
    inner: Arc<Inner<B>>,
}

struct Inner<B: EventFsBackend> {
    backend: Arc<B>,
    runtime: tokio::runtime::Handle,
    capabilities: Capabilities,
    identity: MountIdentity,
    inodes: Mutex<InodeTable>,
    handles: Mutex<HashMap<u64, Arc<OpenHandle>>>,
    next_handle: AtomicU64,
    active_consumers: Mutex<BTreeSet<ConsumerKey>>,
}

#[derive(Debug)]
struct InodeTable {
    next: u64,
    by_inode: HashMap<u64, Node>,
    by_node: BTreeMap<Node, u64>,
    sizes: HashMap<u64, u64>,
    lookup_refs: HashMap<u64, u64>,
    directory_refs: HashMap<u64, u64>,
}

impl InodeTable {
    fn new() -> Self {
        Self {
            next: INodeNo::ROOT.0 + 1,
            by_inode: HashMap::from([(INodeNo::ROOT.0, Node::Root)]),
            by_node: BTreeMap::from([(Node::Root, INodeNo::ROOT.0)]),
            sizes: HashMap::new(),
            lookup_refs: HashMap::new(),
            directory_refs: HashMap::new(),
        }
    }

    fn ensure(&mut self, node: Node, size: u64) -> INodeNo {
        if let Some(ino) = self.by_node.get(&node).copied() {
            self.sizes.insert(ino, size);
            return INodeNo(ino);
        }
        let ino = self.next;
        self.next = self.next.saturating_add(1);
        self.by_inode.insert(ino, node.clone());
        self.by_node.insert(node, ino);
        self.sizes.insert(ino, size);
        INodeNo(ino)
    }

    fn node(&self, ino: INodeNo) -> Option<Node> {
        self.by_inode.get(&ino.0).cloned()
    }

    fn inode(&self, node: &Node) -> Option<INodeNo> {
        self.by_node.get(node).copied().map(INodeNo)
    }

    fn size(&self, ino: INodeNo) -> u64 {
        self.sizes.get(&ino.0).copied().unwrap_or(0)
    }

    fn remember_lookup(&mut self, node: Node, size: u64) -> INodeNo {
        let ino = self.ensure(node, size);
        *self.lookup_refs.entry(ino.0).or_default() += 1;
        ino
    }

    fn forget(&mut self, ino: INodeNo, count: u64) {
        if let Some(value) = self.lookup_refs.get_mut(&ino.0) {
            *value = value.saturating_sub(count);
            if *value == 0 {
                self.lookup_refs.remove(&ino.0);
            }
        }
        self.remove_if_unreferenced(ino);
    }

    fn retain_directory(&mut self, ino: INodeNo) {
        *self.directory_refs.entry(ino.0).or_default() += 1;
    }

    fn release_directory(&mut self, ino: INodeNo) {
        if let Some(value) = self.directory_refs.get_mut(&ino.0) {
            *value = value.saturating_sub(1);
            if *value == 0 {
                self.directory_refs.remove(&ino.0);
            }
        }
        self.remove_if_unreferenced(ino);
    }

    fn remove_if_unreferenced(&mut self, ino: INodeNo) {
        if ino == INodeNo::ROOT
            || self.lookup_refs.contains_key(&ino.0)
            || self.directory_refs.contains_key(&ino.0)
        {
            return;
        }
        if let Some(node) = self.by_inode.remove(&ino.0) {
            self.by_node.remove(&node);
            self.sizes.remove(&ino.0);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ConsumerKey {
    aggregate_type: AggregateType,
    group_name: String,
    consumer_id: String,
}

enum OpenHandle {
    Directory {
        node: Node,
        state: tokio::sync::Mutex<DirectoryState>,
    },
    StaticRead(Vec<u8>),
    StreamingRead {
        shared: Arc<StreamShared>,
        consumer: Option<ConsumerKey>,
    },
    EventWrite {
        aggregate_type: AggregateType,
        buffer: Mutex<BufferedWrite<EventEnvelope>>,
    },
    StateWrite {
        aggregate_type: AggregateType,
        aggregate_id: String,
        revision: Option<u64>,
        buffer: Mutex<BufferedWrite<Vec<u8>>>,
    },
    SettlementWrite {
        aggregate_type: AggregateType,
        group_name: String,
        consumer_id: String,
        buffer: Mutex<BufferedWrite<SettlementEnvelope>>,
    },
}

struct DirectoryState {
    entries: Vec<DirectoryEntry>,
    retained_inodes: Vec<INodeNo>,
    next_page_token: Vec<u8>,
    initialized: bool,
    complete: bool,
}

impl DirectoryState {
    fn new(node: &Node) -> Self {
        Self {
            entries: Vec::new(),
            retained_inodes: Vec::new(),
            next_page_token: Vec::new(),
            initialized: false,
            complete: !matches!(node, Node::States { .. }),
        }
    }
}

struct DirectoryEntry {
    name: String,
    ino: INodeNo,
    kind: FileType,
}

struct StreamShared {
    buffer: Mutex<StreamBuffer>,
    notify: Notify,
    space: Notify,
    pollers: Mutex<Vec<PollNotifier>>,
    closed: AtomicBool,
    error: Mutex<Option<Errno>>,
}

impl StreamShared {
    fn new() -> Self {
        Self {
            buffer: Mutex::new(StreamBuffer::default()),
            notify: Notify::new(),
            space: Notify::new(),
            pollers: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
            error: Mutex::new(None),
        }
    }

    async fn push(&self, bytes: &[u8]) -> bool {
        if bytes.len() > MAX_STREAM_BUFFER_BYTES {
            self.fail(Errno::EFBIG);
            return false;
        }
        loop {
            let space = self.space.notified();
            let pushed = {
                let mut buffer = self.buffer.lock().expect("stream buffer poisoned");
                if buffer.is_closed() {
                    return false;
                }
                if buffer.is_empty()
                    || buffer.len().saturating_add(bytes.len()) <= MAX_STREAM_BUFFER_BYTES
                {
                    buffer.push(bytes);
                    true
                } else {
                    false
                }
            };
            if pushed {
                self.wake();
                return true;
            }
            space.await;
        }
    }

    fn fail(&self, error: Errno) {
        *self.error.lock().expect("stream error poisoned") = Some(error);
        self.buffer.lock().expect("stream buffer poisoned").close();
        self.closed.store(true, Ordering::Release);
        self.space.notify_one();
        self.wake();
    }

    fn close(&self) {
        self.buffer.lock().expect("stream buffer poisoned").close();
        self.closed.store(true, Ordering::Release);
        self.space.notify_one();
        self.wake();
    }

    fn wake(&self) {
        // 每个句柄只有一个阻塞 reader；保留 permit 可覆盖“检查后、await 前”的窗口。
        self.notify.notify_one();
        for poller in self.pollers.lock().expect("pollers poisoned").drain(..) {
            let _ = poller.notify();
        }
    }

    fn ready(&self) -> bool {
        self.buffer.lock().expect("stream buffer poisoned").ready()
    }

    fn ready_or_register(&self, register: impl FnOnce()) -> bool {
        let buffer = self.buffer.lock().expect("stream buffer poisoned");
        if buffer.ready() {
            true
        } else {
            register();
            false
        }
    }

    async fn read(&self, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
        loop {
            let notified = self.notify.notified();
            let result = self
                .buffer
                .lock()
                .expect("stream buffer poisoned")
                .read(offset, size)
                .map_err(write_errno)?;
            if let Some(data) = result {
                if !data.is_empty() {
                    // 每个流只有一个生产任务，保留 permit 避免背压解除时丢失唤醒。
                    self.space.notify_one();
                }
                if data.is_empty() {
                    if let Some(error) = *self.error.lock().expect("stream error poisoned") {
                        return Err(error);
                    }
                }
                return Ok(data);
            }
            notified.await;
        }
    }
}

impl<B: EventFsBackend> Clone for EventFs<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<B: EventFsBackend> EventFs<B> {
    /// 创建挂载实例。
    ///
    /// # 参数
    /// `backend` 是唯一权威后端；`capabilities` 已在挂载前协商；`identity` 控制本地权限。
    ///
    /// # 返回
    /// 返回尚未挂载的 FUSE 实例。
    pub fn new(
        backend: Arc<B>,
        runtime: tokio::runtime::Handle,
        capabilities: Capabilities,
        identity: MountIdentity,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                backend,
                runtime,
                capabilities,
                identity,
                inodes: Mutex::new(InodeTable::new()),
                handles: Mutex::new(HashMap::new()),
                next_handle: AtomicU64::new(1),
                active_consumers: Mutex::new(BTreeSet::new()),
            }),
        }
    }
}

impl<B: EventFsBackend> Inner<B> {
    fn node(&self, ino: INodeNo) -> Result<Node, Errno> {
        self.inodes
            .lock()
            .expect("inode table poisoned")
            .node(ino)
            .ok_or(Errno::ENOENT)
    }

    fn ensure_node(&self, node: Node, size: u64) -> INodeNo {
        self.inodes
            .lock()
            .expect("inode table poisoned")
            .ensure(node, size)
    }

    fn remember_node(&self, node: Node, size: u64) -> INodeNo {
        self.inodes
            .lock()
            .expect("inode table poisoned")
            .remember_lookup(node, size)
    }

    fn retain_directory_node(&self, node: Node, size: u64) -> INodeNo {
        let mut inodes = self.inodes.lock().expect("inode table poisoned");
        let ino = inodes.ensure(node, size);
        inodes.retain_directory(ino);
        ino
    }

    fn release_directory_nodes(&self, inodes_to_release: &[INodeNo]) {
        let mut inodes = self.inodes.lock().expect("inode table poisoned");
        for ino in inodes_to_release {
            inodes.release_directory(*ino);
        }
    }

    fn attr(&self, ino: INodeNo, node: &Node, size: u64, mtime: SystemTime) -> FileAttr {
        let kind = if node.is_directory() {
            FileType::Directory
        } else {
            FileType::RegularFile
        };
        FileAttr {
            ino,
            size,
            blocks: size.div_ceil(512),
            atime: UNIX_EPOCH,
            mtime,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind,
            perm: if node.is_directory() { 0o750 } else { 0o640 },
            nlink: if node.is_directory() { 2 } else { 1 },
            uid: self.identity.uid,
            gid: self.identity.gid,
            rdev: 0,
            flags: 0,
            blksize: 4096,
        }
    }

    fn state_mtime(modified_unix_millis: u64) -> SystemTime {
        UNIX_EPOCH
            .checked_add(Duration::from_millis(modified_unix_millis))
            .unwrap_or(UNIX_EPOCH)
    }

    async fn state_attributes(&self, node: &Node) -> Result<(u64, SystemTime), Errno> {
        let Node::State { aggregate_id, .. } = node else {
            return Err(Errno::EINVAL);
        };
        let aggregate_type = node_aggregate_type(node)?;
        let state = self
            .backend
            .get_state(&aggregate_type, aggregate_id)
            .await
            .map_err(backend_errno)?
            .ok_or(Errno::ENOENT)?;
        Ok((
            state.data.len() as u64,
            Self::state_mtime(state.modified_unix_millis),
        ))
    }

    fn has_pending_state_write(&self, node: &Node) -> bool {
        let Node::State {
            business_space,
            aggregate_type,
            aggregate_id,
        } = node
        else {
            return false;
        };
        let Ok(expected_aggregate_type) = AggregateType::new(business_space, aggregate_type) else {
            return false;
        };
        self.handles
            .lock()
            .expect("handle table poisoned")
            .values()
            .any(|handle| {
                matches!(
                    handle.as_ref(),
                    OpenHandle::StateWrite {
                        aggregate_type,
                        aggregate_id: open_aggregate_id,
                        revision: None,
                        ..
                    } if aggregate_type == &expected_aggregate_type && open_aggregate_id == aggregate_id
                )
            })
    }

    async fn state_attributes_for_getattr(&self, node: &Node) -> Result<(u64, SystemTime), Errno> {
        match self.state_attributes(node).await {
            // CREATE 后内核会在首个 WRITE 前 GETATTR；此时状态尚未提交，
            // 但活动写句柄已证明该 inode 是一次合法的首次创建。
            Err(Errno::ENOENT) if self.has_pending_state_write(node) => Ok((0, UNIX_EPOCH)),
            result => result,
        }
    }

    fn insert_handle(&self, handle: OpenHandle) -> FileHandle {
        let id = self.next_handle.fetch_add(1, Ordering::Relaxed);
        self.handles
            .lock()
            .expect("handle table poisoned")
            .insert(id, Arc::new(handle));
        FileHandle(id)
    }

    fn handle(&self, fh: FileHandle) -> Result<Arc<OpenHandle>, Errno> {
        self.handles
            .lock()
            .expect("handle table poisoned")
            .get(&fh.0)
            .cloned()
            .ok_or(Errno::EBADF)
    }

    async fn aggregate_types(&self) -> Result<Vec<AggregateType>, Errno> {
        self.backend
            .list_aggregate_types()
            .await
            .map_err(backend_errno)
    }

    async fn lookup_child(
        &self,
        parent: &Node,
        name: &str,
    ) -> Result<(Node, u64, SystemTime), Errno> {
        let child = parent.child(name).map_err(path_errno)?;
        let exists = match (&child, parent) {
            (Node::BusinessSpace(space), Node::Root) => {
                self.inodes
                    .lock()
                    .expect("inode table poisoned")
                    .inode(&child)
                    .is_some()
                    || self
                        .aggregate_types()
                        .await?
                        .iter()
                        .any(|aggregate_type| aggregate_type.business_space == *space)
            }
            (
                Node::AggregateType {
                    business_space,
                    aggregate_type,
                },
                Node::BusinessSpace(_),
            ) => self.aggregate_types().await?.iter().any(|aggregate_type| {
                aggregate_type.business_space == *business_space
                    && aggregate_type.aggregate_type == *aggregate_type
            }),
            (Node::State { .. }, Node::States { .. }) => {
                let (size, mtime) = self.state_attributes(&child).await?;
                return Ok((child, size, mtime));
            }
            (Node::Group { group_name, .. }, Node::Groups { .. }) => {
                let aggregate_type = node_aggregate_type(&child)?;
                self.backend
                    .list_groups(&aggregate_type)
                    .await
                    .map_err(backend_errno)?
                    .iter()
                    .any(|name| name == group_name)
            }
            (Node::Consumer { .. }, Node::Group { .. }) => true,
            (
                Node::Events { .. } | Node::States { .. } | Node::Groups { .. },
                Node::AggregateType { .. },
            ) => true,
            _ => false,
        };
        if exists {
            Ok((child, 0, UNIX_EPOCH))
        } else {
            Err(Errno::ENOENT)
        }
    }

    async fn children(&self, node: &Node) -> Result<Vec<(String, Node, u64)>, Errno> {
        let mut children = Vec::new();
        match node {
            Node::Root => {
                let mut spaces = self
                    .aggregate_types()
                    .await?
                    .into_iter()
                    .map(|aggregate_type| aggregate_type.business_space)
                    .collect::<BTreeSet<_>>();
                spaces.extend(
                    self.inodes
                        .lock()
                        .expect("inode table poisoned")
                        .by_node
                        .keys()
                        .filter_map(|node| match node {
                            Node::BusinessSpace(space) => Some(space.clone()),
                            _ => None,
                        }),
                );
                children.extend(
                    spaces
                        .into_iter()
                        .map(|space| (space.clone(), Node::BusinessSpace(space), 0)),
                );
            }
            Node::BusinessSpace(space) => {
                children.extend(
                    self.aggregate_types()
                        .await?
                        .into_iter()
                        .filter(|aggregate_type| aggregate_type.business_space == *space)
                        .map(|aggregate_type| {
                            (
                                aggregate_type.aggregate_type.clone(),
                                Node::AggregateType {
                                    business_space: aggregate_type.business_space,
                                    aggregate_type: aggregate_type.aggregate_type,
                                },
                                0,
                            )
                        }),
                );
            }
            Node::AggregateType { .. } => {
                for name in ["events.jsonl", "states", "groups"] {
                    children.push((name.into(), node.child(name).map_err(path_errno)?, 0));
                }
            }
            Node::States { .. } => {
                let aggregate_type = node_aggregate_type(node)?;
                children.extend(
                    self.backend
                        .list_states(&aggregate_type)
                        .await
                        .map_err(backend_errno)?
                        .into_iter()
                        .map(|aggregate_id| {
                            let name = format!("{aggregate_id}.json");
                            let child = node.child(&name).expect("服务端身份已校验");
                            (name, child, 0)
                        }),
                );
            }
            Node::Groups { .. } => {
                let aggregate_type = node_aggregate_type(node)?;
                children.extend(
                    self.backend
                        .list_groups(&aggregate_type)
                        .await
                        .map_err(backend_errno)?
                        .into_iter()
                        .map(|name| {
                            let child = node.child(&name).expect("服务端身份已校验");
                            (name, child, 0)
                        }),
                );
            }
            Node::Group { .. } => {}
            Node::Events { .. } | Node::State { .. } | Node::Consumer { .. } => {
                return Err(Errno::ENOTDIR);
            }
        }
        Ok(children)
    }

    async fn load_next_directory_page(
        &self,
        node: &Node,
        state: &mut DirectoryState,
    ) -> Result<(), Errno> {
        if state.complete && state.initialized {
            return Ok(());
        }

        let children = if matches!(node, Node::States { .. }) {
            let aggregate_type = node_aggregate_type(node)?;
            let request_token = state.next_page_token.clone();
            let page = self
                .backend
                .list_states_page(
                    &aggregate_type,
                    request_token.clone(),
                    STATE_DIRECTORY_PAGE_SIZE,
                )
                .await
                .map_err(backend_errno)?;
            if !page.next_page_token.is_empty() && page.next_page_token == request_token {
                return Err(Errno::EIO);
            }
            state.next_page_token = page.next_page_token;
            state.complete = state.next_page_token.is_empty();
            page.aggregate_ids
                .into_iter()
                .map(|aggregate_id| {
                    let name = format!("{aggregate_id}.json");
                    let child = node.child(&name).map_err(path_errno)?;
                    Ok((name, child, 0))
                })
                .collect::<Result<Vec<_>, Errno>>()?
        } else {
            state.complete = true;
            self.children(node).await?
        };

        state.initialized = true;
        for (name, child, size) in children {
            let kind = if child.is_directory() {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            let ino = self.retain_directory_node(child, size);
            state.retained_inodes.push(ino);
            state.entries.push(DirectoryEntry { name, ino, kind });
        }
        Ok(())
    }

    async fn create_directory(&self, parent: &Node, name: &str) -> Result<Node, Errno> {
        let child = parent.child(name).map_err(path_errno)?;
        match (parent, &child) {
            (Node::Root, Node::BusinessSpace(_)) => {}
            (Node::BusinessSpace(_), Node::AggregateType { .. }) => {
                let aggregate_type = node_aggregate_type(&child)?;
                self.backend
                    .register_aggregate_type(&aggregate_type, Uuid::new_v4())
                    .await
                    .map_err(backend_errno)?;
            }
            (Node::Groups { .. }, Node::Group { group_name, .. }) => {
                let aggregate_type = node_aggregate_type(&child)?;
                self.backend
                    .create_group(&aggregate_type, group_name, Uuid::new_v4())
                    .await
                    .map_err(backend_errno)?;
            }
            _ => return Err(Errno::EPERM),
        }
        Ok(child)
    }

    async fn open_handle(
        &self,
        node: Node,
        flags: OpenFlags,
    ) -> Result<(FileHandle, FopenFlags), Errno> {
        if node.is_directory() {
            return Err(Errno::EISDIR);
        }
        if flags.acc_mode() == OpenAccMode::O_RDWR {
            return Err(Errno::EINVAL);
        }
        let direct = FopenFlags::FOPEN_DIRECT_IO | FopenFlags::FOPEN_NONSEEKABLE;
        match (node, flags.acc_mode()) {
            (node @ Node::Events { .. }, OpenAccMode::O_RDONLY) => {
                let aggregate_type = node_aggregate_type(&node)?;
                let mut receiver = self
                    .backend
                    .follow(&aggregate_type)
                    .await
                    .map_err(backend_errno)?;
                let shared = Arc::new(StreamShared::new());
                let producer = shared.clone();
                self.runtime.spawn(async move {
                    while let Some(frame) = receiver.recv().await {
                        match frame {
                            Ok(bytes) => {
                                if !producer.push(&bytes).await {
                                    return;
                                }
                            }
                            Err(error) => {
                                producer.fail(backend_errno(error));
                                return;
                            }
                        }
                    }
                    producer.close();
                });
                Ok((
                    self.insert_handle(OpenHandle::StreamingRead {
                        shared,
                        consumer: None,
                    }),
                    direct,
                ))
            }
            (node @ Node::Events { .. }, OpenAccMode::O_WRONLY) => Ok((
                self.insert_handle(OpenHandle::EventWrite {
                    aggregate_type: node_aggregate_type(&node)?,
                    buffer: Mutex::new(BufferedWrite::new(self.capabilities.max_event_bytes)),
                }),
                FopenFlags::FOPEN_DIRECT_IO,
            )),
            (
                Node::State {
                    business_space,
                    aggregate_type,
                    aggregate_id,
                },
                OpenAccMode::O_RDONLY,
            ) => {
                let aggregate_type =
                    AggregateType::new(business_space, aggregate_type).map_err(backend_errno)?;
                let state = self
                    .backend
                    .get_state(&aggregate_type, &aggregate_id)
                    .await
                    .map_err(backend_errno)?
                    .ok_or(Errno::ENOENT)?;
                Ok((
                    self.insert_handle(OpenHandle::StaticRead(state.data)),
                    FopenFlags::FOPEN_DIRECT_IO,
                ))
            }
            (
                Node::State {
                    business_space,
                    aggregate_type,
                    aggregate_id,
                },
                OpenAccMode::O_WRONLY,
            ) => {
                let aggregate_type =
                    AggregateType::new(business_space, aggregate_type).map_err(backend_errno)?;
                let revision = self
                    .backend
                    .get_state(&aggregate_type, &aggregate_id)
                    .await
                    .map_err(backend_errno)?
                    .map(|state| state.revision);
                Ok((
                    self.insert_handle(OpenHandle::StateWrite {
                        aggregate_type,
                        aggregate_id,
                        revision,
                        buffer: Mutex::new(BufferedWrite::new(self.capabilities.max_state_bytes)),
                    }),
                    FopenFlags::FOPEN_DIRECT_IO,
                ))
            }
            (
                Node::Consumer {
                    business_space,
                    aggregate_type,
                    group_name,
                    consumer_id,
                },
                OpenAccMode::O_RDONLY,
            ) => {
                let key = ConsumerKey {
                    aggregate_type: AggregateType::new(business_space, aggregate_type)
                        .map_err(backend_errno)?,
                    group_name,
                    consumer_id,
                };
                if !self
                    .active_consumers
                    .lock()
                    .expect("active consumers poisoned")
                    .insert(key.clone())
                {
                    return Err(Errno::EBUSY);
                }
                let shared = Arc::new(StreamShared::new());
                self.spawn_group_reader(key.clone(), shared.clone());
                Ok((
                    self.insert_handle(OpenHandle::StreamingRead {
                        shared,
                        consumer: Some(key),
                    }),
                    direct,
                ))
            }
            (
                Node::Consumer {
                    business_space,
                    aggregate_type,
                    group_name,
                    consumer_id,
                },
                OpenAccMode::O_WRONLY,
            ) => Ok((
                self.insert_handle(OpenHandle::SettlementWrite {
                    aggregate_type: AggregateType::new(business_space, aggregate_type)
                        .map_err(backend_errno)?,
                    group_name,
                    consumer_id,
                    buffer: Mutex::new(BufferedWrite::new(self.capabilities.max_event_bytes)),
                }),
                FopenFlags::FOPEN_DIRECT_IO,
            )),
            _ => Err(Errno::EINVAL),
        }
    }

    fn spawn_group_reader(&self, key: ConsumerKey, shared: Arc<StreamShared>) {
        let backend = self.backend.clone();
        self.runtime.spawn(async move {
            let mut unacked = BTreeMap::new();
            while !shared.closed.load(Ordering::Acquire) {
                let fetch = backend.fetch_group(&key.aggregate_type, &key.group_name, &key.consumer_id);
                let fetch_result = if let Some(delay) = renew_delay(&unacked) {
                    if delay <= Duration::from_millis(1) {
                        None
                    } else {
                        tokio::select! {
                            result = fetch => Some(result),
                            _ = tokio::time::sleep(delay) => None,
                        }
                    }
                } else {
                    Some(fetch.await)
                };
                if fetch_result.is_none() {
                    match backend
                        .renew_group(
                            &key.aggregate_type,
                            &key.group_name,
                            &key.consumer_id,
                            unacked.keys().cloned().collect(),
                        )
                        .await
                    {
                        Ok(response) => {
                            unacked = response
                                .results
                                .into_iter()
                                .filter_map(|result| {
                                    (result.status
                                        == AggregateGroupSettlementStatus::AggregateGroupSettlementApplied
                                            as i32
                                        && result.deadline_ms > 0)
                                        .then_some((result.delivery_id, result.deadline_ms))
                                })
                                .collect();
                        }
                        Err(BackendError::Unavailable(_) | BackendError::Timeout(_)) => {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                        Err(error) => {
                            shared.fail(backend_errno(error));
                            break;
                        }
                    }
                    continue;
                }
                match fetch_result.expect("Fetch 结果已判定存在") {
                    Ok(response) => {
                        let empty = response.deliveries.is_empty();
                        for delivery in response.deliveries {
                            unacked.insert(delivery.delivery_id.clone(), delivery.deadline_ms);
                            match codec::delivery_frame(&delivery) {
                                Ok(frame) => {
                                    if !shared.push(&frame).await {
                                        return;
                                    }
                                }
                                Err(_) => {
                                    shared.fail(Errno::EIO);
                                    return;
                                }
                            }
                        }
                        if empty {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                    Err(BackendError::Unavailable(_) | BackendError::Timeout(_)) => {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    Err(error) => {
                        shared.fail(backend_errno(error));
                        break;
                    }
                }
            }
        });
    }

    async fn commit(&self, handle: &OpenHandle) -> Result<(), Errno> {
        match handle {
            OpenHandle::EventWrite {
                aggregate_type,
                buffer,
            } => {
                let request = {
                    let mut buffer = buffer.lock().expect("event buffer poisoned");
                    buffer
                        .begin(|bytes| codec::parse_event(bytes, self.capabilities.max_event_bytes))
                        .map_err(begin_errno)?
                };
                let Some(request) = request else {
                    return Ok(());
                };
                let result = self.backend.append(aggregate_type, &request).await;
                buffer
                    .lock()
                    .expect("event buffer poisoned")
                    .finish(result.is_ok());
                result.map(|_| ()).map_err(backend_errno)
            }
            OpenHandle::StateWrite {
                aggregate_type,
                aggregate_id,
                revision,
                buffer,
            } => {
                let request = {
                    let mut buffer = buffer.lock().expect("state buffer poisoned");
                    buffer
                        .begin(|bytes| {
                            serde_json::from_slice::<serde_json::Value>(bytes)
                                .map(|_| bytes.to_vec())
                        })
                        .map_err(begin_errno)?
                };
                let Some(request) = request else {
                    return Ok(());
                };
                let result = self
                    .backend
                    .put_state(aggregate_type, aggregate_id, *revision, request)
                    .await;
                buffer
                    .lock()
                    .expect("state buffer poisoned")
                    .finish(result.is_ok());
                result.map(|_| ()).map_err(backend_errno)
            }
            OpenHandle::SettlementWrite {
                aggregate_type,
                group_name,
                consumer_id,
                buffer,
            } => {
                let request = {
                    let mut buffer = buffer.lock().expect("settlement buffer poisoned");
                    buffer
                        .begin(|bytes| {
                            codec::parse_settlements(bytes, self.capabilities.max_event_bytes)
                        })
                        .map_err(begin_errno)?
                };
                let Some(request) = request else {
                    return Ok(());
                };
                let result = self
                    .backend
                    .settle_group(aggregate_type, group_name, consumer_id, &request.settlements)
                    .await
                    .and_then(|response| {
                        if response.results.iter().all(|result| {
                            result.status
                                == AggregateGroupSettlementStatus::AggregateGroupSettlementApplied
                                    as i32
                                || result.status
                                    == AggregateGroupSettlementStatus::AggregateGroupSettlementAlreadySettled
                                        as i32
                        }) {
                            Ok(())
                        } else if response.results.iter().any(|result| {
                            result.status
                                == AggregateGroupSettlementStatus::AggregateGroupSettlementStaleLease
                                    as i32
                        }) {
                            Err(BackendError::Stale("delivery lease 已失效".into()))
                        } else {
                            Err(BackendError::PermissionDenied(
                                "delivery 不属于当前 consumer".into(),
                            ))
                        }
                    });
                buffer
                    .lock()
                    .expect("settlement buffer poisoned")
                    .finish(result.is_ok());
                result.map_err(backend_errno)
            }
            OpenHandle::Directory { .. }
            | OpenHandle::StaticRead(_)
            | OpenHandle::StreamingRead { .. } => Ok(()),
        }
    }
}

impl<B: EventFsBackend> Filesystem for EventFs<B> {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> io::Result<()> {
        config
            .add_capabilities(InitFlags::FUSE_ATOMIC_O_TRUNC)
            .map(|_| ())
            .map_err(|unsupported| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("内核缺少 FUSE_ATOMIC_O_TRUNC: {unsupported:?}"),
                )
            })
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Ok(parent) = self.inner.node(parent) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(name) = name.to_str().map(str::to_string) else {
            reply.error(Errno::EINVAL);
            return;
        };
        let inner = self.inner.clone();
        self.inner.runtime.spawn(async move {
            match inner.lookup_child(&parent, &name).await {
                Ok((node, size, mtime)) => {
                    let ino = inner.remember_node(node.clone(), size);
                    reply.entry(&TTL, &inner.attr(ino, &node, size, mtime), Generation(0));
                }
                Err(error) => reply.error(error),
            }
        });
    }

    fn forget(&self, _req: &Request, ino: INodeNo, nlookup: u64) {
        self.inner
            .inodes
            .lock()
            .expect("inode table poisoned")
            .forget(ino, nlookup);
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let Ok(node) = self.inner.node(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if matches!(&node, Node::State { .. }) {
            let inner = self.inner.clone();
            self.inner.runtime.spawn(async move {
                match inner.state_attributes_for_getattr(&node).await {
                    Ok((size, mtime)) => {
                        inner.ensure_node(node.clone(), size);
                        reply.attr(&TTL, &inner.attr(ino, &node, size, mtime));
                    }
                    Err(error) => reply.error(error),
                }
            });
            return;
        }
        let size = self
            .inner
            .inodes
            .lock()
            .expect("inode table poisoned")
            .size(ino);
        reply.attr(&TTL, &self.inner.attr(ino, &node, size, UNIX_EPOCH));
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let Ok(node) = self.inner.node(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if !node.is_directory() {
            reply.error(Errno::ENOTDIR);
            return;
        }
        let mut state = DirectoryState::new(&node);
        self.inner
            .inodes
            .lock()
            .expect("inode table poisoned")
            .retain_directory(ino);
        state.retained_inodes.push(ino);
        let fh = self.inner.insert_handle(OpenHandle::Directory {
            node,
            state: tokio::sync::Mutex::new(state),
        });
        reply.opened(fh, FopenFlags::empty());
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        let Ok(node) = self.inner.node(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Ok(handle) = self.inner.handle(fh) else {
            reply.error(Errno::EBADF);
            return;
        };
        let OpenHandle::Directory {
            node: handle_node, ..
        } = handle.as_ref()
        else {
            reply.error(Errno::EBADF);
            return;
        };
        if handle_node != &node {
            reply.error(Errno::EBADF);
            return;
        }
        let inner = self.inner.clone();
        self.inner.runtime.spawn(async move {
            let mut reply = reply;
            if offset == 0 && reply.add(ino, 1, FileType::Directory, ".") {
                reply.ok();
                return;
            }
            if offset <= 1 {
                let parent_ino = inner.ensure_node(parent_node(&node), 0);
                if reply.add(parent_ino, 2, FileType::Directory, "..") {
                    reply.ok();
                    return;
                }
            }

            let Ok(child_index) = usize::try_from(offset.saturating_sub(2)) else {
                reply.ok();
                return;
            };
            let OpenHandle::Directory { state, .. } = handle.as_ref() else {
                unreachable!("目录句柄已在调度前校验")
            };
            let mut state = state.lock().await;
            let mut fetched_page = false;
            let mut index = child_index;
            loop {
                if index >= state.entries.len() && !state.complete && !fetched_page {
                    if let Err(error) = inner.load_next_directory_page(&node, &mut state).await {
                        reply.error(error);
                        return;
                    }
                    fetched_page = true;
                }
                let Some(entry) = state.entries.get(index) else {
                    break;
                };
                if reply.add(entry.ino, (index + 3) as u64, entry.kind, &entry.name) {
                    break;
                }
                index += 1;
            }
            reply.ok();
        });
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let Ok(parent_node) = self.inner.node(parent) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(name) = name.to_str().map(str::to_string) else {
            reply.error(Errno::EINVAL);
            return;
        };
        let inner = self.inner.clone();
        self.inner.runtime.spawn(async move {
            match inner.create_directory(&parent_node, &name).await {
                Ok(child) => {
                    let ino = inner.remember_node(child.clone(), 0);
                    reply.entry(&TTL, &inner.attr(ino, &child, 0, UNIX_EPOCH), Generation(0));
                }
                Err(error) => reply.error(error),
            }
        });
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let Ok(node) = self.inner.node(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let inner = self.inner.clone();
        self.inner.runtime.spawn(async move {
            match inner.open_handle(node, flags).await {
                Ok((fh, open_flags)) => reply.opened(fh, open_flags),
                Err(error) => reply.error(error),
            }
        });
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let Ok(parent_node) = self.inner.node(parent) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(name) = name.to_str() else {
            reply.error(Errno::EINVAL);
            return;
        };
        let child = match parent_node.child(name) {
            Ok(child @ (Node::State { .. } | Node::Consumer { .. })) => child,
            Ok(_) => {
                reply.error(Errno::EPERM);
                return;
            }
            Err(error) => {
                reply.error(path_errno(error));
                return;
            }
        };
        let inner = self.inner.clone();
        self.inner.runtime.spawn(async move {
            match inner.open_handle(child.clone(), OpenFlags(flags)).await {
                Ok((fh, open_flags)) => {
                    let ino = inner.remember_node(child.clone(), 0);
                    reply.created(
                        &TTL,
                        &inner.attr(ino, &child, 0, UNIX_EPOCH),
                        Generation(0),
                        fh,
                        open_flags,
                    );
                }
                Err(error) => reply.error(error),
            }
        });
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let handle = match self.inner.handle(fh) {
            Ok(handle) => handle,
            Err(error) => {
                reply.error(error);
                return;
            }
        };
        match handle.as_ref() {
            OpenHandle::StaticRead(data) => {
                let start = (offset as usize).min(data.len());
                let end = start.saturating_add(size as usize).min(data.len());
                reply.data(&data[start..end]);
            }
            OpenHandle::StreamingRead { shared, .. } => {
                let shared = shared.clone();
                self.inner.runtime.spawn(async move {
                    match shared.read(offset, size).await {
                        Ok(data) => reply.data(&data),
                        Err(error) => reply.error(error),
                    }
                });
            }
            _ => reply.error(Errno::EBADF),
        }
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let handle = match self.inner.handle(fh) {
            Ok(handle) => handle,
            Err(error) => {
                reply.error(error);
                return;
            }
        };
        let result = match handle.as_ref() {
            OpenHandle::EventWrite { buffer, .. } => buffer
                .lock()
                .expect("event buffer poisoned")
                .write(offset, data),
            OpenHandle::StateWrite { buffer, .. } => buffer
                .lock()
                .expect("state buffer poisoned")
                .write(offset, data),
            OpenHandle::SettlementWrite { buffer, .. } => buffer
                .lock()
                .expect("settlement buffer poisoned")
                .write(offset, data),
            _ => Err(WriteError::Busy),
        };
        match result {
            Ok(count) => reply.written(count),
            Err(error) => reply.error(write_errno(error)),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        let handle = match self.inner.handle(fh) {
            Ok(handle) => handle,
            Err(error) => {
                reply.error(error);
                return;
            }
        };
        let inner = self.inner.clone();
        self.inner.runtime.spawn(async move {
            match inner.commit(&handle).await {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(error),
            }
        });
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let handle = match self.inner.handle(fh) {
            Ok(handle) => handle,
            Err(error) => {
                reply.error(error);
                return;
            }
        };
        let inner = self.inner.clone();
        self.inner.runtime.spawn(async move {
            match inner.commit(&handle).await {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(error),
            }
        });
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let handle = self
            .inner
            .handles
            .lock()
            .expect("handle table poisoned")
            .remove(&fh.0);
        let Some(handle) = handle else {
            reply.error(Errno::EBADF);
            return;
        };
        if let OpenHandle::StreamingRead { shared, consumer } = handle.as_ref() {
            shared.close();
            if let Some(consumer) = consumer {
                self.inner
                    .active_consumers
                    .lock()
                    .expect("active consumers poisoned")
                    .remove(consumer);
            }
        }
        let inner = self.inner.clone();
        self.inner.runtime.spawn(async move {
            match inner.commit(&handle).await {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(error),
            }
        });
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        let handle = {
            let mut handles = self.inner.handles.lock().expect("handle table poisoned");
            if !matches!(
                handles.get(&fh.0).map(Arc::as_ref),
                Some(OpenHandle::Directory { .. })
            ) {
                reply.error(Errno::EBADF);
                return;
            }
            handles.remove(&fh.0).expect("刚验证存在的目录句柄")
        };
        let inner = self.inner.clone();
        self.inner.runtime.spawn(async move {
            let OpenHandle::Directory { state, .. } = handle.as_ref() else {
                unreachable!("releasedir 仅移除目录句柄")
            };
            let state = state.lock().await;
            inner.release_directory_nodes(&state.retained_inodes);
            reply.ok();
        });
    }

    fn poll(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        notifier: PollNotifier,
        _events: PollEvents,
        flags: PollFlags,
        reply: ReplyPoll,
    ) {
        let Ok(handle) = self.inner.handle(fh) else {
            reply.error(Errno::EBADF);
            return;
        };
        let OpenHandle::StreamingRead { shared, .. } = handle.as_ref() else {
            reply.poll(PollEvents::POLLIN | PollEvents::POLLOUT);
            return;
        };
        let ready = if flags.contains(PollFlags::FUSE_POLL_SCHEDULE_NOTIFY) {
            shared.ready_or_register(|| {
                shared
                    .pollers
                    .lock()
                    .expect("pollers poisoned")
                    .push(notifier);
            })
        } else {
            shared.ready()
        };
        if ready {
            reply.poll(PollEvents::POLLIN);
        } else {
            reply.poll(PollEvents::empty());
        }
    }

    fn unlink(&self, _req: &Request, _parent: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::EPERM);
    }

    fn rmdir(&self, _req: &Request, _parent: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::EPERM);
    }

    fn rename(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _newparent: INodeNo,
        _newname: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        reply.error(Errno::EPERM);
    }
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn renew_delay(unacked: &BTreeMap<Vec<u8>, u64>) -> Option<Duration> {
    let earliest = unacked.values().copied().min()?;
    let remaining = earliest.saturating_sub(unix_millis());
    let delay_ms = remaining.saturating_div(2).clamp(1, 3_000);
    Some(Duration::from_millis(delay_ms))
}

fn node_aggregate_type(node: &Node) -> Result<AggregateType, Errno> {
    let (business_space, aggregate_type) = node.aggregate_type().ok_or(Errno::EINVAL)?;
    AggregateType::new(business_space, aggregate_type).map_err(backend_errno)
}

fn parent_node(node: &Node) -> Node {
    match node {
        Node::Root => Node::Root,
        Node::BusinessSpace(_) => Node::Root,
        Node::AggregateType { business_space, .. } => Node::BusinessSpace(business_space.clone()),
        Node::Events {
            business_space,
            aggregate_type,
        }
        | Node::States {
            business_space,
            aggregate_type,
        }
        | Node::Groups {
            business_space,
            aggregate_type,
        } => Node::AggregateType {
            business_space: business_space.clone(),
            aggregate_type: aggregate_type.clone(),
        },
        Node::State {
            business_space,
            aggregate_type,
            ..
        } => Node::States {
            business_space: business_space.clone(),
            aggregate_type: aggregate_type.clone(),
        },
        Node::Group {
            business_space,
            aggregate_type,
            ..
        } => Node::Groups {
            business_space: business_space.clone(),
            aggregate_type: aggregate_type.clone(),
        },
        Node::Consumer {
            business_space,
            aggregate_type,
            group_name,
            ..
        } => Node::Group {
            business_space: business_space.clone(),
            aggregate_type: aggregate_type.clone(),
            group_name: group_name.clone(),
        },
    }
}

fn backend_errno(error: BackendError) -> Errno {
    match error {
        BackendError::InvalidArgument(_) => Errno::EINVAL,
        BackendError::NotFound(_) => Errno::ENOENT,
        BackendError::AlreadyExists(_) => Errno::EEXIST,
        BackendError::Conflict(_) => Errno::EAGAIN,
        BackendError::TooLarge(_) => Errno::EFBIG,
        BackendError::Stale(_) => Errno::ESTALE,
        BackendError::PermissionDenied(_) => Errno::EACCES,
        BackendError::Timeout(_) => Errno::ETIMEDOUT,
        BackendError::Unavailable(_) => Errno::EHOSTUNREACH,
        BackendError::Busy(_) => Errno::EBUSY,
        BackendError::Unsupported(_) => Errno::ENOTSUP,
        BackendError::Internal(_) => Errno::EIO,
    }
}

fn path_errno(error: PathError) -> Errno {
    match error {
        PathError::NotDirectory => Errno::ENOTDIR,
        PathError::NotFound => Errno::ENOENT,
        PathError::InvalidShape | PathError::InvalidName | PathError::InvalidExtension => {
            Errno::EINVAL
        }
    }
}

fn write_errno(error: WriteError) -> Errno {
    match error {
        WriteError::InvalidOffset => Errno::EINVAL,
        WriteError::TooLarge => Errno::EFBIG,
        WriteError::Busy => Errno::EBUSY,
    }
}

fn begin_errno<E>(error: BeginError<E>) -> Errno {
    match error {
        BeginError::Write(error) => write_errno(error),
        BeginError::Prepare(_) => Errno::EINVAL,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap, VecDeque};

    use async_trait::async_trait;
    use es_proto::eventstore::{
        AggregateEvent, AggregateGroupDelivery, AggregateGroupSettlementResult,
        FetchAggregateGroupResponse, RenewAggregateGroupResponse, SettleAggregateGroupResponse,
    };

    use super::*;
    use crate::backend::{BackendResult, StateDocument, StatePage};
    use crate::codec::Settlement;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct StateWriteCall {
        aggregate_type: AggregateType,
        aggregate_id: String,
        revision: Option<u64>,
        data: Vec<u8>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct SettlementCall {
        aggregate_type: AggregateType,
        group_name: String,
        consumer_id: String,
        settlements: Vec<Settlement>,
    }

    #[derive(Default)]
    struct MockBackend {
        aggregate_types: Mutex<BTreeSet<AggregateType>>,
        groups: Mutex<HashMap<AggregateType, BTreeSet<String>>>,
        states: Mutex<HashMap<(AggregateType, String), StateDocument>>,
        appends: Mutex<Vec<(AggregateType, EventEnvelope)>>,
        state_writes: Mutex<Vec<StateWriteCall>>,
        settlements: Mutex<Vec<SettlementCall>>,
        settlement_status: Mutex<Option<i32>>,
        fetch_results: Mutex<VecDeque<BackendResult<FetchAggregateGroupResponse>>>,
        renew_results: Mutex<VecDeque<BackendResult<RenewAggregateGroupResponse>>>,
        renew_calls: Mutex<usize>,
        fetch_delay: Mutex<Option<Duration>>,
        state_page_calls: Mutex<Vec<(Vec<u8>, u32)>>,
    }

    #[async_trait]
    impl EventFsBackend for MockBackend {
        async fn capabilities(&self) -> BackendResult<Capabilities> {
            Ok(Capabilities {
                max_event_bytes: 4096,
                max_state_bytes: 4096,
            })
        }

        async fn list_aggregate_types(&self) -> BackendResult<Vec<AggregateType>> {
            Ok(self
                .aggregate_types
                .lock()
                .expect("aggregate types poisoned")
                .iter()
                .cloned()
                .collect())
        }

        async fn register_aggregate_type(
            &self,
            aggregate_type: &AggregateType,
            _operation_id: Uuid,
        ) -> BackendResult<()> {
            self.aggregate_types
                .lock()
                .expect("aggregate types poisoned")
                .insert(aggregate_type.clone());
            Ok(())
        }

        async fn append(
            &self,
            aggregate_type: &AggregateType,
            event: &EventEnvelope,
        ) -> BackendResult<u64> {
            let mut appends = self.appends.lock().expect("appends poisoned");
            appends.push((aggregate_type.clone(), event.clone()));
            Ok((appends.len() - 1) as u64)
        }

        async fn follow(
            &self,
            _aggregate_type: &AggregateType,
        ) -> BackendResult<tokio::sync::mpsc::Receiver<BackendResult<Vec<u8>>>> {
            let (_sender, receiver) = tokio::sync::mpsc::channel(1);
            Ok(receiver)
        }

        async fn list_states_page(
            &self,
            aggregate_type: &AggregateType,
            page_token: Vec<u8>,
            page_size: u32,
        ) -> BackendResult<StatePage> {
            self.state_page_calls
                .lock()
                .expect("state page calls poisoned")
                .push((page_token.clone(), page_size));
            let start = if page_token.is_empty() {
                0
            } else {
                usize::try_from(u64::from_be_bytes(page_token.try_into().map_err(|_| {
                    BackendError::InvalidArgument("测试 page token 非法".into())
                })?))
                .map_err(|_| BackendError::InvalidArgument("测试 page token 超限".into()))?
            };
            let mut aggregate_ids = self
                .states
                .lock()
                .expect("states poisoned")
                .keys()
                .filter(|(candidate, _)| candidate == aggregate_type)
                .map(|(_, aggregate_id)| aggregate_id.clone())
                .collect::<Vec<_>>();
            aggregate_ids.sort();
            let end = start
                .saturating_add(page_size.max(1) as usize)
                .min(aggregate_ids.len());
            let page = aggregate_ids.get(start..end).unwrap_or_default().to_vec();
            Ok(StatePage {
                aggregate_ids: page,
                next_page_token: (end < aggregate_ids.len())
                    .then(|| (end as u64).to_be_bytes().to_vec())
                    .unwrap_or_default(),
            })
        }

        async fn get_state(
            &self,
            aggregate_type: &AggregateType,
            aggregate_id: &str,
        ) -> BackendResult<Option<StateDocument>> {
            Ok(self
                .states
                .lock()
                .expect("states poisoned")
                .get(&(aggregate_type.clone(), aggregate_id.into()))
                .cloned())
        }

        async fn put_state(
            &self,
            aggregate_type: &AggregateType,
            aggregate_id: &str,
            revision: Option<u64>,
            data: Vec<u8>,
        ) -> BackendResult<StateDocument> {
            self.state_writes
                .lock()
                .expect("state writes poisoned")
                .push(StateWriteCall {
                    aggregate_type: aggregate_type.clone(),
                    aggregate_id: aggregate_id.into(),
                    revision,
                    data: data.clone(),
                });
            let key = (aggregate_type.clone(), aggregate_id.to_string());
            let mut states = self.states.lock().expect("states poisoned");
            let current = states.get(&key).map(|state| state.revision);
            if current != revision {
                return Err(BackendError::Conflict("state revision 冲突".into()));
            }
            let state = StateDocument {
                revision: current.map_or(0, |value| value + 1),
                data,
                modified_unix_millis: 10_000,
            };
            states.insert(key, state.clone());
            Ok(state)
        }

        async fn list_groups(&self, aggregate_type: &AggregateType) -> BackendResult<Vec<String>> {
            Ok(self
                .groups
                .lock()
                .expect("groups poisoned")
                .get(aggregate_type)
                .into_iter()
                .flatten()
                .cloned()
                .collect())
        }

        async fn create_group(
            &self,
            aggregate_type: &AggregateType,
            group_name: &str,
            _operation_id: Uuid,
        ) -> BackendResult<()> {
            self.groups
                .lock()
                .expect("groups poisoned")
                .entry(aggregate_type.clone())
                .or_default()
                .insert(group_name.into());
            Ok(())
        }

        async fn fetch_group(
            &self,
            _aggregate_type: &AggregateType,
            _group_name: &str,
            _consumer_id: &str,
        ) -> BackendResult<FetchAggregateGroupResponse> {
            if let Some(result) = self
                .fetch_results
                .lock()
                .expect("fetch results poisoned")
                .pop_front()
            {
                return result;
            }
            let delay = *self.fetch_delay.lock().expect("fetch delay poisoned");
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            Ok(FetchAggregateGroupResponse::default())
        }

        async fn settle_group(
            &self,
            aggregate_type: &AggregateType,
            group_name: &str,
            consumer_id: &str,
            settlements: &[Settlement],
        ) -> BackendResult<SettleAggregateGroupResponse> {
            self.settlements
                .lock()
                .expect("settlements poisoned")
                .push(SettlementCall {
                    aggregate_type: aggregate_type.clone(),
                    group_name: group_name.into(),
                    consumer_id: consumer_id.into(),
                    settlements: settlements.to_vec(),
                });
            Ok(SettleAggregateGroupResponse {
                results: settlements
                    .iter()
                    .map(|settlement| AggregateGroupSettlementResult {
                        delivery_id: settlement.delivery_id.clone(),
                        status: self
                            .settlement_status
                            .lock()
                            .expect("settlement status poisoned")
                            .unwrap_or(
                                AggregateGroupSettlementStatus::AggregateGroupSettlementApplied
                                    as i32,
                            ),
                        deadline_ms: 0,
                    })
                    .collect(),
            })
        }

        async fn renew_group(
            &self,
            _aggregate_type: &AggregateType,
            _group_name: &str,
            _consumer_id: &str,
            delivery_ids: Vec<Vec<u8>>,
        ) -> BackendResult<RenewAggregateGroupResponse> {
            *self.renew_calls.lock().expect("renew calls poisoned") += 1;
            self.renew_results
                .lock()
                .expect("renew results poisoned")
                .pop_front()
                .unwrap_or_else(|| {
                    Ok(RenewAggregateGroupResponse {
                        results: delivery_ids
                            .into_iter()
                            .map(|delivery_id| AggregateGroupSettlementResult {
                                delivery_id,
                                status:
                                    AggregateGroupSettlementStatus::AggregateGroupSettlementApplied
                                        as i32,
                                deadline_ms: unix_millis().saturating_add(10_000),
                            })
                            .collect(),
                    })
                })
        }
    }

    fn mock_filesystem() -> (EventFs<MockBackend>, Arc<MockBackend>) {
        let backend = Arc::new(MockBackend::default());
        let filesystem = EventFs::new(
            backend.clone(),
            tokio::runtime::Handle::current(),
            Capabilities {
                max_event_bytes: 4096,
                max_state_bytes: 4096,
            },
            MountIdentity {
                uid: 1000,
                gid: 1000,
            },
        );
        (filesystem, backend)
    }

    fn write_chunks<T: Clone>(buffer: &Mutex<BufferedWrite<T>>, chunks: &[&[u8]]) {
        let mut offset = 0;
        for chunk in chunks {
            buffer
                .lock()
                .expect("write buffer poisoned")
                .write(offset, chunk)
                .unwrap();
            offset += chunk.len() as u64;
        }
    }

    fn write_buffer(handle: &OpenHandle, chunks: &[&[u8]]) {
        match handle {
            OpenHandle::EventWrite { buffer, .. } => write_chunks(buffer, chunks),
            OpenHandle::StateWrite { buffer, .. } => write_chunks(buffer, chunks),
            OpenHandle::SettlementWrite { buffer, .. } => write_chunks(buffer, chunks),
            _ => panic!("handle is not writable"),
        }
    }

    fn group_delivery(id: u8, with_event: bool) -> AggregateGroupDelivery {
        AggregateGroupDelivery {
            delivery_id: vec![id],
            event: with_event.then(|| AggregateEvent {
                aggregate_id: format!("order-{id}"),
                aggregate_version: 0,
                event_id: vec![id; 16],
                event_type: "created".into(),
                data: b"{}".to_vec(),
                metadata: b"{}".to_vec(),
                hlc: None,
            }),
            attempt: 1,
            deadline_ms: unix_millis().saturating_add(1_000),
            replayed: false,
        }
    }

    #[test]
    fn errno_mapping_is_stable() {
        let cases = [
            (BackendError::InvalidArgument("x".into()), Errno::EINVAL),
            (BackendError::NotFound("x".into()), Errno::ENOENT),
            (BackendError::AlreadyExists("x".into()), Errno::EEXIST),
            (BackendError::Conflict("x".into()), Errno::EAGAIN),
            (BackendError::TooLarge("x".into()), Errno::EFBIG),
            (BackendError::Stale("x".into()), Errno::ESTALE),
            (BackendError::PermissionDenied("x".into()), Errno::EACCES),
            (BackendError::Timeout("x".into()), Errno::ETIMEDOUT),
            (BackendError::Unavailable("x".into()), Errno::EHOSTUNREACH),
            (BackendError::Busy("x".into()), Errno::EBUSY),
            (BackendError::Unsupported("x".into()), Errno::ENOTSUP),
            (BackendError::Internal("x".into()), Errno::EIO),
        ];
        for (error, expected) in cases {
            assert_eq!(backend_errno(error), expected);
        }
        assert_eq!(path_errno(PathError::NotDirectory), Errno::ENOTDIR);
        assert_eq!(path_errno(PathError::NotFound), Errno::ENOENT);
        for error in [
            PathError::InvalidShape,
            PathError::InvalidName,
            PathError::InvalidExtension,
        ] {
            assert_eq!(path_errno(error), Errno::EINVAL);
        }
    }

    #[tokio::test]
    async fn mkdir_creates_aggregate_type_and_consumer_group() {
        let (filesystem, backend) = mock_filesystem();
        let business_space = filesystem
            .inner
            .create_directory(&Node::Root, "orders")
            .await
            .unwrap();
        let aggregate_type_node = filesystem
            .inner
            .create_directory(&business_space, "order")
            .await
            .unwrap();
        let groups = aggregate_type_node.child("groups").unwrap();
        filesystem
            .inner
            .create_directory(&groups, "payments")
            .await
            .unwrap();

        let aggregate_type = AggregateType::new("orders", "order").unwrap();
        assert_eq!(
            backend.list_aggregate_types().await.unwrap(),
            vec![aggregate_type.clone()]
        );
        assert_eq!(
            backend.list_groups(&aggregate_type).await.unwrap(),
            vec!["payments"]
        );
        assert_eq!(
            filesystem
                .inner
                .create_directory(&aggregate_type_node, "states")
                .await,
            Err(Errno::EPERM)
        );
    }

    #[tokio::test]
    async fn partial_event_write_commits_once_across_repeated_fsync() {
        let (filesystem, backend) = mock_filesystem();
        let node = Node::parse("/orders/order/events.jsonl").unwrap();
        let (handle_id, _) = filesystem
            .inner
            .open_handle(node, OpenFlags(libc::O_WRONLY))
            .await
            .unwrap();
        let handle = filesystem.inner.handle(handle_id).unwrap();
        write_buffer(
            &handle,
            &[
                br#"{"spec_version":"1.0","aggregate_id":"order-1","event_type":"created","data":{"amount":"#,
                b"100}}",
            ],
        );

        filesystem.inner.commit(&handle).await.unwrap();
        filesystem.inner.commit(&handle).await.unwrap();

        let appends = backend.appends.lock().expect("appends poisoned");
        assert_eq!(appends.len(), 1);
        assert_eq!(appends[0].0, AggregateType::new("orders", "order").unwrap());
        assert_eq!(appends[0].1.aggregate_id, "order-1");
        assert_eq!(appends[0].1.data, br#"{"amount":100}"#);
    }

    #[tokio::test]
    async fn state_write_uses_open_revision_and_retries_conflict() {
        let (filesystem, backend) = mock_filesystem();
        let aggregate_type = AggregateType::new("orders", "order").unwrap();
        let key = (aggregate_type.clone(), "order-1".to_string());
        backend.states.lock().expect("states poisoned").insert(
            key.clone(),
            StateDocument {
                revision: 7,
                data: br#"{"balance":100}"#.to_vec(),
                modified_unix_millis: 7_000,
            },
        );
        let node = Node::parse("/orders/order/states/order-1.json").unwrap();
        let (handle_id, _) = filesystem
            .inner
            .open_handle(node, OpenFlags(libc::O_WRONLY))
            .await
            .unwrap();
        let handle = filesystem.inner.handle(handle_id).unwrap();
        write_buffer(&handle, &[br#"{"balance":"#, b"50}"]);

        backend.states.lock().expect("states poisoned").insert(
            key.clone(),
            StateDocument {
                revision: 8,
                data: br#"{"balance":75}"#.to_vec(),
                modified_unix_millis: 8_000,
            },
        );
        assert_eq!(filesystem.inner.commit(&handle).await, Err(Errno::EAGAIN));
        backend.states.lock().expect("states poisoned").insert(
            key.clone(),
            StateDocument {
                revision: 7,
                data: br#"{"balance":100}"#.to_vec(),
                modified_unix_millis: 7_000,
            },
        );
        filesystem.inner.commit(&handle).await.unwrap();
        filesystem.inner.commit(&handle).await.unwrap();

        let writes = backend.state_writes.lock().expect("state writes poisoned");
        assert_eq!(writes.len(), 2);
        assert!(writes.iter().all(|write| write.revision == Some(7)));
        assert_eq!(writes[0].data, br#"{"balance":50}"#);
        assert_eq!(
            backend.states.lock().expect("states poisoned")[&key].revision,
            8
        );
    }

    #[tokio::test]
    async fn consumer_reader_is_exclusive_but_settlement_writer_is_independent() {
        let (filesystem, backend) = mock_filesystem();
        let node = Node::parse("/orders/order/groups/payments/worker-1.jsonl").unwrap();
        let (reader_id, _) = filesystem
            .inner
            .open_handle(node.clone(), OpenFlags(libc::O_RDONLY))
            .await
            .unwrap();
        assert!(matches!(
            filesystem
                .inner
                .open_handle(node.clone(), OpenFlags(libc::O_RDONLY))
                .await,
            Err(Errno::EBUSY)
        ));
        let (writer_id, _) = filesystem
            .inner
            .open_handle(node, OpenFlags(libc::O_WRONLY))
            .await
            .unwrap();
        let writer = filesystem.inner.handle(writer_id).unwrap();
        write_buffer(
            &writer,
            &[br#"{"settlements":[{"delivery_id":"0a0b","action":"ack"}]}"#],
        );
        filesystem.inner.commit(&writer).await.unwrap();
        filesystem.inner.commit(&writer).await.unwrap();

        let settlements = backend.settlements.lock().expect("settlements poisoned");
        assert_eq!(settlements.len(), 1);
        assert_eq!(settlements[0].group_name, "payments");
        assert_eq!(settlements[0].consumer_id, "worker-1");
        assert_eq!(settlements[0].settlements[0].delivery_id, [0x0a, 0x0b]);
        drop(settlements);
        let reader = filesystem.inner.handle(reader_id).unwrap();
        if let OpenHandle::StreamingRead { shared, .. } = reader.as_ref() {
            shared.close();
        }
    }

    #[tokio::test]
    async fn blocked_stream_read_wakes_when_frame_arrives() {
        let shared = Arc::new(StreamShared::new());
        let reader = {
            let shared = shared.clone();
            tokio::spawn(async move { shared.read(0, 64).await })
        };
        tokio::task::yield_now().await;
        assert!(!reader.is_finished());
        assert!(!shared.ready());

        assert!(shared.push(b"{\"kind\":\"caught_up\"}\n").await);
        let data = tokio::time::timeout(Duration::from_secs(1), reader)
            .await
            .expect("阻塞读未被唤醒")
            .expect("读任务失败")
            .unwrap();
        assert_eq!(data, b"{\"kind\":\"caught_up\"}\n");
        assert!(!shared.ready());
        shared.close();
        assert!(shared.ready());
        assert_eq!(
            shared.read(data.len() as u64, 64).await.unwrap(),
            Vec::<u8>::new()
        );
    }

    #[tokio::test]
    async fn stream_buffer_applies_byte_backpressure_and_close_cancels_producer() {
        let shared = Arc::new(StreamShared::new());
        assert!(shared.push(&vec![b'x'; MAX_STREAM_BUFFER_BYTES]).await);
        let producer = {
            let shared = shared.clone();
            tokio::spawn(async move { shared.push(b"y").await })
        };
        tokio::task::yield_now().await;
        assert!(!producer.is_finished(), "缓冲已满时生产者必须等待");
        assert_eq!(shared.read(0, 1).await.unwrap(), b"x");
        assert!(
            tokio::time::timeout(Duration::from_secs(1), producer)
                .await
                .expect("消费后生产者未恢复")
                .expect("生产任务失败")
        );

        let closing = Arc::new(StreamShared::new());
        assert!(closing.push(&vec![b'x'; MAX_STREAM_BUFFER_BYTES]).await);
        let blocked = {
            let closing = closing.clone();
            tokio::spawn(async move { closing.push(b"z").await })
        };
        tokio::task::yield_now().await;
        closing.close();
        assert!(
            !tokio::time::timeout(Duration::from_secs(1), blocked)
                .await
                .expect("关闭后生产者未退出")
                .expect("生产任务失败")
        );

        let oversized = StreamShared::new();
        assert!(!oversized.push(&vec![0; MAX_STREAM_BUFFER_BYTES + 1]).await);
        assert_eq!(oversized.read(0, 1).await, Err(Errno::EFBIG));
    }

    #[tokio::test]
    async fn inode_attributes_and_parent_relationships_are_stable() {
        let (filesystem, _) = mock_filesystem();
        let mut table = InodeTable::new();
        assert_eq!(table.node(INodeNo::ROOT), Some(Node::Root));
        assert_eq!(table.size(INodeNo::ROOT), 0);

        let space = Node::BusinessSpace("orders".into());
        let ino = table.ensure(space.clone(), 3);
        assert_eq!(table.inode(&space), Some(ino));
        assert_eq!(table.size(ino), 3);
        assert_eq!(table.ensure(space.clone(), 9), ino);
        assert_eq!(table.size(ino), 9);

        let directory_attr = filesystem.inner.attr(ino, &space, 0, UNIX_EPOCH);
        assert_eq!(directory_attr.kind, FileType::Directory);
        assert_eq!((directory_attr.perm, directory_attr.nlink), (0o750, 2));
        let event = Node::parse("/orders/order/events.jsonl").unwrap();
        let file_attr = filesystem.inner.attr(INodeNo(99), &event, 513, UNIX_EPOCH);
        assert_eq!(file_attr.kind, FileType::RegularFile);
        assert_eq!(
            (file_attr.blocks, file_attr.perm, file_attr.nlink),
            (2, 0o640, 1)
        );

        assert_eq!(parent_node(&Node::Root), Node::Root);
        assert_eq!(parent_node(&space), Node::Root);
        assert!(matches!(parent_node(&event), Node::AggregateType { .. }));
        assert!(node_aggregate_type(&Node::Root).is_err());
    }

    #[tokio::test]
    async fn state_directory_pages_incrementally_and_releases_inode_references() {
        let (filesystem, backend) = mock_filesystem();
        let aggregate_type = AggregateType::new("orders", "order").unwrap();
        for index in 0..600 {
            backend.states.lock().expect("states poisoned").insert(
                (aggregate_type.clone(), format!("order-{index:03}")),
                StateDocument {
                    revision: 0,
                    data: b"{}".to_vec(),
                    modified_unix_millis: 1,
                },
            );
        }
        let states = Node::parse("/orders/order/states").unwrap();
        let mut directory = DirectoryState::new(&states);

        filesystem
            .inner
            .load_next_directory_page(&states, &mut directory)
            .await
            .unwrap();
        assert_eq!(directory.entries.len(), 256);
        assert!(!directory.complete);
        assert_eq!(
            backend
                .state_page_calls
                .lock()
                .expect("state page calls poisoned")
                .as_slice(),
            &[(Vec::new(), STATE_DIRECTORY_PAGE_SIZE)]
        );

        let protected = directory.entries[0].ino;
        let protected_node = filesystem.inner.node(protected).unwrap();
        assert_eq!(filesystem.inner.remember_node(protected_node, 0), protected);
        filesystem
            .inner
            .load_next_directory_page(&states, &mut directory)
            .await
            .unwrap();
        filesystem
            .inner
            .load_next_directory_page(&states, &mut directory)
            .await
            .unwrap();
        assert_eq!(directory.entries.len(), 600);
        assert!(directory.complete);
        assert_eq!(
            directory
                .entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            600
        );
        assert_eq!(
            backend
                .state_page_calls
                .lock()
                .expect("state page calls poisoned")
                .len(),
            3
        );

        let unprotected = directory.entries[1].ino;
        filesystem
            .inner
            .release_directory_nodes(&directory.retained_inodes);
        assert_eq!(filesystem.inner.node(unprotected), Err(Errno::ENOENT));
        assert!(filesystem.inner.node(protected).is_ok());
        filesystem
            .inner
            .inodes
            .lock()
            .expect("inode table poisoned")
            .forget(protected, 1);
        assert_eq!(filesystem.inner.node(protected), Err(Errno::ENOENT));
    }

    #[tokio::test]
    async fn lookup_and_children_cover_the_complete_namespace() {
        let (filesystem, backend) = mock_filesystem();
        let aggregate_type = AggregateType::new("orders", "order").unwrap();
        backend
            .aggregate_types
            .lock()
            .expect("aggregate types poisoned")
            .insert(aggregate_type.clone());
        backend
            .groups
            .lock()
            .expect("groups poisoned")
            .entry(aggregate_type.clone())
            .or_default()
            .insert("workers".into());
        backend.states.lock().expect("states poisoned").insert(
            (aggregate_type, "order-1".into()),
            StateDocument {
                revision: 1,
                data: br#"{"balance":50}"#.to_vec(),
                modified_unix_millis: 12_345,
            },
        );

        let inner = &filesystem.inner;
        let (space, _, _) = inner.lookup_child(&Node::Root, "orders").await.unwrap();
        let (aggregate_type_node, _, _) = inner.lookup_child(&space, "order").await.unwrap();
        assert_eq!(
            inner.lookup_child(&space, "missing").await,
            Err(Errno::ENOENT)
        );
        let (_, _, _) = inner
            .lookup_child(&aggregate_type_node, "events.jsonl")
            .await
            .unwrap();
        let states = aggregate_type_node.child("states").unwrap();
        let groups = aggregate_type_node.child("groups").unwrap();
        assert_eq!(
            inner.lookup_child(&states, "order-1.json").await.unwrap().1,
            br#"{"balance":50}"#.len() as u64
        );
        assert_eq!(
            inner.lookup_child(&states, "order-1.json").await.unwrap().2,
            UNIX_EPOCH + Duration::from_millis(12_345)
        );
        assert_eq!(
            inner.lookup_child(&states, "missing.json").await,
            Err(Errno::ENOENT)
        );
        let (group, _, _) = inner.lookup_child(&groups, "workers").await.unwrap();
        assert_eq!(
            inner.lookup_child(&groups, "missing").await,
            Err(Errno::ENOENT)
        );
        assert!(inner.lookup_child(&group, "consumer-a.jsonl").await.is_ok());

        assert_eq!(inner.children(&Node::Root).await.unwrap().len(), 1);
        assert_eq!(inner.children(&space).await.unwrap().len(), 1);
        assert_eq!(inner.children(&aggregate_type_node).await.unwrap().len(), 3);
        assert_eq!(inner.children(&states).await.unwrap().len(), 1);
        assert_eq!(inner.children(&groups).await.unwrap().len(), 1);
        assert!(inner.children(&group).await.unwrap().is_empty());
        assert_eq!(
            inner
                .children(&Node::parse("/orders/order/events.jsonl").unwrap())
                .await,
            Err(Errno::ENOTDIR)
        );
    }

    #[tokio::test]
    async fn state_attributes_refresh_backend_for_getattr() {
        let (filesystem, backend) = mock_filesystem();
        let aggregate_type = AggregateType::new("orders", "order").unwrap();
        let node = Node::parse("/orders/order/states/order-1.json").unwrap();
        assert_eq!(
            filesystem.inner.state_attributes(&Node::Root).await,
            Err(Errno::EINVAL)
        );
        let key = (aggregate_type, "order-1".to_string());
        backend.states.lock().expect("states poisoned").insert(
            key.clone(),
            StateDocument {
                revision: 0,
                data: b"old".to_vec(),
                modified_unix_millis: 1_000,
            },
        );
        assert_eq!(
            filesystem.inner.state_attributes(&node).await.unwrap(),
            (3, UNIX_EPOCH + Duration::from_millis(1_000))
        );

        backend.states.lock().expect("states poisoned").insert(
            key,
            StateDocument {
                revision: 1,
                data: b"newer".to_vec(),
                modified_unix_millis: 2_000,
            },
        );
        assert_eq!(
            filesystem.inner.state_attributes(&node).await.unwrap(),
            (5, UNIX_EPOCH + Duration::from_millis(2_000))
        );
    }

    #[tokio::test]
    async fn new_state_getattr_survives_until_first_write() {
        let (filesystem, _backend) = mock_filesystem();
        let node = Node::parse("/orders/order/states/order-new.json").unwrap();
        assert_eq!(
            filesystem.inner.state_attributes_for_getattr(&node).await,
            Err(Errno::ENOENT)
        );

        let (_handle_id, _) = filesystem
            .inner
            .open_handle(node.clone(), OpenFlags(libc::O_WRONLY))
            .await
            .unwrap();
        assert_eq!(
            filesystem
                .inner
                .state_attributes_for_getattr(&node)
                .await
                .unwrap(),
            (0, UNIX_EPOCH)
        );
    }

    #[tokio::test]
    async fn open_modes_static_reads_and_stream_failures_are_explicit() {
        let (filesystem, backend) = mock_filesystem();
        let inner = &filesystem.inner;
        assert!(matches!(
            inner
                .open_handle(Node::Root, OpenFlags(libc::O_RDONLY))
                .await,
            Err(Errno::EISDIR)
        ));
        let events = Node::parse("/orders/order/events.jsonl").unwrap();
        assert!(matches!(
            inner
                .open_handle(events.clone(), OpenFlags(libc::O_RDWR))
                .await,
            Err(Errno::EINVAL)
        ));
        assert!(matches!(
            inner
                .open_handle(
                    Node::parse("/orders/order/states/missing.json").unwrap(),
                    OpenFlags(libc::O_RDONLY),
                )
                .await,
            Err(Errno::ENOENT)
        ));

        let aggregate_type = AggregateType::new("orders", "order").unwrap();
        backend.states.lock().expect("states poisoned").insert(
            (aggregate_type, "order-1".into()),
            StateDocument {
                revision: 1,
                data: b"state".to_vec(),
                modified_unix_millis: 1_000,
            },
        );
        let (static_id, flags) = inner
            .open_handle(
                Node::parse("/orders/order/states/order-1.json").unwrap(),
                OpenFlags(libc::O_RDONLY),
            )
            .await
            .unwrap();
        assert!(flags.contains(FopenFlags::FOPEN_DIRECT_IO));
        let static_handle = inner.handle(static_id).unwrap();
        let OpenHandle::StaticRead(data) = static_handle.as_ref() else {
            panic!("状态只读打开必须创建静态读句柄");
        };
        assert_eq!(data, b"state");
        assert_eq!(inner.handle(FileHandle(u64::MAX)).err(), Some(Errno::EBADF));

        let (handle_id, flags) = inner
            .open_handle(events, OpenFlags(libc::O_RDONLY))
            .await
            .unwrap();
        assert!(flags.contains(FopenFlags::FOPEN_NONSEEKABLE));
        let streaming_read = inner.handle(handle_id).unwrap();
        let OpenHandle::StreamingRead { shared, .. } = streaming_read.as_ref() else {
            panic!("events 读句柄必须是流");
        };
        tokio::task::yield_now().await;
        assert_eq!(shared.read(0, 8).await.unwrap(), Vec::<u8>::new());

        let failed = StreamShared::new();
        failed.fail(Errno::EHOSTUNREACH);
        assert_eq!(failed.read(0, 8).await, Err(Errno::EHOSTUNREACH));
    }

    #[tokio::test]
    async fn group_reader_retries_transient_fetch_and_reports_fatal_or_invalid_delivery() {
        let (filesystem, backend) = mock_filesystem();
        backend
            .fetch_results
            .lock()
            .expect("fetch results poisoned")
            .extend([
                Ok(FetchAggregateGroupResponse {
                    deliveries: vec![group_delivery(1, true)],
                    caught_up: false,
                    throttled: false,
                }),
                Ok(FetchAggregateGroupResponse::default()),
                Err(BackendError::Unavailable("retry".into())),
                Err(BackendError::InvalidArgument("fatal".into())),
            ]);
        let (handle_id, _) = filesystem
            .inner
            .open_handle(
                Node::parse("/orders/order/groups/workers/consumer-retry.jsonl").unwrap(),
                OpenFlags(libc::O_RDONLY),
            )
            .await
            .unwrap();
        let handle = filesystem.inner.handle(handle_id).unwrap();
        let OpenHandle::StreamingRead { shared, .. } = handle.as_ref() else {
            panic!("消费者读句柄必须是流");
        };
        let frame = shared.read(0, 4096).await.expect("读取 delivery frame");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&frame).unwrap()["kind"],
            "delivery"
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while !shared.closed.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("永久 Fetch 错误应关闭流");
        assert_eq!(shared.read(frame.len() as u64, 1).await, Err(Errno::EINVAL));

        backend
            .fetch_results
            .lock()
            .expect("fetch results poisoned")
            .push_back(Ok(FetchAggregateGroupResponse {
                deliveries: vec![group_delivery(2, false)],
                caught_up: false,
                throttled: false,
            }));
        let (invalid_id, _) = filesystem
            .inner
            .open_handle(
                Node::parse("/orders/order/groups/workers/consumer-invalid.jsonl").unwrap(),
                OpenFlags(libc::O_RDONLY),
            )
            .await
            .unwrap();
        let invalid = filesystem.inner.handle(invalid_id).unwrap();
        let OpenHandle::StreamingRead { shared, .. } = invalid.as_ref() else {
            panic!("消费者读句柄必须是流");
        };
        assert_eq!(shared.read(0, 1).await, Err(Errno::EIO));
    }

    #[tokio::test]
    async fn group_reader_renews_unsettled_deliveries() {
        let (filesystem, backend) = mock_filesystem();
        backend
            .fetch_results
            .lock()
            .expect("fetch results poisoned")
            .push_back(Ok(FetchAggregateGroupResponse {
                deliveries: vec![group_delivery(3, true)],
                caught_up: false,
                throttled: false,
            }));
        let (handle_id, _) = filesystem
            .inner
            .open_handle(
                Node::parse("/orders/order/groups/workers/consumer-renew.jsonl").unwrap(),
                OpenFlags(libc::O_RDONLY),
            )
            .await
            .unwrap();
        let handle = filesystem.inner.handle(handle_id).unwrap();
        let OpenHandle::StreamingRead { shared, .. } = handle.as_ref() else {
            panic!("消费者读句柄必须是流");
        };
        let _ = shared.read(0, 4096).await.expect("读取待续租 delivery");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if *backend.renew_calls.lock().expect("renew calls poisoned") > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("未结算 delivery 应在三秒后续租");
        shared.close();
    }

    #[tokio::test]
    async fn short_lease_renewal_preempts_long_poll_fetch() {
        let (filesystem, backend) = mock_filesystem();
        let mut delivery = group_delivery(4, true);
        delivery.deadline_ms = unix_millis().saturating_add(100);
        backend
            .fetch_results
            .lock()
            .expect("fetch results poisoned")
            .push_back(Ok(FetchAggregateGroupResponse {
                deliveries: vec![delivery],
                caught_up: false,
                throttled: false,
            }));
        *backend.fetch_delay.lock().expect("fetch delay poisoned") = Some(Duration::from_secs(10));
        let (handle_id, _) = filesystem
            .inner
            .open_handle(
                Node::parse("/orders/order/groups/workers/consumer-short.jsonl").unwrap(),
                OpenFlags(libc::O_RDONLY),
            )
            .await
            .unwrap();
        let handle = filesystem.inner.handle(handle_id).unwrap();
        let OpenHandle::StreamingRead { shared, .. } = handle.as_ref() else {
            panic!("消费者读句柄必须是流");
        };
        let _ = shared.read(0, 4096).await.expect("读取短租约 delivery");
        tokio::time::timeout(Duration::from_secs(1), async {
            while *backend.renew_calls.lock().expect("renew calls poisoned") == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Renew timer 未抢占长轮询 Fetch");
        shared.close();
    }

    #[tokio::test]
    async fn commit_rejects_invalid_json_and_maps_settlement_outcomes() {
        let (filesystem, backend) = mock_filesystem();
        let state_node = Node::parse("/orders/order/states/order-1.json").unwrap();
        let (state_id, _) = filesystem
            .inner
            .open_handle(state_node, OpenFlags(libc::O_WRONLY))
            .await
            .unwrap();
        let state = filesystem.inner.handle(state_id).unwrap();
        write_buffer(&state, &[b"{"]);
        assert_eq!(filesystem.inner.commit(&state).await, Err(Errno::EINVAL));

        for (status, expected) in [
            (
                AggregateGroupSettlementStatus::AggregateGroupSettlementAlreadySettled as i32,
                Ok(()),
            ),
            (
                AggregateGroupSettlementStatus::AggregateGroupSettlementStaleLease as i32,
                Err(Errno::ESTALE),
            ),
            (
                AggregateGroupSettlementStatus::AggregateGroupSettlementWrongConsumer as i32,
                Err(Errno::EACCES),
            ),
        ] {
            *backend
                .settlement_status
                .lock()
                .expect("settlement status poisoned") = Some(status);
            let (id, _) = filesystem
                .inner
                .open_handle(
                    Node::parse("/orders/order/groups/workers/consumer-a.jsonl").unwrap(),
                    OpenFlags(libc::O_WRONLY),
                )
                .await
                .unwrap();
            let handle = filesystem.inner.handle(id).unwrap();
            write_buffer(
                &handle,
                &[br#"{"settlements":[{"delivery_id":"0a0b","action":"ack"}]}"#],
            );
            assert_eq!(filesystem.inner.commit(&handle).await, expected);
        }

        assert!(
            filesystem
                .inner
                .commit(&OpenHandle::StaticRead(Vec::new()))
                .await
                .is_ok()
        );
    }
}
