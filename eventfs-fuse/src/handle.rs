//! 打开文件句柄的缓冲与一次性提交状态机。

use std::collections::VecDeque;

/// 可写句柄错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteError {
    InvalidOffset,
    TooLarge,
    Busy,
}

#[derive(Debug)]
enum CommitState<T> {
    Open,
    Committing(T),
    Committed,
}

/// 一个完整 JSON 值的一次性提交缓冲。
#[derive(Debug)]
pub struct BufferedWrite<T> {
    bytes: Vec<u8>,
    max_bytes: usize,
    prepared: Option<T>,
    state: CommitState<T>,
}

impl<T: Clone> BufferedWrite<T> {
    /// 创建空缓冲。
    pub fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::new(),
            max_bytes,
            prepared: None,
            state: CommitState::Open,
        }
    }

    /// 按内核给出的 offset 顺序追加分块。
    ///
    /// # 返回
    /// 成功时返回写入字节数。
    ///
    /// # 错误
    /// 非连续 offset、超限，或已准备/提交后继续写时返回 [`WriteError`]。
    pub fn write(&mut self, offset: u64, data: &[u8]) -> Result<u32, WriteError> {
        if !matches!(self.state, CommitState::Open) || self.prepared.is_some() {
            return Err(WriteError::Busy);
        }
        if offset != self.bytes.len() as u64 {
            return Err(WriteError::InvalidOffset);
        }
        if self.bytes.len().saturating_add(data.len()) > self.max_bytes {
            return Err(WriteError::TooLarge);
        }
        self.bytes.extend_from_slice(data);
        Ok(data.len() as u32)
    }

    /// 准备提交；解析结果会跨失败重试复用。
    ///
    /// # 参数
    /// `prepare` 把完整字节转换为包含稳定幂等身份的请求。
    ///
    /// # 返回
    /// 首次返回 `Some(request)`；已经成功提交时返回 `None`。
    ///
    /// # 错误
    /// 并发提交返回 [`WriteError::Busy`]；解析错误原样返回。
    pub fn begin<E>(
        &mut self,
        prepare: impl FnOnce(&[u8]) -> Result<T, E>,
    ) -> Result<Option<T>, BeginError<E>> {
        match &self.state {
            CommitState::Committed => return Ok(None),
            CommitState::Committing(_) => return Err(BeginError::Write(WriteError::Busy)),
            CommitState::Open => {}
        }
        if self.prepared.is_none() {
            self.prepared = Some(prepare(&self.bytes).map_err(BeginError::Prepare)?);
        }
        let request = self.prepared.as_ref().expect("刚准备的请求").clone();
        self.state = CommitState::Committing(request.clone());
        Ok(Some(request))
    }

    /// 完成一次提交尝试；失败时回到可重试状态。
    pub fn finish(&mut self, success: bool) {
        if matches!(self.state, CommitState::Committing(_)) {
            self.state = if success {
                CommitState::Committed
            } else {
                CommitState::Open
            };
        }
    }

    /// 返回是否已经成功提交。
    pub fn committed(&self) -> bool {
        matches!(self.state, CommitState::Committed)
    }

    /// 返回当前缓冲字节数。
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// 返回缓冲是否为空。
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// 准备提交失败。
#[derive(Debug)]
pub enum BeginError<E> {
    Write(WriteError),
    Prepare(E),
}

/// 非 seek JSONL 读缓冲。
#[derive(Debug, Default)]
pub struct StreamBuffer {
    bytes: VecDeque<u8>,
    consumed: u64,
    closed: bool,
}

impl StreamBuffer {
    /// 追加完整 frame。
    pub fn push(&mut self, frame: &[u8]) {
        if !self.closed {
            self.bytes.extend(frame);
        }
    }

    /// 读取下一段数据。
    ///
    /// # 返回
    /// 有数据时返回 `Some(bytes)`；已关闭且耗尽时返回空 bytes；暂不可读返回 `None`。
    ///
    /// # 错误
    /// offset 不是已消费位置时返回 [`WriteError::InvalidOffset`]。
    pub fn read(&mut self, offset: u64, size: u32) -> Result<Option<Vec<u8>>, WriteError> {
        if offset != self.consumed {
            return Err(WriteError::InvalidOffset);
        }
        if self.bytes.is_empty() {
            return Ok(self.closed.then(Vec::new));
        }
        let count = self.bytes.len().min(size as usize);
        let data = self.bytes.drain(..count).collect::<Vec<_>>();
        self.consumed = self.consumed.saturating_add(data.len() as u64);
        Ok(Some(data))
    }

    /// 标记生产端结束。
    pub fn close(&mut self) {
        self.closed = true;
    }

    /// 返回当前是否可读或已到 EOF。
    pub fn ready(&self) -> bool {
        !self.bytes.is_empty() || self.closed
    }

    /// 返回生产端是否已经关闭。
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// 返回尚未消费的字节数。
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// 返回当前是否没有待读字节。
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_commit_reuses_prepared_identity_and_success_is_idempotent() {
        let mut buffer = BufferedWrite::new(16);
        buffer.write(0, b"ab").unwrap();
        buffer.write(2, b"cd").unwrap();
        let first = buffer
            .begin(|bytes| Ok::<_, ()>((bytes.to_vec(), 7)))
            .unwrap()
            .unwrap();
        buffer.finish(false);
        let second = buffer
            .begin(|_| -> Result<(Vec<u8>, i32), ()> { panic!("失败重试不能重新准备") })
            .unwrap()
            .unwrap();
        assert_eq!(first, second);
        buffer.finish(true);
        assert!(
            buffer
                .begin(|_| -> Result<(Vec<u8>, i32), ()> { panic!("成功后不能再准备") })
                .unwrap()
                .is_none()
        );
        assert_eq!(buffer.write(4, b"x"), Err(WriteError::Busy));
    }

    #[test]
    fn writes_enforce_offset_and_size() {
        let mut buffer = BufferedWrite::<()>::new(3);
        assert_eq!(buffer.write(1, b"a"), Err(WriteError::InvalidOffset));
        buffer.write(0, b"ab").unwrap();
        assert_eq!(buffer.write(2, b"cd"), Err(WriteError::TooLarge));
    }

    #[test]
    fn stream_buffer_is_nonseekable_and_reports_eof() {
        let mut buffer = StreamBuffer::default();
        assert_eq!(buffer.read(0, 2).unwrap(), None);
        buffer.push(b"abc");
        assert_eq!(buffer.read(0, 2).unwrap(), Some(b"ab".to_vec()));
        assert_eq!(buffer.read(0, 2), Err(WriteError::InvalidOffset));
        assert_eq!(buffer.read(2, 2).unwrap(), Some(b"c".to_vec()));
        buffer.close();
        assert_eq!(buffer.read(3, 2).unwrap(), Some(Vec::new()));
    }
}
