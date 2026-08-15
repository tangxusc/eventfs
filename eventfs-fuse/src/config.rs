//! eventfs-fuse 挂载配置。

use std::path::PathBuf;

use serde::Deserialize;

/// 默认单个写句柄上限。
pub const DEFAULT_WRITE_BYTES: usize = 1024 * 1024;
/// 即使服务端允许更大 payload，FUSE 首期也不超过 6 MiB。
pub const HARD_WRITE_BYTES: usize = 6 * 1024 * 1024;

/// TOML 配置文件。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MountConfig {
    /// AggregateStore 公共端点。
    pub endpoints: Vec<String>,
    /// 可选 PEM CA；与 `insecure_skip_tls_verify` 互斥。
    pub ca_file: Option<PathBuf>,
    /// 仅用于明确接受自签名服务端的环境。
    #[serde(default)]
    pub insecure_skip_tls_verify: bool,
    /// 事件 envelope 本地上限；还会与服务端限制取最小值。
    #[serde(default = "default_write_bytes")]
    pub max_event_bytes: usize,
    /// 状态 JSON 本地上限；还会与服务端限制取最小值。
    #[serde(default = "default_write_bytes")]
    pub max_state_bytes: usize,
}

fn default_write_bytes() -> usize {
    DEFAULT_WRITE_BYTES
}

impl MountConfig {
    /// 读取并校验 TOML 配置。
    ///
    /// # 参数
    /// `path` 是 UTF-8 TOML 文件路径。
    ///
    /// # 返回
    /// 返回已通过静态边界校验的配置。
    ///
    /// # 错误
    /// 文件不可读、TOML 非法、端点为空、TLS 冲突或上限越界时返回错误。
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| anyhow::anyhow!("读取配置 {} 失败: {error}", path.display()))?;
        let value: Self = toml::from_str(&text)
            .map_err(|error| anyhow::anyhow!("解析配置 {} 失败: {error}", path.display()))?;
        value.validate()?;
        Ok(value)
    }

    /// 校验不需要联网的配置约束。
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.endpoints.is_empty(), "endpoints 至少需要一个地址");
        anyhow::ensure!(
            !(self.ca_file.is_some() && self.insecure_skip_tls_verify),
            "ca_file 与 insecure_skip_tls_verify 互斥"
        );
        for (name, value) in [
            ("max_event_bytes", self.max_event_bytes),
            ("max_state_bytes", self.max_state_bytes),
        ] {
            anyhow::ensure!(value > 0, "{name} 必须大于 0");
            anyhow::ensure!(value <= HARD_WRITE_BYTES, "{name} 不能超过 6 MiB");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_rejects_unknown_conflicting_and_oversized_values() {
        assert!(toml::from_str::<MountConfig>("endpoints=[]\nunknown=1").is_err());
        let mut value: MountConfig = toml::from_str(
            "endpoints=['http://127.0.0.1:50051']\nca_file='ca.pem'\ninsecure_skip_tls_verify=true",
        )
        .unwrap();
        assert!(value.validate().is_err());
        value.ca_file = None;
        value.max_event_bytes = HARD_WRITE_BYTES + 1;
        assert!(value.validate().is_err());
    }

    #[test]
    fn defaults_are_one_mebibyte() {
        let value: MountConfig = toml::from_str("endpoints=['http://127.0.0.1:50051']").unwrap();
        value.validate().unwrap();
        assert_eq!(value.max_event_bytes, DEFAULT_WRITE_BYTES);
        assert_eq!(value.max_state_bytes, DEFAULT_WRITE_BYTES);
    }
}
