//! 端点地址归一化（客户端侧公共规则）。

/// BasicNode.addr 可能不带 scheme，补上 http:// 才是合法的 tonic endpoint。
///
/// 写入 membership 的地址与网络层回连必须遵循同一规则（单一来源），
/// es-raft 网络层、es-server bootstrap 探测、es-ctl 客户端共用此处。
pub fn normalize_endpoint(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{}", addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 裸地址补http前缀() {
        assert_eq!(
            normalize_endpoint("127.0.0.1:50051"),
            "http://127.0.0.1:50051"
        );
    }

    #[test]
    fn 带scheme原样返回() {
        assert_eq!(
            normalize_endpoint("http://127.0.0.1:50051"),
            "http://127.0.0.1:50051"
        );
        assert_eq!(
            normalize_endpoint("https://127.0.0.1:50051"),
            "https://127.0.0.1:50051"
        );
    }
}
