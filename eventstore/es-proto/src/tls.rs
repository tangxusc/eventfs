//! 客户端 TLS 信任策略与端点装配（tonic `Endpoint` 层面）。
//!
//! 仅对 `https://` 端点生效；`http://` 端点保持明文（先查 scheme，零 TLS 成本）。
//! 自签证书场景默认跳过校验；需要严格校验时用指定 CA。
//!
//! rustls 类型（`ServerCertVerifier`）仅在本模块内部使用，不对外暴露——
//! 公共 API 只涉及 tonic 类型，es-raft / es-server / es-client 无需直接依赖 rustls。

use std::sync::Arc;

use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

/// 客户端 TLS 信任策略。
#[derive(Debug, Clone)]
pub enum TlsClientConfig {
    /// 跳过服务端证书校验（自签友好，默认）。
    SkipVerify,
    /// 用指定 CA（PEM，可含多张证书）严格校验对端证书链。
    Ca(Vec<u8>),
}

impl TlsClientConfig {
    /// 把信任策略装配到端点。
    ///
    /// 返回 Err 的场景：CA PEM 解析失败（tonic 在 `ca_certificate` 时解析）。
    pub fn apply(&self, endpoint: Endpoint) -> Result<Endpoint, tonic::transport::Error> {
        match self {
            TlsClientConfig::SkipVerify => endpoint.tls_config_with_verifier(
                ClientTlsConfig::new(),
                Arc::new(NoCertVerify::new()),
            ),
            TlsClientConfig::Ca(pem) => endpoint
                .tls_config(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(pem))),
        }
    }
}

/// 按端点 scheme 装配 TLS：https 应用信任策略（未提供时默认跳过校验）；http 原样返回。
///
/// 所有 https 客户端装配点（Raft RPC 网络、bootstrap 探测、SDK）必须统一走这里——
/// tonic 生成代码的 `connect` 对 https 自动套空 roots，自签场景必然握手失败。
pub fn apply_endpoint_tls(
    endpoint: Endpoint,
    tls: Option<&TlsClientConfig>,
) -> Result<Endpoint, tonic::transport::Error> {
    if endpoint.uri().scheme_str() != Some("https") {
        return Ok(endpoint);
    }
    match tls {
        Some(cfg) => cfg.apply(endpoint),
        None => TlsClientConfig::SkipVerify.apply(endpoint),
    }
}

/// 跳过校验的 rustls verifier（内部实现，不暴露 rustls 类型）。
///
/// `verify_server_cert` 恒通过；tls1.2/1.3 签名校验委托给 provider
/// （`CryptoProvider::get_default()` 回退 ring，与 tonic 建连时的回退链一致，
/// 保证 `supported_verify_schemes` 与握手实际使用的算法一致）。
#[derive(Debug)]
struct NoCertVerify {
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl NoCertVerify {
    fn new() -> Self {
        let provider = rustls::crypto::CryptoProvider::get_default()
            .cloned()
            .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
        Self { provider }
    }
}

impl rustls::client::danger::ServerCertVerifier for NoCertVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tonic::transport::Server;
    use tonic::{Request, Response, Status};

    use crate::eventstore::raft_admin_server::RaftAdmin;
    use crate::eventstore::*;

    /// RaftAdmin stub：GetRaftState 返回默认空状态，其余 unimplemented
    #[derive(Default)]
    struct StubAdmin;

    #[tonic::async_trait]
    impl RaftAdmin for StubAdmin {
        async fn initialize(
            &self,
            _request: Request<InitializeRequest>,
        ) -> Result<Response<InitializeResponse>, Status> {
            Err(Status::unimplemented("stub"))
        }
        async fn add_learner(
            &self,
            _request: Request<AddLearnerRequest>,
        ) -> Result<Response<AddLearnerResponse>, Status> {
            Err(Status::unimplemented("stub"))
        }
        async fn change_membership(
            &self,
            _request: Request<ChangeMembershipRequest>,
        ) -> Result<Response<ChangeMembershipResponse>, Status> {
            Err(Status::unimplemented("stub"))
        }
        async fn get_raft_state(
            &self,
            _request: Request<GetRaftStateRequest>,
        ) -> Result<Response<GetRaftStateResponse>, Status> {
            Ok(Response::new(GetRaftStateResponse::default()))
        }
    }

    /// 生成一张 127.0.0.1 的自签证书，返回 (cert_pem, key_pem)
    fn gen_cert() -> (String, String) {
        let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()])
            .expect("生成自签证书");
        let cert = certified.cert.pem();
        let key = certified.key_pair.serialize_pem();
        (cert, key)
    }

    /// 用给定证书起一个 TLS 的 RaftAdmin 服务，返回 (https 地址, serve 句柄)
    async fn start_tls_admin_server(cert_pem: &str, key_pem: &str) -> (String, tokio::task::JoinHandle<()>) {
        let identity = tonic::transport::Identity::from_pem(cert_pem.as_bytes(), key_pem.as_bytes());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("绑定端口");
        let addr = listener.local_addr().expect("取地址");
        let service = raft_admin_server::RaftAdminServer::new(StubAdmin);

        let handle = tokio::spawn(async move {
            let _ = Server::builder()
                .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
                .expect("TLS 配置")
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await;
        });
        (format!("https://{}", addr), handle)
    }

    /// 用给定信任策略连接 TLS 服务并调用 GetRaftState
    async fn probe_https(
        addr: &str,
        tls: Option<&TlsClientConfig>,
    ) -> Result<(), tonic::transport::Error> {
        let endpoint = Endpoint::from_shared(addr.to_string())?;
        let endpoint = apply_endpoint_tls(endpoint, tls)?;
        let mut client = raft_admin_client::RaftAdminClient::new(endpoint.connect().await?);
        client
            .get_raft_state(GetRaftStateRequest { shard_id: 0 })
            .await
            .expect("RPC 应成功");
        Ok(())
    }

    #[test]
    fn http_端点不受策略影响() {
        let endpoint = Endpoint::from_shared("http://127.0.0.1:50051".to_string()).unwrap();
        let out = apply_endpoint_tls(endpoint, Some(&TlsClientConfig::Ca(vec![b'x'; 8])))
            .expect("http 端点应原样返回");
        assert_eq!(out.uri().scheme_str(), Some("http"));
    }

    #[tokio::test]
    async fn https_skip_verify_握手成功() {
        let (cert, key) = gen_cert();
        let (addr, handle) = start_tls_admin_server(&cert, &key).await;
        probe_https(&addr, Some(&TlsClientConfig::SkipVerify))
            .await
            .expect("跳过校验应握手成功");
        handle.abort();
    }

    #[tokio::test]
    async fn https_无策略默认跳过校验() {
        let (cert, key) = gen_cert();
        let (addr, handle) = start_tls_admin_server(&cert, &key).await;
        probe_https(&addr, None).await.expect("无策略应默认跳过校验");
        handle.abort();
    }

    #[tokio::test]
    async fn https_ca匹配_握手成功() {
        // 服务端与客户端信任同一张自签证书（自签证书自身即 CA）
        let (cert, key) = gen_cert();
        let (addr, handle) = start_tls_admin_server(&cert, &key).await;
        probe_https(&addr, Some(&TlsClientConfig::Ca(cert.into_bytes())))
            .await
            .expect("CA 匹配应握手成功");
        handle.abort();
    }

    #[tokio::test]
    async fn https_ca不匹配_握手失败() {
        // 服务端用一张证书，客户端信任另一张——握手必须失败
        let (cert, key) = gen_cert();
        let (addr, handle) = start_tls_admin_server(&cert, &key).await;
        let (other, _) = gen_cert();
        let err = probe_https(&addr, Some(&TlsClientConfig::Ca(other.into_bytes()))).await;
        assert!(err.is_err(), "CA 不匹配必须握手失败");
        handle.abort();
    }
}
