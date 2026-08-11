//! 构建期 protobuf codegen。
//!
//! tonic 0.14 将 prost codec 拆为独立 crate，构建期需使用 `tonic-prost-build`
//! 而非旧版的 `tonic-build`（详见 docs/design.md 2.2）。

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = ["proto/eventstore.proto", "proto/raft.proto"];

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&protos, &["proto"])?;

    // proto 变更时触发重新生成，否则 cargo 会命中缓存导致改动不生效
    for p in protos {
        println!("cargo:rerun-if-changed={p}");
    }
    Ok(())
}
