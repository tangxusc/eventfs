use es_client::ClientError;
use eventfs_fuse::backend::BackendError;
use eventfs_fuse::codec::{
    AggregateVersionExpectation, CodecError, SettlementAction, parse_event, parse_settlements,
};
use eventfs_fuse::handle::{BeginError, BufferedWrite, StreamBuffer, WriteError};
use eventfs_fuse::path::{Node, PathError};
use tonic::Code;

#[test]
fn production_codec_rejects_every_ambiguous_input_boundary() {
    let valid = br#"{
        "spec_version":"1.0",
        "aggregate_id":"order-1",
        "event_type":"paid",
        "data":{"amount":50},
        "metadata":{}
    }"#;
    assert_eq!(
        parse_event(valid, valid.len()).unwrap().expected_version,
        AggregateVersionExpectation::Any
    );

    for input in [
        br#"{"spec_version":"2.0","aggregate_id":"order-1","event_type":"paid","data":{}}"#.as_slice(),
        br#"{"spec_version":"1.0","aggregate_id":"order-1","event_type":"","data":{}}"#.as_slice(),
        br#"{"spec_version":"1.0","aggregate_id":"order-1","event_type":"paid","data":{},"metadata":[]}"#.as_slice(),
        br#"{"spec_version":"1.0","aggregate_id":"bad/path","event_type":"paid","data":{}}"#.as_slice(),
    ] {
        assert!(parse_event(input, 1024).is_err());
    }
    assert!(matches!(
        parse_event(valid, valid.len() - 1),
        Err(CodecError::TooLarge)
    ));

    let settlement = parse_settlements(
        br#"{"settlements":[{"delivery_id":"00ff","action":"retry","reason":"later"}]}"#,
        1024,
    )
    .unwrap();
    assert_eq!(settlement.settlements[0].action, SettlementAction::Retry);
    for input in [
        br#"{"settlements":[]}"#.as_slice(),
        br#"{"settlements":[{"delivery_id":"","action":"ack"}]}"#.as_slice(),
        br#"{"settlements":[{"delivery_id":"0","action":"ack"}]}"#.as_slice(),
        br#"{"settlements":[{"delivery_id":"zz","action":"ack"}]}"#.as_slice(),
    ] {
        assert!(parse_settlements(input, 1024).is_err());
    }
}

#[test]
fn production_path_parser_covers_each_shape_and_name_guard() {
    assert_eq!(Node::parse("/").unwrap(), Node::Root);
    assert!(matches!(
        Node::parse("/orders/order/groups/workers/consumer-a.jsonl").unwrap(),
        Node::Consumer { .. }
    ));

    for path in [
        "orders",
        "/orders/",
        "/orders//order",
        "/orders/order/states/.json",
        "/orders/order/groups/workers/.jsonl",
    ] {
        assert!(Node::parse(path).is_err(), "必须拒绝 {path}");
    }
    for name in ["", ".", "..", "a/b", "_leading"] {
        assert!(Node::Root.child(name).is_err(), "必须拒绝 {name:?}");
    }
    assert_eq!(
        Node::parse("/orders/order/events.jsonl")
            .unwrap()
            .child("more"),
        Err(PathError::NotDirectory)
    );
}

#[test]
fn production_write_state_machine_covers_retry_busy_and_eof_branches() {
    let mut write = BufferedWrite::new(4);
    write.finish(true);
    assert!(write.is_empty());
    assert_eq!(write.write(1, b"a"), Err(WriteError::InvalidOffset));
    assert_eq!(write.write(0, b"ab").unwrap(), 2);
    assert_eq!(write.write(2, b"cde"), Err(WriteError::TooLarge));

    let prepared = write
        .begin(|bytes| Ok::<_, ()>(bytes.to_vec()))
        .unwrap()
        .unwrap();
    assert_eq!(prepared, b"ab");
    assert!(matches!(
        write.begin(|_| Ok::<_, ()>(Vec::new())),
        Err(BeginError::Write(WriteError::Busy))
    ));
    write.finish(false);
    assert_eq!(
        write
            .begin(|_| -> Result<Vec<u8>, ()> { panic!("失败重试必须复用 prepared") })
            .unwrap()
            .unwrap(),
        b"ab"
    );
    write.finish(true);
    assert!(write.committed());
    assert!(write.begin(|_| Ok::<_, ()>(Vec::new())).unwrap().is_none());
    assert_eq!(write.write(2, b"x"), Err(WriteError::Busy));

    let mut stream = StreamBuffer::default();
    assert!(!stream.ready());
    assert_eq!(stream.read(0, 2).unwrap(), None);
    stream.push(b"abc");
    assert!(stream.ready());
    assert_eq!(stream.read(0, 2).unwrap(), Some(b"ab".to_vec()));
    assert_eq!(stream.read(0, 1), Err(WriteError::InvalidOffset));
    assert_eq!(stream.read(2, 2).unwrap(), Some(b"c".to_vec()));
    assert!(!stream.ready());
    stream.close();
    stream.push(b"ignored");
    assert!(stream.ready());
    assert_eq!(stream.read(3, 2).unwrap(), Some(Vec::new()));
}

#[test]
fn production_backend_error_mapping_preserves_retry_categories() {
    let rpc = |code, message: &str| ClientError::RpcFailed {
        code,
        message: message.into(),
    };
    for (error, expected) in [
        (
            rpc(Code::FailedPrecondition, "payload exceeds limit"),
            "too-large",
        ),
        (rpc(Code::FailedPrecondition, "stale lease"), "stale"),
        (rpc(Code::FailedPrecondition, "occ conflict"), "conflict"),
        (rpc(Code::Unavailable, "offline"), "unavailable"),
    ] {
        let actual = match BackendError::from(error) {
            BackendError::TooLarge(_) => "too-large",
            BackendError::Stale(_) => "stale",
            BackendError::Conflict(_) => "conflict",
            BackendError::Unavailable(_) => "unavailable",
            other => panic!("意外错误分类: {other}"),
        };
        assert_eq!(actual, expected);
    }
}
