//! MQTT v3.1.1 UNSUBSCRIBE の単体テスト。

use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v311::unsubscribe::Unsubscribe;

#[test]
fn zero_packet_id_is_rejected() {
    let unsubscribe = Unsubscribe {
        packet_id: 0,
        topic_filters: vec!["a/b".to_string()],
    };
    let mut buf = [0u8; 256];
    assert_eq!(
        unsubscribe.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        })
    );

    // パケット識別子 0 を含むバイト列を直接デコードすると拒否される。
    let buf = [0xA2, 0x07, 0x00, 0x00, 0x00, 0x03, b'a', b'/', b'b'];
    assert_eq!(Unsubscribe::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn empty_topic_filters_are_rejected() {
    let unsubscribe = Unsubscribe {
        packet_id: 1,
        topic_filters: vec![],
    };
    let mut buf = [0u8; 256];
    assert_eq!(
        unsubscribe.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::EmptyTopicFilters,
        })
    );
}

#[test]
fn incorrect_flags_are_rejected() {
    let unsubscribe = Unsubscribe {
        packet_id: 1,
        topic_filters: vec!["a/b".to_string()],
    };
    let mut buf = [0u8; 256];
    let len = unsubscribe
        .encode(&mut buf)
        .expect("エンコードに成功すること");
    buf[0] = 0xA0;
    assert_eq!(
        Unsubscribe::decode(&buf[..len]),
        Err(DecodeError::InvalidPacketFlags)
    );
}
