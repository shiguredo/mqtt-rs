//! MQTT v3.1.1 SUBSCRIBE の単体テスト。

use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v311::subscribe::{Subscribe, Subscription};

#[test]
fn zero_packet_id_is_rejected() {
    let subscribe = Subscribe {
        packet_id: 0,
        topic_filters: vec![Subscription {
            topic_filter: "a/b".to_string(),
            qos: QoS::AtMostOnce,
        }],
    };
    let mut buf = [0u8; 256];
    assert_eq!(
        subscribe.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        })
    );

    // パケット識別子 0 を含むバイト列を直接デコードすると拒否される。
    let buf = [0x82, 0x08, 0x00, 0x00, 0x00, 0x03, b'a', b'/', b'b', 0x00];
    assert_eq!(Subscribe::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn empty_subscriptions_are_rejected() {
    let subscribe = Subscribe {
        packet_id: 1,
        topic_filters: vec![],
    };
    let mut buf = [0u8; 256];
    assert_eq!(
        subscribe.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::EmptyTopicFilters,
        })
    );
}

#[test]
fn incorrect_flags_are_rejected() {
    let subscribe = Subscribe {
        packet_id: 1,
        topic_filters: vec![Subscription {
            topic_filter: "a/b".to_string(),
            qos: QoS::AtMostOnce,
        }],
    };
    let mut buf = [0u8; 256];
    let len = subscribe
        .encode(&mut buf)
        .expect("エンコードに成功すること");
    buf[0] = 0x80;
    assert_eq!(
        Subscribe::decode(&buf[..len]),
        Err(DecodeError::InvalidPacketFlags)
    );
}
