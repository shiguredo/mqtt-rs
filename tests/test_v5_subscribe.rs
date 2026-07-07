//! MQTT v5.0 SUBSCRIBE パケットの単体テスト。

use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::codec::variable_byte_integer::VariableByteInteger;
use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v5::property::Properties;
use shiguredo_mqtt::v5::subscribe::{RetainHandling, Subscribe, Subscription};

#[test]
fn invalid_flags_are_rejected() {
    let buf = [
        0x80, 0x09, 0x00, 0x01, 0x00, 0x00, 0x03, b'a', b'/', b'b', 0x01,
    ];
    assert_eq!(
        Subscribe::decode(&buf),
        Err(DecodeError::InvalidPacketFlags)
    );
}

#[test]
fn zero_packet_id_is_rejected() {
    let subscribe = Subscribe {
        packet_id: 0,
        subscriptions: vec![Subscription {
            topic_filter: "a".to_string(),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::SendRetained,
        }],
        properties: Properties::new(),
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        subscribe.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        })
    );
}

#[test]
fn empty_subscriptions_are_rejected() {
    let subscribe = Subscribe {
        packet_id: 1,
        subscriptions: vec![],
        properties: Properties::new(),
    };
    let mut buf = [0u8; 16];
    assert_eq!(
        subscribe.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::EmptySubscriptions,
        })
    );
}

#[test]
fn invalid_subscription_options_are_rejected() {
    // オプションバイト内の retain_handling が 3。
    let buf = [
        0x82, 0x09, 0x00, 0x01, 0x00, 0x00, 0x03, b'a', b'/', b'b', 0x31,
    ];
    assert_eq!(Subscribe::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn reserved_options_bits_are_rejected() {
    let buf = [
        0x82, 0x09, 0x00, 0x01, 0x00, 0x00, 0x03, b'a', b'/', b'b', 0xC1,
    ];
    assert_eq!(Subscribe::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn extra_bytes_after_subscribe_are_rejected() {
    // 残り長さを偽装してサブスクリプションの後に余計なバイトを追加する。
    // フレームは完結しているため、余剰バイトは追加入力では解決しない破損として扱う。
    let buf = [
        0x82, 0x0A, 0x00, 0x01, 0x00, 0x00, 0x03, b'a', b'/', b'b', 0x01, 0x00,
    ];
    assert_eq!(Subscribe::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn invalid_topic_filter_with_partial_wildcard_is_rejected() {
    // Topic Filter に a+ のような部分レベルのワイルドカードを含む。
    let subscribe = Subscribe {
        packet_id: 1,
        subscriptions: vec![Subscription {
            topic_filter: "a+".to_string(),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::SendRetained,
        }],
        properties: Properties::new(),
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        subscribe.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::InvalidTopicFilter,
        })
    );
}

#[test]
fn invalid_topic_filter_with_hash_not_at_end_is_rejected() {
    let subscribe = Subscribe {
        packet_id: 1,
        subscriptions: vec![Subscription {
            topic_filter: "#/a".to_string(),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::SendRetained,
        }],
        properties: Properties::new(),
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        subscribe.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::InvalidTopicFilter,
        })
    );
}

#[test]
fn shared_subscription_with_no_local_is_rejected_on_encode() {
    let subscribe = Subscribe {
        packet_id: 1,
        subscriptions: vec![Subscription {
            topic_filter: "$share/group/a".to_string(),
            qos: QoS::AtMostOnce,
            no_local: true,
            retain_as_published: false,
            retain_handling: RetainHandling::SendRetained,
        }],
        properties: Properties::new(),
    };
    let mut buf = [0u8; 64];
    assert_eq!(
        subscribe.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::SharedSubscriptionNoLocal,
        })
    );
}

#[test]
fn shared_subscription_with_no_local_is_rejected_on_decode() {
    // packet_id=1, properties length=0, topic_filter="$share/group/a", options=no_local|QoS0=0x04
    let buf = [
        0x82, 0x14, 0x00, 0x01, 0x00, 0x00, 0x0E, b'$', b's', b'h', b'a', b'r', b'e', b'/', b'g',
        b'r', b'o', b'u', b'p', b'/', b'a', 0x04,
    ];
    assert_eq!(Subscribe::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn duplicate_subscribe_property_identifiers_are_rejected_on_encode() {
    // SUBSCRIBE のプロパティに Subscription Identifier (0x0B) が重複している場合、エンコードを拒否する。
    let mut properties = Properties::new();
    properties.push(
        shiguredo_mqtt::v5::property::Property::SubscriptionIdentifier(VariableByteInteger(1)),
    );
    properties.push(
        shiguredo_mqtt::v5::property::Property::SubscriptionIdentifier(VariableByteInteger(2)),
    );
    let subscribe = Subscribe {
        packet_id: 1,
        subscriptions: vec![Subscription {
            topic_filter: "a".to_string(),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::SendRetained,
        }],
        properties,
    };
    let mut buf = [0u8; 64];
    assert_eq!(
        subscribe.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::DuplicatePropertyIdentifier,
        })
    );
}
