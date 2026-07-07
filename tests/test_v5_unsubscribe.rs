//! MQTT v5.0 UNSUBSCRIBE パケットの単体テスト。

use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v5::property::Properties;
use shiguredo_mqtt::v5::unsubscribe::Unsubscribe;

#[test]
fn invalid_flags_are_rejected() {
    let buf = [0xA0, 0x08, 0x00, 0x01, 0x00, 0x00, 0x03, b'a', b'/', b'b'];
    assert_eq!(
        Unsubscribe::decode(&buf),
        Err(DecodeError::InvalidPacketFlags)
    );
}

#[test]
fn zero_packet_id_is_rejected() {
    let unsubscribe = Unsubscribe {
        packet_id: 0,
        topic_filters: vec!["a".to_string()],
        properties: Properties::new(),
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        unsubscribe.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        })
    );
}

#[test]
fn empty_topic_filters_are_rejected() {
    let unsubscribe = Unsubscribe {
        packet_id: 1,
        topic_filters: vec![],
        properties: Properties::new(),
    };
    let mut buf = [0u8; 16];
    assert_eq!(
        unsubscribe.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::EmptyTopicFilters,
        })
    );
}

#[test]
fn extra_bytes_after_unsubscribe_are_rejected() {
    // 残り長さを偽装してトピックフィルタの後に余計なバイトを追加する。
    let buf = [
        0xA2, 0x09, 0x00, 0x01, 0x00, 0x00, 0x03, b'a', b'/', b'b', 0x00,
    ];
    assert_eq!(Unsubscribe::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn user_property_duplicates_are_allowed_for_unsubscribe_on_encode() {
    // UNSUBSCRIBE のプロパティでは User Property (0x26) の重複が許可される。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::UserProperty(
        "a".to_string(),
        "1".to_string(),
    ));
    properties.push(shiguredo_mqtt::v5::property::Property::UserProperty(
        "b".to_string(),
        "2".to_string(),
    ));
    let unsubscribe = Unsubscribe {
        packet_id: 1,
        topic_filters: vec!["a".to_string()],
        properties,
    };
    let mut buf = [0u8; 64];
    let len = unsubscribe
        .encode(&mut buf)
        .expect("エンコードに成功すること");
    let (decoded, consumed) = Unsubscribe::decode(&buf[..len]).expect("デコードに成功すること");
    assert_eq!(decoded, unsubscribe);
    assert_eq!(consumed, len);
}
