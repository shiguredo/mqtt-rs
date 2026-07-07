//! MQTT v3.1.1 PUBLISH の単体テスト。

use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v311::publish::Publish;

#[test]
fn qos0_with_packet_id_is_rejected() {
    let publish = Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "a/b".to_string(),
        packet_id: Some(1),
        payload: vec![],
    };
    let mut buf = [0u8; 256];
    assert_eq!(
        publish.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::UnexpectedPacketId,
        })
    );
}

#[test]
fn invalid_qos3_flags_are_rejected() {
    // 予約 QoS 3 (flags 0x06) を持つ PUBLISH。
    let buf = [0x36, 0x07, 0x00, 0x01, 0x41, 0x00, 0x01, 0x00, 0x01];
    assert_eq!(Publish::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn qos1_with_zero_packet_id_is_rejected() {
    let publish = Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "a/b".to_string(),
        packet_id: Some(0),
        payload: vec![],
    };
    let mut buf = [0u8; 256];
    assert_eq!(
        publish.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        })
    );
}

#[test]
fn qos2_with_zero_packet_id_is_rejected() {
    let publish = Publish {
        dup: false,
        qos: QoS::ExactlyOnce,
        retain: false,
        topic: "a/b".to_string(),
        packet_id: Some(0),
        payload: vec![],
    };
    let mut buf = [0u8; 256];
    assert_eq!(
        publish.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        })
    );
}

#[test]
fn wildcard_in_topic_name_is_rejected() {
    // QoS 0、トピック名に '+' を含む PUBLISH。
    let buf = [0x30, 0x06, 0x00, 0x03, 0x61, 0x2B, 0x62, 0x00];
    assert_eq!(Publish::decode(&buf), Err(DecodeError::MalformedPacket));

    // QoS 0、トピック名に '#' を含む PUBLISH。
    let buf = [0x30, 0x06, 0x00, 0x03, 0x61, 0x23, 0x62, 0x00];
    assert_eq!(Publish::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn wildcard_in_topic_name_encode_is_rejected() {
    // エンコード時にもトピック名のワイルドカードを拒否する。
    let publish = Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "a+b".to_string(),
        packet_id: None,
        payload: vec![],
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        publish.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::WildcardInTopicName,
        })
    );

    let publish = Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "a#b".to_string(),
        packet_id: None,
        payload: vec![],
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        publish.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::WildcardInTopicName,
        })
    );
}
