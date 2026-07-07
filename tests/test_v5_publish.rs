//! MQTT v5.0 PUBLISH パケットの単体テスト。

use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::codec::variable_byte_integer::VariableByteInteger;
use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v5::property::Properties;
use shiguredo_mqtt::v5::publish::Publish;

#[test]
fn invalid_qos_is_rejected() {
    // MQTT v5.0 §3.3.1.2 [MQTT-3.3.1-4]: タイプ 3 で QoS 3（フラグ 0x06）。
    let buf = [
        0x36, 0x0D, 0x00, 0x05, b'h', b'e', b'l', b'l', b'o', 0x00, 0x01, 0x00, 0x01, 0x02,
    ];
    assert_eq!(Publish::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn dup_with_qos0_is_rejected() {
    let publish = Publish {
        dup: true,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "t".to_string(),
        packet_id: None,
        properties: Properties::new(),
        payload: vec![],
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        publish.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::DupWithQos0,
        })
    );
}

#[test]
fn payload_format_indicator_1_with_invalid_utf8_is_rejected() {
    // MQTT v5.0 §3.3.2.3.2 (Payload Format Indicator):
    // Payload Format Indicator が 1 の場合、Payload は well-formed UTF-8
    // でなければならない。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::PayloadFormatIndicator(1));
    let publish = Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "a/b".to_string(),
        packet_id: None,
        properties,
        // 0xFF 単独は不正な UTF-8 シーケンスである。
        payload: vec![0xFF],
    };
    let mut buf = [0u8; 64];
    assert_eq!(
        publish.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::InvalidPayloadUtf8,
        })
    );
}

#[test]
fn payload_format_indicator_1_with_valid_utf8_is_accepted() {
    // Payload Format Indicator が 1 でも well-formed UTF-8 なら受理される。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::PayloadFormatIndicator(1));
    let publish = Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "a/b".to_string(),
        packet_id: None,
        properties,
        payload: "こんにちは".as_bytes().to_vec(),
    };
    let mut buf = [0u8; 64];
    assert!(publish.encode(&mut buf).is_ok());
}

#[test]
fn payload_format_indicator_0_allows_arbitrary_payload() {
    // MQTT v5.0 §3.3.2.3.2: Payload Format Indicator が 0（未指定バイト）の
    // 場合、Payload は任意のバイト列でよい。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::PayloadFormatIndicator(0));
    let publish = Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "a/b".to_string(),
        packet_id: None,
        properties,
        payload: vec![0xFF, 0xFE],
    };
    let mut buf = [0u8; 64];
    assert!(publish.encode(&mut buf).is_ok());
}

#[test]
fn qos0_with_packet_id_is_rejected() {
    // MQTT v5.0 §3.3.2.2:
    // QoS 0 の PUBLISH には Packet Identifier が存在しない。
    let publish = Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "a/b".to_string(),
        packet_id: Some(1),
        properties: Properties::new(),
        payload: vec![],
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        publish.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::UnexpectedPacketId,
        })
    );
}

#[test]
fn packet_id_required_for_qos1_is_rejected() {
    let publish = Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "t".to_string(),
        packet_id: None,
        properties: Properties::new(),
        payload: vec![],
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        publish.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::MissingPacketId,
        })
    );
}

#[test]
fn zero_packet_id_is_rejected() {
    let publish = Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "t".to_string(),
        packet_id: Some(0),
        properties: Properties::new(),
        payload: vec![],
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        publish.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        })
    );
}

#[test]
fn truncated_packet_is_rejected() {
    let buf = [0x30, 0x10, 0x00, 0x05, b'h'];
    assert_eq!(Publish::decode(&buf), Err(DecodeError::InsufficientData));
}

#[test]
fn zero_packet_id_in_decode_is_rejected() {
    // QoS 1、パケット識別子 0。
    let buf = [
        0x32, 0x0B, 0x00, 0x05, b'h', b'e', b'l', b'l', b'o', 0x00, 0x00, 0x00, b'x',
    ];
    assert_eq!(Publish::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn wildcard_in_topic_name_is_rejected() {
    // Topic Name に + を含む。
    let buf = [0x30, 0x07, 0x00, 0x03, b'a', b'+', b'b', 0x00, b'x'];
    assert_eq!(Publish::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn wildcard_in_topic_name_encode_is_rejected() {
    // エンコード時にも Topic Name のワイルドカードを拒否する。
    let publish = Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "a+b".to_string(),
        packet_id: None,
        properties: Properties::new(),
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

#[test]
fn wildcard_in_response_topic_is_rejected() {
    // Response Topic プロパティに # を含む。
    let buf = [
        0x30, 0x0C, 0x00, 0x01, b't', 0x07, 0x08, 0x00, 0x04, b'r', b'e', b'/', b'#', b'x',
    ];
    assert_eq!(Publish::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn wildcard_in_response_topic_encode_is_rejected() {
    // エンコード時にも Response Topic プロパティのワイルドカードを拒否する。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::ResponseTopic(
        "re/#".to_string(),
    ));
    let publish = Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "t".to_string(),
        packet_id: None,
        properties,
        payload: vec![],
    };
    let mut buf = [0u8; 64];
    assert_eq!(
        publish.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::WildcardInTopicName,
        })
    );
}

#[test]
fn extra_bytes_after_publish_are_rejected() {
    // プロパティ長を偽装して余計なバイトを追加する。
    let buf = [0x30, 0x06, 0x00, 0x01, b't', 0x01, 0xFF, b'x'];
    assert_eq!(Publish::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn duplicate_publish_property_identifiers_are_rejected_on_encode() {
    // PUBLISH のプロパティに Content Type (0x03) が重複している場合、エンコードを拒否する。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::ContentType(
        "a".to_string(),
    ));
    properties.push(shiguredo_mqtt::v5::property::Property::ContentType(
        "b".to_string(),
    ));
    let publish = Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "t".to_string(),
        packet_id: None,
        properties,
        payload: vec![],
    };
    let mut buf = [0u8; 64];
    assert_eq!(
        publish.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::DuplicatePropertyIdentifier,
        })
    );
}

#[test]
fn subscription_identifier_in_client_to_server_publish_is_rejected_on_encode() {
    // Client → Server 方向の PUBLISH に Subscription Identifier (0x0B) を含めることは
    // MQTT v5.0 §3.3.4 [MQTT-3.3.4-6] で禁止されている。
    let mut properties = Properties::new();
    properties.push(
        shiguredo_mqtt::v5::property::Property::SubscriptionIdentifier(VariableByteInteger(1)),
    );
    let publish = Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "t".to_string(),
        packet_id: None,
        properties,
        payload: vec![],
    };
    let mut buf = [0u8; 64];
    assert_eq!(
        publish.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::PropertyValidationFailed,
        })
    );
}

#[test]
fn subscription_identifier_in_server_to_client_publish_is_allowed_on_decode() {
    // Server → Client 方向の PUBLISH では Subscription Identifier (0x0B) が許可される。
    // encode は Client → Server 用なので、Server → Client 向けのバイト列を手動で構築する。
    let buf = [
        0x30, // PUBLISH, QoS 0
        0x06, // Remaining Length = 6
        0x00, 0x01, b't', // Topic Name = "t"
        0x02, // Properties Length = 2
        0x0B, // Subscription Identifier
        0x2A, // 42
    ];
    let (decoded, consumed) = Publish::decode(&buf).expect("デコードに成功すること");
    assert_eq!(consumed, buf.len());
    assert_eq!(decoded.topic, "t");
    assert!(decoded.properties.iter().any(|p| matches!(
        p,
        shiguredo_mqtt::v5::property::Property::SubscriptionIdentifier(VariableByteInteger(42))
    )));
}
