//! MQTT v5.0 SUBACK パケットの単体テスト。

use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v5::property::Properties;
use shiguredo_mqtt::v5::suback::{SubAck, SubAckReasonCode};

#[test]
fn invalid_flags_are_rejected() {
    let buf = [0x92, 0x04, 0x00, 0x01, 0x00, 0x01];
    assert_eq!(SubAck::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn zero_packet_id_is_rejected() {
    let suback = SubAck {
        packet_id: 0,
        reason_codes: vec![SubAckReasonCode::GrantedQoS0],
        properties: Properties::new(),
    };
    let mut buf = [0u8; 16];
    assert_eq!(
        suback.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        })
    );
}

#[test]
fn empty_reason_codes_are_rejected() {
    let suback = SubAck {
        packet_id: 1,
        reason_codes: vec![],
        properties: Properties::new(),
    };
    let mut buf = [0u8; 16];
    assert_eq!(
        suback.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::EmptyReasonCodes,
        })
    );
}

#[test]
fn invalid_reason_code_is_rejected() {
    let buf = [0x90, 0x04, 0x00, 0x01, 0x00, 0xFF];
    assert_eq!(SubAck::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn extra_bytes_after_suback_are_rejected() {
    // 残り長さを偽装して無効な理由コードを追加する。
    let buf = [0x90, 0x05, 0x00, 0x01, 0x00, 0x00, 0xFF];
    assert_eq!(SubAck::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn invalid_property_is_rejected_on_encode() {
    // SUBACK で許可されていない Topic Alias (0x23) を含むプロパティを追加する。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::TopicAlias(1));
    let suback = SubAck {
        packet_id: 1,
        reason_codes: vec![SubAckReasonCode::GrantedQoS0],
        properties,
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        suback.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::PropertyValidationFailed,
        })
    );
}

#[test]
fn duplicate_suback_property_identifiers_are_rejected_on_encode() {
    // SUBACK のプロパティに Reason String (0x1F) が重複している場合、エンコードを拒否する。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::ReasonString(
        "a".to_string(),
    ));
    properties.push(shiguredo_mqtt::v5::property::Property::ReasonString(
        "b".to_string(),
    ));
    let suback = SubAck {
        packet_id: 1,
        reason_codes: vec![SubAckReasonCode::GrantedQoS0],
        properties,
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        suback.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::DuplicatePropertyIdentifier,
        })
    );
}
