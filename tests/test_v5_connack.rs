//! MQTT v5.0 CONNACK パケットの単体テスト。

use shiguredo_mqtt::codec::variable_byte_integer::VariableByteInteger;
use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v5::connack::{ConnAck, ConnectReasonCode};
use shiguredo_mqtt::v5::property::Properties;

#[test]
fn reserved_header_flags_are_rejected() {
    // MQTT v5.0 §2.1.3 [MQTT-2.1.3-1]: CONNACK の予約フラグは 0 でなければならない。
    let buf = [0x21, 0x03, 0x00, 0x00, 0x00];
    assert_eq!(ConnAck::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn session_present_with_non_success_reason_is_rejected() {
    // Session Present が true で理由コードが非ゼロの場合はプロトコル違反。
    let buf = [0x20, 0x03, 0x01, 0x87, 0x00];
    assert_eq!(ConnAck::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn session_present_with_non_success_reason_is_rejected_on_encode() {
    let connack = ConnAck {
        session_present: true,
        reason_code: ConnectReasonCode::NotAuthorized,
        properties: Properties::new(),
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        connack.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::SessionPresentWithNonSuccess,
        })
    );
}

#[test]
fn extra_bytes_after_connack_are_rejected() {
    // 残り長さを偽装して余計なバイトを追加。
    let buf = [0x20, 0x04, 0x00, 0x00, 0x00, 0x00];
    assert_eq!(ConnAck::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn invalid_property_is_rejected_on_encode() {
    // CONNACK で許可されていない Subscription Identifier (0x0B) を含むプロパティを追加する。
    let mut properties = Properties::new();
    properties.push(
        shiguredo_mqtt::v5::property::Property::SubscriptionIdentifier(VariableByteInteger(1)),
    );
    let connack = ConnAck {
        session_present: false,
        reason_code: ConnectReasonCode::Success,
        properties,
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        connack.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::PropertyValidationFailed,
        })
    );
}

#[test]
fn from_u8_accepts_exactly_the_spec_reason_codes() {
    // MQTT v5.0 §3.2.2.2 の Connect Reason Code 表に存在する全 22 値。
    // from_u8 の受理集合がこの表と完全一致することを全数検証する。
    let valid: [u8; 22] = [
        0x00, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8C, 0x90, 0x95,
        0x97, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9F,
    ];
    for value in 0x00..=0xFFu8 {
        let decoded = ConnectReasonCode::from_u8(value);
        if valid.contains(&value) {
            let code = decoded.expect("仕様の表に存在する値は受理されること");
            assert_eq!(code.as_u8(), value);
        } else {
            assert!(
                decoded.is_none(),
                "仕様の表に無い値 0x{value:02X} は受理されないこと"
            );
        }
    }
}

#[test]
fn duplicate_connack_property_identifiers_are_rejected_on_encode() {
    // CONNACK のプロパティに Reason String (0x1F) が重複している場合、エンコードを拒否する。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::ReasonString(
        "a".to_string(),
    ));
    properties.push(shiguredo_mqtt::v5::property::Property::ReasonString(
        "b".to_string(),
    ));
    let connack = ConnAck {
        session_present: false,
        reason_code: ConnectReasonCode::Success,
        properties,
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        connack.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::DuplicatePropertyIdentifier,
        })
    );
}
