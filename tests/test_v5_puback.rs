//! MQTT v5.0 PUBACK パケットの単体テスト。

use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v5::property::Properties;
use shiguredo_mqtt::v5::puback::{PubAck, PubAckReasonCode};

#[test]
fn invalid_flags_are_rejected() {
    let buf = [0x42, 0x02, 0x00, 0x01];
    assert_eq!(PubAck::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn zero_packet_id_is_rejected() {
    let puback = PubAck {
        packet_id: 0,
        reason_code: PubAckReasonCode::Success,
        properties: Properties::new(),
    };
    let mut buf = [0u8; 16];
    assert_eq!(
        puback.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        })
    );
}

#[test]
fn invalid_reason_code_is_rejected() {
    // 残り長さ 3、パケット識別子 1、理由コード 0xFF（無効）。
    let buf = [0x40, 0x03, 0x00, 0x01, 0xFF];
    assert_eq!(PubAck::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn truncated_puback_is_rejected() {
    let buf = [0x40, 0x02, 0x00];
    assert_eq!(PubAck::decode(&buf), Err(DecodeError::InsufficientData));
}

#[test]
fn no_matching_subscribers_is_rejected_on_encode() {
    // MQTT v5.0 §3.4.2.1 (PUBACK Reason Code):
    // 0x10 No matching subscribers はサーバーだけが送信し得るため、
    // クライアント送信（encode）では拒否する。
    let puback = PubAck {
        packet_id: 1,
        reason_code: PubAckReasonCode::NoMatchingSubscribers,
        properties: Properties::new(),
    };
    let mut buf = [0u8; 16];
    assert_eq!(
        puback.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::InvalidReasonCode,
        })
    );
}

#[test]
fn no_matching_subscribers_is_accepted_on_decode() {
    // サーバーから受信した 0x10 No matching subscribers の decode は成功する。
    // 残り長さ 3、パケット識別子 1、理由コード 0x10。
    let buf = [0x40, 0x03, 0x00, 0x01, 0x10];
    let (puback, consumed) = PubAck::decode(&buf).expect("デコードに成功すること");
    assert_eq!(puback.reason_code, PubAckReasonCode::NoMatchingSubscribers);
    assert_eq!(consumed, 5);
}

#[test]
fn invalid_property_is_rejected_on_encode() {
    // PUBACK で許可されていない Topic Alias (0x23) を含むプロパティを追加する。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::TopicAlias(1));
    let puback = PubAck {
        packet_id: 1,
        reason_code: PubAckReasonCode::NotAuthorized,
        properties,
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        puback.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::PropertyValidationFailed,
        })
    );
}

#[test]
fn duplicate_puback_property_identifiers_are_rejected_on_encode() {
    // PUBACK のプロパティに Reason String (0x1F) が重複している場合、エンコードを拒否する。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::ReasonString(
        "a".to_string(),
    ));
    properties.push(shiguredo_mqtt::v5::property::Property::ReasonString(
        "b".to_string(),
    ));
    let puback = PubAck {
        packet_id: 1,
        reason_code: PubAckReasonCode::Success,
        properties,
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        puback.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::DuplicatePropertyIdentifier,
        })
    );
}
