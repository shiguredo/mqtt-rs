//! MQTT v5.0 PUBREC パケットの単体テスト。

use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v5::property::Properties;
use shiguredo_mqtt::v5::pubrec::{PubRec, PubRecReasonCode};

#[test]
fn invalid_flags_are_rejected() {
    let buf = [0x52, 0x02, 0x00, 0x01];
    assert_eq!(PubRec::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn zero_packet_id_is_rejected() {
    let pubrec = PubRec {
        packet_id: 0,
        reason_code: PubRecReasonCode::Success,
        properties: Properties::new(),
    };
    let mut buf = [0u8; 16];
    assert_eq!(
        pubrec.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        })
    );
}

#[test]
fn truncated_pubrec_is_rejected() {
    let buf = [0x50, 0x02, 0x00];
    assert_eq!(PubRec::decode(&buf), Err(DecodeError::InsufficientData));
}

#[test]
fn no_matching_subscribers_is_rejected_on_encode() {
    // MQTT v5.0 §3.5.2.1 (PUBREC Reason Code):
    // 0x10 No matching subscribers はサーバーだけが送信し得るため、
    // クライアント送信（encode）では拒否する。
    let pubrec = PubRec {
        packet_id: 1,
        reason_code: PubRecReasonCode::NoMatchingSubscribers,
        properties: Properties::new(),
    };
    let mut buf = [0u8; 16];
    assert_eq!(
        pubrec.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::InvalidReasonCode,
        })
    );
}

#[test]
fn no_matching_subscribers_is_accepted_on_decode() {
    // サーバーから受信した 0x10 No matching subscribers の decode は成功する。
    // 残り長さ 3、パケット識別子 1、理由コード 0x10。
    let buf = [0x50, 0x03, 0x00, 0x01, 0x10];
    let (pubrec, consumed) = PubRec::decode(&buf).expect("デコードに成功すること");
    assert_eq!(pubrec.reason_code, PubRecReasonCode::NoMatchingSubscribers);
    assert_eq!(consumed, 5);
}

#[test]
fn invalid_property_is_rejected_on_encode() {
    // PUBREC で許可されていない Topic Alias (0x23) を含むプロパティを追加する。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::TopicAlias(1));
    let pubrec = PubRec {
        packet_id: 1,
        reason_code: PubRecReasonCode::NotAuthorized,
        properties,
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        pubrec.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::PropertyValidationFailed,
        })
    );
}

#[test]
fn duplicate_pubrec_property_identifiers_are_rejected_on_encode() {
    // PUBREC のプロパティに Reason String (0x1F) が重複している場合、エンコードを拒否する。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::ReasonString(
        "a".to_string(),
    ));
    properties.push(shiguredo_mqtt::v5::property::Property::ReasonString(
        "b".to_string(),
    ));
    let pubrec = PubRec {
        packet_id: 1,
        reason_code: PubRecReasonCode::Success,
        properties,
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        pubrec.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::DuplicatePropertyIdentifier,
        })
    );
}
