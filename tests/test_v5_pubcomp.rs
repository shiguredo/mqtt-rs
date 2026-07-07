//! MQTT v5.0 PUBCOMP パケットの単体テスト。

use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v5::property::Properties;
use shiguredo_mqtt::v5::pubcomp::{PubComp, PubCompReasonCode};

#[test]
fn invalid_flags_are_rejected() {
    let buf = [0x72, 0x02, 0x00, 0x01];
    assert_eq!(PubComp::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn zero_packet_id_is_rejected() {
    let pubcomp = PubComp {
        packet_id: 0,
        reason_code: PubCompReasonCode::Success,
        properties: Properties::new(),
    };
    let mut buf = [0u8; 16];
    assert_eq!(
        pubcomp.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        })
    );
}

#[test]
fn invalid_property_is_rejected_on_encode() {
    // PUBCOMP で許可されていない Topic Alias (0x23) を含むプロパティを追加する。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::TopicAlias(1));
    let pubcomp = PubComp {
        packet_id: 1,
        reason_code: PubCompReasonCode::PacketIdentifierNotFound,
        properties,
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        pubcomp.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::PropertyValidationFailed,
        })
    );
}

#[test]
fn duplicate_pubcomp_property_identifiers_are_rejected_on_encode() {
    // PUBCOMP のプロパティに Reason String (0x1F) が重複している場合、エンコードを拒否する。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::ReasonString(
        "a".to_string(),
    ));
    properties.push(shiguredo_mqtt::v5::property::Property::ReasonString(
        "b".to_string(),
    ));
    let pubcomp = PubComp {
        packet_id: 1,
        reason_code: PubCompReasonCode::Success,
        properties,
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        pubcomp.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::DuplicatePropertyIdentifier,
        })
    );
}
