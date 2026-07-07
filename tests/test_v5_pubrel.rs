//! MQTT v5.0 PUBREL パケットの単体テスト。

use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v5::property::Properties;
use shiguredo_mqtt::v5::pubrel::{PubRel, PubRelReasonCode};

#[test]
fn invalid_flags_are_rejected() {
    let buf = [0x60, 0x02, 0x00, 0x01];
    assert_eq!(PubRel::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn zero_packet_id_is_rejected() {
    let pubrel = PubRel {
        packet_id: 0,
        reason_code: PubRelReasonCode::Success,
        properties: Properties::new(),
    };
    let mut buf = [0u8; 16];
    assert_eq!(
        pubrel.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        })
    );
}

#[test]
fn invalid_property_is_rejected_on_encode() {
    // PUBREL で許可されていない Topic Alias (0x23) を含むプロパティを追加する。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::TopicAlias(1));
    let pubrel = PubRel {
        packet_id: 1,
        reason_code: PubRelReasonCode::PacketIdentifierNotFound,
        properties,
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        pubrel.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::PropertyValidationFailed,
        })
    );
}

#[test]
fn duplicate_pubrel_property_identifiers_are_rejected_on_encode() {
    // PUBREL のプロパティに Reason String (0x1F) が重複している場合、エンコードを拒否する。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::ReasonString(
        "a".to_string(),
    ));
    properties.push(shiguredo_mqtt::v5::property::Property::ReasonString(
        "b".to_string(),
    ));
    let pubrel = PubRel {
        packet_id: 1,
        reason_code: PubRelReasonCode::Success,
        properties,
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        pubrel.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::DuplicatePropertyIdentifier,
        })
    );
}
