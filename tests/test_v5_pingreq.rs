//! MQTT v5.0 PINGREQ パケットの単体テスト。

use shiguredo_mqtt::error::DecodeError;
use shiguredo_mqtt::v5::pingreq::PingReq;

#[test]
fn non_zero_remaining_length_is_rejected() {
    let buf = [0xC0, 0x01, 0x00];
    assert_eq!(PingReq::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn invalid_flags_are_rejected() {
    let buf = [0xC1, 0x00];
    assert_eq!(PingReq::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}
