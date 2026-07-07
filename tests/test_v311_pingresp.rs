//! MQTT v3.1.1 PINGRESP の単体テスト。

use shiguredo_mqtt::error::DecodeError;
use shiguredo_mqtt::v311::pingresp::PingResp;

#[test]
fn non_zero_remaining_length_is_rejected() {
    let buf = [0xD0, 0x01, 0x00];
    assert_eq!(PingResp::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn reserved_header_flags_are_rejected() {
    let pingresp = PingResp;
    let mut buf = [0u8; 16];
    let len = pingresp.encode(&mut buf).expect("エンコードに成功すること");
    // 固定ヘッダーの予約フラグ（bit 0-3）を設定する。
    buf[0] |= 0x0F;
    assert_eq!(
        PingResp::decode(&buf[..len]),
        Err(DecodeError::InvalidPacketFlags)
    );
}
