//! MQTT v3.1.1 PINGREQ の単体テスト。

use shiguredo_mqtt::error::DecodeError;
use shiguredo_mqtt::v311::pingreq::PingReq;

#[test]
fn non_zero_remaining_length_is_rejected() {
    let buf = [0xC0, 0x01, 0x00];
    assert_eq!(PingReq::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn reserved_header_flags_are_rejected() {
    let pingreq = PingReq;
    let mut buf = [0u8; 16];
    let len = pingreq.encode(&mut buf).expect("エンコードに成功すること");
    // 固定ヘッダーの予約フラグ（bit 0-3）を設定する。
    buf[0] |= 0x0F;
    assert_eq!(
        PingReq::decode(&buf[..len]),
        Err(DecodeError::InvalidPacketFlags)
    );
}
