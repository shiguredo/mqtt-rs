//! MQTT v3.1.1 PUBREC の単体テスト。

use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v311::pubrec::PubRec;

#[test]
fn zero_packet_id_is_rejected() {
    let pubrec = PubRec { packet_id: 0 };
    let mut buf = [0u8; 16];
    assert_eq!(
        pubrec.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        })
    );

    // パケット識別子 0 を含むバイト列を直接デコードすると拒否される。
    let buf = [0x50, 0x02, 0x00, 0x00];
    assert_eq!(PubRec::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn reserved_header_flags_are_rejected() {
    let pubrec = PubRec { packet_id: 1234 };
    let mut buf = [0u8; 16];
    let len = pubrec.encode(&mut buf).expect("エンコードに成功すること");
    // 固定ヘッダーの予約フラグ（bit 0-3）を設定する。
    buf[0] |= 0x0F;
    assert_eq!(
        PubRec::decode(&buf[..len]),
        Err(DecodeError::InvalidPacketFlags)
    );
}

#[test]
fn extra_bytes_after_pubrec_are_rejected() {
    let pubrec = PubRec { packet_id: 1234 };
    let mut buf = [0u8; 16];
    let len = pubrec.encode(&mut buf).expect("エンコードに成功すること");
    // 残り長さを 1 バイト増やす。
    buf[1] += 1;
    assert_eq!(
        PubRec::decode(&buf[..len + 1]),
        Err(DecodeError::MalformedPacket)
    );
}
