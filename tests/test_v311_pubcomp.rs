//! MQTT v3.1.1 PUBCOMP の単体テスト。

use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v311::pubcomp::PubComp;

#[test]
fn zero_packet_id_is_rejected() {
    let pubcomp = PubComp { packet_id: 0 };
    let mut buf = [0u8; 16];
    assert_eq!(
        pubcomp.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        })
    );

    // パケット識別子 0 を含むバイト列を直接デコードすると拒否される。
    let buf = [0x70, 0x02, 0x00, 0x00];
    assert_eq!(PubComp::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn reserved_header_flags_are_rejected() {
    let pubcomp = PubComp { packet_id: 1234 };
    let mut buf = [0u8; 16];
    let len = pubcomp.encode(&mut buf).expect("エンコードに成功すること");
    // 固定ヘッダーの予約フラグ（bit 0-3）を設定する。
    buf[0] |= 0x0F;
    assert_eq!(
        PubComp::decode(&buf[..len]),
        Err(DecodeError::InvalidPacketFlags)
    );
}

#[test]
fn extra_bytes_after_pubcomp_are_rejected() {
    let pubcomp = PubComp { packet_id: 1234 };
    let mut buf = [0u8; 16];
    let len = pubcomp.encode(&mut buf).expect("エンコードに成功すること");
    // 残り長さを 1 バイト増やす。
    buf[1] += 1;
    assert_eq!(
        PubComp::decode(&buf[..len + 1]),
        Err(DecodeError::MalformedPacket)
    );
}
