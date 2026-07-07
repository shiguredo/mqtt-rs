//! MQTT v3.1.1 PUBREL の単体テスト。

use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v311::pubrel::PubRel;

#[test]
fn incorrect_flags_are_rejected() {
    let pubrel = PubRel { packet_id: 1234 };
    let mut buf = [0u8; 16];
    let len = pubrel.encode(&mut buf).expect("エンコードに成功すること");
    // 必須の bit 1 フラグをクリアする。
    buf[0] = 0x60;
    assert_eq!(
        PubRel::decode(&buf[..len]),
        Err(DecodeError::InvalidPacketFlags)
    );
}

#[test]
fn zero_packet_id_is_rejected() {
    let pubrel = PubRel { packet_id: 0 };
    let mut buf = [0u8; 16];
    assert_eq!(
        pubrel.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        })
    );

    // パケット識別子 0 を含むバイト列を直接デコードすると拒否される。
    let buf = [0x62, 0x02, 0x00, 0x00];
    assert_eq!(PubRel::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn extra_bytes_after_pubrel_are_rejected() {
    let pubrel = PubRel { packet_id: 1234 };
    let mut buf = [0u8; 16];
    let len = pubrel.encode(&mut buf).expect("エンコードに成功すること");
    // 残り長さを 1 バイト増やす。
    buf[1] += 1;
    assert_eq!(
        PubRel::decode(&buf[..len + 1]),
        Err(DecodeError::MalformedPacket)
    );
}
