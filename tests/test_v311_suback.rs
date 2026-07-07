//! MQTT v3.1.1 SUBACK の単体テスト。

use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v311::suback::{SubAck, SubscribeReturnCode};

#[test]
fn empty_return_codes_are_rejected() {
    let suback = SubAck {
        packet_id: 1,
        return_codes: vec![],
    };
    let mut buf = [0u8; 16];
    assert_eq!(
        suback.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::EmptyReturnCodes,
        })
    );
}

#[test]
fn zero_packet_id_is_rejected() {
    let suback = SubAck {
        packet_id: 0,
        return_codes: vec![SubscribeReturnCode::SuccessQoS0],
    };
    let mut buf = [0u8; 16];
    assert_eq!(
        suback.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        })
    );

    // パケット識別子 0 を含むバイト列を直接デコードすると拒否される。
    let buf = [0x90, 0x03, 0x00, 0x00, 0x00];
    assert_eq!(SubAck::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn invalid_return_code_is_rejected() {
    let suback = SubAck {
        packet_id: 1,
        return_codes: vec![SubscribeReturnCode::SuccessQoS0],
    };
    let mut buf = [0u8; 16];
    let len = suback.encode(&mut buf).expect("エンコードに成功すること");
    // 唯一のリターンコードを不正な値で上書きする。
    buf[len - 1] = 0x03;
    assert_eq!(
        SubAck::decode(&buf[..len]),
        Err(DecodeError::MalformedPacket)
    );
}

#[test]
fn reserved_header_flags_are_rejected() {
    let suback = SubAck {
        packet_id: 7,
        return_codes: vec![SubscribeReturnCode::SuccessQoS1],
    };
    let mut buf = [0u8; 16];
    let len = suback.encode(&mut buf).expect("エンコードに成功すること");
    // 固定ヘッダーの予約フラグ（bit 0-3）を設定する。
    buf[0] |= 0x0F;
    assert_eq!(
        SubAck::decode(&buf[..len]),
        Err(DecodeError::InvalidPacketFlags)
    );
}
