//! MQTT v3.1.1 CONNACK の単体テスト。

use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v311::connack::{ConnAck, ConnectReturnCode};

#[test]
fn session_present_with_non_accepted_return_code_is_rejected() {
    let connack = ConnAck {
        session_present: true,
        return_code: ConnectReturnCode::NotAuthorized,
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        connack.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::SessionPresentWithNonSuccess,
        })
    );
}

#[test]
fn reserved_acknowledge_flags_are_rejected() {
    let connack = ConnAck {
        session_present: false,
        return_code: ConnectReturnCode::Accepted,
    };
    let mut buf = [0u8; 32];
    let len = connack.encode(&mut buf).expect("エンコードに成功すること");
    // acknowledge flags バイトの予約ビットを設定する。
    buf[2] |= 0x02;
    assert_eq!(
        ConnAck::decode(&buf[..len]),
        Err(DecodeError::MalformedPacket)
    );
}

#[test]
fn reserved_header_flags_are_rejected() {
    let connack = ConnAck {
        session_present: false,
        return_code: ConnectReturnCode::Accepted,
    };
    let mut buf = [0u8; 32];
    let len = connack.encode(&mut buf).expect("エンコードに成功すること");
    // 固定ヘッダーの予約フラグ（bit 0-3）を設定する。
    buf[0] |= 0x0F;
    assert_eq!(
        ConnAck::decode(&buf[..len]),
        Err(DecodeError::InvalidPacketFlags)
    );
}

#[test]
fn extra_bytes_after_connack_are_rejected() {
    let connack = ConnAck {
        session_present: false,
        return_code: ConnectReturnCode::Accepted,
    };
    let mut buf = [0u8; 32];
    let len = connack.encode(&mut buf).expect("エンコードに成功すること");
    // 残り長さを 1 バイト増やす。
    buf[1] += 1;
    assert_eq!(
        ConnAck::decode(&buf[..len + 1]),
        Err(DecodeError::MalformedPacket)
    );
}
