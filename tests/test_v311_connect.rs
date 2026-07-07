//! MQTT v3.1.1 CONNECT の単体テスト。

use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v311::connect::{Connect, Will};

#[test]
fn password_without_username_is_rejected() {
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_session: true,
        keep_alive: 60,
        will: None,
        username: None,
        password: Some(vec![0xAB]),
    };
    let mut buf = [0u8; 256];
    assert_eq!(
        connect.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::PasswordWithoutUsername,
        })
    );
}

#[test]
fn empty_client_id_with_clean_session_false_is_rejected() {
    // 長さ 0 の ClientId は CleanSession=1 と組み合わせなければならない。
    let connect = Connect {
        client_id: String::new(),
        clean_session: false,
        keep_alive: 60,
        will: None,
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    assert_eq!(
        connect.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::EmptyClientIdWithoutCleanSession,
        })
    );
}

#[test]
fn empty_client_id_with_clean_session_true_is_allowed() {
    // 長さ 0 の ClientId は CleanSession=1 であれば許容される。
    let connect = Connect {
        client_id: String::new(),
        clean_session: true,
        keep_alive: 60,
        will: None,
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    let len = connect.encode(&mut buf).expect("エンコードに成功すること");
    let (decoded, consumed) = Connect::decode(&buf[..len]).expect("デコードに成功すること");
    assert_eq!(consumed, len);
    assert_eq!(decoded, connect);
}

#[test]
fn reserved_connect_flag_is_rejected() {
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_session: true,
        keep_alive: 60,
        will: None,
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    let len = connect.encode(&mut buf).expect("エンコードに成功すること");
    // 予約フラグ（Connect Flags バイトの bit 0）を設定する。
    // Connect Flags は固定ヘッダー 2 バイト + プロトコル名長 2 バイト + "MQTT" 4 バイト
    // + プロトコルレベル 1 バイトの直後にあるため、インデックスは 9 となる。
    buf[9] |= 0x01;
    assert_eq!(
        Connect::decode(&buf[..len]),
        Err(DecodeError::MalformedPacket)
    );
}

#[test]
fn reserved_header_flags_are_rejected() {
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_session: true,
        keep_alive: 60,
        will: None,
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    let len = connect.encode(&mut buf).expect("エンコードに成功すること");
    // 固定ヘッダーの予約フラグ（bit 0-3）を設定する。
    buf[0] |= 0x0F;
    assert_eq!(
        Connect::decode(&buf[..len]),
        Err(DecodeError::InvalidPacketFlags)
    );
}

#[test]
fn empty_will_topic_is_rejected() {
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_session: true,
        keep_alive: 60,
        will: Some(Will {
            topic: "".to_string(),
            payload: vec![0x01],
            qos: QoS::AtMostOnce,
            retain: false,
        }),
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    assert_eq!(
        connect.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::EmptyTopicName,
        })
    );
}

#[test]
fn wildcard_in_will_topic_is_rejected() {
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_session: true,
        keep_alive: 60,
        will: Some(Will {
            topic: "will/#".to_string(),
            payload: vec![0x01],
            qos: QoS::AtMostOnce,
            retain: false,
        }),
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    assert_eq!(
        connect.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::WildcardInTopicName,
        })
    );
}

#[test]
fn extra_bytes_after_connect_are_rejected() {
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_session: true,
        keep_alive: 60,
        will: None,
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    let len = connect.encode(&mut buf).expect("エンコードに成功すること");
    // 残り長さを 1 バイト増やす。
    buf[1] += 1;
    assert_eq!(
        Connect::decode(&buf[..len + 1]),
        Err(DecodeError::MalformedPacket)
    );
}

#[test]
fn debug_masks_password_and_will_payload() {
    // Debug 出力に password や Will payload の平文が含まれないこと。
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_session: true,
        keep_alive: 60,
        will: Some(Will {
            topic: "will/topic".to_string(),
            payload: vec![0x01, 0x02, 0x03],
            qos: QoS::AtMostOnce,
            retain: false,
        }),
        username: Some("user".to_string()),
        password: Some(vec![0xAB, 0xCD]),
    };
    let debug = format!("{:?}", connect);
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("0xAB"));
    assert!(!debug.contains("0xCD"));
    assert!(!debug.contains("0x01"));
    assert!(!debug.contains("0x02"));
    assert!(!debug.contains("0x03"));
}
