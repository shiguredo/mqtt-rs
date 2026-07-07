//! MQTT v5.0 CONNECT パケットの単体テスト。

use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v5::connect::{Connect, Will};
use shiguredo_mqtt::v5::property::Properties;

#[test]
fn password_without_username_is_allowed() {
    // MQTT v5.0 では Password のみの送信が許可されている。
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties: Properties::new(),
        will: None,
        username: None,
        password: Some(vec![0xAB]),
    };
    let mut buf = [0u8; 256];
    let len = connect.encode(&mut buf).expect("エンコードに成功すること");
    let (decoded, consumed) = Connect::decode(&buf[..len]).expect("デコードに成功すること");
    assert_eq!(decoded, connect);
    assert_eq!(consumed, len);
}

#[test]
fn extra_bytes_after_connect_payload_are_rejected() {
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties: Properties::new(),
        will: None,
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    let len = connect.encode(&mut buf).expect("エンコードに成功すること");
    let mut buf = buf.to_vec();
    // 残り長さを 1 増やして余計なバイトを追加。
    buf[1] += 1;
    buf[len] = 0x00;
    assert_eq!(
        Connect::decode(&buf[..len + 1]),
        Err(DecodeError::MalformedPacket)
    );
}

#[test]
fn reserved_header_flags_are_rejected() {
    // MQTT v5.0 §2.1.3 [MQTT-2.1.3-1]: CONNECT の予約フラグは 0 でなければならない。
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties: Properties::new(),
        will: None,
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    let len = connect.encode(&mut buf).expect("エンコードに成功すること");
    buf[0] = 0x11;
    assert_eq!(
        Connect::decode(&buf[..len]),
        Err(DecodeError::InvalidPacketFlags)
    );
}

#[test]
fn authentication_data_without_method_is_rejected() {
    // MQTT v5.0 §3.1.2.11.10: Authentication Data は Authentication Method と共にのみ送信可能。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::AuthenticationData(
        vec![0xAB],
    ));
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties,
        will: None,
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    // エンコード時にプロパティ検証で拒否される。
    assert_eq!(
        connect.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::PropertyValidationFailed,
        })
    );

    // decode 経由でも拒否されることを確認するため、プロパティを直接エンコードした
    // 手作りのバイト列で検証する。
    // Authentication Method なしで Authentication Data のみを持つ CONNECT パケット。
    let raw = [
        0x10, 0x13, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x02, 0x16, 0x01,
        0xAB, 0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1',
    ];
    assert_eq!(Connect::decode(&raw), Err(DecodeError::MalformedPacket));
}

#[test]
fn empty_client_id_with_clean_start_false_is_allowed() {
    // MQTT v5.0 §3.1.3.1 [MQTT-3.1.3-6]:
    // サーバーは長さ 0 バイトの Client ID を許容してよい（MQTT v3.1.1 と異なり、
    // 空 Client ID に Clean Start を要求する規範は MQTT v5.0 にはない）ため、
    // Clean Start=false かつ Client ID が空の CONNECT も形式エラーとしない。
    // 空 Client ID の可否はサーバーの裁量であり、本ライブラリは形式検証のみ行う。
    let connect = Connect {
        client_id: "".to_string(),
        clean_start: false,
        keep_alive: 60,
        properties: Properties::new(),
        will: None,
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    let len = connect.encode(&mut buf).expect("エンコードに成功すること");
    let (decoded, consumed) = Connect::decode(&buf[..len]).expect("デコードに成功すること");
    assert_eq!(decoded, connect);
    assert_eq!(consumed, len);
}

#[test]
fn empty_will_topic_is_rejected() {
    // MQTT v5.0 §3.3.2.1: Will Topic は空にできない。
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties: Properties::new(),
        will: Some(Will {
            topic: "".to_string(),
            payload: vec![0x01],
            qos: QoS::AtMostOnce,
            retain: false,
            properties: Properties::new(),
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
    // MQTT v5.0 §3.1.3.3 / MQTT v5.0 §3.3.2.1 [MQTT-3.3.2-2]:
    // Will Topic Name にワイルドカード文字は使用できない。
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties: Properties::new(),
        will: Some(Will {
            topic: "will/#".to_string(),
            payload: vec![0x01],
            qos: QoS::AtMostOnce,
            retain: false,
            properties: Properties::new(),
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
fn will_payload_format_indicator_1_with_invalid_utf8_is_rejected() {
    // MQTT v5.0 §3.1.3.2.3 (Payload Format Indicator):
    // Payload Format Indicator が 1 の場合、Will Payload は
    // well-formed UTF-8 でなければならない。
    let mut will_properties = Properties::new();
    will_properties.push(shiguredo_mqtt::v5::property::Property::PayloadFormatIndicator(1));
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties: Properties::new(),
        will: Some(Will {
            topic: "will/topic".to_string(),
            // 0xFF 単独は不正な UTF-8 シーケンスである。
            payload: vec![0xFF],
            qos: QoS::AtMostOnce,
            retain: false,
            properties: will_properties,
        }),
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    assert_eq!(
        connect.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::InvalidPayloadUtf8,
        })
    );
}

#[test]
fn will_payload_format_indicator_1_with_valid_utf8_is_accepted() {
    // Payload Format Indicator が 1 でも well-formed UTF-8 なら受理される。
    let mut will_properties = Properties::new();
    will_properties.push(shiguredo_mqtt::v5::property::Property::PayloadFormatIndicator(1));
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties: Properties::new(),
        will: Some(Will {
            topic: "will/topic".to_string(),
            payload: "さようなら".as_bytes().to_vec(),
            qos: QoS::AtMostOnce,
            retain: false,
            properties: will_properties,
        }),
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    assert!(connect.encode(&mut buf).is_ok());
}

#[test]
fn will_payload_format_indicator_0_allows_arbitrary_payload() {
    // MQTT v5.0 §3.1.3.2.3: Payload Format Indicator が 0（未指定バイト）の
    // 場合、Will Payload は任意のバイト列でよい。
    let mut will_properties = Properties::new();
    will_properties.push(shiguredo_mqtt::v5::property::Property::PayloadFormatIndicator(0));
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties: Properties::new(),
        will: Some(Will {
            topic: "will/topic".to_string(),
            payload: vec![0xFF, 0xFE],
            qos: QoS::AtMostOnce,
            retain: false,
            properties: will_properties,
        }),
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    assert!(connect.encode(&mut buf).is_ok());
}

#[test]
fn wildcard_in_will_response_topic_is_rejected() {
    // MQTT v5.0 §3.3.2.3.5 (Response Topic):
    // Will Properties に含まれる Response Topic にもワイルドカード文字は使用できない。
    let mut will_properties = Properties::new();
    will_properties.push(shiguredo_mqtt::v5::property::Property::ResponseTopic(
        "will/resp/#".to_string(),
    ));
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties: Properties::new(),
        will: Some(Will {
            topic: "will/topic".to_string(),
            payload: vec![0x01],
            qos: QoS::AtMostOnce,
            retain: false,
            properties: will_properties,
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
fn wildcard_in_will_response_topic_on_decode_is_rejected() {
    // Will Properties の Response Topic に + を含む CONNECT パケットをデコードした場合、
    // MalformedPacket となることを確認する。
    // まず正常な Response Topic を持つ CONNECT をエンコードし、Response Topic の
    // 文字列部分を書き換えて + を含めることで、構造は正しいまま内容だけを不正にする。
    let mut will_properties = Properties::new();
    will_properties.push(shiguredo_mqtt::v5::property::Property::ResponseTopic(
        "wi/xa".to_string(),
    ));
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties: Properties::new(),
        will: Some(Will {
            topic: "will/top?".to_string(),
            payload: vec![0xAB],
            qos: QoS::AtMostOnce,
            retain: false,
            properties: will_properties,
        }),
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    let len = connect.encode(&mut buf).expect("エンコードに成功すること");
    // "wi/xa" の 'x' を '+' に書き換える。
    let x_pos = buf[..len]
        .windows(5)
        .position(|w| w == b"wi/xa")
        .expect("Response Topic 文字列が見つかること");
    buf[x_pos + 3] = b'+';
    assert_eq!(
        Connect::decode(&buf[..len]),
        Err(DecodeError::MalformedPacket)
    );
}

#[test]
fn duplicate_connect_property_identifiers_are_rejected_on_encode() {
    // CONNECT のプロパティに Session Expiry Interval (0x11) が重複している場合、エンコードを拒否する。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::SessionExpiryInterval(60));
    properties.push(shiguredo_mqtt::v5::property::Property::SessionExpiryInterval(120));
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties,
        will: None,
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    assert_eq!(
        connect.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::DuplicatePropertyIdentifier,
        })
    );
}

#[test]
fn duplicate_will_property_identifiers_are_rejected_on_encode() {
    // Will Properties に Message Expiry Interval (0x02) が重複している場合、エンコードを拒否する。
    let mut will_properties = Properties::new();
    will_properties.push(shiguredo_mqtt::v5::property::Property::MessageExpiryInterval(60));
    will_properties.push(shiguredo_mqtt::v5::property::Property::MessageExpiryInterval(120));
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties: Properties::new(),
        will: Some(Will {
            topic: "will/topic".to_string(),
            payload: vec![0x01],
            qos: QoS::AtMostOnce,
            retain: false,
            properties: will_properties,
        }),
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    assert_eq!(
        connect.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::DuplicatePropertyIdentifier,
        })
    );
}

#[test]
fn connect_with_invalid_property_is_rejected_on_encode() {
    // CONNECT のプロパティに Payload Format Indicator (0x01) を含めると、
    // encode 時の許可リスト検証で拒否される。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::PayloadFormatIndicator(0));
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties,
        will: None,
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    assert_eq!(
        connect.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::PropertyValidationFailed,
        })
    );
}

#[test]
fn will_with_invalid_property_is_rejected_on_encode() {
    // Will Properties に Session Expiry Interval (0x11) を含めると、
    // encode 時の許可リスト検証で拒否される。
    let mut will_properties = Properties::new();
    will_properties.push(shiguredo_mqtt::v5::property::Property::SessionExpiryInterval(60));
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties: Properties::new(),
        will: Some(Will {
            topic: "will/topic".to_string(),
            payload: vec![0x01],
            qos: QoS::AtMostOnce,
            retain: false,
            properties: will_properties,
        }),
        username: None,
        password: None,
    };
    let mut buf = [0u8; 256];
    assert_eq!(
        connect.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::PropertyValidationFailed,
        })
    );
}

#[test]
fn debug_masks_password_and_will_payload() {
    // Debug 出力に password や Will payload の平文が含まれないこと。
    let connect = Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties: Properties::new(),
        will: Some(Will {
            topic: "will/topic".to_string(),
            payload: vec![0x01, 0x02, 0x03],
            qos: QoS::AtMostOnce,
            retain: false,
            properties: Properties::new(),
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
