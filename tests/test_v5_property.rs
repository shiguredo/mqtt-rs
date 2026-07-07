//! MQTT v5.0 プロパティの公開 encode/decode 単体テスト。

use shiguredo_mqtt::codec::variable_byte_integer::VariableByteInteger;
use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v5::property::{Properties, Property};

#[test]
fn invalid_boolean_like_property_values_are_rejected() {
    // 0 または 1 のみ有効なプロパティに 2 を設定するとエラーになる。
    let invalid = Property::PayloadFormatIndicator(2);
    let mut buf = [0u8; 16];
    assert_eq!(
        invalid.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::InvalidPropertyValue,
        })
    );
}

#[test]
fn zero_receive_maximum_is_rejected() {
    // 受信最大値 (Receive Maximum) は 0 より大きくなければならない。
    let invalid = Property::ReceiveMaximum(0);
    let mut buf = [0u8; 16];
    assert_eq!(
        invalid.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::InvalidPropertyValue,
        })
    );
}

#[test]
fn zero_maximum_packet_size_is_rejected() {
    // 最大パケットサイズ (Maximum Packet Size) は 0 より大きくなければならない。
    let invalid = Property::MaximumPacketSize(0);
    let mut buf = [0u8; 16];
    assert_eq!(
        invalid.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::InvalidPropertyValue,
        })
    );
}

#[test]
fn zero_topic_alias_is_rejected() {
    // トピックエイリアス (Topic Alias) は 0 より大きくなければならない。
    let invalid = Property::TopicAlias(0);
    let mut buf = [0u8; 16];
    assert_eq!(
        invalid.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::InvalidPropertyValue,
        })
    );
}

#[test]
fn zero_topic_alias_maximum_is_allowed() {
    // トピックエイリアス最大値 (Topic Alias Maximum) は 0 も有効値である。
    let valid = Property::TopicAliasMaximum(0);
    let mut buf = [0u8; 16];
    let len = valid.encode(&mut buf).expect("エンコードに成功すること");
    let (decoded, consumed) = Property::decode(&buf[..len]).expect("デコードに成功すること");
    assert_eq!(decoded, valid);
    assert_eq!(consumed, len);
}

#[test]
fn zero_subscription_identifier_is_rejected() {
    // サブスクリプション識別子 (Subscription Identifier) は 0 より大きくなければならない。
    let invalid = Property::SubscriptionIdentifier(VariableByteInteger(0));
    let mut buf = [0u8; 16];
    assert_eq!(
        invalid.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::InvalidPropertyValue,
        })
    );
}

#[test]
fn invalid_property_value_on_decode_is_rejected() {
    // 不正な値域のプロパティはデコード時に MalformedPacket となる。
    // 受信最大値 (Receive Maximum) = 0 を手動でエンコードしたバイト列。
    let buf = [0x03, 0x21, 0x00, 0x00];
    assert_eq!(Properties::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn wildcard_in_response_topic_is_rejected() {
    // MQTT v5.0 §3.3.2.3.5 [MQTT-3.3.2-14]:
    // Response Topic にワイルドカード文字を含めてはならない。
    let invalid_plus = Property::ResponseTopic("a/+".to_string());
    let mut buf = [0u8; 32];
    assert_eq!(
        invalid_plus.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::WildcardInTopicName,
        })
    );

    let invalid_hash = Property::ResponseTopic("a/#".to_string());
    assert_eq!(
        invalid_hash.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::WildcardInTopicName,
        })
    );
}

#[test]
fn empty_response_topic_is_rejected() {
    // MQTT v5.0 §3.3.2.3.5 [MQTT-3.3.2-13]: Response Topic は Topic Name
    // として使われる UTF-8 Encoded String でなければならない。
    // MQTT v5.0 §4.7.3 [MQTT-4.7.3-1]:
    // すべての Topic Name と Topic Filter は最低 1 文字でなければならない。
    let invalid = Property::ResponseTopic(String::new());
    let mut buf = [0u8; 32];
    assert_eq!(
        invalid.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::EmptyTopicName,
        })
    );
}

#[test]
fn empty_response_topic_on_decode_is_rejected() {
    // 空の Response Topic を手動でエンコードしたバイト列。
    // プロパティ長 3、Response Topic (0x08)、トピック長 0。
    let buf = [0x03, 0x08, 0x00, 0x00];
    assert_eq!(Properties::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn wildcard_in_response_topic_on_decode_is_rejected() {
    // Response Topic に # を含むプロパティを手動でエンコードしたバイト列。
    // プロパティ長 7、Response Topic (0x08)、トピック長 4、"re/#"。
    let buf = [0x07, 0x08, 0x00, 0x04, b'r', b'e', b'/', b'#'];
    assert_eq!(Properties::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn valid_response_topic_is_allowed() {
    // ワイルドカード文字を含まない Response Topic は許可される。
    let valid = Property::ResponseTopic("response/topic".to_string());
    let mut buf = [0u8; 32];
    let len = valid.encode(&mut buf).expect("エンコードに成功すること");
    let (decoded, consumed) = Property::decode(&buf[..len]).expect("デコードに成功すること");
    assert_eq!(decoded, valid);
    assert_eq!(consumed, len);
}

#[test]
fn duplicate_property_identifiers_are_rejected() {
    // ユーザープロパティ (User Property) 以外のプロパティ識別子の重複は許可されない。
    let mut props = Properties::new();
    props.push(Property::SessionExpiryInterval(60));
    props.push(Property::SessionExpiryInterval(120));
    let mut buf = [0u8; 32];
    let len = props.encode(&mut buf).expect("エンコードに成功すること");
    assert_eq!(
        Properties::decode(&buf[..len]),
        Err(DecodeError::MalformedPacket)
    );
}

#[test]
fn user_property_duplicates_are_allowed() {
    // ユーザープロパティ (User Property) は同じ識別子でも複数回出現できる。
    let mut props = Properties::new();
    props.push(Property::UserProperty("a".to_string(), "1".to_string()));
    props.push(Property::UserProperty("b".to_string(), "2".to_string()));
    let mut buf = [0u8; 64];
    let len = props.encode(&mut buf).expect("エンコードに成功すること");
    let (decoded, consumed) = Properties::decode(&buf[..len]).expect("デコードに成功すること");
    assert_eq!(decoded, props);
    assert_eq!(consumed, len);
}

#[test]
fn multiple_subscription_identifiers_are_allowed_for_publish() {
    // PUBLISH では複数の Subscription Identifier (0x0B) が許可される。
    let mut props = Properties::new();
    props.push(Property::SubscriptionIdentifier(VariableByteInteger(1)));
    props.push(Property::SubscriptionIdentifier(VariableByteInteger(2)));
    let mut buf = [0u8; 64];
    let len = props.encode(&mut buf).expect("エンコードに成功すること");
    let (decoded, consumed) =
        Properties::decode_for_publish(&buf[..len]).expect("デコードに成功すること");
    assert_eq!(decoded, props);
    assert_eq!(consumed, len);
}

#[test]
fn unknown_property_identifier_is_rejected() {
    // 0x2A を超えるプロパティ識別子は不正として拒否される。
    let buf = [0x2B];
    assert_eq!(Property::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn property_encode_receive_maximum_exact_and_short_buffer() {
    // 固定長 Receive Maximum: 識別子 1 + u16 の 2 = 必要長 3。
    // encoded_len() に依存しない固定値で同方向退行も検出する。
    // 値 0 は validate_value が先に InvalidPropertyValue になるため使わない。
    let property = Property::ReceiveMaximum(10);
    const EXACT_LEN: usize = 3;
    assert_eq!(property.encoded_len(), EXACT_LEN);

    let mut exact = [0u8; EXACT_LEN];
    assert_eq!(
        property.encode(&mut exact),
        Ok(EXACT_LEN),
        "必要長ちょうどのバッファでエンコードに成功すること"
    );

    let mut short = [0u8; EXACT_LEN - 1];
    assert_eq!(
        property.encode(&mut short),
        Err(EncodeError::BufferTooSmall),
        "必要長 - 1 のバッファでは BufferTooSmall であること"
    );
}

#[test]
fn property_encode_content_type_exact_and_short_buffer() {
    // 可変長 Content Type のちょうど / -1 境界。
    let property = Property::ContentType("text/plain".to_string());
    let exact_len = property.encoded_len();

    let mut exact = vec![0u8; exact_len];
    assert_eq!(
        property.encode(&mut exact),
        Ok(exact_len),
        "必要長ちょうどのバッファでエンコードに成功すること"
    );

    let mut short = vec![0u8; exact_len - 1];
    assert_eq!(
        property.encode(&mut short),
        Err(EncodeError::BufferTooSmall),
        "必要長 - 1 のバッファでは BufferTooSmall であること"
    );
}

#[test]
fn property_encode_user_property_exact_and_short_buffer() {
    // User Property のちょうど / -1 境界。
    let property = Property::UserProperty("key".to_string(), "value".to_string());
    let exact_len = property.encoded_len();

    let mut exact = vec![0u8; exact_len];
    assert_eq!(
        property.encode(&mut exact),
        Ok(exact_len),
        "必要長ちょうどのバッファでエンコードに成功すること"
    );

    let mut short = vec![0u8; exact_len - 1];
    assert_eq!(
        property.encode(&mut short),
        Err(EncodeError::BufferTooSmall),
        "必要長 - 1 のバッファでは BufferTooSmall であること"
    );
}

#[test]
fn properties_encode_empty_exact_and_short_buffer() {
    // 空 Properties の必要長はプロパティ長 VBI の 1 バイト。
    let props = Properties::new();
    assert_eq!(props.encoded_len(), 1);

    let mut exact = [0u8; 1];
    assert_eq!(
        props.encode(&mut exact),
        Ok(1),
        "空 Properties は長さ 1 ちょうどで成功すること"
    );

    let mut short: [u8; 0] = [];
    assert_eq!(
        props.encode(&mut short),
        Err(EncodeError::BufferTooSmall),
        "空 Properties でも長さ 0 では BufferTooSmall であること"
    );
}

#[test]
fn properties_encode_single_exact_and_short_buffer() {
    // 単一プロパティのちょうど / -1 境界。
    let mut props = Properties::new();
    props.push(Property::ReceiveMaximum(10));
    let exact_len = props.encoded_len();

    let mut exact = vec![0u8; exact_len];
    assert_eq!(
        props.encode(&mut exact),
        Ok(exact_len),
        "必要長ちょうどのバッファでエンコードに成功すること"
    );

    let mut short = vec![0u8; exact_len - 1];
    assert_eq!(
        props.encode(&mut short),
        Err(EncodeError::BufferTooSmall),
        "必要長 - 1 のバッファでは BufferTooSmall であること"
    );
}

#[test]
fn properties_encode_multiple_exact_and_short_buffer() {
    // 複数プロパティのちょうど / -1 境界。
    let mut props = Properties::new();
    props.push(Property::ReceiveMaximum(10));
    props.push(Property::ContentType("text/plain".to_string()));
    props.push(Property::UserProperty("a".to_string(), "1".to_string()));
    let exact_len = props.encoded_len();

    let mut exact = vec![0u8; exact_len];
    assert_eq!(
        props.encode(&mut exact),
        Ok(exact_len),
        "必要長ちょうどのバッファでエンコードに成功すること"
    );

    let mut short = vec![0u8; exact_len - 1];
    assert_eq!(
        props.encode(&mut short),
        Err(EncodeError::BufferTooSmall),
        "必要長 - 1 のバッファでは BufferTooSmall であること"
    );
}

#[test]
fn debug_masks_authentication_data() {
    // Debug 出力に AuthenticationData の平文が含まれないこと。
    // Connect の password / Will payload と同じ方針。
    let property = Property::AuthenticationData(vec![0xAB, 0xCD, 0xEF]);
    let debug = format!("{:?}", property);
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("AB"));
    assert!(!debug.contains("CD"));
    assert!(!debug.contains("EF"));
    // Vec<u8> の Debug は十進表示になるため、平文バイト値が出ていないことも確認する。
    assert!(!debug.contains("171"));
    assert!(!debug.contains("205"));
    assert!(!debug.contains("239"));
}
