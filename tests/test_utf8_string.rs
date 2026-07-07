//! MQTT UTF-8 エンコード文字列の単体テスト。

use shiguredo_mqtt::codec::utf8_string::Utf8String;
use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};

#[test]
fn invalid_utf8_is_rejected() {
    let buf = [0x00, 0x02, 0xC0, 0x80];
    assert_eq!(Utf8String::decode(&buf), Err(DecodeError::InvalidUtf8));
}

#[test]
fn insufficient_data_is_rejected() {
    let buf = [0x00];
    assert_eq!(Utf8String::decode(&buf), Err(DecodeError::InsufficientData));
}

#[test]
fn null_in_decode_is_rejected() {
    let buf = [0x00, 0x01, 0x00];
    assert_eq!(Utf8String::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn null_in_encode_is_rejected() {
    let s = Utf8String("a\0b".to_string());
    let mut buf = [0u8; 16];
    assert_eq!(
        s.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::NullInUtf8String,
        })
    );
}

#[test]
fn surrogate_codepoint_in_decode_is_rejected() {
    // U+D800 は UTF-8 では 0xED 0xA0 0x80 となる。
    let buf = [0x00, 0x03, 0xED, 0xA0, 0x80];
    assert_eq!(Utf8String::decode(&buf), Err(DecodeError::InvalidUtf8));
}

#[test]
fn max_length_with_exact_buffer_succeeds() {
    // MQTT v5.0 §1.5.4 / MQTT v3.1.1 §1.5.3:
    // UTF-8 文字列の最大長は 65,535 バイトである。
    // 必要長ちょうどのバッファでエンコードが成功することを確認する。
    let s = Utf8String("a".repeat(u16::MAX as usize));
    let mut buf = vec![0u8; 2 + u16::MAX as usize];
    let len = s
        .encode(&mut buf)
        .expect("最大長の文字列のエンコードに成功すること");
    assert_eq!(len, 2 + u16::MAX as usize);
}

#[test]
fn max_length_with_short_buffer_is_rejected() {
    // 必要長より 1 バイト少ないバッファでは BufferTooSmall が返る。
    let s = Utf8String("a".repeat(u16::MAX as usize));
    let mut buf = vec![0u8; 2 + u16::MAX as usize - 1];
    assert_eq!(s.encode(&mut buf), Err(EncodeError::BufferTooSmall));
}

#[test]
fn over_max_length_is_rejected() {
    // MQTT v5.0 §1.5.4 / MQTT v3.1.1 §1.5.3:
    // 65,536 バイトは長さフィールド（2 バイト）の上限を超えるため拒否される。
    // バッファが十分でも長さチェックが先に行われる。
    let s = Utf8String("a".repeat(u16::MAX as usize + 1));
    let mut buf = vec![0u8; 2 + u16::MAX as usize + 1];
    assert_eq!(
        s.encode(&mut buf),
        Err(EncodeError::PacketTooLarge {
            size: u16::MAX as usize + 1,
            limit: u16::MAX as usize,
        })
    );
}

#[test]
fn short_string_with_exact_buffer_succeeds() {
    // 長さフィールド上限とは独立した必要長チェック経路を検証する。
    let s = Utf8String("a".repeat(10));
    let mut buf = vec![0u8; 12];
    let len = s
        .encode(&mut buf)
        .expect("短い文字列のエンコードに成功すること");
    assert_eq!(len, 12);
}

#[test]
fn short_string_with_short_buffer_is_rejected() {
    // 短い文字列でも 1 バイト不足で BufferTooSmall が返ることを確認する。
    let s = Utf8String("a".repeat(10));
    let mut buf = vec![0u8; 11];
    assert_eq!(s.encode(&mut buf), Err(EncodeError::BufferTooSmall));
}
