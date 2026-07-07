//! MQTT バイナリデータの単体テスト。

use shiguredo_mqtt::codec::binary_data::BinaryData;
use shiguredo_mqtt::error::{DecodeError, EncodeError};

#[test]
fn empty_data_roundtrips() {
    let data = BinaryData(Vec::new());
    let mut buf = [0u8; 2];
    let len = data
        .encode(&mut buf)
        .expect("空データのエンコードに失敗しました");
    assert_eq!(len, 2);
    assert_eq!(buf, [0x00, 0x00]);

    let (decoded, consumed) = BinaryData::decode(&buf).expect("空データのデコードに失敗しました");
    assert_eq!(consumed, 2);
    assert_eq!(decoded, data);
}

#[test]
fn insufficient_data_is_rejected() {
    let buf = [0x00, 0x05, 0x01, 0x02];
    assert_eq!(BinaryData::decode(&buf), Err(DecodeError::InsufficientData));
}

#[test]
fn max_length_with_exact_buffer_succeeds() {
    // MQTT v5.0 §1.5.6:
    // Binary Data の最大長は 65,535 バイトである。
    // 必要長ちょうどのバッファでエンコードが成功することを確認する。
    let data = BinaryData(vec![0xAB; u16::MAX as usize]);
    let mut buf = vec![0u8; 2 + u16::MAX as usize];
    let len = data
        .encode(&mut buf)
        .expect("最大長のデータのエンコードに成功すること");
    assert_eq!(len, 2 + u16::MAX as usize);
}

#[test]
fn max_length_with_short_buffer_is_rejected() {
    // 必要長より 1 バイト少ないバッファでは BufferTooSmall が返る。
    let data = BinaryData(vec![0xAB; u16::MAX as usize]);
    let mut buf = vec![0u8; 2 + u16::MAX as usize - 1];
    assert_eq!(data.encode(&mut buf), Err(EncodeError::BufferTooSmall));
}

#[test]
fn over_max_length_is_rejected() {
    // MQTT v5.0 §1.5.6:
    // 65,536 バイトは長さフィールド（2 バイト）の上限を超えるため拒否される。
    // バッファが十分でも長さチェックが先に行われる。
    let data = BinaryData(vec![0xAB; u16::MAX as usize + 1]);
    let mut buf = vec![0u8; 2 + u16::MAX as usize + 1];
    assert_eq!(
        data.encode(&mut buf),
        Err(EncodeError::PacketTooLarge {
            size: u16::MAX as usize + 1,
            limit: u16::MAX as usize,
        })
    );
}

#[test]
fn short_data_with_exact_buffer_succeeds() {
    // 長さフィールド上限とは独立した必要長チェック経路を検証する。
    let data = BinaryData(vec![0xAB; 10]);
    let mut buf = vec![0u8; 12];
    let len = data
        .encode(&mut buf)
        .expect("短いデータのエンコードに成功すること");
    assert_eq!(len, 12);
}

#[test]
fn short_data_with_short_buffer_is_rejected() {
    // 短いデータでも 1 バイト不足で BufferTooSmall が返ることを確認する。
    let data = BinaryData(vec![0xAB; 10]);
    let mut buf = vec![0u8; 11];
    assert_eq!(data.encode(&mut buf), Err(EncodeError::BufferTooSmall));
}
