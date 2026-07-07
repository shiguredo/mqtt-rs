//! MQTT 可変長バイト整数の単体テスト。

use shiguredo_mqtt::codec::variable_byte_integer::VariableByteInteger;
use shiguredo_mqtt::error::{DecodeError, EncodeError};

#[test]
fn overlong_encoding_is_rejected() {
    // 継続バイトを用いて 0 をエンコードした例。
    let buf = [0x80, 0x00];
    assert_eq!(
        VariableByteInteger::decode(&buf),
        Err(DecodeError::MalformedPacket)
    );
}

#[test]
fn maximum_value_boundary() {
    let v = VariableByteInteger(VariableByteInteger::MAX);
    let mut buf = [0u8; 4];
    let len = v
        .encode(&mut buf)
        .expect("可変長バイト整数のエンコードに失敗しました");
    assert_eq!(buf[..len], [0xFF, 0xFF, 0xFF, 0x7F]);
    let (decoded, consumed) =
        VariableByteInteger::decode(&buf[..len]).expect("可変長バイト整数のデコードに失敗しました");
    assert_eq!(decoded, v);
    assert_eq!(consumed, len);
}

#[test]
fn exceeds_max_is_rejected() {
    // 268,435,456 は 5 バイト目が必要なため、この列は不正である。
    let buf = [0x80, 0x80, 0x80, 0x80];
    assert_eq!(
        VariableByteInteger::decode(&buf),
        Err(DecodeError::MalformedPacket)
    );
}

#[test]
fn too_many_continuation_bytes_is_rejected() {
    // 継続ビットがセットされた 5 バイト列。
    let buf = [0x80, 0x80, 0x80, 0x80, 0x00];
    assert_eq!(
        VariableByteInteger::decode(&buf),
        Err(DecodeError::MalformedPacket)
    );
}

// MQTT v5.0 §1.5.5 Table 1-1 / MQTT v3.1.1 §2.2.3 Table 2.4:
// 符号化長の期待値は仕様の Table の固定値としてアサートする。
// encoded_len() から導出しないのは、encoded_len() と encode() が
// 同方向に退行した場合に検出できなくなるためである。
const BOUNDARY_CASES: &[(u32, usize)] = &[
    (0, 1),
    (1, 1),
    (127, 1),
    (128, 2),
    (16_383, 2),
    (16_384, 3),
    (2_097_151, 3),
    (2_097_152, 4),
    (268_435_455, 4),
];

#[test]
fn encoding_length_boundaries() {
    for &(value, expected_len) in BOUNDARY_CASES {
        let v = VariableByteInteger(value);
        let mut buf = vec![0u8; expected_len];
        let len = v
            .encode(&mut buf)
            .expect("代表値のエンコードに成功すること");
        assert_eq!(
            len, expected_len,
            "値 {value} の符号化長が {expected_len} バイトであること"
        );
    }
}

#[test]
fn short_buffer_is_rejected_at_boundaries() {
    // 符号化長の各境界について、必要長 - 1 のバッファで BufferTooSmall が返ることを確認する。
    // 値 0・1・127（1 バイト）の不足側は空バッファを渡す。
    for &(value, expected_len) in BOUNDARY_CASES {
        let v = VariableByteInteger(value);
        let mut buf = vec![0u8; expected_len - 1];
        assert_eq!(
            v.encode(&mut buf),
            Err(EncodeError::BufferTooSmall),
            "値 {value} の不足バッファで BufferTooSmall が返ること"
        );
    }
}

#[test]
fn over_max_is_rejected() {
    // VariableByteInteger::MAX + 1 は表現不可能であり、
    // バッファが十分でも PacketTooLarge が返ることを確認する。
    let v = VariableByteInteger(VariableByteInteger::MAX + 1);
    let mut buf = vec![0u8; 4];
    assert_eq!(
        v.encode(&mut buf),
        Err(EncodeError::PacketTooLarge {
            size: VariableByteInteger::MAX as usize + 1,
            limit: VariableByteInteger::MAX as usize,
        })
    );
}
