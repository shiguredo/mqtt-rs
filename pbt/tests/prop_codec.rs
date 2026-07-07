//! 共有コーデックプリミティブのプロパティベースラウンドトリップテスト。

use proptest::prelude::*;
use shiguredo_mqtt::codec::binary_data::BinaryData;
use shiguredo_mqtt::codec::utf8_string::Utf8String;
use shiguredo_mqtt::codec::variable_byte_integer::VariableByteInteger;

proptest! {
    #[test]
    fn variable_byte_integer_roundtrip(
        v in prop_oneof![
            Just(0u32),
            Just(1u32),
            Just(127u32),
            Just(128u32),
            Just(16_383u32),
            Just(16_384u32),
            Just(2_097_151u32),
            Just(268_435_455u32),
            0u32..=268_435_455u32,
        ],
    ) {
        let val = VariableByteInteger(v);
        let mut buf = [0u8; 4];
        let len = val.encode(&mut buf).expect("可変長バイト整数のエンコードに失敗");
        let (decoded, consumed) = VariableByteInteger::decode(&buf[..len]).expect("可変長バイト整数のデコードに失敗");
        prop_assert_eq!(decoded, val);
        prop_assert_eq!(consumed, len);
    }

    #[test]
    fn utf8_string_roundtrip(
        s in prop_oneof![
            Just(String::new()),
            Just("a".repeat(u16::MAX as usize)),
            proptest::collection::vec(
                any::<char>().prop_filter("U+0000 を除外", |c| *c != '\0'),
                0..1000,
            ).prop_map(|v| v.into_iter().collect::<String>()),
        ],
    ) {
        let val = Utf8String(s.clone());
        let mut buf = vec![0u8; 2 + s.len() + 10];
        let len = val.encode(&mut buf).expect("UTF-8 文字列のエンコードに失敗");
        let (decoded, consumed) = Utf8String::decode(&buf[..len]).expect("UTF-8 文字列のデコードに失敗");
        prop_assert_eq!(decoded, val);
        prop_assert_eq!(consumed, len);
    }

    #[test]
    fn binary_data_roundtrip(data in proptest::collection::vec(any::<u8>(), 0..1000)) {
        let val = BinaryData(data.clone());
        let mut buf = vec![0u8; 2 + data.len() + 10];
        let len = val.encode(&mut buf).expect("バイナリデータのエンコードに失敗");
        let (decoded, consumed) = BinaryData::decode(&buf[..len]).expect("バイナリデータのデコードに失敗");
        prop_assert_eq!(decoded, val);
        prop_assert_eq!(consumed, len);
    }
}
