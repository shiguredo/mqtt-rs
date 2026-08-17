//! 共有コーデックプリミティブのプロパティベースラウンドトリップテスト。

use noprop::Ratio;
use shiguredo_mqtt::codec::binary_data::BinaryData;
use shiguredo_mqtt::codec::utf8_string::Utf8String;
use shiguredo_mqtt::codec::variable_byte_integer::VariableByteInteger;

/// 可変長バイト整数の符号化境界。
const VBI_BOUNDARIES: &[usize] = &[0, 1, 127, 128, 16_383, 16_384, 2_097_151, 268_435_455];

#[test]
fn variable_byte_integer_roundtrip() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        // 境界値に 1/5 の確率で入り、それ以外は全域から一様に引く。
        let value = noprop::sample_with_boundaries(ctx, VBI_BOUNDARIES, Ratio::one_nth(5), |ctx| {
            noprop::sample_usize_in(ctx, 0..=268_435_455)
        }) as u32;

        let val = VariableByteInteger(value);
        let mut buf = [0u8; 4];
        let len = val
            .encode(&mut buf)
            .expect("可変長バイト整数のエンコードに失敗");
        let (decoded, consumed) =
            VariableByteInteger::decode(&buf[..len]).expect("可変長バイト整数のデコードに失敗");
        assert_eq!(decoded, val);
        assert_eq!(consumed, len);
        Ok(())
    })?;
    Ok(())
}

#[test]
fn utf8_string_roundtrip() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        // 境界値（空・最大長）に 1/5 の確率で入り、それ以外はランダム文字列を引く。
        let s = noprop::sample_with_boundaries(
            ctx,
            &["".to_string(), "a".repeat(u16::MAX as usize)],
            Ratio::one_nth(5),
            |ctx| {
                let len = noprop::sample_usize_in(ctx, 0..1000);
                let mut s = String::with_capacity(len);
                for _ in 0..len {
                    // U+0000 だけを除外する。受容率は 0.999999 を超え、
                    // max_attempts=8 で枯渇する確率は実質ゼロである。
                    let c = noprop::sample_with_rejection(ctx, 8, |ctx| {
                        let c = noprop::sample_char(ctx);
                        (c != '\0').then_some(c)
                    });
                    s.push(c);
                }
                s
            },
        );

        let val = Utf8String(s.clone());
        let mut buf = vec![0u8; 2 + s.len() + 10];
        let len = val
            .encode(&mut buf)
            .expect("UTF-8 文字列のエンコードに失敗");
        let (decoded, consumed) =
            Utf8String::decode(&buf[..len]).expect("UTF-8 文字列のデコードに失敗");
        assert_eq!(decoded, val);
        assert_eq!(consumed, len);
        Ok(())
    })?;
    Ok(())
}

#[test]
fn binary_data_roundtrip() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let len = noprop::sample_usize_in(ctx, 0..=1000);
        let data = noprop::sample_bytes_vec(ctx, len);

        let val = BinaryData(data.clone());
        let mut buf = vec![0u8; 2 + data.len() + 10];
        let len = val
            .encode(&mut buf)
            .expect("バイナリデータのエンコードに失敗");
        let (decoded, consumed) =
            BinaryData::decode(&buf[..len]).expect("バイナリデータのデコードに失敗");
        assert_eq!(decoded, val);
        assert_eq!(consumed, len);
        Ok(())
    })?;
    Ok(())
}
