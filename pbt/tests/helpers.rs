//! PBT テスト用の共通サンプラ関数。
//!
//! 統合テストのサブモジュールとして各 `tests/prop_*.rs` から
//! `mod helpers;` で読み込まれる。統合テストファイルごとに
//! 使わない関数があると dead_code 警告が出るため、モジュール
//! 冒頭で一括抑制している。

#![allow(dead_code)]

use noprop::TestCaseContext;
use shiguredo_mqtt::codec::qos::QoS;

/// 文字列サンプラ。
///
/// U+0000 とトピックワイルドカード文字（`+`, `#`）を除外した文字列を生成する。
/// 長さは 0..100。
pub fn sample_string(ctx: &mut TestCaseContext) -> String {
    sample_string_in(ctx, 0, 100)
}

/// 文字列サンプラ（長さ指定）。
///
/// U+0000 とトピックワイルドカード文字（`+`, `#`）を除外した文字列を生成する。
/// 長さは `min..=max` から一様に引く。
pub fn sample_string_in(ctx: &mut TestCaseContext, min: usize, max: usize) -> String {
    let len = noprop::sample_usize_in(ctx, min..=max);
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        // 除外文字は全 Unicode スカラ値に対して 3 種のみであり、
        // 受容率は 0.999997 を超える。max_attempts=8 で枯渇する確率は
        // 実質ゼロであり、受容率に基づく上限として妥当である。
        let c = noprop::sample_with_rejection(ctx, 8, |ctx| {
            let c = noprop::sample_char(ctx);
            (c != '\0' && c != '+' && c != '#').then_some(c)
        });
        s.push(c);
    }
    s
}

/// トピックフィルター用の 1 レベル文字列サンプラ。
///
/// `/`, `+`, `#`, NUL を含まない通常文字列、または単一レベルワイルドカード `+` を生成する。
pub fn sample_topic_filter_level(ctx: &mut TestCaseContext) -> String {
    if noprop::sample_ratio(ctx, noprop::Ratio::one_nth(8)) {
        return "+".to_string();
    }
    let len = noprop::sample_usize_in(ctx, 0..20);
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        // 除外文字は 4 種のみであり、受容率は 0.999996 を超える。
        // レベル文字列の長さ上限 20 と合わせて max_attempts=8 で十分である。
        let c = noprop::sample_with_rejection(ctx, 8, |ctx| {
            let c = noprop::sample_char(ctx);
            (c != '\0' && c != '/' && c != '+' && c != '#').then_some(c)
        });
        s.push(c);
    }
    s
}

/// ワイルドカードを含む有効なトピックフィルターサンプラ。
///
/// レベル列を `/` で連結し、確率的に末尾へ `/#` または `#` を付与する。
/// 生成されるフィルターは MQTT 仕様で有効な構文のみを含む。
pub fn sample_topic_filter(ctx: &mut TestCaseContext) -> String {
    // フィルターが空になるケース（レベルがすべて空かつ suffix なし）だけを
    // 棄却して再生成する。空になる確率は 0.3% 程度であり、
    // max_attempts=8 で枯渇する確率は実質ゼロである。
    noprop::sample_with_rejection(ctx, 8, |ctx| {
        let filter = sample_topic_filter_unchecked(ctx);
        (!filter.is_empty()).then_some(filter)
    })
}

/// 空文字列になり得るトピックフィルターの内部サンプラ。
fn sample_topic_filter_unchecked(ctx: &mut TestCaseContext) -> String {
    let suffix = noprop::sample_choice(ctx, &["", "/#", "#"]);
    let level_count = noprop::sample_usize_in(ctx, 0..5);

    let mut filter = String::new();
    for i in 0..level_count {
        if i > 0 {
            filter.push('/');
        }
        filter.push_str(&sample_topic_filter_level(ctx));
    }
    match suffix {
        "#" => {
            // MQTT v5.0 §4.7.1.2 [MQTT-4.7.1-1] / MQTT v3.1.1 §4.7.1.2 [MQTT-4.7.1-2]:
            // # は単独で最後のレベルにのみ許可される。
            if !filter.is_empty() {
                filter.push('/');
            }
            filter.push('#');
        }
        "/#" => filter.push_str("/#"),
        _ => {}
    }
    filter
}

/// バイナリデータサンプラ。長さは 0..100。
pub fn sample_binary(ctx: &mut TestCaseContext) -> Vec<u8> {
    let len = noprop::sample_usize_in(ctx, 0..100);
    noprop::sample_bytes_vec(ctx, len)
}

/// QoS サンプラ。
pub fn sample_qos(ctx: &mut TestCaseContext) -> QoS {
    noprop::sample_choice(ctx, &[QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce])
}

/// Option サンプラ。確率 1/2 で Some(value)、それ以外は None を返す。
pub fn sample_option<T, F>(ctx: &mut TestCaseContext, f: F) -> Option<T>
where
    F: FnOnce(&mut TestCaseContext) -> T,
{
    if noprop::sample_bool(ctx) {
        Some(f(ctx))
    } else {
        None
    }
}

/// 残り長さの符号化長が変化する境界値。
///
/// MQTT v5.0 §1.5.5 Table 1-1 / MQTT v3.1.1 §2.2.3 Table 2.4 に基づく。
/// 127→128 で 1→2 バイト、16,383→16,384 で 2→3 バイトに変化する。
pub const REMAINING_LENGTH_BOUNDARIES: &[usize] = &[127, 128, 16_383, 16_384];

/// 目標の残り長さからペイロード長を逆算する。
///
/// `checked_sub` で減算し、オーバーヘッドが目標を超える場合は
/// テスト実行時に必ず失敗させる（将来オーバーヘッドが増えた場合に気付けるようにする）。
pub fn payload_len_for_remaining_length(target: usize, overhead: usize) -> usize {
    target
        .checked_sub(overhead)
        .expect("オーバーヘッドが残り長さの境界値を超えないこと")
}
