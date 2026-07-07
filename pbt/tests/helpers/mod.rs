//! PBT テスト用の共通ストラテジとヘルパ関数。
//!
//! 統合テストのサブモジュールとして各 `tests/prop_*.rs` から
//! `mod helpers;` で読み込まれる。統合テストファイルごとに
//! 使わない関数があると dead_code 警告が出るため、モジュール
//! 冒頭で一括抑制している。

#![allow(dead_code)]

use proptest::prelude::*;
use proptest::sample::select;
use shiguredo_mqtt::codec::qos::QoS;

/// 文字列生成ストラテジ。
///
/// U+0000 とトピックワイルドカード文字（`+`, `#`）を除外した文字列を生成する。
pub fn string_strategy() -> impl Strategy<Value = String> {
    proptest::collection::vec(
        any::<char>().prop_filter(
            "U+0000 とトピックワイルドカード文字を除外",
            |c| *c != '\0' && *c != '+' && *c != '#',
        ),
        0..100,
    )
    .prop_map(|chars| chars.into_iter().collect())
}

/// トピックフィルター用の 1 レベル文字列生成ストラテジ。
///
/// `/`, `+`, `#`, NUL を含まない通常文字列、または単一レベルワイルドカード `+` を生成する。
fn topic_filter_level_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        proptest::collection::vec(
            any::<char>().prop_filter(
                "レベル文字列に /, +, #, NUL は使用できない",
                |c| *c != '\0' && *c != '/' && *c != '+' && *c != '#',
            ),
            0..20,
        )
        .prop_map(|chars| chars.into_iter().collect()),
        Just("+".to_string()),
    ]
}

/// ワイルドカードを含む有効なトピックフィルター生成ストラテジ。
///
/// レベル列を `/` で連結し、確率的に末尾へ `/#` または `#` を付与する。
/// 生成されるフィルターは MQTT 仕様で有効な構文のみを含む。
pub fn topic_filter_strategy() -> impl Strategy<Value = String> {
    (
        proptest::collection::vec(topic_filter_level_strategy(), 0..5),
        prop_oneof![Just(""), Just("/#"), Just("#")],
    )
        .prop_map(|(levels, suffix)| {
            let mut filter = levels.join("/");
            match suffix {
                "#" => {
                    // MQTT v5.0 §4.7.1.2 [MQTT-4.7.1-1] / MQTT v3.1.1 §4.7.1.2 [MQTT-4.7.1-2]: # は単独で最後のレベルにのみ許可される。
                    if !filter.is_empty() {
                        filter.push('/');
                    }
                    filter.push('#');
                }
                "/#" => filter.push_str("/#"),
                _ => {}
            }
            filter
        })
        .prop_filter("Topic Filter は空にできない", |s| !s.is_empty())
}

/// バイナリデータ生成ストラテジ。
pub fn binary_strategy() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..100)
}

/// QoS 生成ストラテジ。
pub fn qos_strategy() -> impl Strategy<Value = QoS> {
    select(&[QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce])
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
