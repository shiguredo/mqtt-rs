//! MQTT トピックフィルターの構文検証。

use alloc::vec::Vec;

/// トピックフィルターの構文を検証する。
///
/// MQTT v5.0 §4.7.1 (Topic wildcards) [MQTT-4.7.1-1][MQTT-4.7.1-2] /
/// MQTT v3.1.1 §4.7.1 (Topic wildcards) [MQTT-4.7.1-2][MQTT-4.7.1-3] のワイルドカード規則に従う。
/// `#` は単独で最後のレベルにのみ許可され、`+` は単独で 1 つのレベル全体を占める場合のみ許可される。
///
/// 本番コードから利用される内部ユーティリティ。
#[doc(hidden)]
pub fn is_valid_topic_filter(filter: &str) -> bool {
    if filter.is_empty() {
        return false;
    }
    let levels: Vec<&str> = filter.split('/').collect();
    for (i, level) in levels.iter().enumerate() {
        if level.contains('#') {
            // # は単独で最後のレベルにのみ許可される。
            if *level != "#" || i != levels.len() - 1 {
                return false;
            }
        }
        if level.contains('+') && *level != "+" {
            // + は単独でレベル全体を占める場合のみ許可される。
            return false;
        }
    }
    true
}

/// MQTT v5.0 のトピックフィルター構文を検証する。
///
/// 通常のワイルドカード規則に加え、`$share/{ShareName}/{filter}` 形式の共有サブスクリプションを検証する。
/// MQTT v5.0 §4.8.2 を参照。
pub(crate) fn is_valid_v5_topic_filter(filter: &str) -> bool {
    if let Some(rest) = filter.strip_prefix("$share/") {
        // `$share/{ShareName}/{filter}` の形式。
        // ShareName とフィルターを `/` で分離する。
        if let Some(slash_pos) = rest.find('/') {
            let share_name = &rest[..slash_pos];
            let actual_filter = &rest[slash_pos + 1..];
            // MQTT v5.0 §4.8.2 [MQTT-4.8.2-1][MQTT-4.8.2-2]: ShareName は 1 文字以上で、`/`, `+`, `#` を含んではならない。
            // 本実装では最初の `/` で ShareName とフィルターを分離するため、`/` は構文上含まれない。
            if share_name.is_empty() || share_name.contains('+') || share_name.contains('#') {
                return false;
            }
            return is_valid_topic_filter(actual_filter);
        }
        // `$share/` の後ろに `/` がない。
        return false;
    }
    is_valid_topic_filter(filter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_topic_filters() {
        assert!(is_valid_topic_filter("a/b"));
        assert!(is_valid_topic_filter("a/+/b"));
        assert!(is_valid_topic_filter("a/#"));
        assert!(is_valid_topic_filter("#"));
        assert!(is_valid_topic_filter("+"));
        assert!(is_valid_topic_filter("+/a/#"));
    }

    #[test]
    fn empty_topic_levels_are_allowed() {
        // MQTT 仕様ではトピックレベルが空（連続する `/` や先頭／末尾の `/`）でも構文上許容される。
        assert!(is_valid_topic_filter("/a"));
        assert!(is_valid_topic_filter("a/"));
        assert!(is_valid_topic_filter("a//b"));
        assert!(is_valid_topic_filter("/"));
        assert!(is_valid_topic_filter("///"));
        assert!(is_valid_topic_filter("/#"));
        assert!(is_valid_topic_filter("+/"));
    }

    #[test]
    fn invalid_topic_filters() {
        assert!(!is_valid_topic_filter(""));
        assert!(!is_valid_topic_filter("a+"));
        assert!(!is_valid_topic_filter("a#"));
        assert!(!is_valid_topic_filter("#/a"));
        assert!(!is_valid_topic_filter("a/+/b#"));
    }

    #[test]
    fn valid_v5_shared_subscriptions() {
        // 通常のトピックフィルターも受け入れる。
        assert!(is_valid_v5_topic_filter("a/b"));
        assert!(is_valid_v5_topic_filter("a/+/b"));
        assert!(is_valid_v5_topic_filter("a/#"));
        // 有効な共有サブスクリプション。
        assert!(is_valid_v5_topic_filter("$share/group/a/b"));
        assert!(is_valid_v5_topic_filter("$share/mygroup/#"));
        assert!(is_valid_v5_topic_filter("$share/G1/+/status"));
        // フィルター部に空レベルを含む共有サブスクリプションも許容される。
        assert!(is_valid_v5_topic_filter("$share/group//a"));
        assert!(is_valid_v5_topic_filter("$share/group/a//b"));
    }

    #[test]
    fn invalid_v5_shared_subscriptions() {
        // ShareName が空。
        assert!(!is_valid_v5_topic_filter("$share//a/b"));
        // `$share/` の後ろに `/` がない。
        assert!(!is_valid_v5_topic_filter("$share/group"));
        // ShareName に `+` を含む。
        assert!(!is_valid_v5_topic_filter("$share/gr+up/a/b"));
        // ShareName に `#` を含む。
        assert!(!is_valid_v5_topic_filter("$share/gr#up/a/b"));
        // 無効なトピックフィルター（ワイルドカードの誤用）。
        assert!(!is_valid_v5_topic_filter("$share/group/a#/b"));
    }
}
