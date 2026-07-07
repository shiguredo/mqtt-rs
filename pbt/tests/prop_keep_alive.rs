//! KeepAlive のプロパティベーステスト。

use proptest::prelude::*;
use shiguredo_mqtt::state::keep_alive::KeepAlive;

proptest! {
    /// Keep Alive = 0 の場合は常に PINGREQ 不要。
    #[test]
    fn keep_alive_zero_never_requires_pingreq(now in 0u64..=1_000_000_000u64) {
        let ka = KeepAlive::new(0);
        prop_assert!(!ka.should_send_pingreq(now));
    }

    /// Keep Alive = 0 の場合は、PINGRESP 待ちが立っていても常にタイムアウトしない
    /// （`Keep Alive = 0` 分岐が `pingreq_sent_at.is_some()` より優先されるため）。
    #[test]
    fn keep_alive_zero_never_times_out_even_when_awaiting_pingresp(
        t in 0u64..=1_000_000u64,
        now in 0u64..=1_000_000u64,
    ) {
        let mut ka = KeepAlive::new(0);
        ka.pingreq_sent(t);
        prop_assert!(!ka.has_timed_out(now));
    }

    /// 最終アクティビティから Keep Alive 間隔未満では PINGREQ 不要、
    /// 間隔以上では必要になる。
    ///
    /// MQTT v5.0 §3.1.2.10 / MQTT v3.1.1 §3.1.2.10:
    /// クライアントは Keep Alive 間隔を超えてパケットを送らない状態を避ける。
    #[test]
    fn should_send_pingreq_respects_interval_since_activity(
        keep_alive in 1u16..=3600u16,
        activity_at in 0u64..=1_000_000u64,
        offset in 0u64..=1_000_000u64,
    ) {
        let mut ka = KeepAlive::new(keep_alive);
        ka.activity(activity_at);
        let interval_ms = (keep_alive as u64) * 1000;
        let now = activity_at.saturating_add(offset);
        if offset < interval_ms {
            prop_assert!(!ka.should_send_pingreq(now));
        } else {
            prop_assert!(ka.should_send_pingreq(now));
        }
    }

    /// アクティビティ直後は PINGREQ 不要。
    #[test]
    fn no_pingreq_immediately_after_activity(
        keep_alive in 1u16..=3600u16,
        now in 0u64..=1_000_000u64,
    ) {
        let mut ka = KeepAlive::new(keep_alive);
        ka.activity(now);
        prop_assert!(!ka.should_send_pingreq(now));
    }

    /// PINGREQ を一度も送っていない状態では、任意の `now` でタイムアウトしない。
    #[test]
    fn no_timeout_when_pingreq_never_sent(
        keep_alive in 1u16..=3600u16,
        now in 0u64..=1_000_000u64,
    ) {
        let ka = KeepAlive::new(keep_alive);
        prop_assert!(!ka.has_timed_out(now));
    }

    /// PINGREQ 送信後、Keep Alive 間隔（ms）以降で常にタイムアウトが成立する。
    #[test]
    fn timeout_after_keep_alive_since_pingreq(
        keep_alive in 1u16..=3600u16,
        offset in 0u64..=1_000_000u64,
    ) {
        let mut ka = KeepAlive::new(keep_alive);
        ka.pingreq_sent(0);
        let deadline_ms = (keep_alive as u64) * 1000;
        prop_assert!(ka.has_timed_out(deadline_ms.saturating_add(offset)));
    }

    /// PINGREQ 送信後、応答期限未満では常にタイムアウトしない。
    /// timeout_after_keep_alive_since_pingreq と対を成す境界検証（負側）。
    #[test]
    fn no_timeout_before_keep_alive_since_pingreq(
        (keep_alive, now) in (1u16..=3600u16).prop_flat_map(|k| {
            let deadline_ms = (k as u64) * 1000;
            (Just(k), 0u64..deadline_ms)
        }),
    ) {
        let mut ka = KeepAlive::new(keep_alive);
        ka.pingreq_sent(0);
        prop_assert!(!ka.has_timed_out(now));
    }

    /// PINGRESP 受信後は、任意の `now` でタイムアウトしない。
    #[test]
    fn no_timeout_after_pingresp_received(
        keep_alive in 1u16..=3600u16,
        t in 0u64..=1_000_000u64,
        now in 0u64..=1_000_000u64,
    ) {
        let mut ka = KeepAlive::new(keep_alive);
        ka.pingreq_sent(t);
        ka.pingresp_received();
        prop_assert!(!ka.has_timed_out(now));
        prop_assert!(!ka.is_awaiting_pingresp());
    }

    /// 連続した pingreq_sent は最新の送信時刻だけを基準にする。
    #[test]
    fn pingreq_sent_uses_latest_send_time(
        keep_alive in 1u16..=3600u16,
        first in 0u64..=500_000u64,
        delta in 1u64..=500_000u64,
    ) {
        let mut ka = KeepAlive::new(keep_alive);
        ka.pingreq_sent(first);
        let second = first + delta;
        ka.pingreq_sent(second);
        let deadline_ms = (keep_alive as u64) * 1000;
        prop_assert!(!ka.has_timed_out(second + deadline_ms - 1));
        prop_assert!(ka.has_timed_out(second + deadline_ms));
        prop_assert!(ka.is_awaiting_pingresp());
    }

    /// PINGRESP 待ち中に Keep Alive を短縮すると、新しい間隔でタイムアウト判定される。
    #[test]
    fn shortened_keep_alive_updates_timeout_deadline(
        original in 2u16..=3600u16,
        shortened in 1u16..=3600u16,
    ) {
        prop_assume!(shortened < original);
        let mut ka = KeepAlive::new(original);
        ka.pingreq_sent(0);
        ka.set_keep_alive(shortened);
        let new_deadline = (shortened as u64) * 1000;
        prop_assert!(!ka.has_timed_out(new_deadline - 1));
        prop_assert!(ka.has_timed_out(new_deadline));
    }

    /// 時刻が巻き戻った（`now < pingreq_sent_at`）場合は `saturating_sub` により
    /// タイムアウトしない。`t` から `now` の範囲を導出することで棄却率をゼロに保つ。
    #[test]
    fn no_timeout_when_clock_regresses(
        keep_alive in 1u16..=3600u16,
        (t, now) in (1u64..=1_000u64).prop_flat_map(|t| (Just(t), 0u64..t)),
    ) {
        let mut ka = KeepAlive::new(keep_alive);
        ka.pingreq_sent(t);
        prop_assert!(!ka.has_timed_out(now));
    }

    /// PINGREQ 送信後に Keep Alive を 0 に切り替えると、Keep Alive 機構が
    /// 無効化され、任意の `now` でタイムアウトしない。
    #[test]
    fn no_timeout_after_set_keep_alive_zero(
        keep_alive in 1u16..=3600u16,
        now in 0u64..=1_000_000u64,
    ) {
        let mut ka = KeepAlive::new(keep_alive);
        ka.pingreq_sent(0);
        ka.set_keep_alive(0);
        prop_assert!(!ka.has_timed_out(now));
    }

    /// reset 後は Keep Alive 無効・PINGRESP 待ちなしに戻る。
    #[test]
    fn reset_clears_all_state(
        keep_alive in 1u16..=3600u16,
        t in 0u64..=1_000_000u64,
        now in 0u64..=1_000_000u64,
    ) {
        let mut ka = KeepAlive::new(keep_alive);
        ka.pingreq_sent(t);
        ka.reset();
        prop_assert_eq!(ka.keep_alive_secs(), 0);
        prop_assert!(!ka.is_awaiting_pingresp());
        prop_assert!(!ka.should_send_pingreq(now));
        prop_assert!(!ka.has_timed_out(now));
    }

    /// PINGRESP 待ちが立っていない状態での pingresp_received は副作用がない。
    #[test]
    fn pingresp_received_is_idempotent_when_not_awaiting(
        keep_alive in 0u16..=3600u16,
    ) {
        let mut ka = KeepAlive::new(keep_alive);
        prop_assert!(!ka.is_awaiting_pingresp());
        ka.pingresp_received();
        prop_assert!(!ka.is_awaiting_pingresp());
        prop_assert_eq!(ka.keep_alive_secs(), keep_alive);
    }
}
