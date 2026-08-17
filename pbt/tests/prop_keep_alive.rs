//! KeepAlive のプロパティベーステスト。

use shiguredo_mqtt::state::keep_alive::KeepAlive;

/// Keep Alive = 0 の場合は常に PINGREQ 不要。
#[test]
fn keep_alive_zero_never_requires_pingreq() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let now = noprop::sample_u64_in(ctx, 0..=1_000_000_000);
        let ka = KeepAlive::new(0);
        assert!(!ka.should_send_pingreq(now));
        Ok(())
    })?;
    Ok(())
}

/// Keep Alive = 0 の場合は、PINGRESP 待ちが立っていても常にタイムアウトしない
/// （`Keep Alive = 0` 分岐が `pingreq_sent_at.is_some()` より優先されるため）。
#[test]
fn keep_alive_zero_never_times_out_even_when_awaiting_pingresp() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let t = noprop::sample_u64_in(ctx, 0..=1_000_000);
        let now = noprop::sample_u64_in(ctx, 0..=1_000_000);
        let mut ka = KeepAlive::new(0);
        ka.pingreq_sent(t);
        assert!(!ka.has_timed_out(now));
        Ok(())
    })?;
    Ok(())
}

/// 最終アクティビティから Keep Alive 間隔未満では PINGREQ 不要、
/// 間隔以上では必要になる。
///
/// MQTT v5.0 §3.1.2.10 / MQTT v3.1.1 §3.1.2.10:
/// クライアントは Keep Alive 間隔を超えてパケットを送らない状態を避ける。
#[test]
fn should_send_pingreq_respects_interval_since_activity() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let pingreq_needed = std::cell::Cell::new(0usize);
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let keep_alive = noprop::sample_usize_in(ctx, 1..=3600) as u16;
        let activity_at = noprop::sample_u64_in(ctx, 0..=1_000_000);
        let offset = noprop::sample_u64_in(ctx, 0..=1_000_000);

        let mut ka = KeepAlive::new(keep_alive);
        ka.activity(activity_at);
        let interval_ms = (keep_alive as u64) * 1000;
        let now = activity_at.saturating_add(offset);
        if offset < interval_ms {
            assert!(!ka.should_send_pingreq(now));
        } else {
            assert!(ka.should_send_pingreq(now));
            pingreq_needed.set(pingreq_needed.get() + 1);
        }
        Ok(())
    })?;

    // offset >= interval のケースが一度も生成されないと PINGREQ 必要側の
    // 分岐が空振りになるため、ゲートで到達を保証する。
    assert!(
        pingreq_needed.get() > 0,
        "PINGREQ が必要になる分岐が一度も検証されなかった\n{runner}"
    );
    Ok(())
}

/// アクティビティ直後は PINGREQ 不要。
#[test]
fn no_pingreq_immediately_after_activity() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let keep_alive = noprop::sample_usize_in(ctx, 1..=3600) as u16;
        let now = noprop::sample_u64_in(ctx, 0..=1_000_000);
        let mut ka = KeepAlive::new(keep_alive);
        ka.activity(now);
        assert!(!ka.should_send_pingreq(now));
        Ok(())
    })?;
    Ok(())
}

/// PINGREQ を一度も送っていない状態では、任意の `now` でタイムアウトしない。
#[test]
fn no_timeout_when_pingreq_never_sent() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let keep_alive = noprop::sample_usize_in(ctx, 1..=3600) as u16;
        let now = noprop::sample_u64_in(ctx, 0..=1_000_000);
        let ka = KeepAlive::new(keep_alive);
        assert!(!ka.has_timed_out(now));
        Ok(())
    })?;
    Ok(())
}

/// PINGREQ 送信後、Keep Alive 間隔（ms）以降で常にタイムアウトが成立する。
#[test]
fn timeout_after_keep_alive_since_pingreq() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let keep_alive = noprop::sample_usize_in(ctx, 1..=3600) as u16;
        let offset = noprop::sample_u64_in(ctx, 0..=1_000_000);
        let mut ka = KeepAlive::new(keep_alive);
        ka.pingreq_sent(0);
        let deadline_ms = (keep_alive as u64) * 1000;
        assert!(ka.has_timed_out(deadline_ms.saturating_add(offset)));
        Ok(())
    })?;
    Ok(())
}

/// PINGREQ 送信後、応答期限未満では常にタイムアウトしない。
/// timeout_after_keep_alive_since_pingreq と対を成す境界検証（負側）。
#[test]
fn no_timeout_before_keep_alive_since_pingreq() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let keep_alive = noprop::sample_usize_in(ctx, 1..=3600) as u16;
        let deadline_ms = (keep_alive as u64) * 1000;
        // 応答期限未満を valid-by-construction で生成する（棄却なし）。
        let now = noprop::sample_u64_in(ctx, 0..deadline_ms);

        let mut ka = KeepAlive::new(keep_alive);
        ka.pingreq_sent(0);
        assert!(!ka.has_timed_out(now));
        Ok(())
    })?;
    Ok(())
}

/// PINGRESP 受信後は、任意の `now` でタイムアウトしない。
#[test]
fn no_timeout_after_pingresp_received() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let keep_alive = noprop::sample_usize_in(ctx, 1..=3600) as u16;
        let t = noprop::sample_u64_in(ctx, 0..=1_000_000);
        let now = noprop::sample_u64_in(ctx, 0..=1_000_000);
        let mut ka = KeepAlive::new(keep_alive);
        ka.pingreq_sent(t);
        ka.pingresp_received();
        assert!(!ka.has_timed_out(now));
        assert!(!ka.is_awaiting_pingresp());
        Ok(())
    })?;
    Ok(())
}

/// 連続した pingreq_sent は最新の送信時刻だけを基準にする。
#[test]
fn pingreq_sent_uses_latest_send_time() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let keep_alive = noprop::sample_usize_in(ctx, 1..=3600) as u16;
        let first = noprop::sample_u64_in(ctx, 0..=500_000);
        let delta = noprop::sample_u64_in(ctx, 1..=500_000);
        let mut ka = KeepAlive::new(keep_alive);
        ka.pingreq_sent(first);
        let second = first + delta;
        ka.pingreq_sent(second);
        let deadline_ms = (keep_alive as u64) * 1000;
        assert!(!ka.has_timed_out(second + deadline_ms - 1));
        assert!(ka.has_timed_out(second + deadline_ms));
        assert!(ka.is_awaiting_pingresp());
        Ok(())
    })?;
    Ok(())
}

/// PINGRESP 待ち中に Keep Alive を短縮すると、新しい間隔でタイムアウト判定される。
#[test]
fn shortened_keep_alive_updates_timeout_deadline() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let original = noprop::sample_usize_in(ctx, 2..=3600) as u16;
        // 短縮後の間隔を valid-by-construction で original 未満に制約する（棄却なし）。
        let shortened = noprop::sample_usize_in(ctx, 1..=(original - 1) as usize) as u16;

        let mut ka = KeepAlive::new(original);
        ka.pingreq_sent(0);
        ka.set_keep_alive(shortened);
        let new_deadline = (shortened as u64) * 1000;
        assert!(!ka.has_timed_out(new_deadline - 1));
        assert!(ka.has_timed_out(new_deadline));
        Ok(())
    })?;
    Ok(())
}

/// 時刻が巻き戻った（`now < pingreq_sent_at`）場合は `saturating_sub` により
/// タイムアウトしない。`t` から `now` の範囲を導出することで棄却率をゼロに保つ。
#[test]
fn no_timeout_when_clock_regresses() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let keep_alive = noprop::sample_usize_in(ctx, 1..=3600) as u16;
        let t = noprop::sample_u64_in(ctx, 1..=1_000);
        // 時刻の巻き戻りを valid-by-construction で生成する（棄却なし）。
        let now = noprop::sample_u64_in(ctx, 0..t);

        let mut ka = KeepAlive::new(keep_alive);
        ka.pingreq_sent(t);
        assert!(!ka.has_timed_out(now));
        Ok(())
    })?;
    Ok(())
}

/// PINGREQ 送信後に Keep Alive を 0 に切り替えると、Keep Alive 機構が
/// 無効化され、任意の `now` でタイムアウトしない。
#[test]
fn no_timeout_after_set_keep_alive_zero() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let keep_alive = noprop::sample_usize_in(ctx, 1..=3600) as u16;
        let now = noprop::sample_u64_in(ctx, 0..=1_000_000);
        let mut ka = KeepAlive::new(keep_alive);
        ka.pingreq_sent(0);
        ka.set_keep_alive(0);
        assert!(!ka.has_timed_out(now));
        Ok(())
    })?;
    Ok(())
}

/// reset 後は Keep Alive 無効・PINGRESP 待ちなしに戻る。
#[test]
fn reset_clears_all_state() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let keep_alive = noprop::sample_usize_in(ctx, 1..=3600) as u16;
        let t = noprop::sample_u64_in(ctx, 0..=1_000_000);
        let now = noprop::sample_u64_in(ctx, 0..=1_000_000);
        let mut ka = KeepAlive::new(keep_alive);
        ka.pingreq_sent(t);
        ka.reset();
        assert_eq!(ka.keep_alive_secs(), 0);
        assert!(!ka.is_awaiting_pingresp());
        assert!(!ka.should_send_pingreq(now));
        assert!(!ka.has_timed_out(now));
        Ok(())
    })?;
    Ok(())
}

/// PINGRESP 待ちが立っていない状態での pingresp_received は副作用がない。
#[test]
fn pingresp_received_is_idempotent_when_not_awaiting() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let keep_alive = noprop::sample_usize_in(ctx, 0..=3600) as u16;
        let mut ka = KeepAlive::new(keep_alive);
        assert!(!ka.is_awaiting_pingresp());
        ka.pingresp_received();
        assert!(!ka.is_awaiting_pingresp());
        assert_eq!(ka.keep_alive_secs(), keep_alive);
        Ok(())
    })?;
    Ok(())
}
