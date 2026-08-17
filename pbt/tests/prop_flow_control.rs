//! FlowControl のプロパティベーステスト。

use shiguredo_mqtt::state::flow_control::{FlowControl, FlowControlError};
use std::cell::Cell;

/// 送信方向: 未確認 PUBLISH 数が Receive Maximum を超えず、
/// `publish_sent()` の成否が quota の残量と一致する。
///
/// MQTT v5.0 §4.9 [MQTT-4.9.0-2]:
/// send quota が 0 になったら QoS > 0 の PUBLISH を送信してはならない。
#[test]
fn flow_control_respects_send_limit() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let exceeded = Cell::new(0usize);
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let max = noprop::sample_usize_in(ctx, 1..=100) as u16;
        let sends = noprop::sample_usize_in(ctx, 0..=200) as u16;
        let acks = noprop::sample_usize_in(ctx, 0..=200) as u16;

        let mut fc = FlowControl::new();
        fc.set_receive_maximum(max)
            .expect("Receive Maximum を設定できること");

        // quota が残っている間の publish_sent() は Ok、枯渇後は Err になる。
        let mut expected: u16 = 0;
        for _ in 0..sends {
            let result = fc.publish_sent();
            if expected < max {
                assert_eq!(result, Ok(()));
                expected += 1;
            } else {
                assert_eq!(result, Err(FlowControlError::ReceiveMaximumExceeded));
                exceeded.set(exceeded.get() + 1);
            }
        }
        for _ in 0..acks {
            fc.publish_acked();
            expected = expected.saturating_sub(1);
        }

        // outstanding_count がモデルと一致し、receive_maximum を超過しないこと。
        assert_eq!(fc.outstanding_count(), expected);
        assert!(fc.outstanding_count() <= max);
        // available() と outstanding_count() の整合性。
        assert_eq!(fc.available(), max.saturating_sub(fc.outstanding_count()));
        // can_send() が outstanding_count < receive_maximum と一致すること。
        assert_eq!(fc.can_send(), fc.outstanding_count() < max);
        Ok(())
    })?;

    // sends > max のケースが一度も生成されないと Err パスが空振りになるため、
    // ゲートで到達を保証する。
    // p 推定値: sends ~ U(0..=200)・max ~ U(1..=100) で P(sends > max) ≈ 0.74。
    // 256 ケースでの miss 確率は 0.26^256 ≈ 0。
    assert!(
        exceeded.get() > 0,
        "Receive Maximum 超過パスが一度も検証されなかった\n{runner}"
    );

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}

/// 受信方向: 未確認受信 PUBLISH 数が own Receive Maximum を超えず、
/// `publish_received()` の成否が受信枠の残量と一致する。
///
/// MQTT v5.0 §4.9 [MQTT-4.9.0-1]:
/// 受信者は Receive Maximum を超える未確認 QoS > 0 の PUBLISH を受け取ってはならない。
#[test]
fn flow_control_respects_receive_limit() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let exceeded = Cell::new(0usize);
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let max = noprop::sample_usize_in(ctx, 1..=100) as u16;
        let receives = noprop::sample_usize_in(ctx, 0..=200) as u16;
        let acks = noprop::sample_usize_in(ctx, 0..=200) as u16;

        let mut fc = FlowControl::new();
        fc.set_own_receive_maximum(max)
            .expect("Receive Maximum を設定できること");

        let mut expected: u16 = 0;
        for _ in 0..receives {
            let result = fc.publish_received();
            if expected < max {
                assert_eq!(result, Ok(()));
                expected += 1;
            } else {
                assert_eq!(result, Err(FlowControlError::ReceiveMaximumExceeded));
                exceeded.set(exceeded.get() + 1);
            }
        }
        for _ in 0..acks {
            fc.publish_ack_sent();
            expected = expected.saturating_sub(1);
        }

        assert_eq!(fc.incoming_count(), expected);
        assert!(fc.incoming_count() <= max);
        assert_eq!(
            fc.incoming_available(),
            max.saturating_sub(fc.incoming_count())
        );
        assert_eq!(fc.can_receive(), fc.incoming_count() < max);
        Ok(())
    })?;

    // receives > max のケースが一度も生成されないと Err パスが空振りになるため、
    // ゲートで到達を保証する。
    // p 推定値: receives ~ U(0..=200)・max ~ U(1..=100) で P(receives > max) ≈ 0.74。
    // 256 ケースでの miss 確率は 0.26^256 ≈ 0。
    assert!(
        exceeded.get() > 0,
        "Receive Maximum 超過パスが一度も検証されなかった\n{runner}"
    );

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}

/// 送信・受信の両方向は独立しており、一方の操作が他方のカウンタを変えない。
#[test]
fn send_and_receive_quotas_are_independent() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let send_exceeded = Cell::new(0usize);
    let recv_exceeded = Cell::new(0usize);
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let send_max = noprop::sample_usize_in(ctx, 1..=50) as u16;
        let recv_max = noprop::sample_usize_in(ctx, 1..=50) as u16;
        let sends = noprop::sample_usize_in(ctx, 0..=60) as u16;
        let receives = noprop::sample_usize_in(ctx, 0..=60) as u16;

        let mut fc = FlowControl::new();
        fc.set_receive_maximum(send_max)
            .expect("Receive Maximum を設定できること");
        fc.set_own_receive_maximum(recv_max)
            .expect("Receive Maximum を設定できること");

        let mut send_expected: u16 = 0;
        for _ in 0..sends {
            if send_expected < send_max {
                assert_eq!(fc.publish_sent(), Ok(()));
                send_expected += 1;
            } else {
                assert_eq!(
                    fc.publish_sent(),
                    Err(FlowControlError::ReceiveMaximumExceeded)
                );
                send_exceeded.set(send_exceeded.get() + 1);
            }
        }

        let mut recv_expected: u16 = 0;
        for _ in 0..receives {
            if recv_expected < recv_max {
                assert_eq!(fc.publish_received(), Ok(()));
                recv_expected += 1;
            } else {
                assert_eq!(
                    fc.publish_received(),
                    Err(FlowControlError::ReceiveMaximumExceeded)
                );
                recv_exceeded.set(recv_exceeded.get() + 1);
            }
        }

        assert_eq!(fc.outstanding_count(), send_expected);
        assert_eq!(fc.incoming_count(), recv_expected);
        Ok(())
    })?;

    // どちらか一方の超過パスが一度も生成されないと Err パスが空振りになるため、
    // 両方のゲートで到達を保証する。
    // p 推定値: sends / receives ~ U(0..=60)・max ~ U(1..=50) で P(超過) ≈ 0.57。
    // 256 ケースでの miss 確率は 0.43^256 ≈ 0。
    assert!(
        send_exceeded.get() > 0,
        "送信方向の Receive Maximum 超過パスが一度も検証されなかった\n{runner}"
    );
    assert!(
        recv_exceeded.get() > 0,
        "受信方向の Receive Maximum 超過パスが一度も検証されなかった\n{runner}"
    );

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}

/// リセット後はデフォルト値に戻り、未確認カウントも 0 になる。
/// `keep_own_receive_maximum` が true のときだけ own_receive_maximum を保持する。
#[test]
fn flow_control_reset() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let send_max = noprop::sample_usize_in(ctx, 1..=100) as u16;
        let recv_max = noprop::sample_usize_in(ctx, 1..=100) as u16;
        let keep_own = noprop::sample_bool(ctx);

        let mut fc = FlowControl::new();
        fc.set_receive_maximum(send_max)
            .expect("Receive Maximum を設定できること");
        fc.set_own_receive_maximum(recv_max)
            .expect("Receive Maximum を設定できること");
        fc.publish_sent()
            .expect("quota が残っている間は送信できること");
        fc.publish_received()
            .expect("quota が残っている間は受信できること");
        fc.reset(keep_own);

        assert_eq!(fc.outstanding_count(), 0);
        assert_eq!(fc.incoming_count(), 0);
        assert!(!fc.is_initialized());
        assert_eq!(fc.receive_maximum(), 65535);
        if keep_own {
            assert_eq!(fc.own_receive_maximum(), recv_max);
        } else {
            assert_eq!(fc.own_receive_maximum(), 65535);
        }
        Ok(())
    })?;

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}
