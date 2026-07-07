//! FlowControl のプロパティベーステスト。

use proptest::prelude::*;
use shiguredo_mqtt::state::flow_control::{FlowControl, FlowControlError};

proptest! {
    /// 送信方向: 未確認 PUBLISH 数が Receive Maximum を超えず、
    /// `publish_sent()` の成否が quota の残量と一致する。
    ///
    /// MQTT v5.0 §4.9 [MQTT-4.9.0-2]:
    /// send quota が 0 になったら QoS > 0 の PUBLISH を送信してはならない。
    #[test]
    fn flow_control_respects_send_limit(
        max in 1u16..=100u16,
        sends in 0u16..=200u16,
        acks in 0u16..=200u16,
    ) {
        let mut fc = FlowControl::new();
        fc.set_receive_maximum(max)
            .expect("Receive Maximum を設定できること");

        // quota が残っている間の publish_sent() は Ok、枯渇後は Err になる。
        let mut expected: u16 = 0;
        for _ in 0..sends {
            let result = fc.publish_sent();
            if expected < max {
                prop_assert_eq!(result, Ok(()));
                expected += 1;
            } else {
                prop_assert_eq!(result, Err(FlowControlError::ReceiveMaximumExceeded));
            }
        }
        for _ in 0..acks {
            fc.publish_acked();
            expected = expected.saturating_sub(1);
        }

        // outstanding_count がモデルと一致し、receive_maximum を超過しないこと。
        prop_assert_eq!(fc.outstanding_count(), expected);
        prop_assert!(fc.outstanding_count() <= max);
        // available() と outstanding_count() の整合性。
        prop_assert_eq!(fc.available(), max.saturating_sub(fc.outstanding_count()));
        // can_send() が outstanding_count < receive_maximum と一致すること。
        prop_assert_eq!(fc.can_send(), fc.outstanding_count() < max);
    }

    /// 受信方向: 未確認受信 PUBLISH 数が own Receive Maximum を超えず、
    /// `publish_received()` の成否が受信枠の残量と一致する。
    ///
    /// MQTT v5.0 §4.9 [MQTT-4.9.0-1]:
    /// 受信者は Receive Maximum を超える未確認 QoS > 0 の PUBLISH を受け取ってはならない。
    #[test]
    fn flow_control_respects_receive_limit(
        max in 1u16..=100u16,
        receives in 0u16..=200u16,
        acks in 0u16..=200u16,
    ) {
        let mut fc = FlowControl::new();
        fc.set_own_receive_maximum(max)
            .expect("Receive Maximum を設定できること");

        let mut expected: u16 = 0;
        for _ in 0..receives {
            let result = fc.publish_received();
            if expected < max {
                prop_assert_eq!(result, Ok(()));
                expected += 1;
            } else {
                prop_assert_eq!(result, Err(FlowControlError::ReceiveMaximumExceeded));
            }
        }
        for _ in 0..acks {
            fc.publish_ack_sent();
            expected = expected.saturating_sub(1);
        }

        prop_assert_eq!(fc.incoming_count(), expected);
        prop_assert!(fc.incoming_count() <= max);
        prop_assert_eq!(
            fc.incoming_available(),
            max.saturating_sub(fc.incoming_count())
        );
        prop_assert_eq!(fc.can_receive(), fc.incoming_count() < max);
    }

    /// 送信・受信の両方向は独立しており、一方の操作が他方のカウンタを変えない。
    #[test]
    fn send_and_receive_quotas_are_independent(
        send_max in 1u16..=50u16,
        recv_max in 1u16..=50u16,
        sends in 0u16..=60u16,
        receives in 0u16..=60u16,
    ) {
        let mut fc = FlowControl::new();
        fc.set_receive_maximum(send_max)
            .expect("Receive Maximum を設定できること");
        fc.set_own_receive_maximum(recv_max)
            .expect("Receive Maximum を設定できること");

        let mut send_expected: u16 = 0;
        for _ in 0..sends {
            if send_expected < send_max {
                prop_assert_eq!(fc.publish_sent(), Ok(()));
                send_expected += 1;
            } else {
                prop_assert_eq!(
                    fc.publish_sent(),
                    Err(FlowControlError::ReceiveMaximumExceeded)
                );
            }
        }

        let mut recv_expected: u16 = 0;
        for _ in 0..receives {
            if recv_expected < recv_max {
                prop_assert_eq!(fc.publish_received(), Ok(()));
                recv_expected += 1;
            } else {
                prop_assert_eq!(
                    fc.publish_received(),
                    Err(FlowControlError::ReceiveMaximumExceeded)
                );
            }
        }

        prop_assert_eq!(fc.outstanding_count(), send_expected);
        prop_assert_eq!(fc.incoming_count(), recv_expected);
    }

    /// リセット後はデフォルト値に戻り、未確認カウントも 0 になる。
    /// `keep_own_receive_maximum` が true のときだけ own_receive_maximum を保持する。
    #[test]
    fn flow_control_reset(
        send_max in 1u16..=100u16,
        recv_max in 1u16..=100u16,
        keep_own in any::<bool>(),
    ) {
        let mut fc = FlowControl::new();
        fc.set_receive_maximum(send_max)
            .expect("Receive Maximum を設定できること");
        fc.set_own_receive_maximum(recv_max)
            .expect("Receive Maximum を設定できること");
        fc.publish_sent().expect("quota が残っている間は送信できること");
        fc.publish_received()
            .expect("quota が残っている間は受信できること");
        fc.reset(keep_own);

        prop_assert_eq!(fc.outstanding_count(), 0);
        prop_assert_eq!(fc.incoming_count(), 0);
        prop_assert!(!fc.is_initialized());
        prop_assert_eq!(fc.receive_maximum(), 65535);
        if keep_own {
            prop_assert_eq!(fc.own_receive_maximum(), recv_max);
        } else {
            prop_assert_eq!(fc.own_receive_maximum(), 65535);
        }
    }
}
