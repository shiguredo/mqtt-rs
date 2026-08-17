//! QosFlowManager のプロパティベーステスト。

use noprop::TestCaseContext;
use shiguredo_mqtt::state::qos_flow::{Action, QosFlowManager};

/// QoS 1 フロー: PUBLISH 送信 → PUBACK 受信で完了する。
#[test]
fn qos1_flow_completes() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet_id = noprop::sample_usize_in(ctx, 1..=65535) as u16;
        let mut manager = QosFlowManager::new();
        manager.publish_sent_qos1(packet_id);
        assert!(manager.is_active(packet_id));
        assert_eq!(
            manager.needs_retransmission(packet_id),
            Some(Action::ResendPublish { packet_id })
        );

        assert_eq!(
            manager.puback_received(packet_id),
            Ok(Some(Action::Complete { packet_id }))
        );
        assert!(!manager.is_active(packet_id));
        assert_eq!(manager.needs_retransmission(packet_id), None);
        Ok(())
    })?;
    Ok(())
}

/// QoS 2 フロー: PUBLISH → PUBREC → PUBREL → PUBCOMP で完了する。
#[test]
fn qos2_flow_completes() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet_id = noprop::sample_usize_in(ctx, 1..=65535) as u16;
        let mut manager = QosFlowManager::new();

        manager.publish_sent_qos2(packet_id);
        assert!(manager.is_active(packet_id));
        assert_eq!(
            manager.needs_retransmission(packet_id),
            Some(Action::ResendPublish { packet_id })
        );

        assert_eq!(
            manager.pubrec_received(packet_id, 0x00),
            Ok(Some(Action::SendPubrel { packet_id }))
        );
        assert!(manager.is_active(packet_id));
        assert_eq!(
            manager.needs_retransmission(packet_id),
            Some(Action::ResendPubrel { packet_id })
        );

        assert_eq!(
            manager.pubcomp_received(packet_id),
            Ok(Some(Action::Complete { packet_id }))
        );
        assert!(!manager.is_active(packet_id));
        assert_eq!(manager.needs_retransmission(packet_id), None);
        Ok(())
    })?;
    Ok(())
}

/// 該当フローの有無に関わらず、PUBREL には常に PUBCOMP 系の応答が返る。
/// MQTT v5.0 §4.3.3 [MQTT-4.3.3-11] / MQTT v3.1.1 §4.3.3 [MQTT-4.3.3-2]:
/// 受信者は PUBREL に対して同じ Packet Identifier の PUBCOMP で
/// 応答しなければならない。
#[test]
fn pubrel_always_answered_with_pubcomp() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet_id = noprop::sample_usize_in(ctx, 1..=65535) as u16;
        let mut manager = QosFlowManager::new();

        // フローがない状態では PUBCOMP の再送が要求される。
        assert_eq!(
            manager.pubrel_received(packet_id),
            Ok(Some(Action::ResendPubcomp { packet_id }))
        );

        // 受信フローが PUBREL 待ちの状態では PUBCOMP の送信が要求される。
        manager.publish_received_qos2(packet_id);
        assert_eq!(
            manager.pubrel_received(packet_id),
            Ok(Some(Action::SendPubcomp { packet_id }))
        );

        // フロー完了後の再送 PUBREL にも PUBCOMP の再送が要求される。
        assert_eq!(
            manager.pubrel_received(packet_id),
            Ok(Some(Action::ResendPubcomp { packet_id }))
        );
        Ok(())
    })?;
    Ok(())
}

/// QoS 2 フロー: Reason Code 0x80 未満の PUBREC は PUBREL 送信、
/// 0x80 以上はフロー中断となる。
/// MQTT v5.0 §4.3.3 [MQTT-4.3.3-4] / MQTT v5.0 §4.4 [MQTT-4.4.0-2] を参照。
#[test]
fn qos2_pubrec_reason_code_boundary() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet_id = noprop::sample_usize_in(ctx, 1..=65535) as u16;
        let reason_code = noprop::sample_u8(ctx);
        let mut manager = QosFlowManager::new();
        manager.publish_sent_qos2(packet_id);

        let action = manager.pubrec_received(packet_id, reason_code);
        if reason_code < 0x80 {
            assert_eq!(action, Ok(Some(Action::SendPubrel { packet_id })));
            assert!(manager.is_active(packet_id));
        } else {
            assert_eq!(action, Ok(Some(Action::Aborted { packet_id })));
            assert!(!manager.is_active(packet_id));
            assert_eq!(manager.needs_retransmission(packet_id), None);
        }
        Ok(())
    })?;
    Ok(())
}

/// 送信フローと受信フローは同じパケット識別子でも互いに干渉しない。
/// MQTT v5.0 §2.2.1 を参照。
#[test]
fn send_and_receive_flows_are_independent() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet_id = noprop::sample_usize_in(ctx, 1..=65535) as u16;
        let mut manager = QosFlowManager::new();

        manager.publish_sent_qos2(packet_id);
        manager.publish_received_qos2(packet_id);
        assert_eq!(manager.active_flow_count(), 2);

        // 送信フローが受信フローに破壊されず完了できる。
        assert_eq!(
            manager.pubrec_received(packet_id, 0x00),
            Ok(Some(Action::SendPubrel { packet_id }))
        );
        assert_eq!(
            manager.pubcomp_received(packet_id),
            Ok(Some(Action::Complete { packet_id }))
        );

        // 受信フローも独立して完了できる。
        assert_eq!(
            manager.pubrel_received(packet_id),
            Ok(Some(Action::SendPubcomp { packet_id }))
        );
        assert_eq!(manager.active_flow_count(), 0);
        Ok(())
    })?;
    Ok(())
}

/// 未完了の QoS 1 フローは再送を要求する。
#[test]
fn qos1_flow_needs_retransmission() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet_id = noprop::sample_usize_in(ctx, 1..=65535) as u16;
        let mut manager = QosFlowManager::new();
        manager.publish_sent_qos1(packet_id);

        assert_eq!(
            manager.needs_retransmission(packet_id),
            Some(Action::ResendPublish { packet_id })
        );
        Ok(())
    })?;
    Ok(())
}

/// 完了後のフローは再送を要求しない。
#[test]
fn completed_flow_no_retransmission() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet_id = noprop::sample_usize_in(ctx, 1..=65535) as u16;
        let mut manager = QosFlowManager::new();
        manager.publish_sent_qos1(packet_id);
        assert!(
            manager
                .puback_received(packet_id)
                .is_ok_and(|a| a.is_some())
        );
        assert_eq!(manager.needs_retransmission(packet_id), None);
        assert!(!manager.is_active(packet_id));
        Ok(())
    })?;
    Ok(())
}

/// リセット後はすべてのフローがクリアされる。
#[test]
fn reset_clears_all_flows() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let ids_len = noprop::sample_usize_in(ctx, 0..=50);
        let mut ids = Vec::with_capacity(ids_len);
        let mut unique = std::collections::BTreeSet::new();
        for _ in 0..ids_len {
            let id = noprop::sample_usize_in(ctx, 1..=65535) as u16;
            // フローは識別子ごとに 1 本のため、重複時はケースごと棄却する。
            if !unique.insert(id) {
                ctx.reject_case();
            }
            ids.push(id);
        }

        let mut manager = QosFlowManager::new();
        for id in &ids {
            manager.publish_sent_qos1(*id);
        }
        assert_eq!(manager.active_flow_count(), ids.len());
        manager.reset();
        assert_eq!(manager.active_flow_count(), 0);
        for id in &ids {
            assert!(!manager.is_active(*id));
            assert_eq!(manager.needs_retransmission(*id), None);
        }
        Ok(())
    })?;
    Ok(())
}

/// ランダム操作列テスト用の操作。
#[derive(Debug, Clone)]
enum FlowOp {
    /// QoS 1 の PUBLISH 送信。
    SendQos1(u16),
    /// QoS 2 の PUBLISH 送信。
    SendQos2(u16),
    /// PUBACK 受信。
    Puback(u16),
    /// PUBREC 受信（Reason Code 付き）。
    Pubrec(u16, u8),
    /// PUBCOMP 受信。
    Pubcomp(u16),
    /// QoS 2 の PUBLISH 受信（受信方向。送信方向の順序に影響しないこと）。
    PublishReceivedQos2(u16),
    /// PUBREL 受信（受信方向。送信方向の順序に影響しないこと）。
    PubrelReceived(u16),
    /// フローの強制クリア。
    Release(u16),
}

fn sample_flow_op(ctx: &mut TestCaseContext) -> FlowOp {
    // 識別子を小さい範囲に絞り、再利用・衝突を積極的に発生させる。
    // 0 は無効な識別子であり、送信 API が無視することの検証も兼ねる。
    let id = noprop::sample_usize_in(ctx, 0..=5) as u16;
    match noprop::sample_usize_in(ctx, 0..8) {
        0 => FlowOp::SendQos1(id),
        1 => FlowOp::SendQos2(id),
        2 => FlowOp::Puback(id),
        3 => FlowOp::Pubrec(id, noprop::sample_choice(ctx, &[0x00u8, 0x80u8])),
        4 => FlowOp::Pubcomp(id),
        5 => FlowOp::PublishReceivedQos2(id),
        6 => FlowOp::PubrelReceived(id),
        _ => FlowOp::Release(id),
    }
}

/// モデル側のフロー状態。
#[derive(Debug, Clone, Copy, PartialEq)]
enum ModelState {
    /// PUBACK または PUBREC を待っている（PUBLISH 送信済み）。
    AwaitingAck,
    /// PUBCOMP を待っている（初回 PUBREC 受信済み）。
    AwaitingComp,
}

/// ランダムな操作列に対して pending_packet_ids の順序がイベント発生順と一致する。
/// PUBLISH 再送対象は送信イベント順、PUBREL 再送対象は初回 PUBREC 受信イベント順
/// （MQTT v5.0 §4.6 [MQTT-4.6.0-1] / MQTT v5.0 §4.6 [MQTT-4.6.0-4]）。
/// モデルは「識別子の並び + 状態」で、送信時に末尾へ追加し、
/// 初回 PUBREC 受信時に末尾へ移動する。
#[test]
fn pending_order_matches_event_order() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let ops_len = noprop::sample_usize_in(ctx, 1..=80);
        let mut manager = QosFlowManager::new();
        // モデル: (packet_id, 状態) のイベント発生順リスト。
        let mut model: Vec<(u16, ModelState)> = Vec::new();

        for _ in 0..ops_len {
            let op = sample_flow_op(ctx);
            match op {
                FlowOp::SendQos1(id) | FlowOp::SendQos2(id) => {
                    let qos2 = matches!(op, FlowOp::SendQos2(_));
                    if id != 0 && !model.iter().any(|&(m, _)| m == id) {
                        // 未登録のときだけ末尾に追加する（アクティブ中の重複送信は無視される）。
                        model.push((id, ModelState::AwaitingAck));
                    }
                    if qos2 {
                        manager.publish_sent_qos2(id);
                    } else {
                        manager.publish_sent_qos1(id);
                    }
                }
                FlowOp::Puback(id) => {
                    // QoS 1 の完了。QoS 2 のフローに対する PUBACK は状態不一致で無視される。
                    // モデルでは QoS を区別しないため、manager の戻り値で完了を判定する。
                    if manager.puback_received(id).is_ok_and(|a| a.is_some()) {
                        model.retain(|&(m, _)| m != id);
                    }
                }
                FlowOp::Pubrec(id, rc) => {
                    match manager.pubrec_received(id, rc) {
                        Ok(Some(Action::Aborted { .. })) => {
                            model.retain(|&(m, _)| m != id);
                        }
                        Ok(Some(Action::SendPubrel { .. })) => {
                            let already_comp = model
                                .iter()
                                .any(|&(m, s)| m == id && s == ModelState::AwaitingComp);
                            if !already_comp {
                                // 初回 PUBREC: 末尾へ移動して状態を更新する。
                                model.retain(|&(m, _)| m != id);
                                model.push((id, ModelState::AwaitingComp));
                            }
                            // 重複 PUBREC では順序を変えない。
                        }
                        _ => {}
                    }
                }
                FlowOp::Pubcomp(id) => {
                    if manager.pubcomp_received(id).is_ok_and(|a| a.is_some()) {
                        model.retain(|&(m, _)| m != id);
                    }
                }
                FlowOp::PublishReceivedQos2(id) => {
                    // 受信方向のフローは送信方向の順序に影響しない。モデルは変更しない。
                    manager.publish_received_qos2(id);
                }
                FlowOp::PubrelReceived(id) => {
                    // 受信方向のフローは送信方向の順序に影響しない。モデルは変更しない。
                    let _ = manager.pubrel_received(id);
                }
                FlowOp::Release(id) => {
                    manager.release(id);
                    model.retain(|&(m, _)| m != id);
                }
            }

            let expected: Vec<u16> = model.iter().map(|&(m, _)| m).collect();
            assert_eq!(manager.pending_packet_ids(), expected);
        }
        Ok(())
    })?;
    Ok(())
}
