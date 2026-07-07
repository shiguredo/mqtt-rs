use shiguredo_mqtt::state::qos_flow::{Action, FlowError, QosFlowManager};

// 正常系の QoS 1/2 完了・PUBREC Reason Code 境界・送受信独立性・
// PUBREL 応答・reset・pending 順序は pbt/tests/prop_qos_flow.rs で検証する。
// ここには状態不一致・受信側の重複 PUBLISH・ゼロ識別子など
// PBT 化しにくいエラーパスと固定シナリオを残す。

// ====================================================================
// QoS 2 送信側: 重複 PUBREC / エラー PUBREC の状態分岐
// ====================================================================

#[test]
fn qos2_flow_resend_pubrec_triggers_repubrel() {
    // MQTT v5.0 §4.3.3 [MQTT-4.3.3-4]: PUBREC の重複受信には PUBREL を再送する。
    let mut manager = QosFlowManager::new();
    let packet_id = 20;
    manager.publish_sent_qos2(packet_id);
    manager
        .pubrec_received(packet_id, 0x00)
        .expect("初回 PUBREC で PUBREL 送信になること");

    let action = manager
        .pubrec_received(packet_id, 0x00)
        .expect("重複 PUBREC でも PUBREL を返すこと");
    assert_eq!(action, Some(Action::SendPubrel { packet_id }));
}

#[test]
fn qos2_flow_error_pubrec_after_pubrel_sent_keeps_flow() {
    // PUBREL 送信後（PUBCOMP 待ち）のエラー PUBREC は状態不一致として無視し、
    // フローを維持する。
    let mut manager = QosFlowManager::new();
    let packet_id = 20;
    manager.publish_sent_qos2(packet_id);
    manager
        .pubrec_received(packet_id, 0x00)
        .expect("初回 PUBREC で PUBREL 送信になること");

    assert_eq!(
        manager.pubrec_received(packet_id, 0x87),
        Err(FlowError::StateMismatch)
    );
    assert!(manager.is_active(packet_id));
    assert_eq!(
        manager.needs_retransmission(packet_id),
        Some(Action::ResendPubrel { packet_id })
    );
}

#[test]
fn error_pubrec_without_active_flow_returns_none() {
    let mut manager = QosFlowManager::new();
    assert_eq!(manager.pubrec_received(100, 0x87), Ok(None));
}

// ====================================================================
// QoS 1 / QoS 2 受信側
// ====================================================================

#[test]
fn qos1_receive_publish_send_puback() {
    let mut manager = QosFlowManager::new();
    let packet_id = 30;
    let action = manager.publish_received_qos1(packet_id);
    assert_eq!(action, Action::SendPuback { packet_id });
    assert!(manager.is_active(packet_id));
    assert!(manager.has_incoming_flow(packet_id));

    manager.puback_sent(packet_id);
    assert!(!manager.is_active(packet_id));
    assert!(!manager.has_incoming_flow(packet_id));
}

#[test]
fn qos1_receive_duplicate_publish_keeps_incoming_flow() {
    // MQTT v5.0 §3.3.4 [MQTT-3.3.4-9]:
    // PUBACK 送信前の同一 Packet Identifier の再着は受信フローを維持する。
    let mut manager = QosFlowManager::new();
    let packet_id = 31;
    manager.publish_received_qos1(packet_id);

    let action = manager.publish_received_qos1(packet_id);
    assert_eq!(action, Action::SendPuback { packet_id });
    assert!(manager.has_incoming_flow(packet_id));
    assert_eq!(manager.active_flow_count(), 1);
}

#[test]
fn qos1_receive_after_puback_is_new_flow() {
    let mut manager = QosFlowManager::new();
    let packet_id = 32;

    manager.publish_received_qos1(packet_id);
    manager.puback_sent(packet_id);
    assert!(!manager.has_incoming_flow(packet_id));

    let action = manager.publish_received_qos1(packet_id);
    assert_eq!(action, Action::SendPuback { packet_id });
    assert!(manager.has_incoming_flow(packet_id));
}

#[test]
fn qos2_receive_publish_send_pubrec_await_pubrel_send_pubcomp() {
    let mut manager = QosFlowManager::new();
    let packet_id = 40;

    let action = manager.publish_received_qos2(packet_id);
    assert_eq!(
        action,
        Action::SendPubrec {
            packet_id,
            is_duplicate: false
        }
    );
    assert!(manager.is_active(packet_id));

    let action = manager
        .pubrel_received(packet_id)
        .expect("PUBREL 受信で PUBCOMP 送信アクションが生成されること");
    assert_eq!(action, Some(Action::SendPubcomp { packet_id }));
    assert!(!manager.is_active(packet_id));
}

#[test]
fn qos2_receive_duplicate_publish_returns_pubrec_again() {
    // MQTT v5.0 §4.3.3 [MQTT-4.3.3-10]:
    // 重複 PUBLISH 受信には PUBREC を再送するが、is_duplicate を true にする。
    let mut manager = QosFlowManager::new();
    let packet_id = 40;
    manager.publish_received_qos2(packet_id);

    let action = manager.publish_received_qos2(packet_id);
    assert_eq!(
        action,
        Action::SendPubrec {
            packet_id,
            is_duplicate: true
        }
    );
}

#[test]
fn qos2_receive_after_pubcomp_is_not_duplicate() {
    let mut manager = QosFlowManager::new();
    let packet_id = 50;

    manager.publish_received_qos2(packet_id);
    manager
        .pubrel_received(packet_id)
        .expect("PUBREL でフロー完了すること");
    assert!(!manager.is_active(packet_id));

    let action = manager.publish_received_qos2(packet_id);
    assert_eq!(
        action,
        Action::SendPubrec {
            packet_id,
            is_duplicate: false
        }
    );
}

#[test]
fn qos1_send_flow_survives_qos2_receive_with_same_id() {
    let mut manager = QosFlowManager::new();
    let packet_id = 50;

    manager.publish_sent_qos1(packet_id);
    manager.publish_received_qos2(packet_id);

    let action = manager
        .puback_received(packet_id)
        .expect("受信フローと並行しても QoS 1 送信フローが完了すること");
    assert_eq!(action, Some(Action::Complete { packet_id }));

    assert!(manager.is_active(packet_id));
    let action = manager
        .pubrel_received(packet_id)
        .expect("受信フローが独立して進行すること");
    assert_eq!(action, Some(Action::SendPubcomp { packet_id }));
}

// ====================================================================
// アクティブフローなし / ゼロ識別子 / release
// ====================================================================

#[test]
fn puback_received_without_active_flow_returns_none() {
    let mut manager = QosFlowManager::new();
    assert_eq!(manager.puback_received(100), Ok(None));
}

#[test]
fn pubrec_received_without_active_flow_returns_none() {
    let mut manager = QosFlowManager::new();
    assert_eq!(manager.pubrec_received(100, 0x00), Ok(None));
}

#[test]
fn pubcomp_received_without_active_flow_returns_none() {
    let mut manager = QosFlowManager::new();
    assert_eq!(manager.pubcomp_received(100), Ok(None));
}

#[test]
fn publish_sent_qos1_ignores_zero_packet_id() {
    let mut manager = QosFlowManager::new();
    manager.publish_sent_qos1(0);
    assert_eq!(manager.active_flow_count(), 0);
}

#[test]
fn release_clears_specific_flow() {
    let mut manager = QosFlowManager::new();
    manager.publish_sent_qos1(1);
    manager.publish_sent_qos1(2);
    manager.release(1);
    assert!(!manager.is_active(1));
    assert!(manager.is_active(2));
}

// ====================================================================
// 状態不一致（誤った制御パケット受信）
// ====================================================================

#[test]
fn puback_received_while_awaiting_pubrec() {
    let mut manager = QosFlowManager::new();
    let packet_id = 1;
    manager.publish_sent_qos2(packet_id);

    assert_eq!(
        manager.puback_received(packet_id),
        Err(FlowError::StateMismatch)
    );
    assert!(manager.is_active(packet_id));
    assert_eq!(
        manager.needs_retransmission(packet_id),
        Some(Action::ResendPublish { packet_id })
    );
}

#[test]
fn puback_received_while_awaiting_pubcomp() {
    let mut manager = QosFlowManager::new();
    let packet_id = 1;
    manager.publish_sent_qos2(packet_id);
    let _ = manager.pubrec_received(packet_id, 0x00);

    assert_eq!(
        manager.puback_received(packet_id),
        Err(FlowError::StateMismatch)
    );
    assert!(manager.is_active(packet_id));
    assert_eq!(
        manager.needs_retransmission(packet_id),
        Some(Action::ResendPubrel { packet_id })
    );
}

#[test]
fn puback_received_while_awaiting_pubrel() {
    let mut manager = QosFlowManager::new();
    let packet_id = 1;
    manager.publish_received_qos2(packet_id);

    assert_eq!(
        manager.puback_received(packet_id),
        Err(FlowError::StateMismatch)
    );
    assert!(manager.is_active(packet_id));
    assert_eq!(manager.needs_retransmission(packet_id), None);
}

#[test]
fn pubrec_received_while_awaiting_puback() {
    let mut manager = QosFlowManager::new();
    let packet_id = 1;
    manager.publish_sent_qos1(packet_id);

    assert_eq!(
        manager.pubrec_received(packet_id, 0x00),
        Err(FlowError::StateMismatch)
    );
    assert!(manager.is_active(packet_id));
    assert_eq!(
        manager.needs_retransmission(packet_id),
        Some(Action::ResendPublish { packet_id })
    );
}

#[test]
fn pubrec_received_while_awaiting_pubrel() {
    let mut manager = QosFlowManager::new();
    let packet_id = 1;
    manager.publish_received_qos2(packet_id);

    assert_eq!(
        manager.pubrec_received(packet_id, 0x00),
        Err(FlowError::StateMismatch)
    );
    assert!(manager.is_active(packet_id));
    assert_eq!(manager.needs_retransmission(packet_id), None);
}

#[test]
fn pubrel_received_while_awaiting_puback() {
    let mut manager = QosFlowManager::new();
    let packet_id = 1;
    manager.publish_sent_qos1(packet_id);

    assert_eq!(
        manager.pubrel_received(packet_id),
        Err(FlowError::StateMismatch)
    );
    assert!(manager.is_active(packet_id));
    assert_eq!(
        manager.needs_retransmission(packet_id),
        Some(Action::ResendPublish { packet_id })
    );
}

#[test]
fn pubrel_received_while_awaiting_pubrec() {
    let mut manager = QosFlowManager::new();
    let packet_id = 1;
    manager.publish_sent_qos2(packet_id);

    assert_eq!(
        manager.pubrel_received(packet_id),
        Err(FlowError::StateMismatch)
    );
    assert!(manager.is_active(packet_id));
    assert_eq!(
        manager.needs_retransmission(packet_id),
        Some(Action::ResendPublish { packet_id })
    );
}

#[test]
fn pubrel_received_while_awaiting_pubcomp() {
    let mut manager = QosFlowManager::new();
    let packet_id = 1;
    manager.publish_sent_qos2(packet_id);
    let _ = manager.pubrec_received(packet_id, 0x00);

    assert_eq!(
        manager.pubrel_received(packet_id),
        Err(FlowError::StateMismatch)
    );
    assert!(manager.is_active(packet_id));
    assert_eq!(
        manager.needs_retransmission(packet_id),
        Some(Action::ResendPubrel { packet_id })
    );
}

#[test]
fn pubcomp_received_while_awaiting_puback() {
    let mut manager = QosFlowManager::new();
    let packet_id = 1;
    manager.publish_sent_qos1(packet_id);

    assert_eq!(
        manager.pubcomp_received(packet_id),
        Err(FlowError::StateMismatch)
    );
    assert!(manager.is_active(packet_id));
    assert_eq!(
        manager.needs_retransmission(packet_id),
        Some(Action::ResendPublish { packet_id })
    );
}

#[test]
fn pubcomp_received_while_awaiting_pubrec() {
    let mut manager = QosFlowManager::new();
    let packet_id = 1;
    manager.publish_sent_qos2(packet_id);

    assert_eq!(
        manager.pubcomp_received(packet_id),
        Err(FlowError::StateMismatch)
    );
    assert!(manager.is_active(packet_id));
    assert_eq!(
        manager.needs_retransmission(packet_id),
        Some(Action::ResendPublish { packet_id })
    );
}

#[test]
fn pubcomp_received_while_awaiting_pubrel() {
    let mut manager = QosFlowManager::new();
    let packet_id = 1;
    manager.publish_received_qos2(packet_id);

    assert_eq!(
        manager.pubcomp_received(packet_id),
        Err(FlowError::StateMismatch)
    );
    assert!(manager.is_active(packet_id));
    assert_eq!(manager.needs_retransmission(packet_id), None);
}

#[test]
fn state_mismatch_then_recover_puback() {
    let mut manager = QosFlowManager::new();
    let packet_id = 1;
    manager.publish_sent_qos1(packet_id);

    // 無関係な PUBCOMP は状態不一致として無視する。
    assert_eq!(
        manager.pubcomp_received(packet_id),
        Err(FlowError::StateMismatch)
    );

    // 正当な PUBACK で正常完了する。
    let action = manager
        .puback_received(packet_id)
        .expect("PUBACK 受信で完了すること");
    assert_eq!(action, Some(Action::Complete { packet_id }));
    assert!(!manager.is_active(packet_id));
}

#[test]
fn state_mismatch_then_recover_pubrel() {
    let mut manager = QosFlowManager::new();
    let packet_id = 1;
    manager.publish_received_qos2(packet_id);

    // 無関係な PUBACK は状態不一致として無視する。
    assert_eq!(
        manager.puback_received(packet_id),
        Err(FlowError::StateMismatch)
    );

    // 正当な PUBREL で PUBCOMP 送信アクションが生成される。
    let action = manager
        .pubrel_received(packet_id)
        .expect("PUBREL 受信で PUBCOMP 送信アクションが生成されること");
    assert_eq!(action, Some(Action::SendPubcomp { packet_id }));
    assert!(!manager.is_active(packet_id));
}
