use super::*;
use crate::state::auth::AuthState;
use crate::state::subscribe::SubscriptionEntry;
use crate::v5::connack::ConnectReasonCode;

#[test]
fn clean_start_resets_session_state() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");

    // サブスクリプションを追加する。
    session.subscription_manager_mut().subscribe_sent(
        1,
        vec![crate::state::subscribe::SubscriptionEntry::new(
            "a/b",
            crate::codec::qos::QoS::AtMostOnce,
        )],
    );
    session
        .subscription_manager_mut()
        .suback_received(1, &[0x00]);

    assert_eq!(session.subscription_manager().active_count(), 1);

    // CONNACK で session_present=false → セッションリセット。
    session.connect_sent();
    session.connected(false);

    // Clean Start のためサブスクリプションはリセットされる。
    assert_eq!(session.subscription_manager().active_count(), 0);
}

#[test]
fn clean_start_resets_qos_flow_and_packet_ids() {
    // Clean Start=true の場合、connected() で QoS フロー・パケット識別子・
    // サブスクリプションのすべてのサブ状態機械がリセットされる。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");

    let id = session
        .packet_id_manager_mut()
        .allocate()
        .expect("パケット識別子を割り当てられること");
    session.qos_flow_mut().publish_sent_qos1(id);
    assert!(session.qos_flow().is_active(id));

    session
        .subscription_manager_mut()
        .subscribe_sent(1, vec![SubscriptionEntry::new("a/b", QoS::AtLeastOnce)]);
    session
        .subscription_manager_mut()
        .suback_received(1, &[0x01]);
    assert_eq!(session.subscription_manager().active_count(), 1);

    session.connect_sent();
    session.connected(false);

    assert_eq!(session.qos_flow().active_flow_count(), 0);
    assert_eq!(session.packet_id_manager().in_use_count(), 0);
    assert_eq!(session.subscription_manager().active_count(), 0);
}

#[test]
fn sub_state_machines_accessible() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");

    let id = session
        .packet_id_manager_mut()
        .allocate()
        .expect("パケット識別子を割り当てられること");
    assert_eq!(id, 1);

    session.qos_flow_mut().publish_sent_qos1(1);
    assert!(session.qos_flow().is_active(1));

    session.keep_alive_mut().set_keep_alive(60);
    assert_eq!(session.keep_alive().keep_alive_secs(), 60);
}

#[test]
fn session_present_false_resets_state_even_with_clean_start_false() {
    // clean_start=false でも session_present=false の場合は
    // サーバーが以前のセッションを破棄しているため、クライアント側もリセットする。
    let mut session =
        Session::new_v5("client-1".into(), false).expect("セッションの作成に成功すること");
    session.subscription_manager_mut().subscribe_sent(
        1,
        vec![SubscriptionEntry::new(
            "a/b",
            crate::codec::qos::QoS::AtLeastOnce,
        )],
    );
    session
        .subscription_manager_mut()
        .suback_received(1, &[0x01]);
    assert_eq!(session.subscription_manager().active_count(), 1);

    session.connect_sent();
    session.connected(false);

    assert_eq!(session.subscription_manager().active_count(), 0);
}

#[test]
fn reset_clears_all() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();
    session.connected(false);
    session
        .packet_id_manager_mut()
        .allocate()
        .expect("パケット識別子を割り当てられること");

    session.reset();

    assert_eq!(session.connection_state(), ConnectionState::Disconnected);
    assert_eq!(session.packet_id_manager().in_use_count(), 0);
    assert_eq!(session.session_expiry_interval(), 0);
}

#[test]
fn reset_clears_all_sub_state_machines() {
    // reset() が触る 10 項目のうち connection_state を除く 9 項目が
    // 初期状態に戻ることを検証する（connection_state は reset_clears_all で検証）。
    // reset_connection_state() が守る MQTT v5.0 §3.3.2.3.4 [MQTT-3.3.2-7] /
    // MQTT v5.0 §4.9 の Network Connection スコープ再初期化とは別レイヤーで、
    // own_maximum も含めた完全破棄が対象。
    let mut session =
        Session::new_v5("client-1".into(), false).expect("セッションの作成に成功すること");
    session
        .configure_for_connect(60, 300, 16, 10, 1024)
        .expect("CONNECT パラメータを設定できること");
    session.connect_sent();

    // apply_connack で server_capabilities を全 6 フィールド非既定値にする。
    // authentication_method は None（Idle 状態のまま Ok を返す）。
    // maximum_qos は Some(QoS::ExactlyOnce) が Protocol Error として拒否されるため
    // Some(QoS::AtLeastOnce) を使う。
    // session_present=true を選ぶ理由: 将来この構築を apply_connack の前に移す
    // 変更にも耐えられるよう、connected() で reset_session_state() を通らない
    // clean_start=false + session_present=true の受理パスを選ぶ。
    session
        .apply_connack(ConnackParams {
            session_present: true,
            reason_code: ConnackReason::V5(ConnectReasonCode::Success),
            session_expiry_interval: Some(300),
            receive_maximum: Some(100),
            topic_alias_maximum: Some(8),
            server_keep_alive: Some(30),
            maximum_packet_size: Some(1024),
            maximum_qos: Some(QoS::AtLeastOnce),
            retain_available: Some(false),
            wildcard_subscription_available: Some(false),
            subscription_identifiers_available: Some(false),
            shared_subscription_available: Some(false),
            assigned_client_identifier: None,
            authentication_method: None,
        })
        .expect("CONNACK を受理すること");

    // packet_id_manager と qos_flow の非初期状態を構築する。
    let publish_id = session
        .packet_id_manager_mut()
        .allocate()
        .expect("PUBLISH 用パケット識別子を割り当てられること");
    session.qos_flow_mut().publish_sent_qos1(publish_id);

    // 追加 4 項目の非初期状態を構築する。
    // (1) サブスクリプション。SUBSCRIBE パケット識別子は publish_id (=1) と
    // 別値を選ぶ（MQTT プロトコル上、同一 packet_id を複数の in-flight パケットに
    // 使うのは避けるため）。
    let subscribe_packet_id = 2u16;
    session.subscription_manager_mut().subscribe_sent(
        subscribe_packet_id,
        vec![SubscriptionEntry::new("a/b", QoS::AtMostOnce)],
    );
    session
        .subscription_manager_mut()
        .suback_received(subscribe_packet_id, &[0x00]);
    // (2) トピックエイリアスマッピング。受信・送信両マップを非零にする
    // （どちらか一方の clear 漏れも検出するため）。
    session
        .topic_alias_manager_mut()
        .resolve_on_receive("a/b", 1)
        .expect("受信エイリアスを解決できること");
    session
        .topic_alias_manager_mut()
        .register_for_send("a/b")
        .expect("送信エイリアスを割り当てられること");
    // (3) 認証状態。Session::connect_sent_with_auth() は内部で
    // reset_connection_state() を呼び、事前構築した topic_alias_manager
    // マッピング・peer_maximum・server_capabilities を破壊するため、
    // auth_state_mut を直接呼んで Idle → InitialAuthenticating に遷移させる。
    session
        .auth_state_mut()
        .connect_with_auth("SCRAM-SHA-256".into());

    // 事前アサーション: 追加 4 項目の非初期状態が実際に構築されたことを対比の
    // 基準として確認する。server_capabilities は apply_connack に渡した非既定値と
    // 同じ ServerCapabilities インスタンスと assert_eq! で厳密比較する
    // （assert_ne! では 1 フィールド差でも成立するため、リセット後の対比が甘くなる）。
    let expected_before_reset = ServerCapabilities {
        maximum_packet_size: Some(1024),
        maximum_qos: QoS::AtLeastOnce,
        retain_available: false,
        wildcard_subscription_available: false,
        subscription_identifiers_available: false,
        shared_subscription_available: false,
    };
    assert!(session.subscription_manager().is_subscribed("a/b"));
    assert_eq!(session.topic_alias_manager().own_maximum(), 16);
    assert_eq!(session.topic_alias_manager().peer_maximum(), 8);
    assert_eq!(session.topic_alias_manager().alias_count(), 2); // 受信 1 + 送信 1
    assert_eq!(
        session.auth_state().state(),
        AuthState::InitialAuthenticating
    );
    assert_eq!(session.server_capabilities(), &expected_before_reset);

    session.reset();

    // 既存 5 項目の事後アサーション。
    assert_eq!(session.packet_id_manager().in_use_count(), 0);
    assert_eq!(session.qos_flow().active_flow_count(), 0);
    assert_eq!(session.session_expiry_interval(), 0);
    assert_eq!(session.keep_alive().keep_alive_secs(), 0);
    assert_eq!(session.flow_control().available(), 65535);
    // reset() が flow_control.reset(false) を選ぶ（configure_for_connect で
    // 積んだ own_receive_maximum も破棄する）ことを検証する。available() は
    // receive_maximum - outstanding_count で own_receive_maximum を見ないため、
    // reset(false) を reset(true) に誤変更した回帰は本 assert だけが捕捉する。
    assert_eq!(session.flow_control().own_receive_maximum(), 65535);
    // 追加 4 項目の事後アサーション。
    assert!(!session.subscription_manager().is_subscribed("a/b"));
    assert_eq!(session.topic_alias_manager().own_maximum(), 0);
    assert_eq!(session.topic_alias_manager().peer_maximum(), 0);
    assert_eq!(session.topic_alias_manager().alias_count(), 0);
    assert_eq!(session.auth_state().state(), AuthState::Idle);
    assert_eq!(session.server_capabilities(), &ServerCapabilities::new());
}

#[test]
fn apply_connack_clean_start_resets() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.subscription_manager_mut().subscribe_sent(
        1,
        vec![SubscriptionEntry::new(
            "a/b",
            crate::codec::qos::QoS::AtLeastOnce,
        )],
    );
    session
        .subscription_manager_mut()
        .suback_received(1, &[0x01]);
    assert_eq!(session.subscription_manager().active_count(), 1);

    session.connect_sent();
    session
        .apply_connack(ConnackParams {
            session_present: false,
            reason_code: ConnackReason::V5(ConnectReasonCode::Success),
            session_expiry_interval: None,
            receive_maximum: None,
            topic_alias_maximum: None,
            server_keep_alive: None,
            maximum_packet_size: None,
            maximum_qos: None,
            retain_available: None,
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            assigned_client_identifier: None,
            authentication_method: None,
        })
        .expect("CONNACK を受理すること");

    assert_eq!(session.subscription_manager().active_count(), 0);
}

#[test]
fn connect_sent_resets_connection_scoped_state() {
    // MQTT v5.0 §3.3.2.3.4 [MQTT-3.3.2-7]: トピックエイリアスの
    // マッピングは Network Connection をまたいで持ち越してはならない。
    // MQTT v5.0 §4.9: send quota と Receive Maximum は
    // 新しい Network Connection ごとに再初期化される。
    let mut session =
        Session::new_v5("client-1".into(), false).expect("セッションの作成に成功すること");
    session
        .configure_for_connect(60, 0, 16, 2, 0)
        .expect("CONNECT パラメータを設定できること");
    session.connect_sent();
    session
        .apply_connack(ConnackParams {
            session_present: false,
            reason_code: ConnackReason::V5(ConnectReasonCode::Success),
            session_expiry_interval: None,
            receive_maximum: Some(5),
            topic_alias_maximum: Some(4),
            server_keep_alive: None,
            maximum_packet_size: None,
            maximum_qos: None,
            retain_available: None,
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            assigned_client_identifier: None,
            authentication_method: None,
        })
        .expect("CONNACK を受理すること");

    // 接続中に接続スコープの状態を作る。
    session
        .topic_alias_manager_mut()
        .register_for_send("sensor/temperature")
        .expect("送信用エイリアスを割り当てられること");
    session
        .flow_control_mut()
        .publish_sent()
        .expect("フロー制御の送信記録に成功すること");
    session
        .flow_control_mut()
        .publish_received()
        .expect("フロー制御の受信記録に成功すること");
    session
        .auth_state_mut()
        .connect_with_auth("SCRAM-SHA-256".into());
    assert_eq!(session.flow_control().outstanding_count(), 1);
    assert_eq!(session.flow_control().incoming_count(), 1);

    // 切断して再接続を開始する。
    session.disconnected();
    session.connect_sent();

    // 接続スコープの状態はすべて再初期化される。
    assert_eq!(
        session
            .topic_alias_manager()
            .find_alias_for_topic("sensor/temperature"),
        None
    );
    assert_eq!(session.topic_alias_manager().peer_maximum(), 0);
    assert_eq!(session.flow_control().receive_maximum(), 65535);
    assert_eq!(session.flow_control().outstanding_count(), 0);
    assert_eq!(session.flow_control().incoming_count(), 0);
    assert!(!session.auth_state().is_authenticating());
    // configure_for_connect で設定した自身の Topic Alias Maximum は維持される。
    assert_eq!(session.topic_alias_manager().own_maximum(), 16);
    // configure_for_connect で設定した自身の Receive Maximum も維持される。
    assert_eq!(session.flow_control().own_receive_maximum(), 2);
}

#[test]
fn session_state_survives_reconnect_with_session_present() {
    // 接続スコープの状態がリセットされても、セッション状態
    // （QoS フロー・サブスクリプション）は session_present=true の
    // 再接続で維持される。
    let mut session =
        Session::new_v5("client-1".into(), false).expect("セッションの作成に成功すること");
    session.connect_sent();
    session.connected(true);

    session.qos_flow_mut().publish_sent_qos1(10);
    session.subscription_manager_mut().subscribe_sent(
        1,
        vec![SubscriptionEntry::new(
            "a/b",
            crate::codec::qos::QoS::AtLeastOnce,
        )],
    );
    session
        .subscription_manager_mut()
        .suback_received(1, &[0x01]);

    session.disconnected();
    session.connect_sent();
    session.connected(true);

    assert!(session.qos_flow().is_active(10));
    assert_eq!(session.subscription_manager().active_count(), 1);
}

#[test]
fn subscriptions_can_be_relisted_after_session_discard() {
    // MQTT v5.0 §3.2.2.1.1 [MQTT-3.2.2-5]:
    // Session Present=0 の CONNACK を受信した場合、クライアントは
    // セッション状態を破棄する。破棄後に購読を再確立するために、
    // 接続断前にアクティブだった購読一覧を取得できる必要がある。
    let mut session =
        Session::new_v5("client-1".into(), false).expect("セッションの作成に成功すること");
    session.connect_sent();
    session.connected(true);

    let mut entry1 = SubscriptionEntry::new("a/b", crate::codec::qos::QoS::AtLeastOnce);
    entry1.no_local = true;
    entry1.retain_handling = crate::v5::subscribe::RetainHandling::DoNotSendRetained;
    entry1.subscription_identifier = Some(42);
    let mut entry2 = SubscriptionEntry::new("c/d", crate::codec::qos::QoS::ExactlyOnce);
    entry2.retain_as_published = true;

    session
        .subscription_manager_mut()
        .subscribe_sent(1, vec![entry1, entry2]);
    session
        .subscription_manager_mut()
        .suback_received(1, &[0x01, 0x02]);

    let active_entries: Vec<SubscriptionEntry> = session
        .subscription_manager()
        .active_subscriptions()
        .into_iter()
        .cloned()
        .collect();
    assert_eq!(active_entries.len(), 2);

    // Session Present=0 の CONNACK を受信し、セッション状態を破棄する。
    session.disconnected();
    session.connect_sent();
    session.connected(false);

    assert_eq!(session.subscription_manager().active_count(), 0);

    // 破棄前に取得した購読一覧を基に codec 層の SUBSCRIBE を再構築できる。
    let resubscribe: Vec<crate::v5::subscribe::Subscription> = active_entries
        .into_iter()
        .map(|entry| entry.into())
        .collect();
    assert_eq!(resubscribe.len(), 2);
    let a = resubscribe
        .iter()
        .find(|s| s.topic_filter == "a/b")
        .expect("a/b が含まれること");
    assert!(a.no_local);
    assert_eq!(
        a.retain_handling,
        crate::v5::subscribe::RetainHandling::DoNotSendRetained
    );
    let c = resubscribe
        .iter()
        .find(|s| s.topic_filter == "c/d")
        .expect("c/d が含まれること");
    assert!(c.retain_as_published);
}

#[test]
fn apply_connack_accepts_session_present_with_existing_session_state() {
    // clean_start=false でセッション状態が存在する場合、session_present=true は正常。
    let mut session =
        Session::new_v5("client-1".into(), false).expect("セッションの作成に成功すること");
    session.subscription_manager_mut().subscribe_sent(
        1,
        vec![SubscriptionEntry::new(
            "a/b",
            crate::codec::qos::QoS::AtLeastOnce,
        )],
    );
    session
        .subscription_manager_mut()
        .suback_received(1, &[0x01]);
    assert_eq!(session.subscription_manager().active_count(), 1);

    session.connect_sent();
    let result = session.apply_connack(ConnackParams {
        session_present: true,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: None,
    });

    assert_eq!(result, Ok(()));
    assert!(session.is_connected());
    // 既存の購読は session_present=true により維持される。
    assert_eq!(session.subscription_manager().active_count(), 1);
}

#[test]
fn connack_params_has_v5_only_fields_detects_all_fields() {
    // v5 専用フィールドのいずれかが Some なら true、すべて None なら false を返す。
    fn base() -> ConnackParams {
        ConnackParams {
            session_present: false,
            reason_code: ConnackReason::V5(ConnectReasonCode::Success),
            session_expiry_interval: None,
            receive_maximum: None,
            topic_alias_maximum: None,
            server_keep_alive: None,
            maximum_packet_size: None,
            maximum_qos: None,
            retain_available: None,
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            assigned_client_identifier: None,
            authentication_method: None,
        }
    }
    assert!(!base().has_v5_only_fields());

    assert!(
        ConnackParams {
            session_expiry_interval: Some(1),
            ..base()
        }
        .has_v5_only_fields()
    );
    assert!(
        ConnackParams {
            receive_maximum: Some(1),
            ..base()
        }
        .has_v5_only_fields()
    );
    assert!(
        ConnackParams {
            topic_alias_maximum: Some(1),
            ..base()
        }
        .has_v5_only_fields()
    );
    assert!(
        ConnackParams {
            server_keep_alive: Some(1),
            ..base()
        }
        .has_v5_only_fields()
    );
    assert!(
        ConnackParams {
            maximum_packet_size: Some(1),
            ..base()
        }
        .has_v5_only_fields()
    );
    assert!(
        ConnackParams {
            maximum_qos: Some(QoS::AtMostOnce),
            ..base()
        }
        .has_v5_only_fields()
    );
    assert!(
        ConnackParams {
            retain_available: Some(false),
            ..base()
        }
        .has_v5_only_fields()
    );
    assert!(
        ConnackParams {
            wildcard_subscription_available: Some(false),
            ..base()
        }
        .has_v5_only_fields()
    );
    assert!(
        ConnackParams {
            subscription_identifiers_available: Some(false),
            ..base()
        }
        .has_v5_only_fields()
    );
    assert!(
        ConnackParams {
            shared_subscription_available: Some(false),
            ..base()
        }
        .has_v5_only_fields()
    );
    assert!(
        ConnackParams {
            assigned_client_identifier: Some("id".into()),
            ..base()
        }
        .has_v5_only_fields()
    );
    assert!(
        ConnackParams {
            authentication_method: Some("METHOD".into()),
            ..base()
        }
        .has_v5_only_fields()
    );
}

// ==================================================================
// 受信パケットの統合ハンドラ
// ==================================================================
#[test]
fn handle_puback_releases_packet_id_and_restores_send_quota() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .flow_control_mut()
        .set_receive_maximum(2)
        .expect("Receive Maximum を設定できること");

    let id = session
        .packet_id_manager_mut()
        .allocate()
        .expect("パケット識別子を割り当てられること");
    session
        .flow_control_mut()
        .publish_sent()
        .expect("送信枠を消費できること");
    session.qos_flow_mut().publish_sent_qos1(id);

    let action = session.handle_puback(id);

    assert_eq!(action, Ok(Some(Action::Complete { packet_id: id })));
    assert!(!session.qos_flow().is_active(id));
    assert_eq!(session.flow_control().outstanding_count(), 0);
    assert_eq!(session.packet_id_manager().in_use_count(), 0);
}

#[test]
fn handle_pubrec_returns_send_pubrel() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");

    let id = session
        .packet_id_manager_mut()
        .allocate()
        .expect("パケット識別子を割り当てられること");
    session.qos_flow_mut().publish_sent_qos2(id);

    let action = session.handle_pubrec(id, 0x00);

    assert_eq!(action, Ok(Some(Action::SendPubrel { packet_id: id })));
    assert!(session.qos_flow().is_active(id));
}

#[test]
fn handle_pubrec_aborted_releases_resources() {
    // MQTT v5.0 §4.4 [MQTT-4.4.0-2]:
    // Reason Code 0x80 以上の PUBREC は確認済み扱いで、
    // 送信クォータとパケット識別子を解放する。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .flow_control_mut()
        .set_receive_maximum(2)
        .expect("Receive Maximum を設定できること");

    let id = session
        .packet_id_manager_mut()
        .allocate()
        .expect("パケット識別子を割り当てられること");
    session
        .flow_control_mut()
        .publish_sent()
        .expect("送信枠を消費できること");
    session.qos_flow_mut().publish_sent_qos2(id);

    let action = session.handle_pubrec(id, 0x87);

    assert_eq!(action, Ok(Some(Action::Aborted { packet_id: id })));
    assert!(!session.qos_flow().is_active(id));
    assert_eq!(session.flow_control().outstanding_count(), 0);
    assert_eq!(session.packet_id_manager().in_use_count(), 0);
}

#[test]
fn handle_pubcomp_releases_resources() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .flow_control_mut()
        .set_receive_maximum(2)
        .expect("Receive Maximum を設定できること");

    let id = session
        .packet_id_manager_mut()
        .allocate()
        .expect("パケット識別子を割り当てられること");
    session
        .flow_control_mut()
        .publish_sent()
        .expect("送信枠を消費できること");
    session.qos_flow_mut().publish_sent_qos2(id);
    let _ = session.handle_pubrec(id, 0x00);

    let action = session.handle_pubcomp(id);

    assert_eq!(action, Ok(Some(Action::Complete { packet_id: id })));
    assert!(!session.qos_flow().is_active(id));
    assert_eq!(session.flow_control().outstanding_count(), 0);
    assert_eq!(session.packet_id_manager().in_use_count(), 0);
}
