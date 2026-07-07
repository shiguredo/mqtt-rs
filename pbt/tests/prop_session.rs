//! Session 状態機械のプロパティベーステスト。

use proptest::prelude::*;
use shiguredo_mqtt::codec::MqttVersion;
use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::state::auth::{AuthAction, AuthError};
use shiguredo_mqtt::state::session::{
    ConnackError, ConnackParams, ConnackReason, ConnectionState, HandleSubackError,
    HandleUnsubackError, Session,
};
use shiguredo_mqtt::state::subscribe::SubscriptionEntry;
use shiguredo_mqtt::v5::connack::ConnectReasonCode;
use shiguredo_mqtt::v311::connack::ConnectReturnCode;

/// v3.1.1 セッションに対して v5 専用フィールドのどれを 1 つ設定するかを表す。
#[derive(Debug, Clone)]
enum V5OnlyField {
    SessionExpiryInterval,
    ReceiveMaximum,
    TopicAliasMaximum,
    ServerKeepAlive,
    MaximumPacketSize,
    MaximumQoS,
    RetainAvailable,
    WildcardSubscriptionAvailable,
    SubscriptionIdentifiersAvailable,
    SharedSubscriptionAvailable,
    AssignedClientIdentifier,
    AuthenticationMethod,
}

impl Arbitrary for V5OnlyField {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(Self::SessionExpiryInterval),
            Just(Self::ReceiveMaximum),
            Just(Self::TopicAliasMaximum),
            Just(Self::ServerKeepAlive),
            Just(Self::MaximumPacketSize),
            Just(Self::MaximumQoS),
            Just(Self::RetainAvailable),
            Just(Self::WildcardSubscriptionAvailable),
            Just(Self::SubscriptionIdentifiersAvailable),
            Just(Self::SharedSubscriptionAvailable),
            Just(Self::AssignedClientIdentifier),
            Just(Self::AuthenticationMethod),
        ]
        .boxed()
    }
}

proptest! {
    /// Session の接続ライフサイクルと clean_start 保持。
    ///
    /// clean_start は再接続時のセッション破棄判定に効くため、
    /// ライフサイクル後も getter が初期値を保持することを検証する。
    #[test]
    fn session_proptest_connect_disconnect_lifecycle(
        clean_start in any::<bool>(),
        protocol_version in prop_oneof![Just(MqttVersion::V5), Just(MqttVersion::V311)],
    ) {
        let mut session = Session::new(protocol_version, "client-1".into(), clean_start)
            .expect("セッションの作成に成功すること");
        prop_assert_eq!(session.clean_start(), clean_start);
        prop_assert!(!session.is_connected());

        session.connect_sent();
        prop_assert!(session.is_active());
        prop_assert!(!session.is_connected());
        prop_assert_eq!(session.connection_state(), ConnectionState::Connecting);

        session.connected(false);
        prop_assert!(session.is_connected());
        prop_assert_eq!(session.connection_state(), ConnectionState::Connected);
        prop_assert_eq!(session.clean_start(), clean_start);

        session.disconnect_sent();
        prop_assert_eq!(session.connection_state(), ConnectionState::Disconnecting);
        session.disconnected();
        prop_assert!(!session.is_connected());
        prop_assert_eq!(session.connection_state(), ConnectionState::Disconnected);
        // 切断後も clean_start フラグは次の接続開始まで保持される。
        prop_assert_eq!(session.clean_start(), clean_start);
    }

    /// PINGRESP 受信で KeepAlive の PINGRESP 待ち状態がクリアされる。
    #[test]
    fn session_proptest_pingresp_received_clears_awaiting_state(
        keep_alive in 1u16..=3600u16,
    ) {
        let mut session = Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
        session.keep_alive_mut().set_keep_alive(keep_alive);

        // PINGREQ 送信で PINGRESP 待ちが立ち、Keep Alive 間隔経過でタイムアウトする。
        let deadline_ms = (keep_alive as u64) * 1000;
        session.pingreq_sent(0);
        prop_assert!(session.keep_alive().is_awaiting_pingresp());
        prop_assert!(session.keep_alive().has_timed_out(deadline_ms));

        // PINGRESP 受信で待ち状態が解除され、以降はタイムアウトしない。
        session.pingresp_received();
        prop_assert!(!session.keep_alive().is_awaiting_pingresp());
        prop_assert!(!session.keep_alive().has_timed_out(deadline_ms.saturating_mul(10)));
    }

    /// AUTH 受信・成功 CONNACK・再認証開始が AuthStateMachine に委譲される。
    ///
    /// 初回認証中の AUTH 0x18 の Continue、成功 CONNACK での初回認証完了、
    /// 再認証開始と AUTH 0x00 での再認証完了を 1 サイクルで検証する。
    #[test]
    fn session_proptest_auth_received_delegation(
        auth_method in "[a-zA-Z0-9-]{3,16}",
    ) {
        let mut session = Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
        session
            .connect_sent_with_auth(auth_method.clone())
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert!(session.is_initial_authenticating());

        // 初回認証中の AUTH 0x18 は Continue。
        let action = session.auth_received(0x18, Some(auth_method.as_str()));
        prop_assert_eq!(action, AuthAction::Continue);
        prop_assert!(session.is_initial_authenticating());

        // 成功 CONNACK (Method 一致) で初回認証が完了する。
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
                authentication_method: Some(auth_method.clone()),
            })
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert!(session.auth_state().is_authenticated());

        // 再認証開始と AUTH 0x00 での完了。
        session
            .reauthenticate_sent()
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert!(session.is_reauthenticating());
        let action = session.auth_received(0x00, Some(auth_method.as_str()));
        prop_assert_eq!(action, AuthAction::Authenticated);
        prop_assert!(session.auth_state().is_authenticated());
    }

    /// 未確認 QoS 1 PUBLISH が再送対象として返る。
    #[test]
    fn session_proptest_pending_retransmissions_qos1(
        packet_id in 1u16..=65535u16,
    ) {
        let mut session = Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
        session.qos_flow_mut().publish_sent_qos1(packet_id);

        let retrans = session.pending_retransmissions();
        prop_assert_eq!(retrans.len(), 1);
        prop_assert_eq!(retrans[0].0, packet_id);
    }

    /// PUBACK 受信後は再送対象から除外される。
    #[test]
    fn session_proptest_pending_retransmissions_empty_after_puback(
        packet_id in 1u16..=65535u16,
    ) {
        let mut session = Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
        session.qos_flow_mut().publish_sent_qos1(packet_id);
        prop_assert!(
            session
                .qos_flow_mut()
                .puback_received(packet_id)
                .is_ok_and(|a| a.is_some())
        );

        let retrans = session.pending_retransmissions();
        prop_assert!(retrans.is_empty());
    }

    /// PUBREL の再送は send quota を消費しない
    /// （MQTT v5.0 §4.9 [MQTT-4.9.0-2] / MQTT v5.0 §4.9 [MQTT-4.9.0-3]）。
    #[test]
    fn session_proptest_resend_pubrel_does_not_consume_send_quota(
        flow_count in 1u16..=50u16,
    ) {
        let mut session = Session::new_v5("client-1".into(), false).expect("セッションの作成に成功すること");
        session.connect_sent();
        session.connected(true);
        session
            .flow_control_mut()
            .set_receive_maximum(flow_count)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;

        // send quota を使い切るまで QoS 2 PUBLISH を送信し、PUBREC を受信して
        // PUBCOMP 待ち（PUBREL 再送対象）にする。
        // 成功 PUBREC では send quota は回復しない（回復は PUBCOMP 受信時）。
        for packet_id in 1..=flow_count {
            session.qos_flow_mut().publish_sent_qos2(packet_id);
            session
                .flow_control_mut()
                .publish_sent()
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert!(
                session
                    .qos_flow_mut()
                    .pubrec_received(packet_id, 0x00)
                    .is_ok_and(|a| a.is_some())
            );
        }
        prop_assert!(!session.flow_control().can_send());

        // send quota が 0 でもすべての PUBREL を再送でき、quota は変化しない。
        for packet_id in 1..=flow_count {
            let action = session.resend_pubrel(packet_id);
            prop_assert_eq!(
                action,
                Some(shiguredo_mqtt::state::qos_flow::Action::ResendPubrel { packet_id })
            );
            prop_assert_eq!(session.flow_control().outstanding_count(), flow_count);
        }
    }

    /// QoS 2 受信フローの受信枠は成功 PUBREC 送信では解放されず、
    /// PUBCOMP 送信で解放される（MQTT v5.0 §3.3.4 [MQTT-3.3.4-9]）。
    #[test]
    fn session_proptest_qos2_incoming_quota_released_at_pubcomp(
        flow_count in 1u16..=50u16,
    ) {
        let mut session = Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
        session
            .configure_for_connect(60, 0, 0, flow_count, 0)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;

        // Receive Maximum いっぱいまで QoS 2 PUBLISH を受信し、成功 PUBREC を送信する。
        for packet_id in 1..=flow_count {
            session
                .handle_publish_qos2(packet_id)
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            session.pubrec_sent(0x00);
        }
        // 成功 PUBREC の送信では受信方向の未確認カウントが維持される。
        prop_assert_eq!(session.flow_control().incoming_count(), flow_count);
        prop_assert!(!session.flow_control().can_receive());

        // PUBREL 受信 → PUBCOMP 送信で 1 フローずつ受信枠が解放される。
        for packet_id in 1..=flow_count {
            session
                .handle_pubrel(packet_id)
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            session.pubcomp_sent();
            prop_assert_eq!(
                session.flow_control().incoming_count(),
                flow_count - packet_id
            );
        }
        prop_assert!(session.flow_control().can_receive());
    }

    /// configure_for_connect でパラメータが正しく設定される。
    #[test]
    fn session_proptest_configure_for_connect(
        keep_alive in 0u16..=65535u16,
        session_expiry in 0u32..=u32::MAX,
        topic_alias_max in 0u16..=65535u16,
        receive_maximum in 1u16..=65535u16,
        maximum_packet_size in 0u32..=u32::MAX,
    ) {
        let mut session = Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
        session.configure_for_connect(keep_alive, session_expiry, topic_alias_max, receive_maximum, maximum_packet_size)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;

        prop_assert_eq!(session.keep_alive().keep_alive_secs(), keep_alive);
        prop_assert_eq!(session.session_expiry_interval(), session_expiry);
        prop_assert_eq!(session.topic_alias_manager().own_maximum(), topic_alias_max);
        prop_assert_eq!(session.flow_control().own_receive_maximum(), receive_maximum);
        if maximum_packet_size > 0 {
            prop_assert_eq!(session.limits().max_packet_size, maximum_packet_size as usize);
        }
    }

    /// apply_connack で接続が確立しパラメータが反映される。
    #[test]
    fn session_proptest_apply_connack(
        session_expiry in any::<u32>(),
        receive_max in 1u16..=100u16,
        server_keep_alive in 0u16..=3600u16,
        topic_alias_max in 0u16..=16u16,
    ) {
        // clean_start=false なら session_present の真偽に関わらず受理される。
        // ここでは false に固定してパラメータ適用の検証に集中する。
        let mut session = Session::new_v5("client-1".into(), false).expect("セッションの作成に成功すること");
        session.connect_sent();

        session
            .apply_connack(ConnackParams {
                session_present: false,
                reason_code: ConnackReason::V5(ConnectReasonCode::Success),
                session_expiry_interval: Some(session_expiry),
                receive_maximum: Some(receive_max),
                topic_alias_maximum: Some(topic_alias_max),
                server_keep_alive: Some(server_keep_alive),
                maximum_packet_size: None,
                maximum_qos: None,
                retain_available: None,
                wildcard_subscription_available: None,
                subscription_identifiers_available: None,
                shared_subscription_available: None,
                assigned_client_identifier: None,
                authentication_method: None,
            })
            .map_err(|e| TestCaseError::fail(e.to_string()))?;

        prop_assert!(session.is_connected());
        // session_expiry、keep_alive は reset_session_state の対象外のため、
        // session_present に関わらず値が反映される。
        prop_assert_eq!(session.session_expiry_interval(), session_expiry);
        prop_assert_eq!(session.keep_alive().keep_alive_secs(), server_keep_alive);
        // apply_connack はリセット処理の後にパラメータを適用するため、
        // session_present に関わらず CONNACK の値が反映される。
        prop_assert_eq!(session.flow_control().receive_maximum(), receive_max);
        prop_assert_eq!(session.topic_alias_manager().peer_maximum(), topic_alias_max);
    }

    /// Connecting 以外の状態で apply_connack を呼ぶと InvalidConnectionState が返る。
    ///
    /// v5 と v3.1.1 の両方で検証する。
    #[test]
    fn session_proptest_apply_connack_rejects_invalid_connection_state(
        protocol_version in prop_oneof![Just(MqttVersion::V5), Just(MqttVersion::V311)],
        state in prop_oneof![
            Just(ConnectionState::Disconnected),
            Just(ConnectionState::Connected),
            Just(ConnectionState::Disconnecting),
        ],
    ) {
        let mut session = Session::new(protocol_version, "client-1".into(), true)
            .expect("セッションの作成に成功すること");
        match state {
            ConnectionState::Disconnected => {}
            ConnectionState::Connected => {
                session.connect_sent();
                session.connected(false);
            }
            ConnectionState::Disconnecting => {
                session.connect_sent();
                session.connected(false);
                session.disconnect_sent();
            }
            ConnectionState::Connecting => unreachable!(),
        }
        let before = session.connection_state();
        let reason_code = match protocol_version {
            MqttVersion::V5 => ConnackReason::V5(ConnectReasonCode::Success),
            MqttVersion::V311 => ConnackReason::V311(ConnectReturnCode::Accepted),
        };

        let result = session.apply_connack(ConnackParams {
            session_present: false,
            reason_code,
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

        prop_assert_eq!(result, Err(ConnackError::InvalidConnectionState));
        prop_assert_eq!(session.connection_state(), before);
    }

    /// v5 セッションに v3.1.1 の Return Code を渡すと VersionMismatch が返る。
    #[test]
    fn session_proptest_apply_connack_rejects_v311_return_code_for_v5_session(
        session_present in any::<bool>(),
    ) {
        let mut session = Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
        session.connect_sent();

        let result = session.apply_connack(ConnackParams {
            session_present,
            reason_code: ConnackReason::V311(ConnectReturnCode::Accepted),
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

        prop_assert_eq!(result, Err(ConnackError::VersionMismatch));
        prop_assert_eq!(session.connection_state(), ConnectionState::Connecting);
        prop_assert!(!session.is_connected());
    }

    /// v3.1.1 セッションに v5 の Reason Code を渡すと VersionMismatch が返る。
    #[test]
    fn session_proptest_apply_connack_rejects_v5_reason_code_for_v311_session(
        session_present in any::<bool>(),
    ) {
        let mut session = Session::new_v311("client-1".into(), true).expect("セッションの作成に成功すること");
        session.connect_sent();

        let result = session.apply_connack(ConnackParams {
            session_present,
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

        prop_assert_eq!(result, Err(ConnackError::VersionMismatch));
        prop_assert_eq!(session.connection_state(), ConnectionState::Connecting);
        prop_assert!(!session.is_connected());
    }

    /// v3.1.1 セッションに v5 専用フィールドを 1 つ以上含めると V5OnlyParameters が返る。
    ///
    /// u16 のフィールドは 0 も含める。特に receive_maximum=Some(0) は v5 セッションでは
    /// InvalidReceiveMaximum となる境界値だが、v3.1.1 セッションでは V5OnlyParameters が先に返る。
    #[test]
    fn session_proptest_apply_connack_rejects_v5_only_params_for_v311_session(
        field in any::<V5OnlyField>(),
        bool_value in any::<bool>(),
        u16_value in 0u16..=65535u16,
        u32_value in any::<u32>(),
        qos_value in prop_oneof!(
            Just(QoS::AtMostOnce),
            Just(QoS::AtLeastOnce),
            Just(QoS::ExactlyOnce)
        ),
        string_value in "[a-zA-Z0-9-]{3,16}",
    ) {
        let mut params = ConnackParams {
            session_present: false,
            reason_code: ConnackReason::V311(ConnectReturnCode::Accepted),
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
        };
        match field {
            V5OnlyField::SessionExpiryInterval => params.session_expiry_interval = Some(u32_value),
            V5OnlyField::ReceiveMaximum => params.receive_maximum = Some(u16_value),
            V5OnlyField::TopicAliasMaximum => params.topic_alias_maximum = Some(u16_value),
            V5OnlyField::ServerKeepAlive => params.server_keep_alive = Some(u16_value),
            V5OnlyField::MaximumPacketSize => params.maximum_packet_size = Some(u32_value),
            V5OnlyField::MaximumQoS => params.maximum_qos = Some(qos_value),
            V5OnlyField::RetainAvailable => params.retain_available = Some(bool_value),
            V5OnlyField::WildcardSubscriptionAvailable => {
                params.wildcard_subscription_available = Some(bool_value)
            }
            V5OnlyField::SubscriptionIdentifiersAvailable => {
                params.subscription_identifiers_available = Some(bool_value)
            }
            V5OnlyField::SharedSubscriptionAvailable => {
                params.shared_subscription_available = Some(bool_value)
            }
            V5OnlyField::AssignedClientIdentifier => {
                params.assigned_client_identifier = Some(string_value)
            }
            V5OnlyField::AuthenticationMethod => {
                params.authentication_method = Some(string_value)
            }
        }

        let mut session = Session::new_v311("client-1".into(), true).expect("セッションの作成に成功すること");
        session.connect_sent();

        let result = session.apply_connack(params);
        prop_assert_eq!(result, Err(ConnackError::V5OnlyParameters));
        prop_assert_eq!(session.connection_state(), ConnectionState::Connecting);
    }

    /// v3.1.1 セッションで connect_sent_with_auth() を呼ぶと V5OnlyOperation で拒否され、
    /// Session の状態が一切変化しない (副作用ゼロ)。
    ///
    /// 拡張認証パケット AUTH は MQTT v5.0 §3.15 の v5 専用パケットであり、
    /// MQTT v3.1.1 §2.2.1 Table 2.1 の制御パケット type 15 は Reserved / Forbidden。
    /// 任意の Authentication Method 文字列を与えても拒否は一定であり、拒否時に
    /// connect_sent() 経由の reset_connection_state() も auth_state の遷移も起きない。
    #[test]
    fn session_proptest_connect_sent_with_auth_rejects_v311_session(
        auth_method in ".*",
    ) {
        let mut session = Session::new_v311("client-1".into(), true).expect("セッションの作成に成功すること");

        // 拒否時の副作用ゼロを検証するため、呼び出し前の観測値をすべて退避する。
        let before_connection_state = session.connection_state();
        let before_is_initial_authenticating = session.is_initial_authenticating();
        let before_is_authenticating = session.is_authenticating();
        let before_is_reauthenticating = session.is_reauthenticating();
        let before_auth_state = session.auth_state().state();
        let before_auth_method = session.auth_state().auth_method().map(String::from);
        let before_receive_maximum = session.flow_control().receive_maximum();
        let before_outstanding_count = session.flow_control().outstanding_count();
        let before_own_receive_maximum = session.flow_control().own_receive_maximum();
        let before_incoming_count = session.flow_control().incoming_count();
        let before_own_maximum = session.topic_alias_manager().own_maximum();
        let before_peer_maximum = session.topic_alias_manager().peer_maximum();
        let before_alias_count = session.topic_alias_manager().alias_count();
        let before_keep_alive_secs = session.keep_alive().keep_alive_secs();
        let before_is_awaiting_pingresp = session.keep_alive().is_awaiting_pingresp();
        let before_server_capabilities = session.server_capabilities().clone();

        // v3.1.1 セッションでの呼び出しは V5OnlyOperation で拒否される。
        let result = session.connect_sent_with_auth(auth_method);
        prop_assert_eq!(result, Err(AuthError::V5OnlyOperation));

        // 退避した観測値がすべて呼び出し前と一致する (pristine 状態のまま)。
        prop_assert_eq!(session.connection_state(), before_connection_state);
        prop_assert_eq!(session.is_initial_authenticating(), before_is_initial_authenticating);
        prop_assert_eq!(session.is_authenticating(), before_is_authenticating);
        prop_assert_eq!(session.is_reauthenticating(), before_is_reauthenticating);
        prop_assert_eq!(session.auth_state().state(), before_auth_state);
        prop_assert_eq!(session.auth_state().auth_method().map(String::from), before_auth_method);
        prop_assert_eq!(session.flow_control().receive_maximum(), before_receive_maximum);
        prop_assert_eq!(session.flow_control().outstanding_count(), before_outstanding_count);
        prop_assert_eq!(session.flow_control().own_receive_maximum(), before_own_receive_maximum);
        prop_assert_eq!(session.flow_control().incoming_count(), before_incoming_count);
        prop_assert_eq!(session.topic_alias_manager().own_maximum(), before_own_maximum);
        prop_assert_eq!(session.topic_alias_manager().peer_maximum(), before_peer_maximum);
        prop_assert_eq!(session.topic_alias_manager().alias_count(), before_alias_count);
        prop_assert_eq!(session.keep_alive().keep_alive_secs(), before_keep_alive_secs);
        prop_assert_eq!(session.keep_alive().is_awaiting_pingresp(), before_is_awaiting_pingresp);
        prop_assert_eq!(session.server_capabilities(), &before_server_capabilities);
    }

    /// v3.1.1 セッションに v5 専用フィールドがすべて None の ConnackParams を渡すと正常に接続する。
    ///
    /// session_present の組み合わせを網羅する:
    /// - clean_start=true の場合、session_present は false のみ受理可能
    ///   （ true は SessionPresentProtocolError ）。
    /// - clean_start=false の場合、session_present は true / false の両方とも受理可能。
    #[test]
    fn session_proptest_apply_connack_accepts_v311_with_none_v5_fields(
        clean_start in any::<bool>(),
        session_present in any::<bool>(),
    ) {
        // clean_start=true かつ session_present=true の組み合わせは別のテストで検証するため除外する。
        prop_assume!(!clean_start || !session_present);

        let mut session = Session::new_v311("client-1".into(), clean_start).expect("セッションの作成に成功すること");
        session.connect_sent();

        session
            .apply_connack(ConnackParams {
                session_present,
                reason_code: ConnackReason::V311(ConnectReturnCode::Accepted),
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
            .map_err(|e| TestCaseError::fail(e.to_string()))?;

        prop_assert!(session.is_connected());
    }

    /// PUBLISH (QoS > 0) 送信シーケンス:
    /// allocate_packet_id → publish_sent → handle_puback で
    /// flow_control / qos_flow / packet_id_manager がすべて 0 に戻る。
    #[test]
    fn session_proptest_publish_sent_then_handle_puback_returns_to_zero(
        qos in prop_oneof![Just(QoS::AtLeastOnce), Just(QoS::ExactlyOnce)],
        receive_maximum in 1u16..=50u16,
    ) {
        let mut session = Session::new_v5("client-1".into(), true)
            .expect("セッションの作成に成功すること");
        session
            .flow_control_mut()
            .set_receive_maximum(receive_maximum)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;

        let id = session
            .allocate_packet_id()
            .expect("パケット識別子を割り当てられること");
        session
            .publish_sent(qos, id)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        // QoS 1 は handle_puback、QoS 2 は handle_pubrec → handle_pubcomp
        // (Session-level 統合ハンドラ) を経由してフローを完了させる。
        match qos {
            QoS::AtLeastOnce => {
                session
                    .handle_puback(id)
                    .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
            }
            QoS::ExactlyOnce => {
                session
                    .handle_pubrec(id, 0x00)
                    .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
                session
                    .handle_pubcomp(id)
                    .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
            }
            QoS::AtMostOnce => unreachable!(),
        }

        prop_assert_eq!(session.flow_control().outstanding_count(), 0);
        prop_assert_eq!(session.qos_flow().active_flow_count(), 0);
        prop_assert_eq!(session.packet_id_manager().in_use_count(), 0);
    }

    /// PUBLISH (QoS > 0) 送信 → abort_publish で状態がすべて回復する
    /// (quota が元に戻り、qos_flow と packet_id_manager も解放される)。
    #[test]
    fn session_proptest_publish_sent_then_abort_publish_recovers_all(
        qos in prop_oneof![Just(QoS::AtLeastOnce), Just(QoS::ExactlyOnce)],
        receive_maximum in 1u16..=50u16,
    ) {
        let mut session = Session::new_v5("client-1".into(), true)
            .expect("セッションの作成に成功すること");
        session
            .flow_control_mut()
            .set_receive_maximum(receive_maximum)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;

        let id = session
            .allocate_packet_id()
            .expect("パケット識別子を割り当てられること");
        session
            .publish_sent(qos, id)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        session.abort_publish(id);

        prop_assert_eq!(session.flow_control().outstanding_count(), 0);
        prop_assert_eq!(session.qos_flow().active_flow_count(), 0);
        prop_assert_eq!(session.packet_id_manager().in_use_count(), 0);
    }

    /// PUBLISH (QoS > 0) の publish_sent が Err(ReceiveMaximumExceeded) を返した
    /// 直後に release_packet_id を呼ぶと、flow_control は Err パスで未変更のまま、
    /// packet_id_manager だけが減る。
    #[test]
    fn session_proptest_publish_sent_err_then_release_packet_id_matches_contract(
        receive_maximum in 1u16..=20u16,
    ) {
        let mut session = Session::new_v5("client-1".into(), true)
            .expect("セッションの作成に成功すること");
        session
            .flow_control_mut()
            .set_receive_maximum(receive_maximum)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;

        // クォータを使い切るまで publish_sent を成功させる。
        let ids: Vec<u16> = (0..receive_maximum)
            .map(|_| session
                .allocate_packet_id()
                .expect("クォータ内はパケット識別子を割り当てられること"))
            .collect();
        for &id in &ids {
            session
                .publish_sent(QoS::AtLeastOnce, id)
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
        }
        prop_assert_eq!(session.flow_control().outstanding_count(), receive_maximum);

        // クォータ枯渇後の 1 件は publish_sent が Err を返す想定。
        let overflow_id = session
            .allocate_packet_id()
            .expect("パケット識別子はまだ余っていること");
        prop_assert!(session
            .publish_sent(QoS::AtLeastOnce, overflow_id)
            .is_err());
        prop_assert_eq!(session.flow_control().outstanding_count(), receive_maximum);
        prop_assert!(!session.qos_flow().is_active(overflow_id));

        // 呼び出し規約どおり release_packet_id で復旧。
        session.release_packet_id(overflow_id);
        prop_assert_eq!(
            session.packet_id_manager().in_use_count(),
            receive_maximum as usize
        );
        prop_assert_eq!(session.flow_control().outstanding_count(), receive_maximum);
    }

    /// SUBSCRIBE → handle_suback(Ok) で subscription_manager の active に登録され、
    /// パケット識別子が解放される。
    #[test]
    fn session_proptest_subscribe_sent_then_handle_suback_ok_activates(
        sub_count in 1u8..=10u8,
    ) {
        let mut session = Session::new_v5("client-1".into(), true)
            .expect("セッションの作成に成功すること");
        let id = session
            .allocate_packet_id()
            .expect("パケット識別子を割り当てられること");
        let entries: Vec<SubscriptionEntry> = (0..sub_count)
            .map(|i| SubscriptionEntry::new(format!("topic/{i}"), QoS::AtLeastOnce))
            .collect();
        session.subscribe_sent(id, entries);

        // reason_codes は全て GrantedQoS1 (0x01) にして全成功のケース。
        let reason_codes: Vec<u8> = (0..sub_count).map(|_| 0x01u8).collect();
        let confirmed = session
            .handle_suback(id, &reason_codes)
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;

        prop_assert_eq!(confirmed.len(), sub_count as usize);
        prop_assert_eq!(
            session.subscription_manager().active_count(),
            sub_count as usize
        );
        prop_assert_eq!(session.packet_id_manager().in_use_count(), 0);
    }

    /// SUBSCRIBE → abort_subscribe で pending から削除され、パケット識別子も解放される。
    #[test]
    fn session_proptest_subscribe_sent_then_abort_subscribe_drops_pending(
        sub_count in 1u8..=10u8,
    ) {
        let mut session = Session::new_v5("client-1".into(), true)
            .expect("セッションの作成に成功すること");
        let id = session
            .allocate_packet_id()
            .expect("パケット識別子を割り当てられること");
        let entries: Vec<SubscriptionEntry> = (0..sub_count)
            .map(|i| SubscriptionEntry::new(format!("topic/{i}"), QoS::AtLeastOnce))
            .collect();
        session.subscribe_sent(id, entries);

        session.abort_subscribe(id);

        prop_assert_eq!(session.packet_id_manager().in_use_count(), 0);
        prop_assert!(session
            .subscription_manager()
            .pending_subscribe_ids()
            .is_empty());
        prop_assert_eq!(session.subscription_manager().active_count(), 0);
    }

    /// SUBSCRIBE → handle_suback(reason_codes.len != sub_count) で
    /// Err(PendingNotFound) が返り、パケット識別子が解放され、
    /// pending も破棄されている (Err パスでも packet_id リークがない)。
    ///
    /// strategy: 基準数 n (>= 2)、差分 delta、上下方向 over を独立に生成し、
    /// 下方向のときは (delta - 1) % (n - 1) で actual_len を [1, n - 1] に丸めて
    /// 常に `actual_len != n` かつ `actual_len >= 1` を strategy レベルで保証する。
    /// これにより prop_filter / prop_assume を使わずに全生成がテスト対象になる。
    #[test]
    fn session_proptest_subscribe_sent_then_handle_suback_v5_mismatch_is_err(
        n in 2u8..=10u8,
        delta in 1u8..=5u8,
        over in prop::bool::ANY,
    ) {
        let actual_len: usize = if over {
            (n as usize) + (delta as usize)
        } else {
            // [1, n - 1] に丸めて空スライス (v3.1.1 相当の全成功扱い) を回避する。
            let d = ((delta as usize) - 1) % ((n as usize) - 1);
            1 + d
        };
        prop_assert_ne!(actual_len, n as usize);
        prop_assert!(actual_len >= 1);

        let mut session = Session::new_v5("client-1".into(), true)
            .expect("セッションの作成に成功すること");
        let id = session
            .allocate_packet_id()
            .expect("パケット識別子を割り当てられること");
        let entries: Vec<SubscriptionEntry> = (0..n)
            .map(|i| SubscriptionEntry::new(format!("topic/{i}"), QoS::AtLeastOnce))
            .collect();
        session.subscribe_sent(id, entries);

        let reason_codes: Vec<u8> = (0..actual_len).map(|_| 0x01u8).collect();
        prop_assert_eq!(
            session.handle_suback(id, &reason_codes),
            Err(HandleSubackError::PendingNotFound)
        );
        prop_assert_eq!(session.packet_id_manager().in_use_count(), 0);
        prop_assert!(session
            .subscription_manager()
            .pending_subscribe_ids()
            .is_empty());
    }

    /// UNSUBSCRIBE → handle_unsuback(reason_codes.len != topic_count) で
    /// Err(PendingNotFound) が返り、パケット識別子が解放され、pending も破棄される。
    /// v5.0 経路のみ (v3.1.1 は length check なし)。strategy は SUBACK 側と同じ
    /// 方式で常に `actual_len != n` かつ `actual_len >= 1` を保証する
    /// (prop_assume! / prop_filter を使わない)。
    #[test]
    fn session_proptest_unsubscribe_sent_then_handle_unsuback_v5_mismatch_is_err(
        n in 2u8..=10u8,
        delta in 1u8..=5u8,
        over in prop::bool::ANY,
    ) {
        let actual_len: usize = if over {
            (n as usize) + (delta as usize)
        } else {
            let d = ((delta as usize) - 1) % ((n as usize) - 1);
            1 + d
        };
        prop_assert_ne!(actual_len, n as usize);
        prop_assert!(actual_len >= 1);

        let mut session = Session::new_v5("client-1".into(), true)
            .expect("セッションの作成に成功すること");
        let id = session
            .allocate_packet_id()
            .expect("パケット識別子を割り当てられること");
        let topic_filters: Vec<String> = (0..n).map(|i| format!("topic/{i}")).collect();
        session.unsubscribe_sent(id, topic_filters);

        let reason_codes: Vec<u8> = vec![0x00u8; actual_len];
        prop_assert_eq!(
            session.handle_unsuback(id, &reason_codes),
            Err(HandleUnsubackError::PendingNotFound)
        );
        prop_assert_eq!(session.packet_id_manager().in_use_count(), 0);
        prop_assert!(session
            .subscription_manager()
            .pending_unsubscribe_ids()
            .is_empty());
    }

    /// UNSUBSCRIBE → handle_unsuback(v5.0 全成功 reason_codes) で active から
    /// トピックフィルタが削除され、パケット識別子が解放される。
    #[test]
    fn session_proptest_unsubscribe_sent_then_handle_unsuback_v5_ok_removes_active(
        topic_count in 1u8..=10u8,
    ) {
        let mut session = Session::new_v5("client-1".into(), true)
            .expect("セッションの作成に成功すること");

        // 事前に active に登録しておく。
        let sub_id = session
            .allocate_packet_id()
            .expect("パケット識別子を割り当てられること");
        let entries: Vec<SubscriptionEntry> = (0..topic_count)
            .map(|i| SubscriptionEntry::new(format!("topic/{i}"), QoS::AtLeastOnce))
            .collect();
        session.subscribe_sent(sub_id, entries);
        let reason_codes: Vec<u8> = (0..topic_count).map(|_| 0x01u8).collect();
        session
            .handle_suback(sub_id, &reason_codes)
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;

        // UNSUBSCRIBE → UNSUBACK (v5.0 全成功 0x00)。
        let unsub_id = session
            .allocate_packet_id()
            .expect("パケット識別子を割り当てられること");
        let topic_filters: Vec<String> = (0..topic_count).map(|i| format!("topic/{i}")).collect();
        session.unsubscribe_sent(unsub_id, topic_filters);
        let unsub_reason_codes: Vec<u8> = (0..topic_count).map(|_| 0x00u8).collect();
        let filters = session
            .handle_unsuback(unsub_id, &unsub_reason_codes)
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;

        prop_assert_eq!(filters.len(), topic_count as usize);
        prop_assert_eq!(session.subscription_manager().active_count(), 0);
        prop_assert_eq!(session.packet_id_manager().in_use_count(), 0);
    }

    /// UNSUBSCRIBE → handle_unsuback(v3.1.1 空スライス) で全成功扱いになる。
    #[test]
    fn session_proptest_unsubscribe_sent_then_handle_unsuback_v311_empty_ok(
        topic_count in 1u8..=10u8,
    ) {
        let mut session = Session::new_v311("client-1".into(), true)
            .expect("セッションの作成に成功すること");

        // 事前に active に登録しておく。
        let sub_id = session
            .allocate_packet_id()
            .expect("パケット識別子を割り当てられること");
        let entries: Vec<SubscriptionEntry> = (0..topic_count)
            .map(|i| SubscriptionEntry::new(format!("topic/{i}"), QoS::AtMostOnce))
            .collect();
        session.subscribe_sent(sub_id, entries);
        let return_codes: Vec<u8> = (0..topic_count).map(|_| 0x00u8).collect();
        session
            .handle_suback(sub_id, &return_codes)
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;

        // UNSUBSCRIBE → UNSUBACK (空スライス)。
        let unsub_id = session
            .allocate_packet_id()
            .expect("パケット識別子を割り当てられること");
        let topic_filters: Vec<String> = (0..topic_count).map(|i| format!("topic/{i}")).collect();
        session.unsubscribe_sent(unsub_id, topic_filters);
        let filters = session
            .handle_unsuback(unsub_id, &[])
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;

        prop_assert_eq!(filters.len(), topic_count as usize);
        prop_assert_eq!(session.subscription_manager().active_count(), 0);
        prop_assert_eq!(session.packet_id_manager().in_use_count(), 0);
    }
}
