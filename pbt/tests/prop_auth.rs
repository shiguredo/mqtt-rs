//! AuthStateMachine のプロパティベーステスト。
//!
//! `AuthStateMachine::connect_with_auth` は `pub(crate)` に降格されており、
//! pbt からは `Session::connect_sent_with_auth` 経由でしか初回認証を開始できない。
//! そのため全プロパティは `Session` API を通して状態遷移を検証する。

use proptest::prelude::*;
use shiguredo_mqtt::state::auth::{AuthAction, AuthState};
use shiguredo_mqtt::state::session::{ConnackParams, ConnackReason, Session};
use shiguredo_mqtt::v5::connack::ConnectReasonCode;

/// 認証方式一致で初回認証の完了 CONNACK を組み立てる補助関数。
fn success_connack_with_auth(method: &str) -> ConnackParams {
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
        authentication_method: Some(method.to_string()),
    }
}

/// 検証用の Session を初回認証開始状態で作る補助関数。
fn session_with_initial_auth(method: &str) -> Session {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .connect_sent_with_auth(method.into())
        .expect("v5 セッションで初回認証を開始できること");
    session
}

proptest! {
    /// 初回認証の状態遷移: Idle → InitialAuthenticating → Authenticated。
    ///
    /// 初回認証の完了は成功 CONNACK (MQTT v5.0 §4.12) で行われる。
    #[test]
    fn auth_initial_success_transition(
        auth_method in "[a-zA-Z0-9-]{3,16}",
    ) {
        let mut session = Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
        prop_assert_eq!(session.auth_state().state(), AuthState::Idle);

        session
            .connect_sent_with_auth(auth_method.clone())
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(session.auth_state().state(), AuthState::InitialAuthenticating);
        prop_assert_eq!(session.auth_state().auth_method(), Some(auth_method.as_str()));
        prop_assert!(session.is_initial_authenticating());

        session
            .apply_connack(success_connack_with_auth(&auth_method))
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(session.auth_state().state(), AuthState::Authenticated);
        prop_assert!(session.auth_state().is_authenticated());
    }

    /// 初回認証中の Continue (AUTH 0x18) → 成功 CONNACK での完了。
    #[test]
    fn auth_initial_continue_then_connack_success(
        auth_method in "[a-zA-Z0-9-]{3,16}",
    ) {
        let mut session = session_with_initial_auth(&auth_method);

        let action = session.auth_received(0x18, Some(auth_method.as_str()));
        prop_assert_eq!(action, AuthAction::Continue);
        prop_assert!(session.is_initial_authenticating());

        session
            .apply_connack(success_connack_with_auth(&auth_method))
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert!(session.auth_state().is_authenticated());
    }

    /// 未知の Reason Code は初回認証中でも Failed 扱い (Idle にリセット)。
    #[test]
    fn auth_initial_unknown_reason_code_fails(
        code in 0x01u8..=0x17u8,
    ) {
        let mut session = session_with_initial_auth("SCRAM-SHA-256");
        let action = session.auth_received(code, Some("SCRAM-SHA-256"));
        prop_assert_eq!(action, AuthAction::Failed);
        prop_assert_eq!(session.auth_state().state(), AuthState::Idle);
    }

    /// 再認証シーケンス: Authenticated → Reauthenticating → 0x18 継続 → 0x00 で完了。
    ///
    /// MQTT v5.0 §4.12.1 [MQTT-4.12.1-1]:
    /// 再認証はクライアントが AUTH ReAuthenticate (0x19) を送信して開始し、
    /// AUTH 0x00 で完了する。Method は初回認証と同じ値を使う。
    #[test]
    fn reauthenticate_sequence(auth_method in "[a-zA-Z0-9-]{3,16}") {
        let mut session = session_with_initial_auth(&auth_method);
        session
            .apply_connack(success_connack_with_auth(&auth_method))
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert!(session.auth_state().is_authenticated());

        session
            .reauthenticate_sent()
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(session.auth_state().state(), AuthState::Reauthenticating);
        prop_assert!(session.is_reauthenticating());
        prop_assert!(!session.is_initial_authenticating());

        prop_assert_eq!(
            session.auth_received(0x18, Some(auth_method.as_str())),
            AuthAction::Continue
        );
        prop_assert_eq!(session.auth_state().state(), AuthState::Reauthenticating);

        prop_assert_eq!(
            session.auth_received(0x00, Some(auth_method.as_str())),
            AuthAction::Authenticated
        );
        prop_assert!(session.auth_state().is_authenticated());
    }

    /// 認証状態のリセット後は Idle に戻る。
    #[test]
    fn auth_reset_returns_to_idle(auth_method in "[a-zA-Z0-9-]{3,16}") {
        let mut session = session_with_initial_auth(&auth_method);
        session
            .apply_connack(success_connack_with_auth(&auth_method))
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert!(session.auth_state().is_authenticated());

        session.auth_state_mut().reset();
        prop_assert_eq!(session.auth_state().state(), AuthState::Idle);
        prop_assert_eq!(session.auth_state().auth_method(), None);
    }

    /// 初回認証中はどの AUTH Reason Code でも `Authenticated` に遷移しない。
    ///
    /// 初回認証の完了は成功 CONNACK であり、AUTH パケットではないという不変条件。
    /// Reason Code ごとの遷移まで固定化して、実装の退行を検出する:
    ///
    /// - 0x18 (Continue): `Continue` を返し `InitialAuthenticating` を維持
    /// - 0x00 (Success): `Failed` を返し `Idle` にリセット (初回完了は CONNACK のみ)
    /// - 0x19 (Re-authenticate): `Failed` を返し `Idle` にリセット (Sent by は Client)
    #[test]
    fn initial_authentication_never_completes_via_auth_packet(
        auth_method in "[a-zA-Z0-9-]{3,16}",
        code in prop::sample::select(&[0x00u8, 0x18u8, 0x19u8][..]),
    ) {
        let mut session = session_with_initial_auth(&auth_method);
        let action = session.auth_received(code, Some(auth_method.as_str()));
        prop_assert!(!session.auth_state().is_authenticated(),
            "初回認証中の AUTH 0x{:02x} で Authenticated に遷移しないこと",
            code);
        match code {
            0x18 => {
                prop_assert_eq!(action, AuthAction::Continue);
                prop_assert_eq!(
                    session.auth_state().state(),
                    AuthState::InitialAuthenticating
                );
            }
            0x00 | 0x19 => {
                prop_assert_eq!(action, AuthAction::Failed);
                prop_assert_eq!(session.auth_state().state(), AuthState::Idle);
                prop_assert_eq!(session.auth_state().auth_method(), None);
            }
            _ => unreachable!("proptest strategy generates only 0x00 / 0x18 / 0x19"),
        }
    }

    /// 初回認証中の AUTH で Method が CONNECT と異なる場合は必ず `MethodMismatch`。
    ///
    /// MQTT v5.0 §4.12 [MQTT-4.12.0-5]。
    #[test]
    fn initial_authentication_method_mismatch_is_reported(
        expected in "[a-zA-Z0-9-]{3,16}",
        received in "[a-zA-Z0-9-]{3,16}",
    ) {
        prop_assume!(expected != received);
        let mut session = session_with_initial_auth(&expected);
        let action = session.auth_received(0x18, Some(received.as_str()));
        prop_assert_eq!(action, AuthAction::MethodMismatch);
        prop_assert_eq!(session.auth_state().state(), AuthState::Idle);
    }
}
