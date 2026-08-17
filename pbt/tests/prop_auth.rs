//! AuthStateMachine のプロパティベーステスト。
//!
//! `AuthStateMachine::connect_with_auth` は `pub(crate)` に降格されており、
//! pbt からは `Session::connect_sent_with_auth` 経由でしか初回認証を開始できない。
//! そのため全プロパティは `Session` API を通して状態遷移を検証する。

use noprop::TestCaseContext;
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

/// 認証方式サンプラ。`[a-zA-Z0-9-]{3,16}` 相当を valid-by-construction で生成する。
fn sample_auth_method(ctx: &mut TestCaseContext) -> String {
    let len = noprop::sample_usize_in(ctx, 3..=16);
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        let idx = noprop::sample_usize_in(ctx, 0..63);
        let c = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-"[idx] as char;
        s.push(c);
    }
    s
}

/// 初回認証の状態遷移: Idle → InitialAuthenticating → Authenticated。
///
/// 初回認証の完了は成功 CONNACK (MQTT v5.0 §4.12) で行われる。
#[test]
fn auth_initial_success_transition() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let auth_method = sample_auth_method(ctx);
        let mut session =
            Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
        assert_eq!(session.auth_state().state(), AuthState::Idle);

        session.connect_sent_with_auth(auth_method.clone())?;
        assert_eq!(
            session.auth_state().state(),
            AuthState::InitialAuthenticating
        );
        assert_eq!(
            session.auth_state().auth_method(),
            Some(auth_method.as_str())
        );
        assert!(session.is_initial_authenticating());

        session.apply_connack(success_connack_with_auth(&auth_method))?;
        assert_eq!(session.auth_state().state(), AuthState::Authenticated);
        assert!(session.auth_state().is_authenticated());
        Ok(())
    })?;
    Ok(())
}

/// 初回認証中の Continue (AUTH 0x18) → 成功 CONNACK での完了。
#[test]
fn auth_initial_continue_then_connack_success() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let auth_method = sample_auth_method(ctx);
        let mut session = session_with_initial_auth(&auth_method);

        let action = session.auth_received(0x18, Some(auth_method.as_str()));
        assert_eq!(action, AuthAction::Continue);
        assert!(session.is_initial_authenticating());

        session.apply_connack(success_connack_with_auth(&auth_method))?;
        assert!(session.auth_state().is_authenticated());
        Ok(())
    })?;
    Ok(())
}

/// 未知の Reason Code は初回認証中でも Failed 扱い (Idle にリセット)。
#[test]
fn auth_initial_unknown_reason_code_fails() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let code = noprop::sample_usize_in(ctx, 0x01..=0x17) as u8;
        let mut session = session_with_initial_auth("SCRAM-SHA-256");
        let action = session.auth_received(code, Some("SCRAM-SHA-256"));
        assert_eq!(action, AuthAction::Failed);
        assert_eq!(session.auth_state().state(), AuthState::Idle);
        Ok(())
    })?;
    Ok(())
}

/// 再認証シーケンス: Authenticated → Reauthenticating → 0x18 継続 → 0x00 で完了。
///
/// MQTT v5.0 §4.12.1 [MQTT-4.12.1-1]:
/// 再認証はクライアントが AUTH ReAuthenticate (0x19) を送信して開始し、
/// AUTH 0x00 で完了する。Method は初回認証と同じ値を使う。
#[test]
fn reauthenticate_sequence() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let auth_method = sample_auth_method(ctx);
        let mut session = session_with_initial_auth(&auth_method);
        session.apply_connack(success_connack_with_auth(&auth_method))?;
        assert!(session.auth_state().is_authenticated());

        session.reauthenticate_sent()?;
        assert_eq!(session.auth_state().state(), AuthState::Reauthenticating);
        assert!(session.is_reauthenticating());
        assert!(!session.is_initial_authenticating());

        assert_eq!(
            session.auth_received(0x18, Some(auth_method.as_str())),
            AuthAction::Continue
        );
        assert_eq!(session.auth_state().state(), AuthState::Reauthenticating);

        assert_eq!(
            session.auth_received(0x00, Some(auth_method.as_str())),
            AuthAction::Authenticated
        );
        assert!(session.auth_state().is_authenticated());
        Ok(())
    })?;
    Ok(())
}

/// 認証状態のリセット後は Idle に戻る。
#[test]
fn auth_reset_returns_to_idle() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let auth_method = sample_auth_method(ctx);
        let mut session = session_with_initial_auth(&auth_method);
        session.apply_connack(success_connack_with_auth(&auth_method))?;
        assert!(session.auth_state().is_authenticated());

        session.auth_state_mut().reset();
        assert_eq!(session.auth_state().state(), AuthState::Idle);
        assert_eq!(session.auth_state().auth_method(), None);
        Ok(())
    })?;
    Ok(())
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
fn initial_authentication_never_completes_via_auth_packet() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let auth_method = sample_auth_method(ctx);
        let code = noprop::sample_choice(ctx, &[0x00u8, 0x18u8, 0x19u8]);
        let mut session = session_with_initial_auth(&auth_method);
        let action = session.auth_received(code, Some(auth_method.as_str()));
        assert!(
            !session.auth_state().is_authenticated(),
            "初回認証中の AUTH 0x{:02x} で Authenticated に遷移しないこと",
            code
        );
        match code {
            0x18 => {
                assert_eq!(action, AuthAction::Continue);
                assert_eq!(
                    session.auth_state().state(),
                    AuthState::InitialAuthenticating
                );
            }
            0x00 | 0x19 => {
                assert_eq!(action, AuthAction::Failed);
                assert_eq!(session.auth_state().state(), AuthState::Idle);
                assert_eq!(session.auth_state().auth_method(), None);
            }
            _ => unreachable!("sample_choice は 0x00 / 0x18 / 0x19 のみを生成する"),
        }
        Ok(())
    })?;
    Ok(())
}

/// 初回認証中の AUTH で Method が CONNECT と異なる場合は必ず `MethodMismatch`。
///
/// MQTT v5.0 §4.12 [MQTT-4.12.0-5]。
#[test]
fn initial_authentication_method_mismatch_is_reported() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let expected = sample_auth_method(ctx);
        // expected と異なる Method を引く。長さ 3..16 の文字列どうしの衝突確率は
        // 1/63^3 未満であり、max_attempts=4 で枯渇する確率は実質ゼロである。
        let received = noprop::sample_with_rejection(ctx, 4, |ctx| {
            let m = sample_auth_method(ctx);
            (m != expected).then_some(m)
        });

        let mut session = session_with_initial_auth(&expected);
        let action = session.auth_received(0x18, Some(received.as_str()));
        assert_eq!(action, AuthAction::MethodMismatch);
        assert_eq!(session.auth_state().state(), AuthState::Idle);
        Ok(())
    })?;
    Ok(())
}
