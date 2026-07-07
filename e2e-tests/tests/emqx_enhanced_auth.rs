//! EMQX に対する MQTT v5.0 Enhanced Authentication (SCRAM-SHA-256) の E2E テスト。
//!
//! `helpers::start_emqx_scram` が Dashboard HTTP API で SCRAM authenticator と
//! テストユーザーを注入した EMQX 5.8.8 を起動する。
//! 初回認証の成功と、誤パスワードによる失敗を検証する。
//!
//! 再認証 (MQTT v5.0 §4.12.1) は本テストの対象外。

mod helpers;

use e2e_tests::v5::client::{ClientError, MqttClient};
use helpers::start_emqx_scram;
use shiguredo_mqtt::v5::connack::ConnectReasonCode;

/// SCRAM-SHA-256 初回認証が成功し、セッションが Authenticated になることを確認する。
///
/// CONNECT (Method + client-first) → AUTH 0x18 → AUTH 0x18 → CONNACK Success
/// のあと、ServerSignature 検証込みで `is_authenticated() == true` になること。
#[tokio::test]
async fn emqx_scram_sha256_initial_auth_success() {
    let guard = start_emqx_scram().await;

    let mut client = MqttClient::connect_tcp(&guard.host, guard.port)
        .await
        .expect("EMQX への TCP 接続に成功すること");

    client
        .connect_v5_with_scram(
            "e2e-scram-success",
            guard.scram_user_id,
            guard.scram_password,
        )
        .await
        .expect("SCRAM-SHA-256 初回認証に成功すること");

    assert!(
        client.session().is_authenticated(),
        "成功 CONNACK 適用後は Authenticated であること"
    );

    client
        .disconnect()
        .await
        .expect("認証成功後に DISCONNECT を送信できること");
}

/// 誤パスワードでは AUTH 往復のあと CONNACK NotAuthorized で拒否されることを確認する。
///
/// CONNECT 直後の即 CONNACK 拒否は期待しない。client-final 送信後に
/// `ConnectRefused(NotAuthorized)` となり、`is_authenticated() == false` であること。
#[tokio::test]
async fn emqx_scram_sha256_initial_auth_wrong_password() {
    let guard = start_emqx_scram().await;

    let mut client = MqttClient::connect_tcp(&guard.host, guard.port)
        .await
        .expect("EMQX への TCP 接続に成功すること");

    let result = client
        .connect_v5_with_scram(
            "e2e-scram-wrong-password",
            guard.scram_user_id,
            "definitely-wrong-password",
        )
        .await;

    match result {
        Err(ClientError::ConnectRefused(ConnectReasonCode::NotAuthorized)) => {}
        other => panic!("誤パスワードは ConnectRefused(NotAuthorized) であること: {other:?}"),
    }

    assert!(
        !client.session().is_authenticated(),
        "認証失敗後は Authenticated でないこと"
    );
}
