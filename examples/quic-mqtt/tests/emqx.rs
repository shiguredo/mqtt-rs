//! EMQX の MQTT over QUIC に対する接続テスト。
//!
//! Docker が必要なため、既定の workspace test からは除外する (CODEBASE.md)。

mod helpers;

use helpers::{connect_client, start_emqx_quic};

/// MQTT v5.0 を QUIC 上で EMQX に接続し、DISCONNECT まで完了できることを確認する。
///
/// EMQX の QUIC listener は既定で無効なため、`start_emqx_quic` が
/// 環境変数で listener を有効化して起動する。証明書は `start_emqx_quic`
/// 内で rcgen により都度生成した自前 CA が署名した server 証明書を使い、
/// クライアント側はその CA を trust root として正規に検証する。
#[tokio::test]
async fn emqx_v5_connect_disconnect() {
    let guard = start_emqx_quic().await;

    let mut client = connect_client(&guard, "quic-mqtt-emqx-v5-connect").await;

    client
        .disconnect()
        .await
        .expect("EMQX に対して DISCONNECT を送信できること");
}
