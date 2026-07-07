//! Mosquitto ブローカーに対する接続テスト。
//!
//! Docker が必要なため、既定の workspace test からは除外する (CODEBASE.md)。

mod helpers;

use helpers::{connect_client, start_mosquitto};

/// MQTT v5.0 で Mosquitto に接続し、DISCONNECT まで完了できることを確認する。
#[tokio::test]
async fn mosquitto_v5_connect_disconnect() {
    let guard = start_mosquitto().await;

    let mut client = connect_client(&guard, "tokio-mqtt-v5-connect").await;

    client
        .disconnect()
        .await
        .expect("Mosquitto に対して DISCONNECT を送信できること");
}
