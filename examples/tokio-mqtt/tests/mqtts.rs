//! Mosquitto に対する MQTT over TLS (mqtts) のテスト。
//!
//! `helpers::start_mosquitto_tls` が rcgen で生成した CA / server 証明書を
//! 注入した Mosquitto を 8883 で起動する。クライアントは `MqttClient::connect_tls`
//! 経由で接続し、CA を trust root として正規にサーバー証明書を検証する。
//!
//! コンテナランタイムが必要なため、既定の workspace test からは除外する (CODEBASE.md)。

mod helpers;

use std::time::Duration;

use helpers::{connect_client_tls, start_mosquitto_tls};
use shiguredo_mqtt::codec::qos::QoS;

/// MQTT v5.0 を mqtts (TCP + TLS) 上で Mosquitto に接続し、DISCONNECT まで完了できることを確認する。
#[tokio::test]
async fn mosquitto_v5_connect_disconnect_mqtts() {
    let guard = start_mosquitto_tls().await;

    let mut client = connect_client_tls(&guard, "tokio-mqtt-v5-mqtts-connect").await;

    client
        .disconnect()
        .await
        .expect("Mosquitto mqtts に対して DISCONNECT を送信できること");
}

/// mqtts 上で QoS 0 の publish / subscribe 往復を検証する。
#[tokio::test]
async fn mosquitto_v5_publish_subscribe_roundtrip_qos0_mqtts() {
    let guard = start_mosquitto_tls().await;

    let mut subscriber = connect_client_tls(&guard, "tokio-mqtt-v5-mqtts-sub-qos0").await;
    subscriber
        .subscribe("test/tokio/v5/mqtts/topic", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = connect_client_tls(&guard, "tokio-mqtt-v5-mqtts-pub-qos0").await;
    publisher
        .publish(
            "test/tokio/v5/mqtts/topic",
            b"hello mqtts qos0",
            QoS::AtMostOnce,
            false,
        )
        .await
        .expect("パブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("PUBLISH パケットの受信に成功すること");
    assert_eq!(received.topic, "test/tokio/v5/mqtts/topic");
    assert_eq!(received.payload, b"hello mqtts qos0");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}
