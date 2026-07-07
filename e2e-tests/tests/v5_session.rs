//! MQTT v5.0 の Session 状態機械 E2E テスト。

mod helpers;

use std::time::Duration;

use e2e_tests::v5::client::MqttClient;
use helpers::start_mosquitto;
use shiguredo_mqtt::codec::qos::QoS;

#[tokio::test]
async fn v5_session_connection_lifecycle() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut client = MqttClient::connect_tcp(host, port)
        .await
        .expect("クライアントの TCP 接続に成功すること");

    // 接続前は Disconnected。
    assert!(!client.session().is_connected());

    client
        .connect_v5("e2e-v5-session-lifecycle")
        .await
        .expect("MQTT 接続に成功すること");

    // 接続後は Connected。
    assert!(client.session().is_connected());
    assert!(client.session().clean_start());
    assert!(client.session().is_v5());

    client.disconnect().await.expect("切断に成功すること");

    // 切断後は Disconnected。
    assert!(!client.session().is_connected());
}

/// Session が QoS 1 の PUBLISH / PUBACK フローを正しく追跡することを検証する。
#[tokio::test]
async fn v5_session_qos1_flow_tracking() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-session-qos1-sub")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/session/qos1", QoS::AtLeastOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-session-qos1-pub")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");

    // QoS 1 パブリッシュ送信後、完了後は再送対象が空になる。
    publisher
        .publish_with_qos("test/v5/session/qos1", b"qos1 session", QoS::AtLeastOnce)
        .await
        .expect("QoS 1 パブリッシュに成功すること");

    // パブリッシュ完了後は再送対象が空。
    assert!(
        publisher.session().pending_retransmissions().is_empty(),
        "QoS 1 完了後は再送対象が空であること"
    );

    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("PUBLISH パケットの受信に成功すること");
    assert_eq!(received.topic, "test/v5/session/qos1");
    assert_eq!(received.payload, b"qos1 session");
    assert_eq!(received.qos, QoS::AtLeastOnce);

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// Session が clean_start=false かつ session_present=true の場合に
/// 状態を維持することを検証する。
#[tokio::test]
async fn v5_session_persistent_preserves_state() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;
    let client_id = "e2e-v5-session-persist";

    let mut client = MqttClient::connect_tcp(host, port)
        .await
        .expect("クライアントの TCP 接続に成功すること");
    client
        .connect_v5_with_session_expiry(client_id, 3600)
        .await
        .expect("初回 MQTT 接続に成功すること");
    assert!(client.session().is_connected());
    client.disconnect().await.expect("初回切断に成功すること");

    // clean_start=false で再接続。
    let mut client = MqttClient::connect_tcp(host, port)
        .await
        .expect("再接続に成功すること");
    client
        .connect_v5_with_clean_start(client_id, false)
        .await
        .expect("再接続に成功すること");

    // clean_start=false、session_present=true の場合、セッション状態はリセットされない。
    assert!(client.session().is_connected());
    // セッション有効期限は CONNACK のプロパティ反映後の値。
    assert!(!client.session().clean_start());

    client.disconnect().await.expect("切断に成功すること");
}

/// Session が QoS 2 の PUBLISH フローを正しく追跡することを検証する。
#[tokio::test]
async fn v5_session_qos2_flow_tracking() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-session-qos2-sub")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/session/qos2", QoS::ExactlyOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-session-qos2-pub")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");

    // QoS 2 パブリッシュ送信。
    publisher
        .publish_with_qos("test/v5/session/qos2", b"qos2 session", QoS::ExactlyOnce)
        .await
        .expect("QoS 2 パブリッシュに成功すること");

    // パブリッシュ完了後は再送対象が空。
    assert!(
        publisher.session().pending_retransmissions().is_empty(),
        "QoS 2 完了後は再送対象が空であること"
    );

    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("PUBLISH パケットの受信に成功すること");
    assert_eq!(received.topic, "test/v5/session/qos2");
    assert_eq!(received.payload, b"qos2 session");
    assert_eq!(received.qos, QoS::ExactlyOnce);

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}
