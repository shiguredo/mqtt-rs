//! MQTT v5.0 の基本シナリオ E2E テスト。

mod helpers;

use std::time::Duration;

use e2e_tests::v5::client::MqttClient;
use helpers::start_mosquitto;
use shiguredo_mqtt::codec::qos::QoS;
use tokio::time::{Instant, sleep};

#[tokio::test]
async fn v5_publish_subscribe_roundtrip_qos0() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-qos0")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/topic", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-qos0")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish("test/v5/topic", b"hello v5 qos0")
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
    assert_eq!(received.topic, "test/v5/topic");
    assert_eq!(received.payload, b"hello v5 qos0");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// QoS 1 の publish / subscribe 往復を検証する。
#[tokio::test]
async fn v5_publish_subscribe_roundtrip_qos1() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-qos1")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/topic/qos1", QoS::AtLeastOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-qos1")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish_with_qos("test/v5/topic/qos1", b"hello v5 qos1", QoS::AtLeastOnce)
        .await
        .expect("QoS 1 パブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("PUBLISH パケットの受信に成功すること");
    assert_eq!(received.topic, "test/v5/topic/qos1");
    assert_eq!(received.payload, b"hello v5 qos1");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// QoS 2 の publish / subscribe 往復を検証する。
#[tokio::test]
async fn v5_publish_subscribe_roundtrip_qos2() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-qos2")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/topic/qos2", QoS::ExactlyOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-qos2")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish_with_qos("test/v5/topic/qos2", b"hello v5 qos2", QoS::ExactlyOnce)
        .await
        .expect("QoS 2 パブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("PUBLISH パケットの受信に成功すること");
    assert_eq!(received.topic, "test/v5/topic/qos2");
    assert_eq!(received.payload, b"hello v5 qos2");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 複数トピックをまとめて購読し、各トピックへの publish が受信できることを検証する。
#[tokio::test]
async fn v5_subscribe_multiple_topics() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-multi")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe_many(&[
            ("test/v5/a", QoS::AtMostOnce),
            ("test/v5/b", QoS::AtMostOnce),
        ])
        .await
        .expect("複数トピックのサブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-multi")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish("test/v5/a", b"message a")
        .await
        .expect("test/v5/a へのパブリッシュに成功すること");
    publisher
        .publish("test/v5/b", b"message b")
        .await
        .expect("test/v5/b へのパブリッシュに成功すること");

    let received_a = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("test/v5/a の PUBLISH パケットの受信に成功すること");
    assert_eq!(received_a.topic, "test/v5/a");
    assert_eq!(received_a.payload, b"message a");

    let received_b = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("test/v5/b の PUBLISH パケットの受信に成功すること");
    assert_eq!(received_b.topic, "test/v5/b");
    assert_eq!(received_b.payload, b"message b");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");
}

/// unsubscribe 後にメッセージが届かなくなることを検証する。
#[tokio::test]
async fn v5_unsubscribe() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-unsub")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/unsub", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-unsub")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish("test/v5/unsub", b"before unsubscribe")
        .await
        .expect("パブリッシュに成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("unsubscribe 前の PUBLISH パケットの受信に成功すること");
    assert_eq!(received.payload, b"before unsubscribe");

    subscriber
        .unsubscribe("test/v5/unsub")
        .await
        .expect("アンサブスクライブに成功すること");
    publisher
        .publish("test/v5/unsub", b"after unsubscribe")
        .await
        .expect("パブリッシュに成功すること");

    let result = subscriber.recv_publish(Duration::from_secs(1)).await;
    assert!(result.is_err(), "unsubscribe 後はメッセージが届かないこと");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");
}

/// retain フラグ付きメッセージが、後から購読したクライアントに配信されることを検証する。
#[tokio::test]
async fn v5_retained_message() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-retain")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish_with_retain("test/v5/retain", b"retained v5 hello")
        .await
        .expect("retain 付きパブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-retain")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/retain", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("retain メッセージの受信に成功すること");
    assert_eq!(received.topic, "test/v5/retain");
    assert_eq!(received.payload, b"retained v5 hello");
    assert!(received.retain);

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// セッション有効期限を設定して clean_start=false で再接続した際に、
/// 未送信メッセージを受信できることを検証する。
#[tokio::test]
async fn v5_persistent_session() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5_with_session_expiry("e2e-v5-subscriber-persistent", 3600)
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/persistent", QoS::AtLeastOnce)
        .await
        .expect("サブスクライブに成功すること");
    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-persistent")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish_with_qos(
            "test/v5/persistent",
            b"offline v5 message",
            QoS::AtLeastOnce,
        )
        .await
        .expect("パブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの再接続に成功すること");
    subscriber
        .connect_v5_with_clean_start("e2e-v5-subscriber-persistent", false)
        .await
        .expect("サブスクライバーの MQTT 再接続に成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("未送信メッセージの受信に成功すること");
    assert_eq!(received.topic, "test/v5/persistent");
    assert_eq!(received.payload, b"offline v5 message");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// Will メッセージが、接続が強制切断された際に配信されることを検証する。
#[tokio::test]
async fn v5_will_message() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-will")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/will", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5_with_will(
            "e2e-v5-publisher-will",
            "test/v5/will",
            b"will v5 payload",
            QoS::AtMostOnce,
        )
        .await
        .expect("Will 付き MQTT 接続に成功すること");

    // TCP 接続を強制切断する。
    publisher.force_disconnect();

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("Will メッセージの受信に成功すること");
    assert_eq!(received.topic, "test/v5/will");
    assert_eq!(received.payload, b"will v5 payload");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 複数のパブリッシャーからのメッセージを 1 つのサブスクライバーが受信できることを検証する。
#[tokio::test]
async fn v5_multiple_publishers() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-multi-pub")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/multi", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    for i in 0..3 {
        let mut publisher = MqttClient::connect_tcp(host, port)
            .await
            .expect("パブリッシャーの TCP 接続に成功すること");
        publisher
            .connect_v5(&format!("e2e-v5-publisher-{i}"))
            .await
            .expect("パブリッシャーの MQTT 接続に成功すること");
        publisher
            .publish("test/v5/multi", format!("message {i}").as_bytes())
            .await
            .expect("パブリッシュに成功すること");
        let deadline = Instant::now() + Duration::from_millis(100);
        while Instant::now() < deadline {
            sleep(Duration::from_millis(10)).await;
        }
        publisher
            .disconnect()
            .await
            .expect("パブリッシャーの切断に成功すること");
    }

    for i in 0..3 {
        let received = subscriber
            .recv_publish(Duration::from_secs(5))
            .await
            .expect("PUBLISH パケットの受信に成功すること");
        assert_eq!(received.topic, "test/v5/multi");
        assert_eq!(received.payload, format!("message {i}").as_bytes());
    }

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// ワイルドカードトピックフィルターで購読し、マッチするトピックのメッセージを受信できることを検証する。
#[tokio::test]
async fn v5_wildcard_subscription() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-wildcard")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/+/wildcard", QoS::AtMostOnce)
        .await
        .expect("ワイルドカードサブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-wildcard")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish("test/v5/foo/wildcard", b"wildcard v5 message")
        .await
        .expect("パブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("ワイルドカード経由の PUBLISH パケットの受信に成功すること");
    assert_eq!(received.topic, "test/v5/foo/wildcard");
    assert_eq!(received.payload, b"wildcard v5 message");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 購読時の QoS がパブリッシュ QoS より低い場合、ブローカーが配信 QoS を下げることを検証する。
#[tokio::test]
async fn v5_qos_downgrade_on_subscription() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-qos-downgrade")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/qos/downgrade", QoS::AtLeastOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-qos-downgrade")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish_with_qos("test/v5/qos/downgrade", b"downgraded v5", QoS::ExactlyOnce)
        .await
        .expect("QoS 2 パブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("PUBLISH パケットの受信に成功すること");
    assert_eq!(received.topic, "test/v5/qos/downgrade");
    assert_eq!(received.qos, QoS::AtLeastOnce);
    assert_eq!(received.payload, b"downgraded v5");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 空のペイロードで retain フラグを付けてパブリッシュすると、retain メッセージが削除されることを検証する。
#[tokio::test]
async fn v5_retained_message_clear() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-retain-clear")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish_with_retain("test/v5/retain/clear", b"retained v5 before clear")
        .await
        .expect("retain 付きパブリッシュに成功すること");
    publisher
        .publish_with_retain("test/v5/retain/clear", b"")
        .await
        .expect("retain クリア用パブリッシュに成功すること");
    sleep(Duration::from_millis(100)).await;
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-retain-clear")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/retain/clear", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let result = subscriber.recv_publish(Duration::from_secs(1)).await;
    assert!(
        result.is_err(),
        "retain がクリアされたため、後から購読してもメッセージは届かないこと"
    );

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 大きなペイロードの publish / subscribe 往復を検証する。
#[tokio::test]
async fn v5_large_payload() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let payload = vec![0xabu8; 64 * 1024];

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-large")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/large", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-large")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish("test/v5/large", &payload)
        .await
        .expect("大きなペイロードのパブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(10))
        .await
        .expect("大きな PUBLISH パケットの受信に成功すること");
    assert_eq!(received.topic, "test/v5/large");
    assert_eq!(received.payload, payload);

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 空のペイロードを publish / subscribe できることを検証する。
#[tokio::test]
async fn v5_empty_payload() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-empty")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/empty", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-empty")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish("test/v5/empty", b"")
        .await
        .expect("空ペイロードのパブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("空ペイロードの PUBLISH パケットの受信に成功すること");
    assert_eq!(received.topic, "test/v5/empty");
    assert!(received.payload.is_empty());

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 同じトピックを購読した複数のサブスクライバーが、それぞれメッセージを受信できることを検証する。
#[tokio::test]
async fn v5_multiple_subscribers() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber_a = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバー A の TCP 接続に成功すること");
    subscriber_a
        .connect_v5("e2e-v5-subscriber-a")
        .await
        .expect("サブスクライバー A の MQTT 接続に成功すること");
    subscriber_a
        .subscribe("test/v5/multi/sub", QoS::AtMostOnce)
        .await
        .expect("サブスクライバー A のサブスクライブに成功すること");

    let mut subscriber_b = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバー B の TCP 接続に成功すること");
    subscriber_b
        .connect_v5("e2e-v5-subscriber-b")
        .await
        .expect("サブスクライバー B の MQTT 接続に成功すること");
    subscriber_b
        .subscribe("test/v5/multi/sub", QoS::AtMostOnce)
        .await
        .expect("サブスクライバー B のサブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-multi-sub")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish("test/v5/multi/sub", b"broadcast v5")
        .await
        .expect("パブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let received_a = subscriber_a
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("サブスクライバー A の PUBLISH パケット受信に成功すること");
    assert_eq!(received_a.topic, "test/v5/multi/sub");
    assert_eq!(received_a.payload, b"broadcast v5");

    let received_b = subscriber_b
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("サブスクライバー B の PUBLISH パケット受信に成功すること");
    assert_eq!(received_b.topic, "test/v5/multi/sub");
    assert_eq!(received_b.payload, b"broadcast v5");

    subscriber_a
        .disconnect()
        .await
        .expect("サブスクライバー A の切断に成功すること");
    subscriber_b
        .disconnect()
        .await
        .expect("サブスクライバー B の切断に成功すること");
}

/// Will メッセージの QoS が 1 の場合にも、強制切断時に配信されることを検証する。
#[tokio::test]
async fn v5_will_message_qos1() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-will-qos1")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/will/qos1", QoS::AtLeastOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5_with_will(
            "e2e-v5-publisher-will-qos1",
            "test/v5/will/qos1",
            b"will v5 qos1 payload",
            QoS::AtLeastOnce,
        )
        .await
        .expect("Will 付き MQTT 接続に成功すること");

    // TCP 接続を強制切断する。
    publisher.force_disconnect();

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("Will メッセージの受信に成功すること");
    assert_eq!(received.topic, "test/v5/will/qos1");
    assert_eq!(received.qos, QoS::AtLeastOnce);
    assert_eq!(received.payload, b"will v5 qos1 payload");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// clean_start=true で再接続すると、以前のセッション情報が破棄されることを検証する。
#[tokio::test]
async fn v5_clean_start_clears_state() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;
    let client_id = "e2e-v5-clean-start";

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5_with_session_expiry(client_id, 3600)
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/clean/clear", QoS::AtLeastOnce)
        .await
        .expect("サブスクライブに成功すること");
    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");

    // クリーンスタートで再接続し、ブローカー上のセッション状態を破棄する。
    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの再接続に成功すること");
    subscriber
        .connect_v5_with_clean_start(client_id, true)
        .await
        .expect("クリーンスタートでの再接続に成功すること");
    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-clean-clear")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish_with_qos(
            "test/v5/clean/clear",
            b"after clean start",
            QoS::AtLeastOnce,
        )
        .await
        .expect("パブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    // セッション情報が破棄されているため、オフライン中のメッセージは受信できない。
    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの再接続に成功すること");
    subscriber
        .connect_v5_with_clean_start(client_id, false)
        .await
        .expect("サブスクライバーの MQTT 再接続に成功すること");

    let result = subscriber.recv_publish(Duration::from_secs(1)).await;
    assert!(
        result.is_err(),
        "クリーンスタート後は購読情報が破棄されているため、メッセージは届かないこと"
    );

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}
