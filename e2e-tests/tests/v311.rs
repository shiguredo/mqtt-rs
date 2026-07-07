//! MQTT v3.1.1 の E2E テスト。

mod helpers;

use std::time::Duration;

use e2e_tests::v311::client::MqttClient;
use helpers::start_mosquitto;
use shiguredo_mqtt::codec::qos::QoS;
use tokio::time::Instant;
use tokio::time::sleep;

/// QoS 0 の publish / subscribe 往復を検証する。
#[tokio::test]
async fn v311_publish_subscribe_roundtrip_qos0() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v311("e2e-subscriber-qos0")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/topic", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v311("e2e-publisher-qos0")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish("test/topic", b"hello qos0")
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
    assert_eq!(received.topic, "test/topic");
    assert_eq!(received.payload, b"hello qos0");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// QoS 1 の publish / subscribe 往復を検証する。
#[tokio::test]
async fn v311_publish_subscribe_roundtrip_qos1() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v311("e2e-subscriber-qos1")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/topic/qos1", QoS::AtLeastOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v311("e2e-publisher-qos1")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish_with_qos("test/topic/qos1", b"hello qos1", QoS::AtLeastOnce)
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
    assert_eq!(received.topic, "test/topic/qos1");
    assert_eq!(received.payload, b"hello qos1");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// QoS 2 の publish / subscribe 往復を検証する。
#[tokio::test]
async fn v311_publish_subscribe_roundtrip_qos2() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v311("e2e-subscriber-qos2")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/topic/qos2", QoS::ExactlyOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v311("e2e-publisher-qos2")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish_with_qos("test/topic/qos2", b"hello qos2", QoS::ExactlyOnce)
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
    assert_eq!(received.topic, "test/topic/qos2");
    assert_eq!(received.payload, b"hello qos2");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 複数トピックをまとめて購読し、各トピックへの publish が受信できることを検証する。
#[tokio::test]
async fn v311_subscribe_multiple_topics() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v311("e2e-subscriber-multi")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe_many(&[("test/a", QoS::AtMostOnce), ("test/b", QoS::AtMostOnce)])
        .await
        .expect("複数トピックのサブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v311("e2e-publisher-multi")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish("test/a", b"message a")
        .await
        .expect("test/a へのパブリッシュに成功すること");
    publisher
        .publish("test/b", b"message b")
        .await
        .expect("test/b へのパブリッシュに成功すること");

    let received_a = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("test/a の PUBLISH パケットの受信に成功すること");
    assert_eq!(received_a.topic, "test/a");
    assert_eq!(received_a.payload, b"message a");

    let received_b = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("test/b の PUBLISH パケットの受信に成功すること");
    assert_eq!(received_b.topic, "test/b");
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
async fn v311_unsubscribe() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v311("e2e-subscriber-unsub")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/unsub", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v311("e2e-publisher-unsub")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish("test/unsub", b"before unsubscribe")
        .await
        .expect("パブリッシュに成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("unsubscribe 前の PUBLISH パケットの受信に成功すること");
    assert_eq!(received.payload, b"before unsubscribe");

    subscriber
        .unsubscribe("test/unsub")
        .await
        .expect("アンサブスクライブに成功すること");
    publisher
        .publish("test/unsub", b"after unsubscribe")
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
async fn v311_retained_message() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v311("e2e-publisher-retain")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish_with_retain("test/retain", b"retained hello")
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
        .connect_v311("e2e-subscriber-retain")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/retain", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("retain メッセージの受信に成功すること");
    assert_eq!(received.topic, "test/retain");
    assert_eq!(received.payload, b"retained hello");
    assert!(received.retain);

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// clean_session=false で再接続した際に、未送信メッセージを受信できることを検証する。
#[tokio::test]
async fn v311_persistent_session() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v311_with_clean_session("e2e-subscriber-persistent", false)
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/persistent", QoS::AtLeastOnce)
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
        .connect_v311("e2e-publisher-persistent")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish_with_qos("test/persistent", b"offline message", QoS::AtLeastOnce)
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
        .connect_v311_with_clean_session("e2e-subscriber-persistent", false)
        .await
        .expect("サブスクライバーの MQTT 再接続に成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("未送信メッセージの受信に成功すること");
    assert_eq!(received.topic, "test/persistent");
    assert_eq!(received.payload, b"offline message");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// Will メッセージが、接続が強制切断された際に配信されることを検証する。
#[tokio::test]
async fn v311_will_message() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v311("e2e-subscriber-will")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/will", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v311_with_will(
            "e2e-publisher-will",
            "test/will",
            b"will payload",
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
    assert_eq!(received.topic, "test/will");
    assert_eq!(received.payload, b"will payload");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 複数のパブリッシャーからのメッセージを 1 つのサブスクライバーが受信できることを検証する。
#[tokio::test]
async fn v311_multiple_publishers() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v311("e2e-subscriber-multi-pub")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/multi", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    for i in 0..3 {
        let mut publisher = MqttClient::connect_tcp(host, port)
            .await
            .expect("パブリッシャーの TCP 接続に成功すること");
        publisher
            .connect_v311(&format!("e2e-publisher-{i}"))
            .await
            .expect("パブリッシャーの MQTT 接続に成功すること");
        publisher
            .publish("test/multi", format!("message {i}").as_bytes())
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
        assert_eq!(received.topic, "test/multi");
        assert_eq!(received.payload, format!("message {i}").as_bytes());
    }

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// ワイルドカードトピックフィルターで購読し、マッチするトピックのメッセージを受信できることを検証する。
#[tokio::test]
async fn v311_wildcard_subscription() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v311("e2e-subscriber-wildcard")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/+/wildcard", QoS::AtMostOnce)
        .await
        .expect("ワイルドカードサブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v311("e2e-publisher-wildcard")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish("test/foo/wildcard", b"wildcard message")
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
    assert_eq!(received.topic, "test/foo/wildcard");
    assert_eq!(received.payload, b"wildcard message");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 購読時の QoS がパブリッシュ QoS より低い場合、ブローカーが配信 QoS を下げることを検証する。
#[tokio::test]
async fn v311_qos_downgrade_on_subscription() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v311("e2e-subscriber-qos-downgrade")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/qos/downgrade", QoS::AtLeastOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v311("e2e-publisher-qos-downgrade")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish_with_qos("test/qos/downgrade", b"downgraded", QoS::ExactlyOnce)
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
    assert_eq!(received.topic, "test/qos/downgrade");
    assert_eq!(received.qos, QoS::AtLeastOnce);
    assert_eq!(received.payload, b"downgraded");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 空のペイロードで retain フラグを付けてパブリッシュすると、retain メッセージが削除されることを検証する。
#[tokio::test]
async fn v311_retained_message_clear() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v311("e2e-publisher-retain-clear")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish_with_retain("test/retain/clear", b"retained before clear")
        .await
        .expect("retain 付きパブリッシュに成功すること");
    publisher
        .publish_with_retain("test/retain/clear", b"")
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
        .connect_v311("e2e-subscriber-retain-clear")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/retain/clear", QoS::AtMostOnce)
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
async fn v311_large_payload() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let payload = vec![0xabu8; 64 * 1024];

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v311("e2e-subscriber-large")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/large", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v311("e2e-publisher-large")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish("test/large", &payload)
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
    assert_eq!(received.topic, "test/large");
    assert_eq!(received.payload, payload);

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 空のペイロードを publish / subscribe できることを検証する。
#[tokio::test]
async fn v311_empty_payload() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v311("e2e-subscriber-empty")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/empty", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v311("e2e-publisher-empty")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish("test/empty", b"")
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
    assert_eq!(received.topic, "test/empty");
    assert!(received.payload.is_empty());

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 同じトピックを購読した複数のサブスクライバーが、それぞれメッセージを受信できることを検証する。
#[tokio::test]
async fn v311_multiple_subscribers() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber_a = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバー A の TCP 接続に成功すること");
    subscriber_a
        .connect_v311("e2e-subscriber-a")
        .await
        .expect("サブスクライバー A の MQTT 接続に成功すること");
    subscriber_a
        .subscribe("test/multi/sub", QoS::AtMostOnce)
        .await
        .expect("サブスクライバー A のサブスクライブに成功すること");

    let mut subscriber_b = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバー B の TCP 接続に成功すること");
    subscriber_b
        .connect_v311("e2e-subscriber-b")
        .await
        .expect("サブスクライバー B の MQTT 接続に成功すること");
    subscriber_b
        .subscribe("test/multi/sub", QoS::AtMostOnce)
        .await
        .expect("サブスクライバー B のサブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v311("e2e-publisher-multi-sub")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish("test/multi/sub", b"broadcast")
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
    assert_eq!(received_a.topic, "test/multi/sub");
    assert_eq!(received_a.payload, b"broadcast");

    let received_b = subscriber_b
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("サブスクライバー B の PUBLISH パケット受信に成功すること");
    assert_eq!(received_b.topic, "test/multi/sub");
    assert_eq!(received_b.payload, b"broadcast");

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
async fn v311_will_message_qos1() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v311("e2e-subscriber-will-qos1")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/will/qos1", QoS::AtLeastOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v311_with_will(
            "e2e-publisher-will-qos1",
            "test/will/qos1",
            b"will qos1 payload",
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
    assert_eq!(received.topic, "test/will/qos1");
    assert_eq!(received.qos, QoS::AtLeastOnce);
    assert_eq!(received.payload, b"will qos1 payload");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// clean_session=true で再接続すると、以前のセッション情報が破棄されることを検証する。
#[tokio::test]
async fn v311_clean_session_clears_state() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;
    let client_id = "e2e-clean-session";

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v311_with_clean_session(client_id, false)
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/clean/clear", QoS::AtLeastOnce)
        .await
        .expect("サブスクライブに成功すること");
    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");

    // クリーンセッションで再接続し、ブローカー上のセッション状態を破棄する。
    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの再接続に成功すること");
    subscriber
        .connect_v311_with_clean_session(client_id, true)
        .await
        .expect("クリーンセッションでの再接続に成功すること");
    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v311("e2e-publisher-clean-clear")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish_with_qos("test/clean/clear", b"after clean", QoS::AtLeastOnce)
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
        .connect_v311_with_clean_session(client_id, false)
        .await
        .expect("サブスクライバーの MQTT 再接続に成功すること");

    let result = subscriber.recv_publish(Duration::from_secs(1)).await;
    assert!(
        result.is_err(),
        "クリーンセッション後は購読情報が破棄されているため、メッセージは届かないこと"
    );

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}
