//! MQTT v5.0 の基本シナリオを MQTT over TCP (Mosquitto) 上で検証する。
//!
//! コンテナランタイムが必要なため、既定の workspace test からは除外する (CODEBASE.md)。

mod helpers;

use std::time::Duration;

use helpers::{connect_client, open_tcp, start_mosquitto};
use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::v5::property::{Properties, Property};
use tokio::time::Instant;

/// QoS 0 の publish / subscribe 往復を検証する。
#[tokio::test]
async fn v5_publish_subscribe_roundtrip_qos0() {
    let guard = start_mosquitto().await;

    let mut subscriber = connect_client(&guard, "tokio-mqtt-v5-sub-qos0").await;
    subscriber
        .subscribe("test/tokio/v5/topic", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = connect_client(&guard, "tokio-mqtt-v5-pub-qos0").await;
    publisher
        .publish(
            "test/tokio/v5/topic",
            b"hello tokio qos0",
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
    assert_eq!(received.topic, "test/tokio/v5/topic");
    assert_eq!(received.payload, b"hello tokio qos0");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// QoS 1 の publish / subscribe 往復を検証する。
#[tokio::test]
async fn v5_publish_subscribe_roundtrip_qos1() {
    let guard = start_mosquitto().await;

    let mut subscriber = connect_client(&guard, "tokio-mqtt-v5-sub-qos1").await;
    subscriber
        .subscribe("test/tokio/v5/topic/qos1", QoS::AtLeastOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = connect_client(&guard, "tokio-mqtt-v5-pub-qos1").await;
    publisher
        .publish(
            "test/tokio/v5/topic/qos1",
            b"hello tokio qos1",
            QoS::AtLeastOnce,
            false,
        )
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
    assert_eq!(received.topic, "test/tokio/v5/topic/qos1");
    assert_eq!(received.payload, b"hello tokio qos1");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// QoS 2 の publish / subscribe 往復を検証する。
#[tokio::test]
async fn v5_publish_subscribe_roundtrip_qos2() {
    let guard = start_mosquitto().await;

    let mut subscriber = connect_client(&guard, "tokio-mqtt-v5-sub-qos2").await;
    subscriber
        .subscribe("test/tokio/v5/topic/qos2", QoS::ExactlyOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = connect_client(&guard, "tokio-mqtt-v5-pub-qos2").await;
    publisher
        .publish(
            "test/tokio/v5/topic/qos2",
            b"hello tokio qos2",
            QoS::ExactlyOnce,
            false,
        )
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
    assert_eq!(received.topic, "test/tokio/v5/topic/qos2");
    assert_eq!(received.payload, b"hello tokio qos2");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 複数トピックを順に購読し、各トピックへの publish が受信できることを検証する。
#[tokio::test]
async fn v5_subscribe_multiple_topics() {
    let guard = start_mosquitto().await;

    let mut subscriber = connect_client(&guard, "tokio-mqtt-v5-sub-multi").await;
    subscriber
        .subscribe("test/tokio/v5/a", QoS::AtMostOnce)
        .await
        .expect("test/tokio/v5/a のサブスクライブに成功すること");
    subscriber
        .subscribe("test/tokio/v5/b", QoS::AtMostOnce)
        .await
        .expect("test/tokio/v5/b のサブスクライブに成功すること");

    let mut publisher = connect_client(&guard, "tokio-mqtt-v5-pub-multi").await;
    publisher
        .publish("test/tokio/v5/a", b"message a", QoS::AtMostOnce, false)
        .await
        .expect("test/tokio/v5/a へのパブリッシュに成功すること");
    publisher
        .publish("test/tokio/v5/b", b"message b", QoS::AtMostOnce, false)
        .await
        .expect("test/tokio/v5/b へのパブリッシュに成功すること");

    let received_a = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("test/tokio/v5/a の PUBLISH パケットの受信に成功すること");
    assert_eq!(received_a.topic, "test/tokio/v5/a");
    assert_eq!(received_a.payload, b"message a");

    let received_b = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("test/tokio/v5/b の PUBLISH パケットの受信に成功すること");
    assert_eq!(received_b.topic, "test/tokio/v5/b");
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

    let mut subscriber = connect_client(&guard, "tokio-mqtt-v5-sub-unsub").await;
    subscriber
        .subscribe("test/tokio/v5/unsub", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = connect_client(&guard, "tokio-mqtt-v5-pub-unsub").await;
    publisher
        .publish(
            "test/tokio/v5/unsub",
            b"before unsubscribe",
            QoS::AtMostOnce,
            false,
        )
        .await
        .expect("パブリッシュに成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("unsubscribe 前の PUBLISH パケットの受信に成功すること");
    assert_eq!(received.payload, b"before unsubscribe");

    subscriber
        .unsubscribe("test/tokio/v5/unsub")
        .await
        .expect("アンサブスクライブに成功すること");
    publisher
        .publish(
            "test/tokio/v5/unsub",
            b"after unsubscribe",
            QoS::AtMostOnce,
            false,
        )
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

    let mut publisher = connect_client(&guard, "tokio-mqtt-v5-pub-retain").await;
    publisher
        .publish(
            "test/tokio/v5/retain",
            b"retained tokio hello",
            QoS::AtMostOnce,
            true,
        )
        .await
        .expect("retain 付きパブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let mut subscriber = connect_client(&guard, "tokio-mqtt-v5-sub-retain").await;
    subscriber
        .subscribe("test/tokio/v5/retain", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("retain メッセージの受信に成功すること");
    assert_eq!(received.topic, "test/tokio/v5/retain");
    assert_eq!(received.payload, b"retained tokio hello");
    assert!(received.retain);

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 空のペイロードで retain フラグを付けてパブリッシュすると、retain メッセージが削除されることを検証する。
#[tokio::test]
async fn v5_retained_message_clear() {
    let guard = start_mosquitto().await;

    let mut publisher = connect_client(&guard, "tokio-mqtt-v5-pub-retain-clear").await;
    publisher
        .publish(
            "test/tokio/v5/retain/clear",
            b"retained tokio before clear",
            QoS::AtMostOnce,
            true,
        )
        .await
        .expect("retain 付きパブリッシュに成功すること");
    publisher
        .publish("test/tokio/v5/retain/clear", b"", QoS::AtMostOnce, true)
        .await
        .expect("空ペイロードの retain クリアに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let mut subscriber = connect_client(&guard, "tokio-mqtt-v5-sub-retain-clear").await;
    subscriber
        .subscribe("test/tokio/v5/retain/clear", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let result = subscriber.recv_publish(Duration::from_secs(1)).await;
    assert!(result.is_err(), "retain クリア後はメッセージが届かないこと");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// ワイルドカードトピックフィルターで購読し、マッチするトピックのメッセージを受信できることを検証する。
#[tokio::test]
async fn v5_wildcard_subscription() {
    let guard = start_mosquitto().await;

    let mut subscriber = connect_client(&guard, "tokio-mqtt-v5-sub-wildcard").await;
    subscriber
        .subscribe("test/tokio/v5/+/wildcard", QoS::AtMostOnce)
        .await
        .expect("ワイルドカードサブスクライブに成功すること");

    let mut publisher = connect_client(&guard, "tokio-mqtt-v5-pub-wildcard").await;
    publisher
        .publish(
            "test/tokio/v5/foo/wildcard",
            b"wildcard tokio message",
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
        .expect("ワイルドカード経由の PUBLISH パケットの受信に成功すること");
    assert_eq!(received.topic, "test/tokio/v5/foo/wildcard");
    assert_eq!(received.payload, b"wildcard tokio message");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 購読時の QoS がパブリッシュ QoS より低い場合、ブローカーが配信 QoS を下げることを検証する。
#[tokio::test]
async fn v5_qos_downgrade_on_subscription() {
    let guard = start_mosquitto().await;

    let mut subscriber = connect_client(&guard, "tokio-mqtt-v5-sub-qos-downgrade").await;
    subscriber
        .subscribe("test/tokio/v5/qos/downgrade", QoS::AtLeastOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = connect_client(&guard, "tokio-mqtt-v5-pub-qos-downgrade").await;
    publisher
        .publish(
            "test/tokio/v5/qos/downgrade",
            b"downgraded tokio",
            QoS::ExactlyOnce,
            false,
        )
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
    assert_eq!(received.topic, "test/tokio/v5/qos/downgrade");
    assert_eq!(received.qos, QoS::AtLeastOnce);
    assert_eq!(received.payload, b"downgraded tokio");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// 大きなペイロードの publish / subscribe 往復を検証する。
#[tokio::test]
async fn v5_large_payload() {
    let guard = start_mosquitto().await;

    let payload = vec![0xabu8; 64 * 1024];

    let mut subscriber = connect_client(&guard, "tokio-mqtt-v5-sub-large").await;
    subscriber
        .subscribe("test/tokio/v5/large", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = connect_client(&guard, "tokio-mqtt-v5-pub-large").await;
    publisher
        .publish("test/tokio/v5/large", &payload, QoS::AtMostOnce, false)
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
    assert_eq!(received.topic, "test/tokio/v5/large");
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

    let mut subscriber = connect_client(&guard, "tokio-mqtt-v5-sub-empty").await;
    subscriber
        .subscribe("test/tokio/v5/empty", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = connect_client(&guard, "tokio-mqtt-v5-pub-empty").await;
    publisher
        .publish("test/tokio/v5/empty", b"", QoS::AtMostOnce, false)
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
    assert_eq!(received.topic, "test/tokio/v5/empty");
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

    let mut subscriber_a = connect_client(&guard, "tokio-mqtt-v5-sub-a").await;
    subscriber_a
        .subscribe("test/tokio/v5/multi/sub", QoS::AtMostOnce)
        .await
        .expect("サブスクライバー A のサブスクライブに成功すること");

    let mut subscriber_b = connect_client(&guard, "tokio-mqtt-v5-sub-b").await;
    subscriber_b
        .subscribe("test/tokio/v5/multi/sub", QoS::AtMostOnce)
        .await
        .expect("サブスクライバー B のサブスクライブに成功すること");

    let mut publisher = connect_client(&guard, "tokio-mqtt-v5-pub-multi-sub").await;
    publisher
        .publish(
            "test/tokio/v5/multi/sub",
            b"broadcast tokio",
            QoS::AtMostOnce,
            false,
        )
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
    assert_eq!(received_a.topic, "test/tokio/v5/multi/sub");
    assert_eq!(received_a.payload, b"broadcast tokio");

    let received_b = subscriber_b
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("サブスクライバー B の PUBLISH パケット受信に成功すること");
    assert_eq!(received_b.topic, "test/tokio/v5/multi/sub");
    assert_eq!(received_b.payload, b"broadcast tokio");

    subscriber_a
        .disconnect()
        .await
        .expect("サブスクライバー A の切断に成功すること");
    subscriber_b
        .disconnect()
        .await
        .expect("サブスクライバー B の切断に成功すること");
}

/// セッション有効期限を設定して clean_start=false で再接続した際に、
/// 未送信メッセージを受信できることを検証する。
#[tokio::test]
async fn v5_persistent_session() {
    let guard = start_mosquitto().await;

    let mut subscriber = open_tcp(&guard).await;
    subscriber
        .connect_with_session_expiry("tokio-mqtt-v5-sub-persistent", 3600)
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/tokio/v5/persistent", QoS::AtLeastOnce)
        .await
        .expect("サブスクライブに成功すること");
    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");

    let mut publisher = connect_client(&guard, "tokio-mqtt-v5-pub-persistent").await;
    publisher
        .publish(
            "test/tokio/v5/persistent",
            b"offline tokio message",
            QoS::AtLeastOnce,
            false,
        )
        .await
        .expect("パブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let mut subscriber = open_tcp(&guard).await;
    subscriber
        .connect_with_clean_start("tokio-mqtt-v5-sub-persistent", false)
        .await
        .expect("サブスクライバーの MQTT 再接続に成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("未送信メッセージの受信に成功すること");
    assert_eq!(received.topic, "test/tokio/v5/persistent");
    assert_eq!(received.payload, b"offline tokio message");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// Will メッセージが、接続が強制切断された際に配信されることを検証する。
#[tokio::test]
async fn v5_will_message() {
    let guard = start_mosquitto().await;

    let mut subscriber = connect_client(&guard, "tokio-mqtt-v5-sub-will").await;
    subscriber
        .subscribe("test/tokio/v5/will", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = open_tcp(&guard).await;
    publisher
        .connect_with_will(
            "tokio-mqtt-v5-pub-will",
            "test/tokio/v5/will",
            b"will tokio payload",
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
    assert_eq!(received.topic, "test/tokio/v5/will");
    assert_eq!(received.payload, b"will tokio payload");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// Will メッセージの QoS が 1 の場合にも、強制切断時に配信されることを検証する。
#[tokio::test]
async fn v5_will_message_qos1() {
    let guard = start_mosquitto().await;

    let mut subscriber = connect_client(&guard, "tokio-mqtt-v5-sub-will-qos1").await;
    subscriber
        .subscribe("test/tokio/v5/will/qos1", QoS::AtLeastOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = open_tcp(&guard).await;
    publisher
        .connect_with_will(
            "tokio-mqtt-v5-pub-will-qos1",
            "test/tokio/v5/will/qos1",
            b"will tokio qos1 payload",
            QoS::AtLeastOnce,
        )
        .await
        .expect("Will 付き MQTT 接続に成功すること");

    publisher.force_disconnect();

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("Will メッセージの受信に成功すること");
    assert_eq!(received.topic, "test/tokio/v5/will/qos1");
    assert_eq!(received.payload, b"will tokio qos1 payload");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// Will Delay Interval が設定された Will メッセージが、指定秒数遅延後に配信されることを検証する。
#[tokio::test]
async fn v5_will_delay_interval() {
    let guard = start_mosquitto().await;

    let mut subscriber = connect_client(&guard, "tokio-mqtt-v5-sub-willdelay").await;
    subscriber
        .subscribe("test/tokio/v5/willdelay", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = open_tcp(&guard).await;
    let mut will_props = Properties::new();
    will_props.push(Property::WillDelayInterval(2));
    publisher
        .connect_with_will_and_properties(
            "tokio-mqtt-v5-pub-willdelay",
            "test/tokio/v5/willdelay",
            b"delayed will tokio",
            QoS::AtMostOnce,
            will_props,
        )
        .await
        .expect("Will Delay Interval 付き MQTT 接続に成功すること");

    let start = Instant::now();
    publisher.force_disconnect();

    let result = subscriber.recv_publish(Duration::from_millis(500)).await;
    assert!(
        result.is_err(),
        "Will Delay Interval 経過前は Will メッセージを受信しないこと"
    );

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("Will Delay Interval 経過後に Will メッセージの受信に成功すること");
    assert!(
        start.elapsed() >= Duration::from_secs(2),
        "Will Delay Interval 分遅延後に配信されること"
    );
    assert_eq!(received.topic, "test/tokio/v5/willdelay");
    assert_eq!(received.payload, b"delayed will tokio");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// clean_start=true で再接続すると、以前のセッション情報が破棄されることを検証する。
#[tokio::test]
async fn v5_clean_start_clears_state() {
    let guard = start_mosquitto().await;
    let client_id = "tokio-mqtt-v5-clean-start";

    let mut subscriber = open_tcp(&guard).await;
    subscriber
        .connect_with_session_expiry(client_id, 3600)
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/tokio/v5/clean/clear", QoS::AtLeastOnce)
        .await
        .expect("サブスクライブに成功すること");
    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");

    // クリーンスタートで再接続し、ブローカー上のセッション状態を破棄する。
    let mut subscriber = open_tcp(&guard).await;
    subscriber
        .connect_with_clean_start(client_id, true)
        .await
        .expect("クリーンスタートでの再接続に成功すること");
    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");

    let mut publisher = connect_client(&guard, "tokio-mqtt-v5-pub-clean-clear").await;
    publisher
        .publish(
            "test/tokio/v5/clean/clear",
            b"after clean start",
            QoS::AtLeastOnce,
            false,
        )
        .await
        .expect("パブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    // セッション情報が破棄されているため、オフライン中のメッセージは受信できない。
    let mut subscriber = open_tcp(&guard).await;
    subscriber
        .connect_with_clean_start(client_id, false)
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
