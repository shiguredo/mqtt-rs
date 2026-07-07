//! MQTT v5.0 固有機能の E2E テスト。

mod helpers;

use std::time::Duration;

use e2e_tests::v5::client::MqttClient;
use helpers::start_mosquitto;
use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::v5::property::Properties;
use shiguredo_mqtt::v5::property::Property;
use shiguredo_mqtt::v5::subscribe::RetainHandling;
use tokio::time::Instant;

#[tokio::test]
async fn v5_topic_alias() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    let mut connect_props = Properties::new();
    connect_props.push(Property::TopicAliasMaximum(10));
    publisher
        .connect_v5_with_properties("e2e-v5-publisher-alias", true, connect_props)
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-alias")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/alias", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    // 1 回目はトピック名付きで Topic Alias を登録する。
    let mut first_props = Properties::new();
    first_props.push(Property::TopicAlias(1));
    publisher
        .publish_with_properties(
            "test/v5/alias",
            b"alias v5 message 1",
            QoS::AtMostOnce,
            false,
            first_props,
        )
        .await
        .expect("1 回目の Topic Alias 付きパブリッシュに成功すること");

    // 2 回目はトピック名を省略して同じ Topic Alias を使用する。
    let mut second_props = Properties::new();
    second_props.push(Property::TopicAlias(1));
    publisher
        .publish_with_properties(
            "",
            b"alias v5 message 2",
            QoS::AtMostOnce,
            false,
            second_props,
        )
        .await
        .expect("2 回目の Topic Alias 付きパブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let received1 = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("1 件目の PUBLISH パケットの受信に成功すること");
    assert_eq!(received1.topic, "test/v5/alias");
    assert_eq!(received1.payload, b"alias v5 message 1");

    let received2 = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("2 件目の PUBLISH パケットの受信に成功すること");
    // ブローカーは Topic Alias を解決してトピック名を復元して転送する。
    assert_eq!(received2.topic, "test/v5/alias");
    assert_eq!(received2.payload, b"alias v5 message 2");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// subscriber が CONNECT で宣言した Receive Maximum により、
/// ブローカーが未確認 QoS 1 配送を 1 件に制限することを検証する。
///
/// MQTT v5.0 §3.1.2.11.3 (Receive Maximum) / MQTT v5.0 §3.3.4 [MQTT-3.3.4-9]:
/// クライアントが CONNECT で Receive Maximum を指定した場合、
/// サーバーは PUBACK / PUBCOMP 待ちの QoS 1/2 PUBLISH を
/// その数を超えて送信してはならない。
///
/// 観測手順:
/// 1. subscriber の CONNECT に ReceiveMaximum(1) を設定する
/// 2. 2 件の QoS 1 メッセージを publish する
/// 3. 1 件目を PUBACK せずに受信し、2 件目が届かないことを確認する
/// 4. 1 件目に PUBACK を送った後、2 件目が届くことを確認する
#[tokio::test]
async fn v5_receive_maximum() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    let mut connect_props = Properties::new();
    connect_props.push(Property::ReceiveMaximum(1));
    subscriber
        .connect_v5_with_properties("e2e-v5-subscriber-recvmax", true, connect_props)
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/recvmax", QoS::AtLeastOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-recvmax")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish_with_qos("test/v5/recvmax", b"recvmax 1", QoS::AtLeastOnce)
        .await
        .expect("1 件目の QoS 1 パブリッシュに成功すること");
    publisher
        .publish_with_qos("test/v5/recvmax", b"recvmax 2", QoS::AtLeastOnce)
        .await
        .expect("2 件目の QoS 1 パブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    // 1 件目は届くが、PUBACK を送らない。
    let received1 = subscriber
        .recv_publish_without_ack(Duration::from_secs(5))
        .await
        .expect("1 件目の PUBLISH パケットの受信に成功すること");
    assert_eq!(received1.payload, b"recvmax 1");
    let packet_id1 = received1
        .packet_id
        .expect("QoS 1 の PUBLISH にはパケット識別子が必要");

    // Receive Maximum=1 のため、PUBACK 前は 2 件目が配送されない。
    let second_before_ack = subscriber
        .recv_publish_without_ack(Duration::from_millis(500))
        .await;
    assert!(
        second_before_ack.is_err(),
        "Receive Maximum=1 かつ未確認 1 件がある間は 2 件目の PUBLISH を受信しないこと"
    );

    // 1 件目を確認応答すると、ブローカーが 2 件目を配送できる。
    subscriber
        .send_puback(packet_id1)
        .await
        .expect("1 件目の PUBACK 送信に成功すること");

    let received2 = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("PUBACK 後に 2 件目の PUBLISH パケットの受信に成功すること");
    assert_eq!(received2.payload, b"recvmax 2");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// subscriber が CONNECT で宣言した Maximum Packet Size を超える PUBLISH が
/// ブローカーから配送されないことを検証する。
///
/// MQTT v5.0 §3.1.2.11.4 (Maximum Packet Size) [MQTT-3.1.2-24]:
/// クライアントが CONNECT で Maximum Packet Size を指定した場合、
/// サーバーはそのサイズを超えるパケットを送信してはならない。
///
/// 値 0 はプロトコルエラーでありローカルのプロパティ検証で拒否されるため、
/// E2E では正の上限（128）を宣言し、上限内メッセージの配送と
/// 上限超過メッセージの非配送を観測する。
#[tokio::test]
async fn v5_maximum_packet_size() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    // 上限 128 バイトを CONNECT で宣言する。
    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    let mut connect_props = Properties::new();
    connect_props.push(Property::MaximumPacketSize(128));
    subscriber
        .connect_v5_with_properties("e2e-v5-subscriber-maxsize", true, connect_props)
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/maxsize", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-maxsize")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");

    // 上限内の小さなメッセージは配送される。
    publisher
        .publish("test/v5/maxsize", b"small")
        .await
        .expect("上限内メッセージのパブリッシュに成功すること");
    let small = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("上限内メッセージの受信に成功すること");
    assert_eq!(small.payload, b"small");

    // トピック名 + 固定ヘッダー + 可変ヘッダーを含めて 128 を超える大きなペイロード。
    let large_payload = vec![b'X'; 200];
    publisher
        .publish("test/v5/maxsize", &large_payload)
        .await
        .expect("上限超過メッセージのパブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    // ブローカーは Maximum Packet Size 超過の PUBLISH を配送しない。
    // 配送した場合でも Decoder が max_packet_size で拒否する。
    let large = subscriber.recv_publish(Duration::from_secs(2)).await;
    assert!(
        large.is_err(),
        "Maximum Packet Size を超える PUBLISH は受信できないこと"
    );

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// User Property が publish / subscribe 往復で保持されることを検証する。
#[tokio::test]
async fn v5_user_property_roundtrip() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-userprop")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/userprop", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-userprop")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");

    let mut props = Properties::new();
    props.push(Property::UserProperty(
        "test-key".to_string(),
        "test-value".to_string(),
    ));
    publisher
        .publish_with_properties(
            "test/v5/userprop",
            b"user prop v5",
            QoS::AtMostOnce,
            false,
            props,
        )
        .await
        .expect("User Property 付きパブリッシュに成功すること");
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let received = subscriber
        .recv_publish(Duration::from_secs(5))
        .await
        .expect("User Property 付き PUBLISH パケットの受信に成功すること");
    assert_eq!(received.payload, b"user prop v5");
    let user_props: Vec<(&str, &str)> = received
        .properties
        .iter()
        .filter_map(|p| {
            if let Property::UserProperty(k, v) = p {
                Some((k.as_str(), v.as_str()))
            } else {
                None
            }
        })
        .collect();
    assert!(
        user_props.contains(&("test-key", "test-value")),
        "受信 PUBLISH に送信した User Property が含まれること"
    );

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// Will Delay Interval が設定された Will メッセージが、指定秒数遅延後に配信されることを検証する。
#[tokio::test]
async fn v5_will_delay_interval() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut subscriber = MqttClient::connect_tcp(host, port)
        .await
        .expect("サブスクライバーの TCP 接続に成功すること");
    subscriber
        .connect_v5("e2e-v5-subscriber-willdelay")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe("test/v5/willdelay", QoS::AtMostOnce)
        .await
        .expect("サブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    let mut will_props = Properties::new();
    will_props.push(Property::WillDelayInterval(2));
    publisher
        .connect_v5_with_will_and_properties(
            "e2e-v5-publisher-willdelay",
            "test/v5/willdelay",
            b"delayed will v5",
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
    assert_eq!(received.topic, "test/v5/willdelay");
    assert_eq!(received.payload, b"delayed will v5");

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}

/// Shared Subscription でメッセージが複数のサブスクライバーに分散されることを検証する。
#[tokio::test]
async fn v5_shared_subscription() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut shared_a = MqttClient::connect_tcp(host, port)
        .await
        .expect("共有サブスクライバー A の TCP 接続に成功すること");
    shared_a
        .connect_v5("e2e-v5-shared-a")
        .await
        .expect("共有サブスクライバー A の MQTT 接続に成功すること");
    shared_a
        .subscribe("$share/group/test/v5/shared", QoS::AtMostOnce)
        .await
        .expect("共有サブスクライバー A のサブスクライブに成功すること");

    let mut shared_b = MqttClient::connect_tcp(host, port)
        .await
        .expect("共有サブスクライバー B の TCP 接続に成功すること");
    shared_b
        .connect_v5("e2e-v5-shared-b")
        .await
        .expect("共有サブスクライバー B の MQTT 接続に成功すること");
    shared_b
        .subscribe("$share/group/test/v5/shared", QoS::AtMostOnce)
        .await
        .expect("共有サブスクライバー B のサブスクライブに成功すること");

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-shared")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    for i in 0..4 {
        publisher
            .publish("test/v5/shared", format!("shared msg {i}").as_bytes())
            .await
            .expect("パブリッシュに成功すること");
    }
    publisher
        .disconnect()
        .await
        .expect("パブリッシャーの切断に成功すること");

    let mut count_a = 0;
    let mut count_b = 0;
    for _ in 0..4 {
        tokio::select! {
            result = shared_a.recv_publish(Duration::from_secs(5)) => {
                result.expect("共有サブスクライバー A が PUBLISH を受信すること");
                count_a += 1;
            }
            result = shared_b.recv_publish(Duration::from_secs(5)) => {
                result.expect("共有サブスクライバー B が PUBLISH を受信すること");
                count_b += 1;
            }
        }
    }
    assert_eq!(
        count_a + count_b,
        4,
        "4 件のメッセージが共有サブスクライバーで受信されること"
    );

    shared_a
        .disconnect()
        .await
        .expect("共有サブスクライバー A の切断に成功すること");
    shared_b
        .disconnect()
        .await
        .expect("共有サブスクライバー B の切断に成功すること");
}

/// No Local オプションで自分が publish したメッセージを受信しないことを検証する。
#[tokio::test]
async fn v5_no_local() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut client = MqttClient::connect_tcp(host, port)
        .await
        .expect("クライアントの TCP 接続に成功すること");
    client
        .connect_v5("e2e-v5-no-local")
        .await
        .expect("クライアントの MQTT 接続に成功すること");
    client
        .subscribe_with_options(
            "test/v5/nolocal",
            QoS::AtMostOnce,
            true,
            false,
            RetainHandling::SendRetained,
        )
        .await
        .expect("No Local サブスクライブに成功すること");

    client
        .publish("test/v5/nolocal", b"self published")
        .await
        .expect("パブリッシュに成功すること");

    let result = client.recv_publish(Duration::from_secs(1)).await;
    assert!(
        result.is_err(),
        "No Local オプションでは自分が publish したメッセージを受信しないこと"
    );

    client
        .disconnect()
        .await
        .expect("クライアントの切断に成功すること");
}

/// Retain Handling で DoNotSendRetained を指定した場合に retain メッセージを受信しないことを検証する。
#[tokio::test]
async fn v5_retain_handling_do_not_send() {
    let guard = start_mosquitto().await;
    let host = &guard.host;
    let port = guard.port;

    let mut publisher = MqttClient::connect_tcp(host, port)
        .await
        .expect("パブリッシャーの TCP 接続に成功すること");
    publisher
        .connect_v5("e2e-v5-publisher-retain-handling")
        .await
        .expect("パブリッシャーの MQTT 接続に成功すること");
    publisher
        .publish_with_retain("test/v5/retain/handling", b"retained handling")
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
        .connect_v5("e2e-v5-subscriber-retain-handling")
        .await
        .expect("サブスクライバーの MQTT 接続に成功すること");
    subscriber
        .subscribe_with_options(
            "test/v5/retain/handling",
            QoS::AtMostOnce,
            false,
            false,
            RetainHandling::DoNotSendRetained,
        )
        .await
        .expect("DoNotSendRetained サブスクライブに成功すること");

    let result = subscriber.recv_publish(Duration::from_secs(1)).await;
    assert!(
        result.is_err(),
        "DoNotSendRetained では既存 retain メッセージを受信しないこと"
    );

    subscriber
        .disconnect()
        .await
        .expect("サブスクライバーの切断に成功すること");
}
