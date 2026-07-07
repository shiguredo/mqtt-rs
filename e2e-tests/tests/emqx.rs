//! EMQX ブローカーに対する接続 smoke test。
//!
//! testcontainers-modules に EMQX 用モジュールが存在しないため、
//! `helpers::start_emqx` / `helpers::start_emqx_quic` が `GenericImage` から
//! `emqx/emqx` イメージを直接起動する。ここでは MQTT v3.1.1 / v5.0 それぞれで
//! CONNECT → CONNACK → DISCONNECT の疎通のみを確認する。
//!
//! publish / subscribe 系のより網羅的な検証は、既存の Mosquitto ベースの
//! テストが担っており、EMQX に対しては現時点では接続確認までに留める。

mod helpers;

use std::time::Duration;

use e2e_tests::v5::client::MqttClient as V5Client;
use e2e_tests::v311::client::MqttClient as V311Client;
use s2n_quic::Client;
use s2n_quic::client::Connect;
use s2n_quic::provider::tls::rustls::Client as TlsClient;
use shiguredo_mqtt::codec::MqttVersion;
use shiguredo_mqtt::decoder::{Decoder, VersionedIncomingPacket};
use shiguredo_mqtt::v5::connack::ConnectReasonCode;
use shiguredo_mqtt::v5::connect::Connect as MqttConnect;
use shiguredo_mqtt::v5::disconnect::{Disconnect, DisconnectReasonCode};
use shiguredo_mqtt::v5::packet::{IncomingPacket, OutgoingPacket};
use shiguredo_mqtt::v5::property::Properties;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use helpers::{start_emqx, start_emqx_quic};

/// MQTT v3.1.1 で EMQX に接続し、DISCONNECT まで完了できることを確認する。
///
/// EMQX 5 系は既定で匿名接続を許可するため、認証情報無しで CONNECT できるはず。
#[tokio::test]
async fn emqx_v311_connect_smoke() {
    let guard = start_emqx().await;

    // TCP レイヤーで EMQX に接続する。
    let mut client = V311Client::connect_tcp(&guard.host, guard.port)
        .await
        .expect("EMQX への TCP 接続に成功すること");

    // MQTT v3.1.1 の CONNECT を送り、CONNACK が Accepted で返ることを確認する。
    client
        .connect_v311("e2e-emqx-v311-smoke")
        .await
        .expect("EMQX への MQTT v3.1.1 接続に成功すること");

    // 正常系の切断まで通ることを確認する。
    client
        .disconnect()
        .await
        .expect("EMQX に対して DISCONNECT を送信できること");
}

/// MQTT v5.0 で EMQX に接続し、DISCONNECT まで完了できることを確認する。
///
/// EMQX 5 系は既定で匿名接続を許可するため、認証情報無しで CONNECT できるはず。
#[tokio::test]
async fn emqx_v5_connect_smoke() {
    let guard = start_emqx().await;

    // TCP レイヤーで EMQX に接続する。
    let mut client = V5Client::connect_tcp(&guard.host, guard.port)
        .await
        .expect("EMQX への TCP 接続に成功すること");

    // MQTT v5.0 の CONNECT を送り、CONNACK が Success で返ることを確認する。
    client
        .connect_v5("e2e-emqx-v5-smoke")
        .await
        .expect("EMQX への MQTT v5.0 接続に成功すること");

    // Normal Disconnection 理由コードで DISCONNECT を送信して切断する。
    client
        .disconnect()
        .await
        .expect("EMQX に対して DISCONNECT を送信できること");
}

/// MQTT v5.0 を QUIC (MQTT over QUIC) 上で EMQX に接続する smoke test。
///
/// TCP 版と同様に CONNECT → CONNACK → DISCONNECT の疎通のみを確認する。
/// EMQX の QUIC listener は既定で無効なため、`start_emqx_quic` が
/// 環境変数で listener を有効化して起動する。証明書は `start_emqx_quic`
/// 内で rcgen により都度生成した自前 CA が署名した server 証明書を使い、
/// クライアント側はその CA を trust root として正規に検証する。
///
/// QUIC クライアントには s2n-quic (rustls provider) を使う。
/// s2n-quic は QUIC DATAGRAM 拡張 (RFC 9221) をデフォルトで有効化しない
/// ため、EMQX 5.8.8 の emqx_quic_connection モジュールが dgram_state_changed/3
/// コールバック未実装であることに起因する crash を踏まない。
#[tokio::test]
async fn emqx_v5_connect_smoke_quic() {
    let guard = start_emqx_quic().await;

    // rcgen で生成した CA (PEM) を trust anchor として登録し、
    // ALPN は EMQX の quicer listener 実装が受け付ける "mqtt" のみ
    // (v5.8 系。既定の "h3" とは非互換なので必ず上書きする)。
    let tls = TlsClient::builder()
        .with_certificate(guard.ca_pem.as_str())
        .expect("CA 証明書を trust anchor として登録できること")
        .with_application_protocols([b"mqtt".as_slice()].into_iter())
        .expect("ALPN protocols の設定に成功すること")
        .build()
        .expect("s2n-quic 用の rustls Client provider の構築に成功すること");

    // ワイルドカードで OS 割り当ての UDP ポートを bind し、
    // 送信用の s2n-quic Client を起動する。
    let client = Client::builder()
        .with_tls(tls)
        .expect("Client に TLS provider を設定できること")
        .with_io("0.0.0.0:0")
        .expect("Client の UDP bind に成功すること")
        .start()
        .expect("s2n-quic Client の起動に成功すること");

    // testcontainers が返す host はホスト名または IP。Client の IO は
    // 0.0.0.0:0 で IPv4 のみを bind するため、IPv6 のアドレスに接続しようと
    // すると失敗する。macOS では localhost の解決が ::1 を優先する場合が
    // あるため、IPv4 のアドレスだけを抽出する。
    let addr = tokio::net::lookup_host(format!("{}:{}", guard.host, guard.udp_port))
        .await
        .expect("EMQX の QUIC アドレスを名前解決できること")
        .find(|a| a.is_ipv4())
        .expect("IPv4 の解決結果が 1 件以上あること");

    // QUIC ハンドシェイクを開始する。SNI は rcgen で発行した server
    // 証明書の SAN/CN と一致させる必要があるため、guard.server_name を渡す。
    let connect = Connect::new(addr).with_server_name(guard.server_name.as_str());
    let mut connection = client
        .connect(connect)
        .await
        .expect("QUIC ハンドシェイクの完了に成功すること");
    // idle timeout でアイドル切断されないよう keep_alive を有効化する。
    connection
        .keep_alive(true)
        .expect("keep_alive を有効化できること");

    // EMQX の MQTT over QUIC 実装は、1 本の bidirectional stream 上で
    // MQTT の制御パケットを TCP と同様のフレーミングで送受信する。
    let stream = connection
        .open_bidirectional_stream()
        .await
        .expect("QUIC bidirectional stream を開設できること");
    let (mut recv, mut send) = stream.split();

    // MQTT v5.0 の CONNECT を送信する。
    let connect_pkt = OutgoingPacket::Connect(MqttConnect {
        client_id: "e2e-emqx-v5-quic-smoke".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties: Properties::new(),
        will: None,
        username: None,
        password: None,
    });
    let connect_bytes = connect_pkt
        .encode_to_vec()
        .expect("CONNECT のエンコードに成功すること");
    send.write_all(&connect_bytes)
        .await
        .expect("QUIC stream への CONNECT 送信に成功すること");
    // 送信バッファに残っている CONNECT を確実に flush する。
    send.flush()
        .await
        .expect("QUIC stream の flush に成功すること");

    // CONNACK を受信する。partial read を考慮し、Decoder が 1 パケット
    // 取り出せるまで recv を繰り返す。
    let mut decoder = Decoder::new(MqttVersion::V5);
    let mut buf = [0u8; 1024];
    let connack = loop {
        if let Some(packet) = decoder
            .decode()
            .expect("受信バイト列のデコードに成功すること")
        {
            match packet {
                VersionedIncomingPacket::V5(IncomingPacket::ConnAck(connack)) => break connack,
                other => panic!("CONNACK 以外を受信: {:?}", other),
            }
        }
        let n = tokio::time::timeout(Duration::from_secs(5), recv.read(&mut buf))
            .await
            .expect("CONNACK 受信がタイムアウトしないこと")
            .expect("QUIC stream からの読み取りに成功すること");
        if n == 0 {
            panic!("EMQX からの応答途中で EOF になった");
        }
        decoder
            .feed(&buf[..n])
            .expect("Decoder への feed に成功すること");
    };
    assert_eq!(
        connack.reason_code,
        ConnectReasonCode::Success,
        "CONNACK の理由コードは Success であること"
    );

    // Normal Disconnection 理由コードで DISCONNECT を送信して切断する。
    let disconnect = OutgoingPacket::Disconnect(Disconnect {
        reason_code: DisconnectReasonCode::NormalDisconnection,
        properties: Properties::new(),
    });
    let disconnect_bytes = disconnect
        .encode_to_vec()
        .expect("DISCONNECT のエンコードに成功すること");
    send.write_all(&disconnect_bytes)
        .await
        .expect("QUIC stream への DISCONNECT 送信に成功すること");
    // 送信側 stream を明示的に完了させる（残バイトを flush してから close）。
    // 主眼は CONNECT / CONNACK の疎通確認なのでエラーは無視してよい。
    let _ = send.close().await;
}
