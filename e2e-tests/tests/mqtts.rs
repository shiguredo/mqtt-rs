//! Mosquitto に対する MQTT over TLS (mqtts) の smoke test。
//!
//! `helpers::start_mosquitto_tls` が rcgen で生成した CA / server 証明書を
//! 注入した Mosquitto を 8883 で起動する。クライアントは tokio-rustls で
//! 接続し、CA を trust root として正規にサーバー証明書を検証する。
//!
//! ここでは CONNECT → CONNACK → DISCONNECT の疎通のみを確認する。
//! publish / subscribe の網羅的な検証は平文 Mosquitto テストが担う。

mod helpers;

use std::sync::Arc;
use std::time::Duration;

use helpers::start_mosquitto_tls;
use rustls::ClientConfig;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
use shiguredo_mqtt::codec::MqttVersion;
use shiguredo_mqtt::decoder::{Decoder, VersionedIncomingPacket};
use shiguredo_mqtt::v5::connack::ConnectReasonCode;
use shiguredo_mqtt::v5::connect::Connect as MqttConnect;
use shiguredo_mqtt::v5::disconnect::{Disconnect, DisconnectReasonCode};
use shiguredo_mqtt::v5::packet::{IncomingPacket, OutgoingPacket};
use shiguredo_mqtt::v5::property::Properties;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// MQTT v5.0 を mqtts (TCP + TLS) 上で Mosquitto に接続する smoke test。
///
/// クライアントは dangerous verifier を使わず、rcgen で発行した CA を
/// RootCertStore に登録してサーバー証明書を検証する。
#[tokio::test]
async fn mosquitto_v5_connect_smoke_mqtts() {
    let guard = start_mosquitto_tls().await;

    // CA (PEM) を trust anchor として登録する。
    let mut roots = RootCertStore::empty();
    let ca = CertificateDer::from_pem_slice(guard.ca_pem.as_bytes())
        .expect("CA 証明書 PEM のパースに成功すること");
    roots
        .add(ca)
        .expect("CA 証明書を RootCertStore に登録できること");

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));

    // TCP 接続後に TLS ハンドシェイクを行う。
    // SNI は証明書の SAN/CN ("localhost") と一致させる。
    let tcp = TcpStream::connect((guard.host.as_str(), guard.port))
        .await
        .expect("Mosquitto TLS ポートへの TCP 接続に成功すること");
    tcp.set_nodelay(true)
        .expect("TCP_NODELAY の設定に成功すること");

    let server_name = ServerName::try_from(guard.server_name.as_str())
        .expect("server_name が ServerName として妥当であること")
        .to_owned();
    let mut tls = connector
        .connect(server_name, tcp)
        .await
        .expect("TLS ハンドシェイクの完了に成功すること");

    // MQTT v5.0 の CONNECT を送信する。
    let connect_pkt = OutgoingPacket::Connect(MqttConnect {
        client_id: "e2e-mosquitto-v5-mqtts-smoke".to_string(),
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
    tls.write_all(&connect_bytes)
        .await
        .expect("TLS stream への CONNECT 送信に成功すること");
    tls.flush()
        .await
        .expect("TLS stream の flush に成功すること");

    // CONNACK を受信する。partial read を考慮し、Decoder が 1 パケット
    // 取り出せるまで read を繰り返す。
    let mut decoder = Decoder::new(MqttVersion::V5);
    let mut buf = [0u8; 1024];
    let connack = loop {
        if let Some(packet) = decoder
            .decode()
            .expect("受信バイト列のデコードに成功すること")
        {
            match packet {
                VersionedIncomingPacket::V5(IncomingPacket::ConnAck(connack)) => break connack,
                other => panic!("CONNACK 以外を受信: {other:?}"),
            }
        }
        let n = tokio::time::timeout(Duration::from_secs(5), tls.read(&mut buf))
            .await
            .expect("CONNACK 受信がタイムアウトしないこと")
            .expect("TLS stream からの読み取りに成功すること");
        if n == 0 {
            panic!("Mosquitto からの応答途中で EOF になった");
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
    tls.write_all(&disconnect_bytes)
        .await
        .expect("TLS stream への DISCONNECT 送信に成功すること");
    tls.flush()
        .await
        .expect("DISCONNECT 送信後の flush に成功すること");
}
