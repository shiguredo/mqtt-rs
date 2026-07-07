//! tokio-mqtt 統合テストの共通ヘルパー。
//!
//! 統合テストのサブモジュールとして各 `tests/*.rs` から
//! `mod helpers;` で読み込まれる。統合テストファイルごとに
//! 使わない関数があると dead_code 警告が出るため、モジュール
//! 冒頭で一括抑制している。

#![allow(dead_code)]

use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DistinguishedName, DnType, IsCa, KeyPair,
};
use testcontainers_modules::mosquitto;
use testcontainers_modules::testcontainers::core::{CopyDataSource, IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep};

use tokio_mqtt::client::MqttClient;

/// Mosquitto コンテナの生存期間をテスト中に保つためのガード。
///
/// `container` フィールドは Drop 時のコンテナ停止のために保持する必要がある。
pub struct MosquittoGuard {
    pub container: ContainerAsync<mosquitto::Mosquitto>,
    pub host: String,
    pub port: u16,
}

/// Mosquitto コンテナを起動し、生存期間を保ちながら (host, port) を返す。
pub async fn start_mosquitto() -> MosquittoGuard {
    let container = mosquitto::Mosquitto::default()
        .start()
        .await
        .expect("Mosquitto コンテナの起動に成功すること");
    let host = container
        .get_host()
        .await
        .expect("コンテナのホスト名の取得に成功すること")
        .to_string();
    let port = container
        .get_host_port_ipv4(1883)
        .await
        .expect("コンテナの 1883 ポート番号の取得に成功すること");
    // TCP レベルで待ち受けが始まるまでポーリングで待つ。
    wait_for_broker(&host, port, "Mosquitto").await;
    MosquittoGuard {
        container,
        host,
        port,
    }
}

/// TLS (mqtts) を有効化した Mosquitto コンテナの生存期間を保つためのガード。
///
/// `ca_pem` はクライアント側の rustls RootCertStore に渡す CA 証明書 (PEM)。
/// `server_name` はサーバー証明書の SAN/CN であり、TLS ハンドシェイクの SNI に使う。
pub struct MosquittoTlsGuard {
    pub container: ContainerAsync<GenericImage>,
    pub host: String,
    /// ホスト側にマップされた 8883/tcp ポート。
    pub port: u16,
    pub ca_pem: String,
    pub server_name: String,
}

/// TLS listener (8883) を有効化した Mosquitto コンテナを起動する。
///
/// `testcontainers_modules::mosquitto` は平文専用の `/mosquitto-no-auth.conf` を
/// 使うため、TLS 用には同じイメージ (`eclipse-mosquitto:2.0.18`) を
/// `GenericImage` で起動し、rcgen で生成した証明書と自前の `mosquitto.conf` を
/// 注入する。クライアントは dangerous verifier ではなく、この CA を trust root
/// として正規に検証する。
pub async fn start_mosquitto_tls() -> MosquittoTlsGuard {
    let generated = generate_localhost_certs();

    // eclipse-mosquitto イメージの慣習に合わせ、設定と証明書を
    // /mosquitto/config/ 配下へ配置する。
    let conf_path = "/mosquitto/config/mosquitto.conf";
    let ca_path = "/mosquitto/config/ca.pem";
    let cert_path = "/mosquitto/config/server.pem";
    let key_path = "/mosquitto/config/server.key";

    // listener 8883 のみを公開する。平文 1883 は立てない。
    // Mosquitto 2.x は既定で匿名接続を拒否するため allow_anonymous true が必要。
    let mosquitto_conf = format!(
        "listener 8883\n\
         protocol mqtt\n\
         cafile {ca_path}\n\
         certfile {cert_path}\n\
         keyfile {key_path}\n\
         require_certificate false\n\
         allow_anonymous true\n\
         persistence false\n"
    );

    // testcontainers_modules::mosquitto と同じタグに固定し、CI の再現性を保つ。
    let tag = "2.0.18";
    let container = GenericImage::new("eclipse-mosquitto", tag)
        .with_exposed_port(8883.tcp())
        .with_wait_for(WaitFor::message_on_stderr(format!(
            "mosquitto version {tag} running"
        )))
        .with_cmd(["mosquitto", "-c", conf_path])
        .with_copy_to(conf_path, CopyDataSource::Data(mosquitto_conf.into_bytes()))
        .with_copy_to(
            ca_path,
            CopyDataSource::Data(generated.ca_pem.clone().into_bytes()),
        )
        .with_copy_to(
            cert_path,
            CopyDataSource::Data(generated.server_cert_pem.into_bytes()),
        )
        .with_copy_to(
            key_path,
            CopyDataSource::Data(generated.server_key_pem.into_bytes()),
        )
        .start()
        .await
        .expect("TLS 有効の Mosquitto コンテナの起動に成功すること");

    let host = container
        .get_host()
        .await
        .expect("コンテナのホスト名の取得に成功すること")
        .to_string();
    let port = container
        .get_host_port_ipv4(8883)
        .await
        .expect("コンテナの 8883 ポート番号の取得に成功すること");
    // TLS ハンドシェイク前でも TCP accept は可能なので、平文と同様にポーリングする。
    wait_for_broker(&host, port, "Mosquitto TLS").await;
    MosquittoTlsGuard {
        container,
        host,
        port,
        ca_pem: generated.ca_pem,
        server_name: generated.server_name,
    }
}

/// Mosquitto に TCP 接続し、MQTT CONNECT まで完了したクライアントを返す。
pub async fn connect_client(guard: &MosquittoGuard, client_id: &str) -> MqttClient {
    let mut client = open_tcp(guard).await;
    client
        .connect(client_id, 60, true)
        .await
        .expect("Mosquitto への MQTT v5.0 接続に成功すること");
    client
}

/// Mosquitto へ TCP 接続したクライアントを返す（MQTT CONNECT 前）。
pub async fn open_tcp(guard: &MosquittoGuard) -> MqttClient {
    MqttClient::connect_tcp(&guard.host, guard.port)
        .await
        .expect("Mosquitto への TCP 接続に成功すること")
}

/// Mosquitto mqtts に接続し、MQTT CONNECT まで完了したクライアントを返す。
pub async fn connect_client_tls(guard: &MosquittoTlsGuard, client_id: &str) -> MqttClient {
    let mut client = open_tls(guard).await;
    client
        .connect(client_id, 60, true)
        .await
        .expect("Mosquitto mqtts への MQTT v5.0 接続に成功すること");
    client
}

/// Mosquitto mqtts へ TLS 接続したクライアントを返す（MQTT CONNECT 前）。
pub async fn open_tls(guard: &MosquittoTlsGuard) -> MqttClient {
    MqttClient::connect_tls(&guard.host, guard.port, &guard.ca_pem, &guard.server_name)
        .await
        .expect("Mosquitto への mqtts (TCP + TLS) 接続に成功すること")
}

/// `start_mosquitto_tls` 内部で使う証明書一式。
struct GeneratedCerts {
    /// クライアントが trust root として渡す CA 証明書 (PEM)。
    ca_pem: String,
    /// Mosquitto へ配置するサーバー証明書 (PEM)。
    server_cert_pem: String,
    /// Mosquitto へ配置するサーバー鍵 (PEM)。
    server_key_pem: String,
    /// サーバー証明書の Subject Alternative Name / CN。
    server_name: String,
}

/// `rcgen` で「自己署名 CA + それが署名した localhost 向けサーバー証明書」を
/// 生成する。テストの都度新しく作るのでネットワークや状態への副作用は無い。
fn generate_localhost_certs() -> GeneratedCerts {
    // CA (Certificate Authority) を生成する。
    let mut ca_params = CertificateParams::new(Vec::<String>::new())
        .expect("CA 用 CertificateParams の作成に成功すること");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let mut ca_dn = DistinguishedName::new();
    ca_dn.push(DnType::CommonName, "shiguredo mqtt-rs tokio-mqtt test CA");
    ca_params.distinguished_name = ca_dn;
    let ca_key = KeyPair::generate().expect("CA 鍵ペアの生成に成功すること");
    // CertifiedIssuer は「自己署名した CA 証明書」と「発行者情報」を
    // ひとまとめに保持し、後段の signed_by に渡す形になる。
    let ca = CertifiedIssuer::self_signed(ca_params, ca_key)
        .expect("CA 証明書 (CertifiedIssuer) の自己署名に成功すること");

    // サーバー証明書を生成する。SAN と CN の両方に "localhost" を含める。
    let server_name = "localhost".to_string();
    let mut server_params = CertificateParams::new(vec![server_name.clone()])
        .expect("server 用 CertificateParams の作成に成功すること");
    let mut server_dn = DistinguishedName::new();
    server_dn.push(DnType::CommonName, &server_name);
    server_params.distinguished_name = server_dn;
    let server_key = KeyPair::generate().expect("server 鍵ペアの生成に成功すること");
    let server_cert = server_params
        .signed_by(&server_key, &ca)
        .expect("server 証明書の CA 署名に成功すること");

    GeneratedCerts {
        ca_pem: ca.pem(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
        server_name,
    }
}

/// ブローカーが指定ポートで接続を受け付けるまでポーリングで待つ。
async fn wait_for_broker(host: &str, port: u16, service_name: &str) {
    let addr = format!("{host}:{port}");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if TcpStream::connect(&addr).await.is_ok() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("{service_name} が {port} ポートで待ち受けるまでのタイムアウト");
        }
        sleep(Duration::from_millis(100)).await;
    }
}
