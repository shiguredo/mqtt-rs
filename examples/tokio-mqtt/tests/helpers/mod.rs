//! tokio-mqtt 統合テストの共通ヘルパー。
//!
//! 統合テストのサブモジュールとして各 `tests/*.rs` から
//! `mod helpers;` で読み込まれる。統合テストファイルごとに
//! 使わない関数があると dead_code 警告が出るため、モジュール
//! 冒頭で一括抑制している。

#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DistinguishedName, DnType, IsCa, KeyPair,
};
use rustls::ClientConfig;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
use shiguredo_container::core::{AccessMode, ContainerPort, IntoContainerPort, Mount};
use shiguredo_container::{AsyncRunner, ContainerAsync, ContainerRequest, GenericImage, ImageExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep};
use tokio_rustls::TlsConnector;

use tokio_mqtt::client::MqttClient;

/// コンテナ生存中にホスト側一時ディレクトリを保持し、Drop で削除する。
///
/// `shiguredo_container` の Linux 実装は `with_copy_to` 未対応のため、
/// 証明書や設定は bind mount で渡す。マウント元が消えるとコンテナから
/// 見えなくなるので、ガードに所有させてコンテナと同じ寿命にする。
struct TempDirGuard(PathBuf);

impl TempDirGuard {
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// 一意な一時ディレクトリを作成する。
fn create_temp_dir(prefix: &str) -> TempDirGuard {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("システム時刻が UNIX_EPOCH 以降であること")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("{prefix}-{}-{}", std::process::id(), nanos));
    fs::create_dir_all(&dir).expect("一時ディレクトリの作成に成功すること");
    TempDirGuard(dir)
}

/// 一時ディレクトリ配下にファイルを書き出す。
fn write_temp_file(dir: &Path, name: &str, contents: impl AsRef<[u8]>) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).expect("一時ファイルの書き出しに成功すること");
    path
}

/// コンテナランタイム向けに、指定 ID のコンテナを同期的に削除する。
///
/// `ContainerAsync` の Drop は tokio Runtime 内だと削除スレッドを join しないため、
/// 連続 E2E で孤立コンテナが溜まり得る。ガードの Drop から明示的に掃除する。
fn force_remove_container(id: &str) {
    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("container")
            .args(["stop", id])
            .output();
        let _ = std::process::Command::new("container")
            .args(["rm", id])
            .output();
    }
    #[cfg(target_os = "linux")]
    {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", id])
            .output();
    }
}

/// コンテナポートをホストへ公開した `ContainerRequest` を返す。
///
/// - macOS: `with_exposed_port` が空きホストポートを自動割当する
/// - Linux: `with_exposed_port` は PortBindings に載らないため、
///   `with_mapped_port(0, …)` で Docker にランダム割当させる
fn publish_ports(
    image: GenericImage,
    ports: impl IntoIterator<Item = ContainerPort>,
) -> ContainerRequest<GenericImage> {
    #[cfg(target_os = "linux")]
    {
        let mut req: ContainerRequest<GenericImage> = image.into();
        for port in ports {
            req = req.with_mapped_port(0, port);
        }
        req
    }
    #[cfg(target_os = "macos")]
    {
        let mut image = image;
        for port in ports {
            image = image.with_exposed_port(port);
        }
        image.into()
    }
}

/// Mosquitto コンテナの生存期間をテスト中に保つためのガード。
///
/// `container` フィールドは Drop 時のコンテナ停止のために保持する必要がある。
pub struct MosquittoGuard {
    container: Option<ContainerAsync<GenericImage>>,
    pub host: String,
    pub port: u16,
}

impl Drop for MosquittoGuard {
    fn drop(&mut self) {
        if let Some(container) = self.container.take() {
            let id = container.id().to_string();
            drop(container);
            force_remove_container(&id);
        }
    }
}

/// Mosquitto コンテナを起動し、生存期間を保ちながら (host, port) を返す。
///
/// 平文 listener (1883) のみ。匿名接続を許可する `/mosquitto-no-auth.conf` を使う。
/// イメージタグは CI 再現性のため固定する。
///
/// Linux ではログ待機が未対応のため、TCP ポーリングで待ち受け開始を確認する。
pub async fn start_mosquitto() -> MosquittoGuard {
    let tag = "2.0.18";
    let container = publish_ports(GenericImage::new("eclipse-mosquitto", tag), [1883.tcp()])
        .with_cmd(["mosquitto", "-c", "/mosquitto-no-auth.conf"])
        .start()
        .await
        .expect("Mosquitto コンテナの起動に成功すること");
    let host = container
        .get_host()
        .await
        .expect("コンテナのホスト名の取得に成功すること")
        .to_string();
    let port = container
        .get_host_port_ipv4(1883.tcp())
        .await
        .expect("コンテナの 1883 ポート番号の取得に成功すること");
    // Linux ではログ待機が未対応のため、TCP のあと MQTT CONNECT で ready を確認する。
    wait_for_mosquitto_mqtt(&host, port).await;
    MosquittoGuard {
        container: Some(container),
        host,
        port,
    }
}

/// TLS (mqtts) を有効化した Mosquitto コンテナの生存期間を保つためのガード。
///
/// `ca_pem` はクライアント側の rustls RootCertStore に渡す CA 証明書 (PEM)。
/// `server_name` はサーバー証明書の SAN/CN であり、TLS ハンドシェイクの SNI に使う。
/// `_temp_dir` は bind mount 元の設定・証明書をコンテナ生存中に保持する。
pub struct MosquittoTlsGuard {
    container: Option<ContainerAsync<GenericImage>>,
    /// bind mount 元。フィールド参照はしないが Drop まで保持する必要がある。
    _temp_dir: TempDirGuard,
    pub host: String,
    /// ホスト側にマップされた 8883/tcp ポート。
    pub port: u16,
    pub ca_pem: String,
    pub server_name: String,
}

impl Drop for MosquittoTlsGuard {
    fn drop(&mut self) {
        if let Some(container) = self.container.take() {
            let id = container.id().to_string();
            drop(container);
            force_remove_container(&id);
        }
    }
}

/// TLS listener (8883) を有効化した Mosquitto コンテナを起動する。
///
/// 平文用の `/mosquitto-no-auth.conf` では足りないため、同じイメージ
/// (`eclipse-mosquitto:2.0.18`) を `GenericImage` で起動し、rcgen で生成した
/// 証明書と自前の `mosquitto.conf` をホスト側一時ディレクトリへ書き出して
/// bind mount する。クライアントは dangerous verifier ではなく、この CA を
/// trust root として正規に検証する。
///
/// Linux では `with_copy_to` が未対応のため bind mount を使う。
pub async fn start_mosquitto_tls() -> MosquittoTlsGuard {
    let generated = generate_localhost_certs();

    // eclipse-mosquitto イメージの慣習に合わせ、設定と証明書を
    // /mosquitto/config/ 配下へ配置する。
    let conf_name = "mosquitto.conf";
    let ca_name = "ca.pem";
    let cert_name = "server.pem";
    let key_name = "server.key";
    let conf_path = format!("/mosquitto/config/{conf_name}");
    let ca_path = format!("/mosquitto/config/{ca_name}");
    let cert_path = format!("/mosquitto/config/{cert_name}");
    let key_path = format!("/mosquitto/config/{key_name}");

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

    let temp_dir = create_temp_dir("mqtt-rs-tokio-mosquitto-tls");
    write_temp_file(temp_dir.path(), conf_name, mosquitto_conf.as_bytes());
    write_temp_file(temp_dir.path(), ca_name, generated.ca_pem.as_bytes());
    write_temp_file(
        temp_dir.path(),
        cert_name,
        generated.server_cert_pem.as_bytes(),
    );
    write_temp_file(
        temp_dir.path(),
        key_name,
        generated.server_key_pem.as_bytes(),
    );

    // 平文 Mosquitto と同じタグに固定し、CI の再現性を保つ。
    let tag = "2.0.18";
    let container = publish_ports(GenericImage::new("eclipse-mosquitto", tag), [8883.tcp()])
        .with_cmd(["mosquitto", "-c", conf_path.as_str()])
        .with_mount(
            Mount::bind_mount(temp_dir.path().to_string_lossy(), "/mosquitto/config")
                .with_access_mode(AccessMode::ReadOnly),
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
        .get_host_port_ipv4(8883.tcp())
        .await
        .expect("コンテナの 8883 ポート番号の取得に成功すること");
    // TCP accept だけでは TLS 用証明書の読み込み前に接続して handshake EOF になる
    // ことがある。Linux ではログ待機が未対応のため、TLS ハンドシェイク自体で待つ。
    wait_for_broker(&host, port, "Mosquitto TLS").await;
    wait_for_tls_listener(&host, port, &generated.ca_pem, &generated.server_name).await;
    MosquittoTlsGuard {
        container: Some(container),
        _temp_dir: temp_dir,
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

/// Mosquitto が MQTT CONNECT を受理するまでポーリングで待つ。
///
/// TCP accept 開始直後はプロトコル処理前で CONNECT が切断されることがある。
/// Linux ではログ待機が未対応のため、実際の MQTT ハンドシェイクで ready を確認する。
async fn wait_for_mosquitto_mqtt(host: &str, port: u16) {
    wait_for_broker(host, port, "Mosquitto").await;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut attempt = 0u64;
    loop {
        attempt += 1;
        let client_id = format!("tokio-mqtt-ready-{attempt}");
        if let Ok(mut client) = MqttClient::connect_tcp(host, port).await
            && client.connect(&client_id, 60, true).await.is_ok()
        {
            let _ = client.disconnect().await;
            return;
        }
        if Instant::now() >= deadline {
            panic!("Mosquitto が MQTT CONNECT を受理するまでのタイムアウト");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// Mosquitto mqtts が TLS ハンドシェイクを完了できるまでポーリングで待つ。
///
/// TCP 待ち受け開始直後は証明書未準備などで handshake が EOF / reset になる
/// ことがある。成功した接続はすぐに閉じる (ready 判定専用)。
async fn wait_for_tls_listener(host: &str, port: u16, ca_pem: &str, server_name: &str) {
    let mut roots = RootCertStore::empty();
    let ca = CertificateDer::from_pem_slice(ca_pem.as_bytes())
        .expect("CA 証明書 PEM のパースに成功すること");
    roots
        .add(ca)
        .expect("CA 証明書を RootCertStore に登録できること");
    let connector = TlsConnector::from(Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ));
    let name = ServerName::try_from(server_name.to_string())
        .expect("server_name が ServerName として妥当であること");

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(tcp) = TcpStream::connect((host, port)).await {
            let _ = tcp.set_nodelay(true);
            if connector.connect(name.clone(), tcp).await.is_ok() {
                return;
            }
        }
        if Instant::now() >= deadline {
            panic!("Mosquitto TLS がハンドシェイク可能になるまでのタイムアウト");
        }
        sleep(Duration::from_millis(100)).await;
    }
}
