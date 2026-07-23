//! tokio-mqtt 統合テストの共通ヘルパー。
//!
//! 統合テストのサブモジュールとして各 `tests/*.rs` から
//! `mod helpers;` で読み込まれる。統合テストファイルごとに
//! 使わない関数があると dead_code 警告が出るため、モジュール
//! 冒頭で一括抑制している。

#![allow(dead_code)]

use shiguredo_container::core::IntoContainerPort;
use shiguredo_container::{AsyncRunner, ContainerAsync, GenericImage, ImageExt, WaitFor};

use tokio_mqtt::client::MqttClient;

/// Mosquitto が listener 受付可能になったことを示すログ断片。
///
/// `mosquitto version X.Y.Z starting` には含まれず、
/// `mosquitto version X.Y.Z running` にだけ現れる。
const MOSQUITTO_RUNNING_LOG: &str = "running";

/// `with_copy_to` はコンテナ起動後に走るため、投入完了を待つシェル。
///
/// イメージ同梱の `/mosquitto/config/mosquitto.conf` が既にあるため、
/// conf の存在待ちだとコピー前に起動してしまう。同梱されない `server.key`
/// の出現を完了合図にする。eclipse-mosquitto イメージ (Alpine) の `/bin/sh` を使う。
const MOSQUITTO_WAIT_CONFIG_AND_EXEC: &str = "while [ ! -f /mosquitto/config/server.key ]; do sleep 0.05; done; exec mosquitto -c /mosquitto/config/mosquitto.conf";

/// Mosquitto の listener 受付可能判定 (ログに `running` が出るまで)。
fn mosquitto_ready() -> WaitFor {
    WaitFor::message_on_either_std(MOSQUITTO_RUNNING_LOG)
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
/// ready は `WaitFor` でログの `running` を待つ。
pub async fn start_mosquitto() -> MosquittoGuard {
    let tag = "2.0.18";
    let container = GenericImage::new("eclipse-mosquitto", tag)
        .with_exposed_port(1883.tcp())
        .with_wait_for(mosquitto_ready())
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
pub struct MosquittoTlsGuard {
    container: Option<ContainerAsync<GenericImage>>,
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
/// 証明書と自前の `mosquitto.conf` を `with_copy_to` で投入する。
/// クライアントは dangerous verifier ではなく、この CA を trust root として
/// 正規に検証する。
///
/// `with_copy_to` は起動後に走るため、CMD 側で設定ファイル出現を待ってから
/// `mosquitto` を `exec` する。
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

    // 平文 Mosquitto と同じタグに固定し、CI の再現性を保つ。
    let tag = "2.0.18";
    let container = GenericImage::new("eclipse-mosquitto", tag)
        .with_exposed_port(8883.tcp())
        .with_wait_for(mosquitto_ready())
        .with_cmd(["/bin/sh", "-c", MOSQUITTO_WAIT_CONFIG_AND_EXEC])
        .with_copy_to(conf_path, mosquitto_conf.into_bytes())
        .with_copy_to(ca_path, generated.ca_pem.clone().into_bytes())
        .with_copy_to(cert_path, generated.server_cert_pem.into_bytes())
        // 既定 0644 のままにする。0600 + root 所有だと権限降下後の
        // mosquitto ユーザーが鍵を読めず TLS が立ち上がらない。
        .with_copy_to(key_path, generated.server_key_pem.into_bytes())
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
    MosquittoTlsGuard {
        container: Some(container),
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
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, DistinguishedName, DnType, IsCa,
        KeyPair,
    };

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
