//! quic-mqtt 統合テストの共通ヘルパー。
//!
//! 統合テストのサブモジュールとして各 `tests/*.rs` から
//! `mod helpers;` で読み込まれる。統合テストファイルごとに
//! 使わない関数があると dead_code 警告が出るため、モジュール
//! 冒頭で一括抑制している。

#![allow(dead_code)]

use std::time::Duration;

use shiguredo_container::core::IntoContainerPort;
use shiguredo_container::core::wait::HttpWaitStrategy;
use shiguredo_container::{AsyncRunner, ContainerAsync, GenericImage, ImageExt, WaitFor};

use quic_mqtt::client::MqttClient;

/// EMQX Dashboard のフル起動判定 (`GET /status` が HTTP 200)。
fn emqx_dashboard_ready() -> WaitFor {
    WaitFor::http(
        HttpWaitStrategy::new("/status")
            .with_port(18083.tcp())
            .with_expected_status_code(200_u16),
    )
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

/// QUIC を有効化した EMQX コンテナの生存期間を保つためのガード。
///
/// `container` は Drop 時にコンテナを停止するために保持する。
/// `host` と `udp_port` は QUIC (UDP) リスナーに接続するためのアドレス情報。
/// `ca_pem` はクライアント側の TLS 設定に trust anchor として渡す、
/// テスト実行時に生成した自己署名 CA 証明書 (PEM)。s2n-quic の
/// rustls provider の `with_certificate(&str)` にそのまま渡せる形式。
pub struct EmqxQuicGuard {
    container: Option<ContainerAsync<GenericImage>>,
    pub host: String,
    pub udp_port: u16,
    pub ca_pem: String,
    /// サーバー証明書に付与した SNI/CN。クライアント側の SNI に用いる。
    pub server_name: String,
}

impl Drop for EmqxQuicGuard {
    fn drop(&mut self) {
        if let Some(container) = self.container.take() {
            let id = container.id().to_string();
            drop(container);
            force_remove_container(&id);
        }
    }
}

/// QUIC リスナーを有効化した EMQX コンテナを起動する。
///
/// EMQX 5 系の既定では QUIC リスナーは無効なため、環境変数で明示的に
/// 有効化する。証明書はイメージ同梱の例示証明書ではなく、`rcgen` で
/// 生成した自前の CA と server 証明書を `with_copy_to` で投入する。
/// こうすることで、クライアント側は無検証 (dangerous verifier) ではなく、
/// 正規にこの CA を trust root として検証できる。
///
/// QUIC の既定ポートは 14567/udp、ALPN は `"mqtt"`（EMQX の quicer
/// listener 実装で確定）。
///
/// `with_copy_to` は起動直後・ready 待機前に走る。EMQX の listener 設定は
/// 起動シーケンス後半のため、証明書は読み取り前に揃う。
pub async fn start_emqx_quic() -> EmqxQuicGuard {
    // localhost 向けの CA と server 証明書を rcgen で生成する。
    let generated = generate_localhost_certs();

    // コンテナ内で EMQX が読み取れる場所に証明書を配置する。EMQX の
    // WorkingDir は /opt/emqx なので、同梱の例示証明書と同じ
    // /opt/emqx/etc/certs/ 配下に置き、環境変数側は WorkingDir 相対の
    // "etc/certs/..." で指定する。ファイル名は同梱の cert.pem / key.pem と
    // 衝突しないように "quic-*.pem" とする。
    let cert_container_path = "/opt/emqx/etc/certs/quic-cert.pem";
    let key_container_path = "/opt/emqx/etc/certs/quic-key.pem";
    let cert_env_path = "etc/certs/quic-cert.pem";
    let key_env_path = "etc/certs/quic-key.pem";

    let container = GenericImage::new("emqx/emqx", "5.8.8")
        .with_exposed_port(14567.udp())
        .with_exposed_port(18083.tcp())
        .with_wait_for(emqx_dashboard_ready())
        .with_startup_timeout(Duration::from_secs(120))
        // 以下は QUIC listener を有効化するための環境変数。
        // EMQX の QUIC listener の証明書設定は `ssl_options` 配下にある
        // （etc/examples/listeners.quic.conf.example を参照）。
        // したがって env override は SSL_OPTIONS を挟む必要がある。
        // 誤って直下の CERTFILE/KEYFILE を指定すると key が無視され、
        // EMQX は同梱の例示証明書 (etc/certs/cert.pem) にフォールバックする。
        .with_env_var("EMQX_LISTENERS__QUIC__DEFAULT__ENABLED", "true")
        .with_env_var(
            "EMQX_LISTENERS__QUIC__DEFAULT__SSL_OPTIONS__CERTFILE",
            cert_env_path,
        )
        .with_env_var(
            "EMQX_LISTENERS__QUIC__DEFAULT__SSL_OPTIONS__KEYFILE",
            key_env_path,
        )
        .with_copy_to(cert_container_path, generated.server_cert_pem.into_bytes())
        .with_copy_to(key_container_path, generated.server_key_pem.into_bytes())
        .start()
        .await
        .expect("QUIC 有効の EMQX コンテナの起動に成功すること");

    let host = container
        .get_host()
        .await
        .expect("コンテナのホスト名の取得に成功すること")
        .to_string();
    // UDP ポートは Tcp とは別の ContainerPort として扱う必要があるため、
    // `udp()` で明示的に UDP 種別として問い合わせる。
    let udp_port = container
        .get_host_port_ipv4(14567.udp())
        .await
        .expect("コンテナの 14567/udp ポート番号の取得に成功すること");

    EmqxQuicGuard {
        container: Some(container),
        host,
        udp_port,
        ca_pem: generated.ca_pem,
        server_name: generated.server_name,
    }
}

/// EMQX QUIC に接続し、MQTT CONNECT まで完了したクライアントを返す。
pub async fn connect_client(guard: &EmqxQuicGuard, client_id: &str) -> MqttClient {
    let mut client = open_quic(guard).await;
    client
        .connect(client_id, 60, true)
        .await
        .expect("EMQX への MQTT v5.0 接続に成功すること");
    client
}

/// EMQX QUIC の bidirectional stream まで開いたクライアントを返す（MQTT CONNECT 前）。
pub async fn open_quic(guard: &EmqxQuicGuard) -> MqttClient {
    MqttClient::connect_quic(
        &guard.host,
        guard.udp_port,
        &guard.ca_pem,
        &guard.server_name,
    )
    .await
    .expect("EMQX への MQTT over QUIC 接続に成功すること")
}

/// `start_emqx_quic` 内部で使う証明書一式。
struct GeneratedCerts {
    /// クライアントが trust root として渡す CA 証明書 (PEM)。
    ca_pem: String,
    /// EMQX へ配置するサーバー証明書 (PEM)。
    server_cert_pem: String,
    /// EMQX へ配置するサーバー鍵 (PEM)。
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
    ca_dn.push(DnType::CommonName, "shiguredo mqtt-rs quic-mqtt test CA");
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
