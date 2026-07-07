//! quic-mqtt 統合テストの共通ヘルパー。
//!
//! 統合テストのサブモジュールとして各 `tests/*.rs` から
//! `mod helpers;` で読み込まれる。統合テストファイルごとに
//! 使わない関数があると dead_code 警告が出るため、モジュール
//! 冒頭で一括抑制している。

#![allow(dead_code)]

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DistinguishedName, DnType, IsCa, KeyPair,
};
use testcontainers_modules::testcontainers::core::{CopyDataSource, IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

use quic_mqtt::client::MqttClient;

/// QUIC を有効化した EMQX コンテナの生存期間を保つためのガード。
///
/// `container` は Drop 時にコンテナを停止するために保持する。
/// `host` と `udp_port` は QUIC (UDP) リスナーに接続するためのアドレス情報。
/// `ca_pem` はクライアント側の TLS 設定に trust anchor として渡す、
/// テスト実行時に生成した自己署名 CA 証明書 (PEM)。s2n-quic の
/// rustls provider の `with_certificate(&str)` にそのまま渡せる形式。
pub struct EmqxQuicGuard {
    pub container: ContainerAsync<GenericImage>,
    pub host: String,
    pub udp_port: u16,
    pub ca_pem: String,
    /// サーバー証明書に付与した SNI/CN。クライアント側の SNI に用いる。
    pub server_name: String,
}

/// QUIC リスナーを有効化した EMQX コンテナを起動する。
///
/// EMQX 5 系の既定では QUIC リスナーは無効なため、環境変数で明示的に
/// 有効化する。証明書はイメージ同梱の例示証明書ではなく、`rcgen` で
/// 生成した自前の CA と server 証明書を都度コンテナへ注入する。こうする
/// ことで、クライアント側は無検証 (dangerous verifier) ではなく、正規に
/// この CA を trust root として検証できる。
///
/// QUIC の既定ポートは 14567/udp、ALPN は `"mqtt"`（EMQX の quicer
/// listener 実装で確定）。
pub async fn start_emqx_quic() -> EmqxQuicGuard {
    // localhost 向けの CA と server 証明書を rcgen で生成する。
    // testcontainers 経由でコンテナに copy して EMQX に読み込ませる。
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
        // 標準の MQTT/TCP リスナー起動後に QUIC listener 起動ログが出るため、
        // "is running now!" を待つ。
        .with_wait_for(WaitFor::message_on_stdout("is running now!"))
        // QUIC は UDP なので UDP ポートとして expose する必要がある。
        .with_exposed_port(14567.udp())
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
        // rcgen で生成した cert/key をコンテナに copy する。
        .with_copy_to(
            cert_container_path,
            CopyDataSource::Data(generated.server_cert_pem.into_bytes()),
        )
        .with_copy_to(
            key_container_path,
            CopyDataSource::Data(generated.server_key_pem.into_bytes()),
        )
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

    // QUIC は UDP のため TCP ポーリングは行わず、
    // WaitFor::message_on_stdout の完了時点で QUIC listener が起動して
    // いることを "Listener quic:default on :14567 started." のログで確認済み。
    EmqxQuicGuard {
        container,
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
