//! E2E テストの共通ヘルパー。
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

/// Mosquitto コンテナの生存期間をテスト中に保つためのガード。
///
/// `container` フィールドは Drop 時のコンテナ停止のために保持する必要がある。
/// モジュール冒頭の `#![allow(dead_code)]` により、参照されないことによる
/// warning は抑制される。
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

/// EMQX コンテナの生存期間をテスト中に保つためのガード。
///
/// `container` フィールドは Drop 時のコンテナ停止のために保持する必要がある
/// （利用者からは参照しないが、モジュール冒頭の `#![allow(dead_code)]` により
/// warning は抑制される）。
pub struct EmqxGuard {
    pub container: ContainerAsync<GenericImage>,
    pub host: String,
    pub port: u16,
}

/// SCRAM-SHA-256 Enhanced Authentication 用 EMQX コンテナのガード。
///
/// Dashboard (18083) 経由で SCRAM authenticator とテストユーザーを注入する。
/// `container` は Drop 時の停止のために保持する。
pub struct EmqxScramGuard {
    pub container: ContainerAsync<GenericImage>,
    pub host: String,
    /// ホスト側にマップされた MQTT 1883/tcp ポート。
    pub port: u16,
    /// ホスト側にマップされた Dashboard 18083/tcp ポート。
    pub dashboard_port: u16,
    /// SCRAM 認証に使うテストユーザー ID (ASCII)。
    pub scram_user_id: &'static str,
    /// SCRAM 認証に使うテストパスワード (ASCII)。
    pub scram_password: &'static str,
}

/// SCRAM E2E 用のテストユーザー ID。SASLprep 対象外の ASCII 定数。
pub const EMQX_SCRAM_USER_ID: &str = "scram-e2e-user";

/// SCRAM E2E 用のテストパスワード。SASLprep 対象外の ASCII 定数。
pub const EMQX_SCRAM_PASSWORD: &str = "scram-e2e-password";

/// Dashboard ログインの既定ユーザー名 (EMQX イメージ既定。本番秘密ではない)。
const DEFAULT_DASHBOARD_USERNAME: &str = "admin";

/// Dashboard ログインの既定パスワード (EMQX イメージ既定。本番秘密ではない)。
const DEFAULT_DASHBOARD_PASSWORD: &str = "public";

/// SCRAM authenticator の API ID (`mechanism:backend`)。URL では `%3A` にエンコードする。
const SCRAM_AUTHENTICATOR_ID_ENCODED: &str = "scram%3Abuilt_in_database";

/// SCRAM-SHA-256 authenticator とテストユーザーを注入した EMQX を起動する。
///
/// 既存の [`start_emqx`] は触らず、Dashboard HTTP API で認証設定を行う。
/// 手順:
/// 1. コンテナ起動 (1883 + 18083)
/// 2. MQTT 待ち (`"is running now!"` + TCP)
/// 3. Dashboard ready (`GET /status` が HTTP 200)
/// 4. `POST /api/v5/login` → Bearer token
/// 5. 既定チェーンに妨害 authenticator があれば削除 (実測: 既定は空のため何もしない)
/// 6. SCRAM authenticator 作成 → テストユーザー登録
pub async fn start_emqx_scram() -> EmqxScramGuard {
    let container = GenericImage::new("emqx/emqx", "5.8.8")
        .with_exposed_port(1883.tcp())
        .with_exposed_port(18083.tcp())
        .with_wait_for(WaitFor::message_on_stdout("is running now!"))
        .start()
        .await
        .expect("SCRAM 用 EMQX コンテナの起動に成功すること");

    let host = container
        .get_host()
        .await
        .expect("コンテナのホスト名の取得に成功すること")
        .to_string();
    let port = container
        .get_host_port_ipv4(1883)
        .await
        .expect("コンテナの 1883 ポート番号の取得に成功すること");
    let dashboard_port = container
        .get_host_port_ipv4(18083)
        .await
        .expect("コンテナの 18083 ポート番号の取得に成功すること");

    wait_for_broker(&host, port, "EMQX SCRAM MQTT").await;
    wait_for_dashboard(&host, dashboard_port).await;

    let http = reqwest::Client::new();
    let token = dashboard_login(&http, &host, dashboard_port).await;
    // 実測 (EMQX 5.8.8): 起動直後の GET /api/v5/authentication は空配列。
    // 既定チェーンに authenticator は無く、SCRAM 追加前に削除・無効化は不要。
    // 将来イメージ側に既定 authenticator が載った場合に備え、一覧を取得して
    // 空であることを要求する (空でなければパニックして検知する)。
    ensure_empty_authentication_chain(&http, &host, dashboard_port, &token).await;
    create_scram_authenticator(&http, &host, dashboard_port, &token).await;
    create_scram_user(
        &http,
        &host,
        dashboard_port,
        &token,
        EMQX_SCRAM_USER_ID,
        EMQX_SCRAM_PASSWORD,
    )
    .await;

    EmqxScramGuard {
        container,
        host,
        port,
        dashboard_port,
        scram_user_id: EMQX_SCRAM_USER_ID,
        scram_password: EMQX_SCRAM_PASSWORD,
    }
}

/// Dashboard の `GET /status` が HTTP 200 になるまでポーリングする。
///
/// `/api/v5/status` は存在しない。login は ready 判定に使わない。
async fn wait_for_dashboard(host: &str, dashboard_port: u16) {
    let url = format!("http://{host}:{dashboard_port}/status");
    let http = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(response) = http.get(&url).send().await
            && response.status().as_u16() == 200
        {
            return;
        }
        if Instant::now() >= deadline {
            panic!("EMQX Dashboard が /status で HTTP 200 を返すまでのタイムアウト");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// Dashboard 資格情報を環境変数または既定値から取得する。
fn dashboard_credentials() -> (String, String) {
    let username = std::env::var("EMQX_DASHBOARD_USERNAME")
        .unwrap_or_else(|_| DEFAULT_DASHBOARD_USERNAME.to_string());
    let password = std::env::var("EMQX_DASHBOARD_PASSWORD")
        .unwrap_or_else(|_| DEFAULT_DASHBOARD_PASSWORD.to_string());
    (username, password)
}

/// JSON 文字列リテラル用に最低限のエスケープを行う。
fn escape_json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out
}

/// オブジェクト JSON から `"name":"..."` 形式の文字列フィールドを取り出す。
fn extract_json_string_field(body: &str, field: &str) -> Option<String> {
    let key = format!("\"{field}\"");
    let start = body.find(&key)?;
    let after_key = &body[start + key.len()..];
    let after_colon = after_key.split_once(':')?.1.trim_start();
    let after_quote = after_colon.strip_prefix('"')?;
    let mut value = String::new();
    let mut chars = after_quote.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                let escaped = chars.next()?;
                value.push(escaped);
            }
            '"' => return Some(value),
            c => value.push(c),
        }
    }
    None
}

/// 応答本文が空の JSON 配列 (`[]`、空白許容) かどうかを返す。
fn is_empty_json_array(body: &str) -> bool {
    let trimmed = body.trim();
    trimmed == "[]"
}

/// `POST /api/v5/login` で Bearer token を取得する。
async fn dashboard_login(http: &reqwest::Client, host: &str, dashboard_port: u16) -> String {
    let (username, password) = dashboard_credentials();
    let url = format!("http://{host}:{dashboard_port}/api/v5/login");
    let body = format!(
        r#"{{"username":"{}","password":"{}"}}"#,
        escape_json_string(&username),
        escape_json_string(&password)
    );
    let response = http
        .post(&url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("Dashboard ログイン要求の送信に成功すること");
    let status = response.status();
    let text = response
        .text()
        .await
        .expect("Dashboard ログイン応答の読み取りに成功すること");
    assert!(
        status.is_success(),
        "Dashboard ログインが成功すること: status={status}, body={text}"
    );
    extract_json_string_field(&text, "token").expect("ログイン応答に token フィールドがあること")
}

/// 認証チェーンが空であることを確認する。
///
/// EMQX 5.8.8 の既定は空。空でない場合は既定チェーン方針の再検討が必要なためパニックする。
async fn ensure_empty_authentication_chain(
    http: &reqwest::Client,
    host: &str,
    dashboard_port: u16,
    token: &str,
) {
    let url = format!("http://{host}:{dashboard_port}/api/v5/authentication");
    let response = http
        .get(&url)
        .bearer_auth(token)
        .send()
        .await
        .expect("認証チェーン一覧の取得に成功すること");
    let status = response.status();
    let text = response
        .text()
        .await
        .expect("認証チェーン一覧の読み取りに成功すること");
    assert!(
        status.is_success(),
        "認証チェーン一覧の取得が成功すること: status={status}, body={text}"
    );
    assert!(
        is_empty_json_array(&text),
        "EMQX 5.8.8 既定の認証チェーンは空である想定。妨害 authenticator の削除方針を見直すこと: {text}"
    );
}

/// SCRAM / built_in_database / sha256 authenticator を作成する。
async fn create_scram_authenticator(
    http: &reqwest::Client,
    host: &str,
    dashboard_port: u16,
    token: &str,
) {
    let url = format!("http://{host}:{dashboard_port}/api/v5/authentication");
    let body = r#"{
        "mechanism": "scram",
        "backend": "built_in_database",
        "algorithm": "sha256",
        "iteration_count": 4096
    }"#;
    let response = http
        .post(&url)
        .bearer_auth(token)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("SCRAM authenticator 作成要求の送信に成功すること");
    let status = response.status();
    let text = response
        .text()
        .await
        .expect("SCRAM authenticator 作成応答の読み取りに成功すること");
    assert!(
        status.is_success(),
        "SCRAM authenticator の作成が成功すること: status={status}, body={text}"
    );
}

/// SCRAM authenticator にテストユーザーを登録する。
async fn create_scram_user(
    http: &reqwest::Client,
    host: &str,
    dashboard_port: u16,
    token: &str,
    user_id: &str,
    password: &str,
) {
    let url = format!(
        "http://{host}:{dashboard_port}/api/v5/authentication/{SCRAM_AUTHENTICATOR_ID_ENCODED}/users"
    );
    let body = format!(
        r#"{{"user_id":"{}","password":"{}"}}"#,
        escape_json_string(user_id),
        escape_json_string(password)
    );
    let response = http
        .post(&url)
        .bearer_auth(token)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("SCRAM ユーザー登録要求の送信に成功すること");
    let status = response.status();
    let text = response
        .text()
        .await
        .expect("SCRAM ユーザー登録応答の読み取りに成功すること");
    assert!(
        status.is_success(),
        "SCRAM ユーザーの登録が成功すること: status={status}, body={text}"
    );
}

/// EMQX コンテナを起動し、生存期間を保ちながら (host, port) を返す。
///
/// testcontainers-modules 0.15 には EMQX 用の専用モジュールが存在しないため、
/// `GenericImage` から `emqx/emqx` イメージを直接組み立てて起動する。
/// イメージの参照元は emqx/emqx-docker (<https://github.com/emqx/emqx-docker>)。
pub async fn start_emqx() -> EmqxGuard {
    // 5.9 系以降は BSL に切り替わり公式 OSS イメージから外れたため、
    // OSS 最終系である 5.8 系の patch を固定して指定する。
    // タグを固定することで CI の再現性を確保する。
    //
    // EMQX は起動時、まず 1883 の TCP リスナーだけを bind した状態で
    // アプリケーション初期化を続ける。この時点で CONNECT を送ると
    // Connection reset by peer で切断されるため、標準出力に
    // "is running now!" が現れるまで待つことで、MQTT サービスが
    // CONNECT を受け付けられる状態になったことを保証する。
    // バージョン非依存にするため、バージョン部分は含めず部分一致で待つ。
    let container = GenericImage::new("emqx/emqx", "5.8.8")
        .with_exposed_port(1883.tcp())
        .with_wait_for(WaitFor::message_on_stdout("is running now!"))
        .start()
        .await
        .expect("EMQX コンテナの起動に成功すること");
    let host = container
        .get_host()
        .await
        .expect("コンテナのホスト名の取得に成功すること")
        .to_string();
    let port = container
        .get_host_port_ipv4(1883)
        .await
        .expect("コンテナの 1883 ポート番号の取得に成功すること");
    // EMQX は起動から MQTT リスナーが上がるまでに数秒かかるため、
    // TCP 接続が成功するまでポーリングで待つ。
    wait_for_broker(&host, port, "EMQX").await;
    EmqxGuard {
        container,
        host,
        port,
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
/// listener 実装で確定）。TCP 版 (`start_emqx`) と関数を分けているのは、
/// QUIC 用の環境変数と UDP ポートの expose を明示的にすることで、意図
/// しないテストで QUIC リスナーが立ち上がる副作用を避けるため。
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
        // TCP smoke test と同様に "is running now!" を待つ。
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

    // QUIC は UDP のため wait_for_broker のような TCP ポーリングは行わず、
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
    ca_dn.push(DnType::CommonName, "shiguredo mqtt-rs e2e test CA");
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
///
/// `service_name` はタイムアウト時のパニックメッセージに含める識別子として使う。
async fn wait_for_broker(host: &str, port: u16, service_name: &str) {
    let addr = format!("{}:{}", host, port);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if TcpStream::connect(&addr).await.is_ok() {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "{} が {} ポートで待ち受けるまでのタイムアウト",
                service_name, port
            );
        }
        sleep(Duration::from_millis(100)).await;
    }
}
