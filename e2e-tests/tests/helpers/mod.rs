//! E2E テストの共通ヘルパー。
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
use shiguredo_container::core::{AccessMode, IntoContainerPort, Mount};
use shiguredo_container::{AsyncRunner, ContainerAsync, GenericImage, ImageExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep};
use tokio_rustls::TlsConnector;

/// Mosquitto の ready 判定用に使うクライアント。
use e2e_tests::v5::client::MqttClient as ProbeClient;

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

/// Mosquitto コンテナの生存期間をテスト中に保つためのガード。
///
/// `container` フィールドは Drop 時のコンテナ停止のために保持する必要がある。
/// モジュール冒頭の `#![allow(dead_code)]` により、参照されないことによる
/// warning は抑制される。
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
    let container = GenericImage::new("eclipse-mosquitto", tag)
        .with_exposed_port(1883.tcp())
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

    let temp_dir = create_temp_dir("mqtt-rs-mosquitto-tls");
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
    let container = GenericImage::new("eclipse-mosquitto", tag)
        .with_exposed_port(8883.tcp())
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

/// EMQX コンテナの生存期間をテスト中に保つためのガード。
///
/// `container` フィールドは Drop 時のコンテナ停止のために保持する必要がある
/// （利用者からは参照しないが、モジュール冒頭の `#![allow(dead_code)]` により
/// warning は抑制される）。
pub struct EmqxGuard {
    container: Option<ContainerAsync<GenericImage>>,
    pub host: String,
    pub port: u16,
}

impl Drop for EmqxGuard {
    fn drop(&mut self) {
        if let Some(container) = self.container.take() {
            let id = container.id().to_string();
            drop(container);
            force_remove_container(&id);
        }
    }
}

/// SCRAM-SHA-256 Enhanced Authentication 用 EMQX コンテナのガード。
///
/// Dashboard (18083) 経由で SCRAM authenticator とテストユーザーを注入する。
/// `container` は Drop 時の停止のために保持する。
pub struct EmqxScramGuard {
    container: Option<ContainerAsync<GenericImage>>,
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

impl Drop for EmqxScramGuard {
    fn drop(&mut self) {
        if let Some(container) = self.container.take() {
            let id = container.id().to_string();
            drop(container);
            force_remove_container(&id);
        }
    }
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
/// 2. MQTT 待ち (TCP) + Dashboard ready (`GET /status` が HTTP 200)
/// 3. `POST /api/v5/login` → Bearer token
/// 4. 既定チェーンに妨害 authenticator があれば削除 (実測: 既定は空のため何もしない)
/// 5. SCRAM authenticator 作成 → テストユーザー登録
///
/// Linux ではログ待機が未対応のため、Dashboard `/status` でフル起動を確認する。
/// EMQX は起動途中で 1883 だけ bind した段階では CONNECT を Connection reset で
/// 落とすため、TCP 待ちだけでは不十分である。
pub async fn start_emqx_scram() -> EmqxScramGuard {
    let container = GenericImage::new("emqx/emqx", "5.8.8")
        .with_exposed_port(1883.tcp())
        .with_exposed_port(18083.tcp())
        .start()
        .await
        .expect("SCRAM 用 EMQX コンテナの起動に成功すること");

    let host = container
        .get_host()
        .await
        .expect("コンテナのホスト名の取得に成功すること")
        .to_string();
    let port = container
        .get_host_port_ipv4(1883.tcp())
        .await
        .expect("コンテナの 1883 ポート番号の取得に成功すること");
    let dashboard_port = container
        .get_host_port_ipv4(18083.tcp())
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
        container: Some(container),
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
/// EMQX が CONNECT を受け付けられる状態になったことの代理指標としても使う
/// (Linux ではログ待機が使えないため)。
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
/// `GenericImage` から `emqx/emqx` イメージを直接組み立てて起動する。
/// イメージの参照元は emqx/emqx-docker (<https://github.com/emqx/emqx-docker>)。
pub async fn start_emqx() -> EmqxGuard {
    // 5.9 系以降は BSL に切り替わり公式 OSS イメージから外れたため、
    // OSS 最終系である 5.8 系の patch を固定して指定する。
    // タグを固定することで CI の再現性を確保する。
    //
    // EMQX は起動時、まず 1883 の TCP リスナーだけを bind した状態で
    // アプリケーション初期化を続ける。この時点で CONNECT を送ると
    // Connection reset by peer で切断される。Linux ではログ待機が
    // 未対応のため、Dashboard `/status` が HTTP 200 になるまで待って
    // フル起動を確認する。
    let container = GenericImage::new("emqx/emqx", "5.8.8")
        .with_exposed_port(1883.tcp())
        .with_exposed_port(18083.tcp())
        .start()
        .await
        .expect("EMQX コンテナの起動に成功すること");
    let host = container
        .get_host()
        .await
        .expect("コンテナのホスト名の取得に成功すること")
        .to_string();
    let port = container
        .get_host_port_ipv4(1883.tcp())
        .await
        .expect("コンテナの 1883 ポート番号の取得に成功すること");
    let dashboard_port = container
        .get_host_port_ipv4(18083.tcp())
        .await
        .expect("コンテナの 18083 ポート番号の取得に成功すること");
    wait_for_broker(&host, port, "EMQX").await;
    wait_for_dashboard(&host, dashboard_port).await;
    EmqxGuard {
        container: Some(container),
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
/// `_temp_dir` は bind mount 元の証明書をコンテナ生存中に保持する。
pub struct EmqxQuicGuard {
    container: Option<ContainerAsync<GenericImage>>,
    /// bind mount 元。フィールド参照はしないが Drop まで保持する必要がある。
    _temp_dir: TempDirGuard,
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
/// 生成した自前の CA と server 証明書を都度ホストへ書き出して bind mount
/// する。こうすることで、クライアント側は無検証 (dangerous verifier)
/// ではなく、正規にこの CA を trust root として検証できる。
///
/// QUIC の既定ポートは 14567/udp、ALPN は `"mqtt"`（EMQX の quicer
/// listener 実装で確定）。TCP 版 (`start_emqx`) と関数を分けているのは、
/// QUIC 用の環境変数と UDP ポートの expose を明示的にすることで、意図
/// しないテストで QUIC リスナーが立ち上がる副作用を避けるため。
///
/// Linux では `with_copy_to` / ログ待機が未対応のため、証明書はファイル単位の
/// bind mount、起動完了は Dashboard `/status` で確認する。
pub async fn start_emqx_quic() -> EmqxQuicGuard {
    // localhost 向けの CA と server 証明書を rcgen で生成する。
    let generated = generate_localhost_certs();

    // コンテナ内で EMQX が読み取れる場所に証明書を配置する。EMQX の
    // WorkingDir は /opt/emqx なので、同梱の例示証明書と同じ
    // /opt/emqx/etc/certs/ 配下に置き、環境変数側は WorkingDir 相対の
    // "etc/certs/..." で指定する。ファイル名は同梱の cert.pem / key.pem と
    // 衝突しないように "quic-*.pem" とする。
    // ディレクトリ全体を mount すると同梱証明書を隠してしまうため、
    // 追加ファイルだけをファイル単位で bind mount する。
    let cert_container_path = "/opt/emqx/etc/certs/quic-cert.pem";
    let key_container_path = "/opt/emqx/etc/certs/quic-key.pem";
    let cert_env_path = "etc/certs/quic-cert.pem";
    let key_env_path = "etc/certs/quic-key.pem";

    let temp_dir = create_temp_dir("mqtt-rs-emqx-quic");
    let host_cert = write_temp_file(
        temp_dir.path(),
        "quic-cert.pem",
        generated.server_cert_pem.as_bytes(),
    );
    let host_key = write_temp_file(
        temp_dir.path(),
        "quic-key.pem",
        generated.server_key_pem.as_bytes(),
    );

    let container = GenericImage::new("emqx/emqx", "5.8.8")
        // QUIC は UDP なので UDP ポートとして expose する必要がある。
        .with_exposed_port(14567.udp())
        // フル起動判定用に Dashboard も公開する (Linux ではログ待機不可)。
        .with_exposed_port(18083.tcp())
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
        .with_mount(
            Mount::bind_mount(host_cert.to_string_lossy(), cert_container_path)
                .with_access_mode(AccessMode::ReadOnly),
        )
        .with_mount(
            Mount::bind_mount(host_key.to_string_lossy(), key_container_path)
                .with_access_mode(AccessMode::ReadOnly),
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
    let dashboard_port = container
        .get_host_port_ipv4(18083.tcp())
        .await
        .expect("コンテナの 18083 ポート番号の取得に成功すること");

    // QUIC は UDP のため wait_for_broker のような TCP ポーリングは行わず、
    // Dashboard が応答した時点で listener 群が起動していることを確認する。
    wait_for_dashboard(&host, dashboard_port).await;
    EmqxQuicGuard {
        container: Some(container),
        _temp_dir: temp_dir,
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
        let client_id = format!("mqtt-rs-ready-{attempt}");
        if let Ok(mut client) = ProbeClient::connect_tcp(host, port).await
            && client.connect_v5(&client_id).await.is_ok()
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
