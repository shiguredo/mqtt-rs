//! E2E テストの共通ヘルパー。
//!
//! 統合テストのサブモジュールとして各 `tests/*.rs` から
//! `mod helpers;` で読み込まれる。統合テストファイルごとに
//! 使わない関数があると dead_code 警告が出るため、モジュール
//! 冒頭で一括抑制している。

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DistinguishedName, DnType, IsCa, KeyPair,
};
use shiguredo_container::core::IntoContainerPort;
use shiguredo_container::core::mounts::Mount;
use shiguredo_container::core::wait::HttpWaitStrategy;
use shiguredo_container::{AsyncRunner, ContainerAsync, GenericImage, ImageExt, WaitFor};
use tokio::time::{Instant, sleep};

/// Mosquitto が listener 受付可能になったことを示すログ断片。
///
/// `mosquitto version X.Y.Z starting` には含まれず、
/// `mosquitto version X.Y.Z running` にだけ現れる。
const MOSQUITTO_RUNNING_LOG: &str = "running";

/// ホスト側の一時ディレクトリを一意な名前で作る。
///
/// 連続テストで衝突しないよう、プロセス ID とナノ秒を混ぜる。
fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("システム時刻が UNIX epoch 以降であること")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
}

/// EMQX Dashboard のフル起動判定 (`GET /status` が HTTP 200)。
fn emqx_dashboard_ready() -> WaitFor {
    WaitFor::http(
        HttpWaitStrategy::new("/status")
            .with_port(18083.tcp())
            .with_expected_status_code(200_u16),
    )
}

/// Mosquitto の listener 受付可能判定 (ログに `running` が出るまで)。
fn mosquitto_ready() -> WaitFor {
    WaitFor::message_on_either_std(MOSQUITTO_RUNNING_LOG)
}

/// Mosquitto コンテナの生存期間をテスト中に保つためのガード。
///
/// `container` フィールドは Drop 時のコンテナ削除のために保持する必要がある。
/// `rm_blocking` は `Drop` 内の同期コンテキストからでも削除完了を待てるため、
/// 外部 CLI に頼らずライブラリの公開 API だけで掃除できる。
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
            let _ = container.rm_blocking();
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
            let _ = container.rm_blocking();
        }
    }
}

/// TLS listener (8883) を有効化した Mosquitto コンテナを起動する。
///
/// 平文用の `/mosquitto-no-auth.conf` では足りないため、同じイメージ
/// (`eclipse-mosquitto:2.0.18`) を `GenericImage` で起動し、rcgen で生成した
/// 証明書と自前の `mosquitto.conf` を `Mount::bind_mount` でホスト一時ディレクトリから
/// `/mosquitto/config` へマウントする。クライアントは dangerous verifier ではなく、
/// この CA を trust root として正規に検証する。
///
/// macOS は `with_copy_to` が start 後に走るため投入完了待ちのシェルが必要だったが、
/// `Mount::bind_mount` は start 前にマウントが完了するため、macOS / Linux どちらでも
/// 設定ファイルを直接参照して `mosquitto` を起動できる。
pub async fn start_mosquitto_tls() -> MosquittoTlsGuard {
    let generated = generate_localhost_certs();
    let host_config_dir = write_mosquitto_tls_config_dir(&generated);

    // 平文 Mosquitto と同じタグに固定し、CI の再現性を保つ。
    // 既定 mode 0644 のままにする。0600 + root 所有だと権限降下後の
    // mosquitto ユーザーが鍵を読めず TLS が立ち上がらない。
    let tag = "2.0.18";
    let host_config_dir_str = host_config_dir
        .to_str()
        .expect("Mosquitto TLS 設定ディレクトリのパスが UTF-8 であること")
        .to_string();
    let container = GenericImage::new("eclipse-mosquitto", tag)
        .with_exposed_port(8883.tcp())
        .with_wait_for(mosquitto_ready())
        .with_mount(Mount::bind_mount(host_config_dir_str, "/mosquitto/config"))
        .with_cmd(["mosquitto", "-c", "/mosquitto/config/mosquitto.conf"])
        .start()
        .await
        .expect("TLS 有効の Mosquitto コンテナの起動に成功すること");
    // マウントは start 前に完了するため、ホスト側ディレクトリはこの時点で不要。
    let _ = std::fs::remove_dir_all(&host_config_dir);

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

/// Mosquitto TLS 用の設定と証明書をホスト一時ディレクトリへ書き出す。
///
/// 戻り値のディレクトリを `with_copy_to("/mosquitto/config", ...)` に渡すと、
/// 配下のファイルがコンテナの `/mosquitto/config/` に一括投入される。
fn write_mosquitto_tls_config_dir(generated: &GeneratedCerts) -> PathBuf {
    // conf 内で参照するコンテナ内パス。
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

    let host_dir = unique_temp_dir("mqtt-rs-mosquitto-tls");
    std::fs::create_dir_all(&host_dir)
        .expect("Mosquitto TLS 設定用のホスト一時ディレクトリの作成に成功すること");
    write_file(&host_dir.join("mosquitto.conf"), mosquitto_conf.as_bytes());
    write_file(&host_dir.join("ca.pem"), generated.ca_pem.as_bytes());
    write_file(
        &host_dir.join("server.pem"),
        generated.server_cert_pem.as_bytes(),
    );
    write_file(
        &host_dir.join("server.key"),
        generated.server_key_pem.as_bytes(),
    );
    host_dir
}

/// ホスト一時ファイルへバイト列を書き込む。
fn write_file(path: &Path, bytes: &[u8]) {
    std::fs::write(path, bytes)
        .unwrap_or_else(|e| panic!("{} への書き込みに成功すること: {e}", path.display()));
}

/// EMQX コンテナの生存期間をテスト中に保つためのガード。
///
/// `container` フィールドは Drop 時のコンテナ削除のために保持する必要がある
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
            let _ = container.rm_blocking();
        }
    }
}

/// SCRAM-SHA-256 Enhanced Authentication 用 EMQX コンテナのガード。
///
/// Dashboard (18083) 経由で SCRAM authenticator とテストユーザーを注入する。
/// `container` は Drop 時の削除のために保持する。
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
            let _ = container.rm_blocking();
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
/// 1. コンテナ起動 (1883 + 18083)。ready は Dashboard `/status` の `WaitFor::http`
/// 2. `POST /api/v5/login` → Bearer token
/// 3. 既定チェーンに妨害 authenticator があれば削除 (実測: 既定は空のため何もしない)
/// 4. SCRAM authenticator 作成 → テストユーザー登録
///
/// EMQX は起動途中で 1883 だけ bind した段階では CONNECT を Connection reset で
/// 落とすため、Dashboard ready をフル起動の代理指標とする。
/// ただし `/status` 200 直後は SCRAM provider が未登録のことがあり、
/// authenticator 作成は `no_available_provider_for` をリトライする。
pub async fn start_emqx_scram() -> EmqxScramGuard {
    let container = GenericImage::new("emqx/emqx", "5.8.8")
        .with_exposed_port(1883.tcp())
        .with_exposed_port(18083.tcp())
        .with_wait_for(emqx_dashboard_ready())
        .with_startup_timeout(Duration::from_secs(120))
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
///
/// Dashboard `/status` が HTTP 200 でも、認証 provider の登録が追いついていない
/// ことがある。その場合 EMQX は `no_available_provider_for` を返すため、
/// provider が利用可能になるまで短い間隔で再試行する。
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
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
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
        if status.is_success() {
            return;
        }
        // 起動直後だけ起きる一時的な拒否。それ以外は即座に失敗させる。
        let provider_not_ready =
            status.as_u16() == 400 && text.contains("no_available_provider_for");
        if !provider_not_ready || Instant::now() >= deadline {
            panic!("SCRAM authenticator の作成が成功すること: status={status}, body={text}");
        }
        sleep(Duration::from_millis(100)).await;
    }
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
    // Connection reset by peer で切断される。Dashboard `/status` が
    // HTTP 200 になるまで `WaitFor::http` で待ってフル起動を確認する。
    let container = GenericImage::new("emqx/emqx", "5.8.8")
        .with_exposed_port(1883.tcp())
        .with_exposed_port(18083.tcp())
        .with_wait_for(emqx_dashboard_ready())
        .with_startup_timeout(Duration::from_secs(120))
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
pub struct EmqxQuicGuard {
    container: Option<ContainerAsync<GenericImage>>,
    /// bind_mount でマウント中のホスト一時ディレクトリ。コンテナ削除後に掃除する。
    host_certs_dir: Option<PathBuf>,
    pub host: String,
    pub udp_port: u16,
    pub ca_pem: String,
    /// サーバー証明書に付与した SNI/CN。クライアント側の SNI に用いる。
    pub server_name: String,
}

impl Drop for EmqxQuicGuard {
    fn drop(&mut self) {
        if let Some(container) = self.container.take() {
            let _ = container.rm_blocking();
        }
        if let Some(dir) = self.host_certs_dir.take() {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

/// QUIC リスナーを有効化した EMQX コンテナを起動する。
///
/// EMQX 5 系の既定では QUIC リスナーは無効なため、環境変数で明示的に
/// 有効化する。証明書はイメージ同梱の例示証明書ではなく、`rcgen` で
/// 生成した自前の CA と server 証明書を `Mount::bind_mount` でホスト一時
/// ディレクトリから `/opt/emqx/etc/quic-certs` へマウントする。こうすることで、
/// クライアント側は無検証 (dangerous verifier) ではなく、正規にこの CA を
/// trust root として検証できる。
///
/// QUIC の既定ポートは 14567/udp、ALPN は `"mqtt"`（EMQX の quicer
/// listener 実装で確定）。TCP 版 (`start_emqx`) と関数を分けているのは、
/// QUIC 用の環境変数と UDP ポートの expose を明示的にすることで、意図
/// しないテストで QUIC リスナーが立ち上がる副作用を避けるため。
///
/// macOS は `with_copy_to` が start 後に走るため、EMQX の QUIC listener
/// 起動時に証明書が揃わないことがある。`Mount::bind_mount` は start 前に
/// マウントが完了するため、macOS / Linux どちらでも証明書を読み取れる。
/// マウント先は同梱の `certs/` (cert.pem / key.pem / cacert.pem) を隠さない
/// よう専用ディレクトリ `quic-certs/` を新設する。
pub async fn start_emqx_quic() -> EmqxQuicGuard {
    // localhost 向けの CA と server 証明書を rcgen で生成する。
    let generated = generate_localhost_certs();

    // コンテナ内で EMQX が読み取れる場所に証明書を配置する。EMQX の
    // WorkingDir は /opt/emqx なので、環境変数側は WorkingDir 相対の
    // "etc/quic-certs/..." で指定する。ファイル名は同梱の cert.pem / key.pem と
    // 衝突しないように "quic-*.pem" とする。
    let cert_env_path = "etc/quic-certs/quic-cert.pem";
    let key_env_path = "etc/quic-certs/quic-key.pem";
    let host_certs_dir = write_emqx_quic_certs_dir(&generated);
    let host_certs_dir_str = host_certs_dir
        .to_str()
        .expect("EMQX QUIC 証明書ディレクトリのパスが UTF-8 であること")
        .to_string();

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
        .with_mount(Mount::bind_mount(
            host_certs_dir_str,
            "/opt/emqx/etc/quic-certs",
        ))
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
        host_certs_dir: Some(host_certs_dir),
        host,
        udp_port,
        ca_pem: generated.ca_pem,
        server_name: generated.server_name,
    }
}

/// EMQX QUIC 用の証明書をホスト一時ディレクトリへ書き出す。
///
/// 戻り値のディレクトリを `Mount::bind_mount` のホスト側に渡す。
fn write_emqx_quic_certs_dir(generated: &GeneratedCerts) -> PathBuf {
    let host_dir = unique_temp_dir("mqtt-rs-emqx-quic-certs");
    std::fs::create_dir_all(&host_dir)
        .expect("EMQX QUIC 証明書用のホスト一時ディレクトリの作成に成功すること");
    write_file(
        &host_dir.join("quic-cert.pem"),
        generated.server_cert_pem.as_bytes(),
    );
    write_file(
        &host_dir.join("quic-key.pem"),
        generated.server_key_pem.as_bytes(),
    );
    host_dir
}

/// `start_emqx_quic` / `start_mosquitto_tls` 内部で使う証明書一式。
struct GeneratedCerts {
    /// クライアントが trust root として渡す CA 証明書 (PEM)。
    ca_pem: String,
    /// ブローカーへ配置するサーバー証明書 (PEM)。
    server_cert_pem: String,
    /// ブローカーへ配置するサーバー鍵 (PEM)。
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
