//! SCRAM-SHA-256 クライアント (RFC 5802 / RFC 7677)。
//!
//! MQTT v5.0 Enhanced Authentication の Authentication Data に載せる
//! client-first / client-final を生成し、成功 CONNACK の server-final に含まれる
//! ServerSignature を検証する。
//!
//! GS2 ヘッダはチャネルバインディング無し・認可 ID 無しの `n,,` を使う。
//! SASLprep は実装しない (E2E テスト用の ASCII 資格情報のみを想定する)。

use std::num::NonZeroU32;

use aws_lc_rs::digest::{self, SHA256, SHA256_OUTPUT_LEN};
use aws_lc_rs::hmac::{self, HMAC_SHA256};
use aws_lc_rs::pbkdf2::{self, PBKDF2_HMAC_SHA256};
use aws_lc_rs::{constant_time, rand};
use base64ct::{Base64, Encoding};

/// SCRAM-SHA-256 の Authentication Method 文字列 (RFC 7677)。
pub const SCRAM_SHA_256_METHOD: &str = "SCRAM-SHA-256";

/// GS2 ヘッダ (チャネルバインディング無し、認可 ID 無し)。
const GS2_HEADER: &str = "n,,";

/// client-final の `c=` 属性値。GS2 ヘッダ `n,,` の Base64 表現 (`biws`)。
const CHANNEL_BINDING_B64: &str = "biws";

/// SCRAM クライアント側のエラー。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramError {
    /// CSPRNG からの乱数取得に失敗した。
    Random,
    /// server-first / server-final の形式が不正。
    MalformedMessage(&'static str),
    /// Base64 のデコードに失敗した。
    Base64,
    /// PBKDF2 の iteration count が 0。
    InvalidIterationCount,
    /// ServerSignature が一致しない。
    ServerSignatureMismatch,
}

/// SCRAM-SHA-256 のクライアント状態。
///
/// `client_first` で開始し、server-first を受けて `client_final` を生成、
/// 成功時の server-final を `verify_server_final` で検証する。
pub struct ScramSha256Client {
    /// client-first-message-bare (`n=...,r=...`)。AuthMessage の一部になる。
    client_first_bare: String,
    /// クライアントが生成した nonce (Base64)。server nonce の接頭辞として使う。
    client_nonce: String,
    /// 平文パスワード (ASCII)。SASLprep は行わない。
    password: String,
    /// client-final 生成後に保持する、期待 ServerSignature (生バイト)。
    expected_server_signature: Option<Vec<u8>>,
}

impl ScramSha256Client {
    /// client-first-message を生成して新しいクライアント状態を返す。
    ///
    /// 戻り値の第 2 要素は MQTT Authentication Data に載せる client-first 全体
    /// (`n,,n=user,r=nonce`)。
    pub fn client_first(username: &str, password: &str) -> Result<(Self, Vec<u8>), ScramError> {
        // RFC 5802 は最低 16 オクテットの乱数エントロピーを推奨する。
        let mut nonce_bytes = [0u8; 16];
        rand::fill(&mut nonce_bytes).map_err(|_| ScramError::Random)?;
        let client_nonce = Base64::encode_string(&nonce_bytes);

        // テスト用 ASCII ユーザーのみを想定し、`=` / `,` の SASL エスケープは行わない。
        let client_first_bare = format!("n={username},r={client_nonce}");
        let client_first = format!("{GS2_HEADER}{client_first_bare}");

        Ok((
            Self {
                client_first_bare,
                client_nonce,
                password: password.to_string(),
                expected_server_signature: None,
            },
            client_first.into_bytes(),
        ))
    }

    /// server-first-message を処理し、client-final-message を返す。
    ///
    /// `server_first` は AUTH 0x18 の Authentication Data (UTF-8)。
    /// salt・iteration・server nonce はここから読み取り、authenticator 側の
    /// 既定値 (例: 4096) をクライアントで決め打ちしない。
    pub fn client_final(&mut self, server_first: &[u8]) -> Result<Vec<u8>, ScramError> {
        let server_first_str =
            std::str::from_utf8(server_first).map_err(|_| ScramError::MalformedMessage("utf8"))?;

        let parsed = parse_server_first(server_first_str)?;
        // サーバー nonce はクライアント nonce を接頭辞として含まなければならない (RFC 5802)。
        if !parsed.nonce.starts_with(&self.client_nonce) {
            return Err(ScramError::MalformedMessage("nonce prefix"));
        }

        let salted_password = hi(&self.password, &parsed.salt, parsed.iteration)?;

        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = digest::digest(&SHA256, &client_key);
        let server_key = hmac_sha256(&salted_password, b"Server Key");

        let client_final_without_proof =
            format!("c={CHANNEL_BINDING_B64},r={nonce}", nonce = parsed.nonce);
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, server_first_str, client_final_without_proof
        );

        let client_signature = hmac_sha256(stored_key.as_ref(), auth_message.as_bytes());
        let mut client_proof = client_key;
        for (dst, src) in client_proof.iter_mut().zip(client_signature.iter()) {
            *dst ^= *src;
        }

        let server_signature = hmac_sha256(&server_key, auth_message.as_bytes());
        self.expected_server_signature = Some(server_signature.to_vec());

        let client_final = format!(
            "{client_final_without_proof},p={proof}",
            proof = Base64::encode_string(&client_proof)
        );
        Ok(client_final.into_bytes())
    }

    /// 成功 CONNACK の Authentication Data (server-final) で ServerSignature を検証する。
    ///
    /// server-final は `v=<Base64(ServerSignature)>` 形式 (RFC 5802)。
    /// Data 欠落や形式不正、署名不一致はエラーとする。
    pub fn verify_server_final(&self, server_final: &[u8]) -> Result<(), ScramError> {
        let expected = self
            .expected_server_signature
            .as_ref()
            .ok_or(ScramError::MalformedMessage("client_final not called"))?;

        let server_final_str =
            std::str::from_utf8(server_final).map_err(|_| ScramError::MalformedMessage("utf8"))?;

        let value = server_final_str
            .strip_prefix("v=")
            .ok_or(ScramError::MalformedMessage("missing v="))?;
        // 追加属性がある場合は最初のカンマ手前だけを取る。
        let value = value.split(',').next().unwrap_or(value);

        let received = Base64::decode_vec(value).map_err(|_| ScramError::Base64)?;
        constant_time::verify_slices_are_equal(expected, &received)
            .map_err(|_| ScramError::ServerSignatureMismatch)
    }
}

/// server-first-message から取り出した属性。
struct ServerFirst {
    nonce: String,
    salt: Vec<u8>,
    iteration: NonZeroU32,
}

/// `r=...,s=...,i=...` 形式の server-first をパースする。
fn parse_server_first(message: &str) -> Result<ServerFirst, ScramError> {
    let mut nonce = None;
    let mut salt = None;
    let mut iteration = None;

    for attr in message.split(',') {
        if let Some(rest) = attr.strip_prefix("r=") {
            nonce = Some(rest.to_string());
        } else if let Some(rest) = attr.strip_prefix("s=") {
            salt = Some(Base64::decode_vec(rest).map_err(|_| ScramError::Base64)?);
        } else if let Some(rest) = attr.strip_prefix("i=") {
            let count: u32 = rest
                .parse()
                .map_err(|_| ScramError::MalformedMessage("iteration"))?;
            iteration = Some(NonZeroU32::new(count).ok_or(ScramError::InvalidIterationCount)?);
        }
        // `m=` や拡張属性は無視する (RFC 5802)。
    }

    Ok(ServerFirst {
        nonce: nonce.ok_or(ScramError::MalformedMessage("missing r="))?,
        salt: salt.ok_or(ScramError::MalformedMessage("missing s="))?,
        iteration: iteration.ok_or(ScramError::MalformedMessage("missing i="))?,
    })
}

/// SaltedPassword = Hi(password, salt, i) = PBKDF2-HMAC-SHA256 (RFC 5802 / 7677)。
fn hi(
    password: &str,
    salt: &[u8],
    iteration: NonZeroU32,
) -> Result<[u8; SHA256_OUTPUT_LEN], ScramError> {
    let mut out = [0u8; SHA256_OUTPUT_LEN];
    pbkdf2::derive(
        PBKDF2_HMAC_SHA256,
        iteration,
        salt,
        password.as_bytes(),
        &mut out,
    );
    Ok(out)
}

/// HMAC-SHA-256。出力は常に 32 バイト。
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; SHA256_OUTPUT_LEN] {
    let key = hmac::Key::new(HMAC_SHA256, key);
    let tag = hmac::sign(&key, data);
    let mut out = [0u8; SHA256_OUTPUT_LEN];
    out.copy_from_slice(tag.as_ref());
    out
}
