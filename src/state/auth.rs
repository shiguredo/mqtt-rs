//! 認証状態機械 (MQTT v5.0 Enhanced Authentication)。
//!
//! MQTT v5.0 §4.12 を参照。
//!
//! AUTH パケットによる拡張認証 (SCRAM 等) の状態を管理する。
//! この状態機械は Sans-I/O であり、認証シーケンスの段階のみを追跡する。
//! 実際の認証データ (チャレンジ／レスポンス) の計算は利用者が行う。
//!
//! MQTT v3.1.1 には AUTH パケットが存在しないため、この状態機械は v5.0 専用である。

use alloc::fmt;
use alloc::string::String;

/// 認証の状態。
///
/// 初回認証と再認証は完了手順が異なるため、`InitialAuthenticating` と
/// `Reauthenticating` に分離している。
///
/// - 初回認証: CONNECT 起点で、成功 CONNACK により完了する
///   (MQTT v5.0 §4.12)。初回認証中の AUTH 0x00 は完了ではない。
/// - 再認証: クライアントの AUTH 0x19 起点で、サーバーの AUTH 0x00 により
///   完了する (MQTT v5.0 §4.12.1)。
///
/// `InitialAuthenticating` と `Reauthenticating` では [`AuthStateMachine::auth_method`]
/// は常に `Some` である。開始 API が必ず設定し、`Idle` へのリセットは
/// `auth_method` を `None` に戻すため、この不変条件は破れない。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthState {
    /// 認証が開始されていない、または完了後の状態機械のリセット後の状態。
    Idle,
    /// 初回認証中。CONNECT で Authentication Method を送信済みで、
    /// 成功 CONNACK を待っている。
    InitialAuthenticating,
    /// 認証が完了した。
    Authenticated,
    /// 再認証中。AUTH 0x19 (Re-authenticate) を送信済みで、
    /// AUTH 0x00 (Success) を待っている。
    Reauthenticating,
}

/// 認証状態機械。
///
/// CONNECT での認証方式指定から始まり、AUTH パケットの交換と成功 CONNACK を
/// 経て初回認証完了までを、また `reauthenticate_sent()` 起点で再認証完了までを
/// 管理する。
#[derive(Debug, Clone)]
pub struct AuthStateMachine {
    /// 現在の認証状態。
    state: AuthState,
    /// 使用中の認証方式。
    ///
    /// CONNECT の Authentication Method プロパティ (MQTT v5.0 §3.1.2.11.9) で
    /// 指定された値。`InitialAuthenticating` / `Reauthenticating` / `Authenticated`
    /// のあいだは `Some` で維持し、`Idle` へのリセットで `None` に戻す。
    auth_method: Option<String>,
}

impl AuthStateMachine {
    /// 認証状態機械を新規作成する。
    pub fn new() -> Self {
        Self {
            state: AuthState::Idle,
            auth_method: None,
        }
    }

    /// 現在の認証状態を返す。
    pub fn state(&self) -> AuthState {
        self.state
    }

    /// 認証方式を返す。
    pub fn auth_method(&self) -> Option<&str> {
        self.auth_method.as_deref()
    }

    /// 認証が進行中 (初回認証中または再認証中) かどうかを返す。
    pub fn is_authenticating(&self) -> bool {
        matches!(
            self.state,
            AuthState::InitialAuthenticating | AuthState::Reauthenticating
        )
    }

    /// 初回認証中かどうかを返す。
    pub fn is_initial_authenticating(&self) -> bool {
        self.state == AuthState::InitialAuthenticating
    }

    /// 再認証中かどうかを返す。
    pub fn is_reauthenticating(&self) -> bool {
        self.state == AuthState::Reauthenticating
    }

    /// 認証が完了しているかどうかを返す。
    pub fn is_authenticated(&self) -> bool {
        self.state == AuthState::Authenticated
    }

    /// CONNECT で認証方式を指定して接続を開始したときに呼び出す。
    ///
    /// 順序の罠を避けるため、外部からの初回認証開始は
    /// [`crate::state::session::Session::connect_sent_with_auth`] を使う。
    /// 本メソッドは同一クレート内 (`Session::connect_sent_with_auth` の内部) からのみ呼び出す。
    ///
    /// MQTT v5.0 §3.1.2.11.9 [MQTT-3.1.2-30]:
    /// Authentication Method を指定した CONNECT は拡張認証の開始であり、
    /// CONNACK 受信までは AUTH または DISCONNECT 以外のパケットを送信してはならない。
    pub(crate) fn connect_with_auth(&mut self, auth_method: String) {
        self.auth_method = Some(auth_method);
        self.state = AuthState::InitialAuthenticating;
    }

    /// AUTH パケットを受信したときに呼び出す。
    ///
    /// `reason_code` は AUTH パケットの Reason Code (0x00 / 0x18 / 0x19)。
    /// `authentication_method` は受信 AUTH の Authentication Method プロパティの値。
    /// codec 層は AUTH に Authentication Method を必須とするが (MQTT v5.0 §3.15.2.2.2)、
    /// 本メソッドの契約としては `None` を許容し、その扱いを次のとおり定める。
    ///
    /// 判定は次の 2 段階で行う。
    ///
    /// 1. 状態 × Reason Code の合法性
    ///    - 合法な組み合わせ:
    ///      - `InitialAuthenticating` + 0x18: `Continue`
    ///      - `Reauthenticating` + 0x18: `Continue`
    ///      - `Reauthenticating` + 0x00: `Authenticated`
    ///    - それ以外はすべて失敗。とくに `InitialAuthenticating` + 0x00 は失敗
    ///      (初回認証の完了は成功 CONNACK のみ、MQTT v5.0 §4.12)。
    ///    - 失敗時は状態を `Idle`、`auth_method` を `None` にリセットして
    ///      [`AuthAction::Failed`] を返す。
    /// 2. Authentication Method 検証 (段階 1 を通過した場合のみ)
    ///    - `Some(method)` が CONNECT の Method と不一致なら、状態を `Idle`、
    ///      `auth_method` を `None` にリセットして [`AuthAction::MethodMismatch`] を返す
    ///      (MQTT v5.0 §4.12 [MQTT-4.12.0-5])。
    ///    - `None`: `Reauthenticating` + 0x00 のみ受理する (MQTT v5.0 §3.15.2.1 の
    ///      Remaining Length 0 省略形に相当する形での送信を防御的に受理する)。
    ///      それ以外は [`AuthAction::MethodMismatch`] を返す。
    ///
    /// [`AuthAction::MethodMismatch`] は「相手による認証拒否 (`Failed`)」ではなく
    /// 「プロトコル違反」に対応する。MQTT v5.0 §3.15 は「CONNECT パケットに同じ
    /// Authentication Method が含まれていなかった場合、AUTH パケットの送信は
    /// Protocol Error」と定めており、利用者が DISCONNECT の Reason Code を選び分けられるよう
    /// `Failed` と区別する。
    pub fn auth_received(
        &mut self,
        reason_code: u8,
        authentication_method: Option<&str>,
    ) -> AuthAction {
        // 段階 1: 状態と Reason Code の合法性を検査する。
        // 合法な組み合わせは 3 つだけであり、それ以外はすべて Failed とする。
        let is_legal = matches!(
            (self.state, reason_code),
            (AuthState::InitialAuthenticating, 0x18)
                | (AuthState::Reauthenticating, 0x18)
                | (AuthState::Reauthenticating, 0x00)
        );
        if !is_legal {
            self.state = AuthState::Idle;
            self.auth_method = None;
            return AuthAction::Failed;
        }

        // 段階 2: Authentication Method を検証する。
        // ここに来た時点で state は InitialAuthenticating か Reauthenticating に
        // 限られ、いずれも auth_method が Some であるという不変条件をもつため、
        // 期待値の取り出しは失敗しない。
        let expected = self
            .auth_method
            .as_deref()
            .expect("auth_method must be set while authenticating");
        let is_reauth_success = matches!(
            (self.state, reason_code),
            (AuthState::Reauthenticating, 0x00)
        );
        match authentication_method {
            Some(method) if method == expected => {
                // 一致。遷移に進む。
            }
            None if is_reauth_success => {
                // Reauthenticating + 0x00 のみ Method 省略を受理する。
            }
            _ => {
                // 不一致または欠落 (Reauthenticating + 0x00 以外)。
                self.state = AuthState::Idle;
                self.auth_method = None;
                return AuthAction::MethodMismatch;
            }
        }

        // 遷移を確定する。
        match (self.state, reason_code) {
            (AuthState::InitialAuthenticating, 0x18) | (AuthState::Reauthenticating, 0x18) => {
                AuthAction::Continue
            }
            (AuthState::Reauthenticating, 0x00) => {
                self.state = AuthState::Authenticated;
                AuthAction::Authenticated
            }
            // 段階 1 で他の組み合わせは弾いているため到達しない。
            _ => unreachable!("legal (state, reason_code) checked in stage 1"),
        }
    }

    /// 成功 CONNACK 受信時の検証と初回認証の完了処理。
    ///
    /// 可視性は `pub(crate)`。外部からは
    /// [`crate::state::session::Session::apply_connack`] 経由でのみ到達する。
    /// 検証規則は状態で網羅的に定める:
    ///
    /// - `InitialAuthenticating`: `None` は
    ///   [`AuthMethodError::Missing`] (MQTT v5.0 §4.12 [MQTT-4.12.0-5])。
    ///   不一致は [`AuthMethodError::Mismatch`]。一致すれば `Authenticated` へ遷移する。
    /// - それ以外の状態: `Some(_)` なら
    ///   [`AuthMethodError::Unexpected`] を返す。根拠は、`Idle` では MQTT v5.0 §4.12
    ///   [MQTT-4.12.0-6] (サーバーは Method なし CONNECT への CONNACK に Method を
    ///   含めてはならない)。`Authenticated` / `Reauthenticating` は CONNACK を受ける
    ///   正規の経路がないため防御的検証とする。`None` なら何もしない。
    ///
    /// エラー時は状態を変更しない。この関数は
    /// [`crate::state::session::Session::apply_connack`] の「最後の fallible 操作」
    /// として使うことを想定している。
    pub(crate) fn connack_received(
        &mut self,
        authentication_method: Option<&str>,
    ) -> Result<(), AuthMethodError> {
        match self.state {
            AuthState::InitialAuthenticating => {
                // 初回認証中は auth_method が必ず Some (不変条件)。
                let expected = self
                    .auth_method
                    .as_deref()
                    .expect("auth_method must be set while initial authenticating");
                match authentication_method {
                    None => Err(AuthMethodError::Missing),
                    Some(method) if method != expected => Err(AuthMethodError::Mismatch),
                    Some(_) => {
                        self.state = AuthState::Authenticated;
                        Ok(())
                    }
                }
            }
            _ => {
                // Idle / Authenticated / Reauthenticating。
                // 認証を開始していない、または既に完了・再認証中のセッションで
                // CONNACK に Authentication Method が含まれるのは予期しない。
                if authentication_method.is_some() {
                    Err(AuthMethodError::Unexpected)
                } else {
                    Ok(())
                }
            }
        }
    }

    /// クライアントが AUTH ReAuthenticate (0x19) を送信したときに呼び出す。
    ///
    /// MQTT v5.0 §4.12.1 [MQTT-4.12.1-1]:
    /// 再認証は初回認証の完了後にクライアントから開始し、Method は初回認証と同じ値を使う。
    /// 状態機械が Method を保持済みのため、引数は追加しない。
    ///
    /// `Authenticated` 状態でのみ成功し、他の状態では状態を変更せず
    /// [`AuthError::NotAuthenticated`] を返す。拒否の根拠は状態ごとに異なる:
    ///
    /// - `Idle`: MQTT v5.0 §4.12 [MQTT-4.12.0-7]。
    ///   CONNECT に Authentication Method を含めなかったクライアントは AUTH を送信して
    ///   はならない。
    /// - `InitialAuthenticating`: MQTT v5.0 §4.12.1 は「再認証は CONNACK 受信後に開始
    ///   できる」と定めており、CONNACK 未受信で再認証を開始することは仕様の前提を欠く。
    /// - `Reauthenticating`: 仕様は進行中の再認証中に新たな再認証を開始することを
    ///   明示的に禁じていない。ただし進行中の AUTH 交換を破棄して新しい交換を始めると、
    ///   サーバー側の状態と噛み合わなくなる可能性がある。本状態機械では防御的に拒否する。
    pub fn reauthenticate_sent(&mut self) -> Result<(), AuthError> {
        if self.state != AuthState::Authenticated {
            return Err(AuthError::NotAuthenticated);
        }
        self.state = AuthState::Reauthenticating;
        Ok(())
    }

    /// 認証状態をリセットする。
    ///
    /// 切断時や新しい接続確立時に呼び出す。
    pub fn reset(&mut self) {
        self.state = AuthState::Idle;
        self.auth_method = None;
    }
}

/// AUTH パケット受信後のアクション。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthAction {
    /// 認証が成功した。これ以上の AUTH 交換は不要。
    Authenticated,
    /// 認証交換を継続する。利用者は応答 AUTH を送信する。
    Continue,
    /// 状態と Reason Code の合法でない組み合わせで AUTH を受信した (プロトコル違反)。
    /// 状態は `Idle` にリセットされる。
    ///
    /// たとえば `Idle` や `Authenticated` での AUTH 受信、初回認証中の AUTH 0x00、
    /// または Reason Code 0x19 (Re-authenticate。MQTT v5.0 §3.15.2.1 Table 3-11 の
    /// Sent by はクライアント) の受信がここに含まれる。
    Failed,
    /// 受信 AUTH の Authentication Method が CONNECT と異なる、または
    /// 必須の Method が欠落していた。MQTT v5.0 §4.12 [MQTT-4.12.0-5]。
    /// プロトコル違反であり、`Failed` と区別する。状態は `Idle` にリセットされる。
    MethodMismatch,
}

/// 拡張認証系 Session 操作が現在のセッション状態で許容されない場合の拒否を表す。
///
/// 個々のバリアントの意味は各バリアントの doc を参照。
/// 意味論の広さについて、本型は下位層 [`AuthStateMachine`] の状態機械上の拒否
/// ([`NotAuthenticated`](Self::NotAuthenticated)) と、上位層
/// [`crate::state::session::Session`] からのみ emit される Session 操作の拒否
/// ([`V5OnlyOperation`](Self::V5OnlyOperation)) の両方を包含する。この意味論拡張は、
/// 拡張認証系 Session 操作全般に対する対称的な命名
/// ([`crate::state::session::ConnackError::V5OnlyParameters`] との対称性を含む) を
/// 優先した設計判断による。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthError {
    /// `Authenticated` 以外の状態で再認証開始が呼ばれた。
    ///
    /// 状態別の拒否根拠は [`AuthStateMachine::reauthenticate_sent`] の doc を参照。
    /// Idle は MQTT v5.0 §4.12 [MQTT-4.12.0-7]、`InitialAuthenticating` は
    /// MQTT v5.0 §4.12.1 (CONNACK 受信後にのみ開始可能)、`Reauthenticating` は
    /// 仕様が明示的に禁じていないため防御的に拒否する。
    NotAuthenticated,

    /// MQTT v3.1.1 セッションで v5 専用の拡張認証系 Session 操作
    /// ([`crate::state::session::Session::connect_sent_with_auth`]) が呼ばれた。
    ///
    /// MQTT v3.1.1 §2.2.1 Table 2.1 の制御パケット type 15 は Reserved / Forbidden
    /// (MQTT v5.0 で AUTH に割り当てられている位置に相当) であり、拡張認証は
    /// MQTT v5.0 のみで定義される (MQTT v5.0 §3.15)。関連: MQTT v5.0 §4.12
    /// [MQTT-4.12.0-7] は Method なし CONNECT のクライアントによるサーバーへの
    /// AUTH 送信を MUST NOT で禁止しており、本 variant は同種違反を Session
    /// 状態機械上で未然に防ぐ。設計判断の全体像は本型 [`AuthError`] の doc を参照。
    V5OnlyOperation,
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAuthenticated => {
                write!(
                    f,
                    "reauthenticate is allowed only after initial authentication completes"
                )
            }
            Self::V5OnlyOperation => {
                write!(
                    f,
                    "extended authentication is only supported for MQTT v5.0 sessions"
                )
            }
        }
    }
}

impl core::error::Error for AuthError {}

/// Authentication Method 検証エラー。
///
/// `AuthStateMachine::connack_received` が返す。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethodError {
    /// 必須の Authentication Method が欠落していた。
    ///
    /// MQTT v5.0 §4.12 [MQTT-4.12.0-5]:
    /// CONNECT に Authentication Method プロパティが含まれる場合、
    /// すべての AUTH パケットおよび成功 CONNACK は同じ Method を含まなければならない。
    Missing,
    /// Authentication Method が CONNECT と一致しなかった。
    Mismatch,
    /// Authentication Method を持たない (初回認証を開始していない、または既に完了・再認証中の)
    /// セッションで CONNACK に Authentication Method が含まれた。
    ///
    /// `Idle` については MQTT v5.0 §4.12 [MQTT-4.12.0-6]:
    /// サーバーは Method なし CONNECT への CONNACK に Method を含めてはならない。
    /// `Authenticated` / `Reauthenticating` については、接続状態機械の
    /// `Connecting` → `Connected` 一方向遷移により、この状態で CONNACK を受ける正規の
    /// 経路は存在しない (MQTT v5.0 §3.2 の CONNACK は CONNECT への一度きりの応答)。それでも
    /// 呼び出しに至った場合は状態機械の異常を意味するため、防御的に検出する。
    Unexpected,
}

impl fmt::Display for AuthMethodError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => {
                write!(f, "Authentication Method is required but missing")
            }
            Self::Mismatch => {
                write!(f, "Authentication Method does not match CONNECT")
            }
            Self::Unexpected => {
                write!(f, "Authentication Method is not expected in this state")
            }
        }
    }
}

impl core::error::Error for AuthMethodError {}

impl Default for AuthStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 便宜のためのヘルパー: 認証方式を指定して InitialAuthenticating に持っていく。
    fn start_initial(method: &str) -> AuthStateMachine {
        let mut auth = AuthStateMachine::new();
        auth.connect_with_auth(method.into());
        auth
    }

    // 便宜のためのヘルパー: Authenticated 状態に持っていく。
    // 初回認証の完了は成功 CONNACK なので connack_received() 経由で持っていく。
    fn complete_initial(method: &str) -> AuthStateMachine {
        let mut auth = start_initial(method);
        auth.connack_received(Some(method))
            .expect("初回認証の完了が成功すること");
        auth
    }

    #[test]
    fn auth_received_method_missing_on_continue_is_mismatch() {
        // 0x18 で Method 欠落は MethodMismatch。
        let mut auth = start_initial("X");
        let action = auth.auth_received(0x18, None);
        assert_eq!(action, AuthAction::MethodMismatch);
        assert_eq!(auth.state(), AuthState::Idle);
    }

    #[test]
    fn reauthenticate_from_non_authenticated_is_rejected() {
        // Authenticated 以外の状態からの再認証開始は状態を変えずに拒否する。
        // 拒否根拠は状態ごとに異なる。Idle は MQTT v5.0 §4.12 [MQTT-4.12.0-7]、
        // InitialAuthenticating は MQTT v5.0 §4.12.1 の CONNACK 受信後制約、
        // Reauthenticating は仕様に明示なしのため防御的。
        // Idle。
        let mut auth = AuthStateMachine::new();
        assert_eq!(auth.reauthenticate_sent(), Err(AuthError::NotAuthenticated));
        assert_eq!(auth.state(), AuthState::Idle);

        // InitialAuthenticating。
        let mut auth = start_initial("X");
        assert_eq!(auth.reauthenticate_sent(), Err(AuthError::NotAuthenticated));
        assert_eq!(auth.state(), AuthState::InitialAuthenticating);
        assert_eq!(auth.auth_method(), Some("X"));

        // Reauthenticating。
        let mut auth = complete_initial("X");
        auth.reauthenticate_sent()
            .expect("最初の再認証開始は成功すること");
        assert_eq!(auth.reauthenticate_sent(), Err(AuthError::NotAuthenticated));
        assert_eq!(auth.state(), AuthState::Reauthenticating);
    }

    #[test]
    fn reauth_success_with_none_method_is_accepted() {
        // Reauthenticating + 0x00 では、MQTT v5.0 §3.15.2.1 の
        // Remaining Length 0 省略形に相当する経路への防御的仕様として、
        // Method 欠落 (None) を受理する (現行 codec は decode 時に Method を要求するため
        // decode 経由の到達はない)。
        let mut auth = complete_initial("X");
        auth.reauthenticate_sent()
            .expect("再認証開始が成功すること");
        assert_eq!(auth.auth_received(0x00, None), AuthAction::Authenticated);
        assert_eq!(auth.state(), AuthState::Authenticated);
    }

    #[test]
    fn reauth_success_with_mismatched_method_is_mismatch() {
        let mut auth = complete_initial("X");
        auth.reauthenticate_sent()
            .expect("再認証開始が成功すること");
        assert_eq!(
            auth.auth_received(0x00, Some("Y")),
            AuthAction::MethodMismatch
        );
        assert_eq!(auth.state(), AuthState::Idle);
        assert_eq!(auth.auth_method(), None);
    }

    #[test]
    fn connack_missing_method_on_initial_auth_is_missing() {
        // 初回認証中の成功 CONNACK に Method が無いのは違反
        // (MQTT v5.0 §4.12 [MQTT-4.12.0-5])。
        let mut auth = start_initial("X");
        assert_eq!(auth.connack_received(None), Err(AuthMethodError::Missing));
        // 失敗時は状態を変えない。
        assert_eq!(auth.state(), AuthState::InitialAuthenticating);
        assert_eq!(auth.auth_method(), Some("X"));
    }

    #[test]
    fn connack_mismatched_method_on_initial_auth_is_mismatch() {
        let mut auth = start_initial("X");
        assert_eq!(
            auth.connack_received(Some("Y")),
            Err(AuthMethodError::Mismatch)
        );
        assert_eq!(auth.state(), AuthState::InitialAuthenticating);
        assert_eq!(auth.auth_method(), Some("X"));
    }

    #[test]
    fn connack_method_on_idle_is_unexpected() {
        // MQTT v5.0 §4.12 [MQTT-4.12.0-6]:
        // サーバーは Method なし CONNECT への CONNACK に Method を含めてはならない。
        let mut auth = AuthStateMachine::new();
        assert_eq!(
            auth.connack_received(Some("X")),
            Err(AuthMethodError::Unexpected)
        );
        assert_eq!(auth.state(), AuthState::Idle);
    }

    #[test]
    fn connack_none_on_idle_is_ok() {
        let mut auth = AuthStateMachine::new();
        assert!(auth.connack_received(None).is_ok());
        assert_eq!(auth.state(), AuthState::Idle);
    }

    #[test]
    fn connack_method_on_authenticated_is_unexpected() {
        // Authenticated 状態で CONNACK を受ける正規の経路は無いが、防御的検証で拒否する。
        let mut auth = complete_initial("X");
        assert_eq!(
            auth.connack_received(Some("X")),
            Err(AuthMethodError::Unexpected)
        );
        assert_eq!(auth.state(), AuthState::Authenticated);
    }

    #[test]
    fn connack_method_on_reauthenticating_is_unexpected() {
        let mut auth = complete_initial("X");
        auth.reauthenticate_sent()
            .expect("再認証開始が成功すること");
        assert_eq!(
            auth.connack_received(Some("X")),
            Err(AuthMethodError::Unexpected)
        );
        assert_eq!(auth.state(), AuthState::Reauthenticating);
    }

    #[test]
    fn transition_matrix_covers_all_combinations() {
        // 遷移規則の全組み合わせ (状態 × Reason Code × Method 一致・不一致・省略) の網羅。
        // (現状, 受信 Reason Code, 受信 Method, 期待するアクション, 期待する遷移後の状態)
        // 遷移後の状態: Idle は失敗、その他は成功後の状態。
        //
        // 初回認証中の完了 (AUTH 0x00) は失敗であることを含む重要なケースを網羅する。
        struct Case {
            desc: &'static str,
            start: fn() -> AuthStateMachine,
            reason_code: u8,
            method: Option<&'static str>,
            expected_action: AuthAction,
            expected_state: AuthState,
        }
        let cases: &[Case] = &[
            // InitialAuthenticating。
            Case {
                desc: "初回認証中 0x00 一致は Failed",
                start: || start_initial("X"),
                reason_code: 0x00,
                method: Some("X"),
                expected_action: AuthAction::Failed,
                expected_state: AuthState::Idle,
            },
            Case {
                desc: "初回認証中 0x18 一致は Continue",
                start: || start_initial("X"),
                reason_code: 0x18,
                method: Some("X"),
                expected_action: AuthAction::Continue,
                expected_state: AuthState::InitialAuthenticating,
            },
            Case {
                desc: "初回認証中 0x18 不一致は MethodMismatch",
                start: || start_initial("X"),
                reason_code: 0x18,
                method: Some("Y"),
                expected_action: AuthAction::MethodMismatch,
                expected_state: AuthState::Idle,
            },
            Case {
                desc: "初回認証中 0x18 None は MethodMismatch",
                start: || start_initial("X"),
                reason_code: 0x18,
                method: None,
                expected_action: AuthAction::MethodMismatch,
                expected_state: AuthState::Idle,
            },
            Case {
                desc: "初回認証中 0x19 は Failed",
                start: || start_initial("X"),
                reason_code: 0x19,
                method: Some("X"),
                expected_action: AuthAction::Failed,
                expected_state: AuthState::Idle,
            },
            // Reauthenticating。
            Case {
                desc: "再認証中 0x00 一致は Authenticated",
                start: || {
                    let mut a = complete_initial("X");
                    a.reauthenticate_sent().expect("再認証開始が成功すること");
                    a
                },
                reason_code: 0x00,
                method: Some("X"),
                expected_action: AuthAction::Authenticated,
                expected_state: AuthState::Authenticated,
            },
            Case {
                desc: "再認証中 0x00 None は Authenticated",
                start: || {
                    let mut a = complete_initial("X");
                    a.reauthenticate_sent().expect("再認証開始が成功すること");
                    a
                },
                reason_code: 0x00,
                method: None,
                expected_action: AuthAction::Authenticated,
                expected_state: AuthState::Authenticated,
            },
            Case {
                desc: "再認証中 0x00 不一致は MethodMismatch",
                start: || {
                    let mut a = complete_initial("X");
                    a.reauthenticate_sent().expect("再認証開始が成功すること");
                    a
                },
                reason_code: 0x00,
                method: Some("Y"),
                expected_action: AuthAction::MethodMismatch,
                expected_state: AuthState::Idle,
            },
            Case {
                desc: "再認証中 0x18 一致は Continue",
                start: || {
                    let mut a = complete_initial("X");
                    a.reauthenticate_sent().expect("再認証開始が成功すること");
                    a
                },
                reason_code: 0x18,
                method: Some("X"),
                expected_action: AuthAction::Continue,
                expected_state: AuthState::Reauthenticating,
            },
            Case {
                desc: "再認証中 0x18 None は MethodMismatch",
                start: || {
                    let mut a = complete_initial("X");
                    a.reauthenticate_sent().expect("再認証開始が成功すること");
                    a
                },
                reason_code: 0x18,
                method: None,
                expected_action: AuthAction::MethodMismatch,
                expected_state: AuthState::Idle,
            },
            Case {
                desc: "再認証中 0x19 は Failed",
                start: || {
                    let mut a = complete_initial("X");
                    a.reauthenticate_sent().expect("再認証開始が成功すること");
                    a
                },
                reason_code: 0x19,
                method: Some("X"),
                expected_action: AuthAction::Failed,
                expected_state: AuthState::Idle,
            },
            Case {
                // 段階 1 で合法な組み合わせだが、Method が異なるため段階 2 で MethodMismatch。
                desc: "再認証中 0x18 不一致は MethodMismatch",
                start: || {
                    let mut a = complete_initial("X");
                    a.reauthenticate_sent().expect("再認証開始が成功すること");
                    a
                },
                reason_code: 0x18,
                method: Some("Y"),
                expected_action: AuthAction::MethodMismatch,
                expected_state: AuthState::Idle,
            },
            // Authenticated。
            Case {
                desc: "認証済み 0x00 は Failed",
                start: || complete_initial("X"),
                reason_code: 0x00,
                method: Some("X"),
                expected_action: AuthAction::Failed,
                expected_state: AuthState::Idle,
            },
            Case {
                desc: "認証済み 0x18 は Failed",
                start: || complete_initial("X"),
                reason_code: 0x18,
                method: Some("X"),
                expected_action: AuthAction::Failed,
                expected_state: AuthState::Idle,
            },
            Case {
                desc: "認証済み 0x19 は Failed",
                start: || complete_initial("X"),
                reason_code: 0x19,
                method: Some("X"),
                expected_action: AuthAction::Failed,
                expected_state: AuthState::Idle,
            },
            // Idle。
            Case {
                desc: "Idle での 0x00 は Failed",
                start: AuthStateMachine::new,
                reason_code: 0x00,
                method: Some("X"),
                expected_action: AuthAction::Failed,
                expected_state: AuthState::Idle,
            },
            Case {
                desc: "Idle での 0x18 は Failed",
                start: AuthStateMachine::new,
                reason_code: 0x18,
                method: Some("X"),
                expected_action: AuthAction::Failed,
                expected_state: AuthState::Idle,
            },
            Case {
                desc: "Idle での 0x19 は Failed",
                start: AuthStateMachine::new,
                reason_code: 0x19,
                method: Some("X"),
                expected_action: AuthAction::Failed,
                expected_state: AuthState::Idle,
            },
        ];

        for case in cases {
            let mut auth = (case.start)();
            let action = auth.auth_received(case.reason_code, case.method);
            assert_eq!(action, case.expected_action, "{}", case.desc);
            assert_eq!(auth.state(), case.expected_state, "{}", case.desc);
        }
    }
}
