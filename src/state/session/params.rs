use alloc::fmt;
use alloc::string::String;

use crate::codec::qos::QoS;
use crate::v5::connack::ConnectReasonCode;
use crate::v311::connack::ConnectReturnCode;

/// CONNACK 適用時のエラー。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnackError {
    /// CONNACK の Reason Code が成功以外だった。
    ConnectionRefused,
    /// Clean Start=1 / Clean Session=1 なのに Session Present=1 の CONNACK を受信した。
    ///
    /// MQTT v5.0 §3.2.2.1.1 [MQTT-3.2.2-2] / [MQTT-3.2.2-4]:
    /// Clean Start=1 のときサーバーは Session Present を 0 にしなければならず、
    /// Clean Start=1 のクライアントが Session Present=1 を受信したら
    /// Network Connection を閉じなければならない。
    /// MQTT v3.1.1 §3.2.2.2 [MQTT-3.2.2-1]:
    /// Clean Session=1 のときサーバーは Session Present を 0 にしなければならない。
    /// v3.1.1 ではクライアント側の切断義務は定められていないが、
    /// サーバーが違反した CONNACK を受信した場合、ライブラリはプロトコル違反として拒否する。
    SessionPresentProtocolError,
    /// CONNACK の Receive Maximum が 0 だった。
    ///
    /// MQTT v5.0 §3.2.2.3.3:
    /// Receive Maximum に 0 を指定することは Protocol Error である。
    InvalidReceiveMaximum,
    /// CONNACK の Maximum Packet Size が 0 だった。
    ///
    /// MQTT v5.0 §3.2.2.3.6:
    /// Maximum Packet Size に 0 を指定することは Protocol Error である。
    InvalidMaximumPacketSize,
    /// CONNACK の Maximum QoS が 2 (`QoS::ExactlyOnce`) だった。
    ///
    /// MQTT v5.0 §3.2.2.3.4:
    /// Maximum QoS プロパティの値は 0 または 1 のみであり、それ以外は Protocol Error である。
    InvalidMaximumQoS,
    /// 空の Client Identifier で接続したのに、成功 CONNACK の
    /// Assigned Client Identifier が欠落しているか空文字列だった。
    ///
    /// MQTT v5.0 §3.2.2.3.7 [MQTT-3.2.2-16]:
    /// クライアントが長さゼロの Client Identifier で接続した場合、サーバーは
    /// Assigned Client Identifier を含む CONNACK で応答しなければならず、
    /// その値はサーバー内で他のどのセッションにも現在使われていない新しい
    /// Client Identifier でなければならない。空文字列はこの要求を満たさない。
    AssignedClientIdentifierMissing,
    /// 初回認証中の成功 CONNACK に Authentication Method が含まれていなかった。
    ///
    /// MQTT v5.0 §4.12 [MQTT-4.12.0-5]:
    /// CONNECT に Authentication Method プロパティが含まれる場合、
    /// すべての AUTH パケットおよび成功 CONNACK は CONNECT と同じ値の
    /// Authentication Method プロパティを含まなければならない。
    AuthenticationMethodMissing,
    /// 初回認証中の成功 CONNACK の Authentication Method が CONNECT と一致しなかった。
    ///
    /// MQTT v5.0 §4.12 [MQTT-4.12.0-5]。
    AuthenticationMethodMismatch,
    /// 初回認証を開始していない、または認証済み・再認証中のセッションに対して
    /// CONNACK に Authentication Method が含まれた。
    ///
    /// `Idle` については MQTT v5.0 §4.12 [MQTT-4.12.0-6]:
    /// サーバーは Method なし CONNECT への CONNACK に Method を含めてはならない。
    /// `Authenticated` / `Reauthenticating` については、接続状態機械の
    /// `Connecting` → `Connected` 一方向遷移により、この状態で CONNACK を受ける正規の
    /// 経路は存在しない (MQTT v5.0 §3.2 の CONNACK は CONNECT への一度きりの応答)。
    /// それでも到達した場合は状態機械の異常を意味するため、防御的に検出する。
    UnexpectedAuthenticationMethod,
    /// CONNACK を適用できるのは `Connecting` 状態のみである。
    ///
    /// MQTT v3.1.1 では CONNACK はサーバーからクライアントへ送信される最初のパケットである
    /// (MQTT v3.1.1 §3.2 [MQTT-3.2.0-1])。
    /// MQTT v5.0 では AUTH 以外のパケットを送信する前に CONNACK を送信しなければならない
    /// (MQTT v5.0 §3.2 [MQTT-3.2.0-1])。
    /// したがって CONNECT 送信前や接続確立後に適用されるべきではない。
    InvalidConnectionState,
    /// CONNACK の応答理由のバリアントとセッションのプロトコルバージョンが一致しない。
    ///
    /// MQTT v5.0 の CONNACK は Connect Reason Code を (MQTT v5.0 §3.2.2.2)、
    /// MQTT v3.1.1 の CONNACK は Connect Return Code を (MQTT v3.1.1 §3.2.2.3)
    /// 可変ヘッダに含める。セッションのプロトコルバージョンと一致しない
    /// Reason Code / Return Code を受信したら誤用として拒否する。
    VersionMismatch,
    /// MQTT v3.1.1 セッションに MQTT v5.0 専用の CONNACK プロパティが含まれている。
    ///
    /// MQTT v3.1.1 の CONNACK 可変ヘッダは Session Present (MQTT v3.1.1 §3.2.2.2) と
    /// Connect Return Code (MQTT v3.1.1 §3.2.2.3) のみであり、Properties フィールドは
    /// 存在しない。
    V5OnlyParameters,
}

impl fmt::Display for ConnackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConnectionRefused => {
                write!(f, "CONNACK reason code indicates connection refused")
            }
            Self::SessionPresentProtocolError => {
                write!(f, "session_present=1 received with clean_start=true")
            }
            Self::InvalidReceiveMaximum => {
                write!(f, "CONNACK Receive Maximum must not be 0")
            }
            Self::InvalidMaximumPacketSize => {
                write!(f, "CONNACK Maximum Packet Size must not be 0")
            }
            Self::InvalidMaximumQoS => {
                write!(f, "CONNACK Maximum QoS must not be 2")
            }
            Self::AssignedClientIdentifierMissing => {
                write!(
                    f,
                    "CONNACK for a zero length Client Identifier must contain a non-empty Assigned Client Identifier"
                )
            }
            Self::AuthenticationMethodMissing => {
                write!(
                    f,
                    "successful CONNACK during initial authentication must contain Authentication Method"
                )
            }
            Self::AuthenticationMethodMismatch => {
                write!(f, "CONNACK Authentication Method does not match CONNECT")
            }
            Self::UnexpectedAuthenticationMethod => {
                write!(
                    f,
                    "CONNACK contains Authentication Method but session did not start extended authentication"
                )
            }
            Self::InvalidConnectionState => {
                write!(f, "CONNACK can only be applied in Connecting state")
            }
            Self::VersionMismatch => {
                write!(
                    f,
                    "CONNACK reason code variant does not match session protocol version"
                )
            }
            Self::V5OnlyParameters => {
                write!(
                    f,
                    "CONNACK for MQTT v3.1.1 session contains v5-only parameters"
                )
            }
        }
    }
}

impl core::error::Error for ConnackError {}

/// CONNACK の応答理由。
///
/// MQTT v5.0 の Connect Reason Code と MQTT v3.1.1 の Connect Return Code を
/// 型安全に区別する。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnackReason {
    /// MQTT v5.0 の理由コード。
    V5(ConnectReasonCode),
    /// MQTT v3.1.1 のリターンコード。
    V311(ConnectReturnCode),
}

impl ConnackReason {
    /// この応答が成功（接続を受理された）かどうかを返す。
    pub(super) fn is_success(self) -> bool {
        match self {
            Self::V5(code) => code == ConnectReasonCode::Success,
            Self::V311(code) => code == ConnectReturnCode::Accepted,
        }
    }
}

/// CONNACK パケットから取得するパラメータ。
///
/// 各フィールドの `None` は CONNACK に該当プロパティが含まれていなかった
/// ことを表し、仕様の既定値が使用される。
#[derive(Debug)]
pub struct ConnackParams {
    /// 前回のセッションが継続されているかどうか。
    pub session_present: bool,
    /// CONNACK の応答理由。
    ///
    /// MQTT v5.0 では `ConnackReason::V5(ConnectReasonCode::Success)`、
    /// MQTT v3.1.1 では `ConnackReason::V311(ConnectReturnCode::Accepted)` が
    /// 接続受理を表す。
    pub reason_code: ConnackReason,
    /// セッション有効期限間隔（秒）。
    pub session_expiry_interval: Option<u32>,
    /// 受信最大値。
    pub receive_maximum: Option<u16>,
    /// トピックエイリアス最大値。
    pub topic_alias_maximum: Option<u16>,
    /// サーバーが指定する Keep Alive 間隔（秒）。
    pub server_keep_alive: Option<u16>,
    /// サーバーが受け入れる最大パケットサイズ（MQTT v5.0 §3.2.2.3.6）。
    pub maximum_packet_size: Option<u32>,
    /// サーバーがサポートする最大 QoS（MQTT v5.0 §3.2.2.3.4）。
    pub maximum_qos: Option<QoS>,
    /// Retain メッセージのサポート有無（MQTT v5.0 §3.2.2.3.5）。
    pub retain_available: Option<bool>,
    /// ワイルドカードサブスクリプションのサポート有無（MQTT v5.0 §3.2.2.3.11）。
    pub wildcard_subscription_available: Option<bool>,
    /// サブスクリプション識別子のサポート有無（MQTT v5.0 §3.2.2.3.12）。
    pub subscription_identifiers_available: Option<bool>,
    /// 共有サブスクリプションのサポート有無（MQTT v5.0 §3.2.2.3.13）。
    pub shared_subscription_available: Option<bool>,
    /// サーバーが割り当てたクライアント識別子（MQTT v5.0 §3.2.2.3.7）。
    ///
    /// 空の Client Identifier で接続した場合にサーバーが返す。
    /// 空文字列は空の Client Identifier で接続したセッションで
    /// [`ConnackError::AssignedClientIdentifierMissing`] として拒否される
    /// (MQTT v5.0 §3.2.2.3.7 [MQTT-3.2.2-16])。
    pub assigned_client_identifier: Option<String>,
    /// CONNACK に含まれる Authentication Method (MQTT v5.0 §3.2.2.3.17)。
    ///
    /// MQTT v5.0 §4.12 [MQTT-4.12.0-5]:
    /// CONNECT に Authentication Method プロパティが含まれる場合、成功 CONNACK は
    /// CONNECT と同じ値の Authentication Method プロパティを含まなければならない。
    /// [`Session::apply_connack`](crate::state::session::Session::apply_connack) は本フィールドと初回認証中の `AuthStateMachine`
    /// の保持する Method を比較し、不一致・欠落・予期しない受信を
    /// [`ConnackError::AuthenticationMethodMissing`] /
    /// [`ConnackError::AuthenticationMethodMismatch`] /
    /// [`ConnackError::UnexpectedAuthenticationMethod`] として返す。
    pub authentication_method: Option<String>,
}

impl ConnackParams {
    /// MQTT v5.0 専用の CONNACK プロパティがいずれか含まれているかどうかを返す。
    ///
    /// MQTT v3.1.1 の CONNACK 可変ヘッダは Session Present (MQTT v3.1.1 §3.2.2.2) と
    /// Connect Return Code (MQTT v3.1.1 §3.2.2.3) のみであり、以下のプロパティは存在しない。
    /// v3.1.1 セッションに対して本メソッドが true を返す `ConnackParams` を
    /// `apply_connack()` に渡し、かつ接続状態・バージョン一致の検証を通過すると
    /// [`ConnackError::V5OnlyParameters`] となる。
    pub(super) fn has_v5_only_fields(&self) -> bool {
        self.session_expiry_interval.is_some()
            || self.receive_maximum.is_some()
            || self.topic_alias_maximum.is_some()
            || self.server_keep_alive.is_some()
            || self.maximum_packet_size.is_some()
            || self.maximum_qos.is_some()
            || self.retain_available.is_some()
            || self.wildcard_subscription_available.is_some()
            || self.subscription_identifiers_available.is_some()
            || self.shared_subscription_available.is_some()
            || self.assigned_client_identifier.is_some()
            || self.authentication_method.is_some()
    }
}
