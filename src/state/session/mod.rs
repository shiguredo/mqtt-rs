//! MQTT セッション状態管理。
//!
//! MQTT v5.0 §3.1.2.4 (Clean Start)、MQTT v5.0 §3.1.2.11.2 (Session Expiry Interval) を参照。
//! MQTT v3.1.1 §3.1.2.4 (Clean Session) を参照。
//!
//! セッション状態は MQTT 接続のライフサイクルを通じて管理される。
//! この状態機械は Sans-I/O であり、接続状態、セッション永続化設定、
//! および各サブ状態機械のライフサイクルを管理する。

use alloc::fmt;
use alloc::string::String;
use alloc::vec::Vec;

use crate::codec::MqttVersion;
use crate::codec::limits::Limits;
use crate::codec::qos::QoS;
use crate::state::auth::{AuthAction, AuthError, AuthMethodError, AuthStateMachine};
use crate::state::flow_control::{FlowControl, FlowControlError};
use crate::state::keep_alive::{KeepAlive, Timestamp};
use crate::state::packet_id::PacketIdManager;
use crate::state::qos_flow::{Action, FlowError, QosFlowManager};
use crate::state::subscribe::{SubscriptionEntry, SubscriptionManager};
use crate::state::topic_alias::TopicAliasManager;

mod capabilities;
mod params;

pub use capabilities::{ServerCapabilities, ServerCapabilityError};
pub use params::{ConnackError, ConnackParams, ConnackReason};

#[cfg(test)]
mod tests;

/// セッション作成時のエラー。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionError {
    /// 空の client_id に対して clean_session / clean_start が false である。
    InvalidClientId,
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidClientId => {
                write!(
                    f,
                    "empty client_id requires clean_session/clean_start to be true"
                )
            }
        }
    }
}

impl core::error::Error for SessionError {}

/// [`Session::handle_suback`] のエラー。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleSubackError {
    /// SUBACK に対応する pending SUBSCRIBE が存在しない、
    /// または理由コード数が pending の要求数と一致しない。
    ///
    /// pending 不在はサーバー実装のバグ、または client 側の状態破損を示唆する。
    /// 理由コード数不一致もサーバー仕様違反である。SUBACK の理由コード順序は
    /// SUBSCRIBE のトピックフィルタ順序と一致 MUST
    /// (MQTT v5.0 §3.9.3 [MQTT-3.9.3-1] / MQTT v3.1.1 §3.9.3 [MQTT-3.9.3-1])
    /// で、各理由コードは対応するトピックフィルタを指すため、長さの一致は
    /// この順序規範の暗黙の帰結として要求される。
    /// いずれの場合もパケット識別子は解放済みで、再利用可能な状態になっている。
    PendingNotFound,
}

impl fmt::Display for HandleSubackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PendingNotFound => {
                write!(
                    f,
                    "no pending SUBSCRIBE for the packet identifier, or SUBACK reason code count mismatch"
                )
            }
        }
    }
}

impl core::error::Error for HandleSubackError {}

/// [`Session::handle_unsuback`] のエラー。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleUnsubackError {
    /// UNSUBACK に対応する pending UNSUBSCRIBE が存在しない、
    /// または理由コード数が pending の要求数と一致しない。
    ///
    /// pending 不在はサーバー実装のバグ、または client 側の状態破損を示唆する。
    /// 理由コード数不一致もサーバー仕様違反である。UNSUBACK の理由コード順序は
    /// UNSUBSCRIBE のトピックフィルタ順序と一致 MUST
    /// (MQTT v5.0 §3.11.3 [MQTT-3.11.3-1]) で、各理由コードは対応するトピック
    /// フィルタを指すため、長さの一致はこの順序規範の暗黙の帰結として要求される。
    /// MQTT v3.1.1 の UNSUBACK にはペイロードが無いため、v3.1.1 経路では理由コード数
    /// 不一致は原理的に発生せず、本エラーは pending 不在のみを意味する。
    /// いずれの場合もパケット識別子は解放済みで、再利用可能な状態になっている。
    PendingNotFound,
}

impl fmt::Display for HandleUnsubackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PendingNotFound => {
                write!(
                    f,
                    "no pending UNSUBSCRIBE for the packet identifier, or UNSUBACK reason code count mismatch"
                )
            }
        }
    }
}

impl core::error::Error for HandleUnsubackError {}

/// MQTT 接続の状態。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// 接続が確立されていない。
    Disconnected,
    /// CONNECT を送信し、CONNACK を待っている。
    Connecting,
    /// 接続が確立され、通常の通信が可能。
    Connected,
    /// DISCONNECT を送信し、接続を終了しようとしている。
    Disconnecting,
}

/// MQTT セッション。
///
/// 1 つの MQTT 接続の全状態を管理する。
/// この構造体は各サブ状態機械を内包し、接続のライフサイクルに
/// 応じた適切な初期化とリセットを提供する。
#[derive(Debug, Clone)]
pub struct Session {
    /// MQTT プロトコルバージョン。
    protocol_version: MqttVersion,
    /// 接続状態。
    connection_state: ConnectionState,
    /// クライアント識別子。
    client_id: String,
    /// クリーンスタート（v5.0）／クリーンセッション（v3.1.1）。
    clean_start: bool,
    /// セッション有効期限間隔（v5.0 のみ、v3.1.1 では 0）。
    session_expiry_interval: u32,
    /// パケット識別子プール。
    packet_id_manager: PacketIdManager,
    /// QoS フロー状態機械。
    qos_flow: QosFlowManager,
    /// フロー制御。
    flow_control: FlowControl,
    /// サブスクリプション管理。
    subscription_manager: SubscriptionManager,
    /// トピックエイリアス管理（v5.0 のみ有効）。
    topic_alias_manager: TopicAliasManager,
    /// Keep Alive 補助。
    keep_alive: KeepAlive,
    /// 認証状態機械（v5.0 のみ有効）。
    auth_state: AuthStateMachine,
    /// CONNACK で通知されたサーバー能力値（v5.0 のみ有効）。
    server_capabilities: ServerCapabilities,
    /// パケットサイズ制限。
    limits: Limits,
}

impl Session {
    /// 新しいセッションを指定されたプロトコルバージョンで作成する。
    ///
    /// `client_id` が空文字列の場合、Clean Start は true でなければならない（v3.1.1）。
    /// MQTT v3.1.1 §3.1.3.1 [MQTT-3.1.3-7] を参照。
    /// v5.0 では空 ClientID はサーバーが一意の ClientID を割り当てる。
    /// MQTT v5.0 §3.1.3.1 [MQTT-3.1.3-6] を参照。
    pub fn new(
        protocol_version: MqttVersion,
        client_id: String,
        clean_start: bool,
    ) -> Result<Self, SessionError> {
        // MQTT v3.1.1 §3.1.3.1 [MQTT-3.1.3-7]:
        // client_id が空の場合、Clean Session は true でなければならない。
        if protocol_version == MqttVersion::V311 && client_id.is_empty() && !clean_start {
            return Err(SessionError::InvalidClientId);
        }
        let keep_alive = KeepAlive::new(0);
        Ok(Self {
            protocol_version,
            connection_state: ConnectionState::Disconnected,
            client_id,
            clean_start,
            session_expiry_interval: 0,
            packet_id_manager: PacketIdManager::new(),
            qos_flow: QosFlowManager::new(),
            flow_control: FlowControl::new(),
            subscription_manager: SubscriptionManager::new(),
            topic_alias_manager: TopicAliasManager::new(),
            keep_alive,
            auth_state: AuthStateMachine::new(),
            server_capabilities: ServerCapabilities::new(),
            limits: Limits::new(),
        })
    }

    /// MQTT v5.0 用のセッションを新規作成する。
    pub fn new_v5(client_id: String, clean_start: bool) -> Result<Self, SessionError> {
        Self::new(MqttVersion::V5, client_id, clean_start)
    }

    /// MQTT v3.1.1 用のセッションを新規作成する。
    pub fn new_v311(client_id: String, clean_session: bool) -> Result<Self, SessionError> {
        Self::new(MqttVersion::V311, client_id, clean_session)
    }

    // ======================================================================
    // 基本情報
    // ======================================================================

    /// プロトコルバージョンを返す。
    pub fn protocol_version(&self) -> MqttVersion {
        self.protocol_version
    }

    /// MQTT v5.0 セッションかどうかを返す。
    pub fn is_v5(&self) -> bool {
        self.protocol_version == MqttVersion::V5
    }

    /// MQTT v3.1.1 セッションかどうかを返す。
    pub fn is_v311(&self) -> bool {
        self.protocol_version == MqttVersion::V311
    }

    /// クライアント識別子を返す。
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// 接続状態を返す。
    pub fn connection_state(&self) -> ConnectionState {
        self.connection_state
    }

    /// クリーンスタート／クリーンセッションが指定されているかどうかを返す。
    pub fn clean_start(&self) -> bool {
        self.clean_start
    }

    /// セッション有効期限間隔（秒）を返す。
    pub fn session_expiry_interval(&self) -> u32 {
        self.session_expiry_interval
    }

    /// セッション有効期限間隔を設定する。
    ///
    /// CONNACK の Session Expiry Interval プロパティ（0x11）を
    /// 受信したとき、または CONNECT 送信時に設定する。
    pub fn set_session_expiry_interval(&mut self, interval: u32) {
        self.session_expiry_interval = interval;
    }

    /// パケットサイズ制限を返す。
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// CONNECT 送信前にパラメータを一括設定する。
    ///
    /// このメソッドは CONNECT パケットを構築する前に呼び出すことで、
    /// Keep Alive 間隔、セッション有効期限間隔、トピックエイリアス最大値、
    /// 自身の Receive Maximum、自身の Maximum Packet Size
    /// （自身が受け入れ可能な値）をまとめて設定する。
    ///
    /// `receive_maximum` は CONNECT の Receive Maximum プロパティ（0x21）に、
    /// `maximum_packet_size` は Maximum Packet Size プロパティ（0x27）に対応する。
    /// `maximum_packet_size` が 0 の場合は制限なしを表し、
    /// デフォルトのパケットサイズ制限が維持される。
    ///
    /// `receive_maximum` が 0 の場合は `FlowControlError::InvalidReceiveMaximum` を返す。
    /// MQTT v5.0 §3.1.2.11.3:
    /// Receive Maximum に 0 を指定することは Protocol Error である。
    pub fn configure_for_connect(
        &mut self,
        keep_alive_secs: u16,
        session_expiry_interval: u32,
        topic_alias_maximum: u16,
        receive_maximum: u16,
        maximum_packet_size: u32,
    ) -> Result<(), FlowControlError> {
        self.keep_alive.set_keep_alive(keep_alive_secs);
        self.session_expiry_interval = session_expiry_interval;
        self.topic_alias_manager
            .set_own_maximum(topic_alias_maximum);
        self.flow_control.set_own_receive_maximum(receive_maximum)?;
        if maximum_packet_size > 0 {
            self.limits = Limits::new().with_max_packet_size(maximum_packet_size as usize);
        }
        Ok(())
    }

    /// CONNACK 受信時にパラメータを一括設定し、接続確立を記録する。
    ///
    /// 内部は「検証フェーズ → 適用フェーズ」の 2 段構成で、
    /// エラー時には状態を一切変更しない (auth_state / flow_control /
    /// connection_state を含む全てのサブ状態機械が事前値のまま保たれる) ことを
    /// 保証する。検証フェーズで拒否要因を順に確認し、全て通過した後に
    /// [`connected`](Self::connected) と各パラメータを反映する適用フェーズが実行される。
    ///
    /// 未指定のパラメータは `None` を設定することで既定値が維持される。
    ///
    /// 戻り値:
    /// - `Ok(())`: CONNACK を受理し、接続が確立した。
    /// - `Err(ConnackError::InvalidConnectionState)`: 接続状態が
    ///   `ConnectionState::Connecting` ではなかった。MQTT v3.1.1 では CONNACK は
    ///   サーバーからクライアントへ送信される最初のパケットであり
    ///   (MQTT v3.1.1 §3.2 [MQTT-3.2.0-1])、MQTT v5.0 では AUTH 以外のパケットを
    ///   送信する前に CONNACK を送信しなければならない
    ///   (MQTT v5.0 §3.2 [MQTT-3.2.0-1])。
    /// - `Err(ConnackError::VersionMismatch)`: `ConnackReason` のバリアントと
    ///   セッションの MQTT プロトコルバージョンが一致しなかった。MQTT v5.0 の
    ///   CONNACK は Connect Reason Code (MQTT v5.0 §3.2.2.2)、MQTT v3.1.1 の
    ///   CONNACK は Connect Return Code (MQTT v3.1.1 §3.2.2.3) を可変ヘッダに含む。
    /// - `Err(ConnackError::V5OnlyParameters)`: MQTT v3.1.1 セッションに対して
    ///   MQTT v5.0 専用の CONNACK プロパティが含まれていた。MQTT v3.1.1 の
    ///   CONNACK 可変ヘッダは Session Present (MQTT v3.1.1 §3.2.2.2) と
    ///   Connect Return Code (MQTT v3.1.1 §3.2.2.3) のみであり、Properties フィールドは
    ///   存在しない。
    /// - `Err(ConnackError::ConnectionRefused)`: CONNACK の Reason Code / Return Code が
    ///   成功以外だった (MQTT v5.0 §3.2.2.2、MQTT v3.1.1 §3.2.2.3)。
    /// - `Err(ConnackError::SessionPresentProtocolError)`: Clean Start / Clean Session =1
    ///   なのに `session_present=true` だった。MQTT v5.0 §3.2.2.1.1
    ///   [MQTT-3.2.2-2] / [MQTT-3.2.2-4] に従い、利用者は Network Connection を
    ///   閉じる必要がある。MQTT v3.1.1 §3.2.2.2 [MQTT-3.2.2-1] でも
    ///   Clean Session=1 時の Session Present=0 がサーバーに要求されるため、
    ///   ライブラリは同様にプロトコル違反として拒否する。
    /// - `Err(ConnackError::AssignedClientIdentifierMissing)`: 空の Client Identifier で
    ///   接続したのに成功 CONNACK の Assigned Client Identifier が欠落しているか
    ///   空文字列だった (MQTT v5.0 §3.2.2.3.7 [MQTT-3.2.2-16])。
    /// - `Err(ConnackError::InvalidReceiveMaximum)`: `receive_maximum` が `Some(0)`
    ///   だった (MQTT v5.0 §3.2.2.3.3)。
    /// - `Err(ConnackError::InvalidMaximumPacketSize)`: `maximum_packet_size` が `Some(0)`
    ///   だった (MQTT v5.0 §3.2.2.3.6)。
    /// - `Err(ConnackError::InvalidMaximumQoS)`: `maximum_qos` が `Some(QoS::ExactlyOnce)`
    ///   だった (MQTT v5.0 §3.2.2.3.4)。
    /// - `Err(ConnackError::AuthenticationMethodMissing)`: 初回認証中に成功 CONNACK の
    ///   Authentication Method プロパティが欠落 (MQTT v5.0 §4.12 [MQTT-4.12.0-5])。
    /// - `Err(ConnackError::AuthenticationMethodMismatch)`: 初回認証中に成功 CONNACK の
    ///   Authentication Method が CONNECT と不一致 (MQTT v5.0 §4.12 [MQTT-4.12.0-5])。
    /// - `Err(ConnackError::UnexpectedAuthenticationMethod)`: 拡張認証を開始していない
    ///   セッションで CONNACK に Authentication Method が含まれた
    ///   (MQTT v5.0 §4.12 [MQTT-4.12.0-6] ほか)。
    ///
    /// Clean Start=0 で `session_present=true` の場合は、クライアント側の
    /// QoS フローや購読が空でも受理する。クライアント Session State
    /// (MQTT v5.0 §4.1) は未完了の QoS 1/2 メッセージのみであり、正常切断後は
    /// 空になり得る。空の状態でサーバー側セッションを再開するのは
    /// 永続セッションの正常な利用である。
    pub fn apply_connack(&mut self, params: ConnackParams) -> Result<(), ConnackError> {
        // ====================================================================
        // 検証フェーズ: 全て非破壊で行う。ここでエラーを返した場合、
        // auth_state / flow_control を含む全てのサブ状態機械は変更されない。
        // ====================================================================

        // CONNACK は CONNECT への応答としてのみ適用される。
        // MQTT v3.1.1 では CONNACK がサーバーからクライアントへ送信される最初のパケットであり
        // (MQTT v3.1.1 §3.2 [MQTT-3.2.0-1])、MQTT v5.0 では AUTH 以外のパケットを
        // 送信する前に CONNACK を送信しなければならない (MQTT v5.0 §3.2 [MQTT-3.2.0-1])。
        if self.connection_state() != ConnectionState::Connecting {
            return Err(ConnackError::InvalidConnectionState);
        }

        // MQTT v5.0 の CONNACK は Connect Reason Code (MQTT v5.0 §3.2.2.2)、
        // MQTT v3.1.1 の CONNACK は Connect Return Code (MQTT v3.1.1 §3.2.2.3) を
        // 可変ヘッダに含む。セッションのプロトコルバージョンと一致しない
        // Reason Code / Return Code を受信したら誤用として拒否する。
        let version_matches = match params.reason_code {
            ConnackReason::V5(_) => self.is_v5(),
            ConnackReason::V311(_) => self.is_v311(),
        };
        if !version_matches {
            return Err(ConnackError::VersionMismatch);
        }

        // MQTT v3.1.1 の CONNACK 可変ヘッダは Session Present (MQTT v3.1.1 §3.2.2.2) と
        // Connect Return Code (MQTT v3.1.1 §3.2.2.3) のみであり、v5 専用プロパティは存在しない。
        // 構造違反はセマンティクス（Reason Code の成功 / 失敗）より先に検出する。
        if self.is_v311() && params.has_v5_only_fields() {
            return Err(ConnackError::V5OnlyParameters);
        }

        // CONNACK の Reason Code / Return Code が成功以外の場合は接続を拒否する。
        // MQTT v5.0 では MQTT v5.0 §3.2.2.2、MQTT v3.1.1 では MQTT v3.1.1 §3.2.2.3 を参照。
        if !params.reason_code.is_success() {
            return Err(ConnackError::ConnectionRefused);
        }

        // MQTT v3.1.1 §3.2.2.2 [MQTT-3.2.2-1]、
        // MQTT v5.0 §3.2.2.1.1 [MQTT-3.2.2-2] / [MQTT-3.2.2-4]:
        // Clean Start / Clean Session =1 のときサーバーは Session Present を 0 に
        // しなければならない。
        // MQTT v5.0 では Clean Start=1 のクライアントが Session Present=1 を受信したら
        // Network Connection を閉じなければならない。
        // v3.1.1 でもサーバーは Clean Session=1 時に Session Present=0 にしなければならないため、
        // いずれのバージョンでも session_present=true はプロトコル違反として拒否する。
        //
        // Clean Start=0 かつ session_present=true は、ローカル状態が空でも受理する。
        // 正常切断後はクライアント Session State (未完了 QoS メッセージ) が空でも
        // サーバー側セッションの再開は正当な利用である。
        if params.session_present && self.clean_start {
            return Err(ConnackError::SessionPresentProtocolError);
        }

        // MQTT v5.0 §3.2.2.3.7 [MQTT-3.2.2-16]:
        // クライアントが長さゼロの Client Identifier で接続した場合、サーバーは
        // Assigned Client Identifier を含む CONNACK で応答しなければならない。
        // 空文字列の Assigned Client Identifier は「サーバー内で他のどのセッションにも
        // 現在使われていない新しい Client Identifier」として機能せず、client_id が空のまま
        // 残るため、欠落と同様に拒否する。
        // Assigned Client Identifier は MQTT v5.0 のみに存在するため、
        // MQTT v3.1.1 のセッションでは検証しない。この分岐は `is_v5()` ガードで守る
        // （前段の V5OnlyParameters は全 v5 専用フィールドが None のとき発火しない
        // ため、そのケースはこのガードが最終的な防御となる）。
        if self.is_v5()
            && self.client_id.is_empty()
            && params
                .assigned_client_identifier
                .as_deref()
                .is_none_or(str::is_empty)
        {
            return Err(ConnackError::AssignedClientIdentifierMissing);
        }

        // MQTT v5.0 §3.2.2.3.3:
        // Receive Maximum に 0 を指定することは Protocol Error である。
        // 検証を非破壊化するため、ここでは値を適用せずに 0 の検出だけ行う。
        // 実際の適用は成功が確定した後 (`connected()` 以降) にまとめて行う。
        if matches!(params.receive_maximum, Some(0)) {
            return Err(ConnackError::InvalidReceiveMaximum);
        }

        // MQTT v5.0 §3.2.2.3.6:
        // Maximum Packet Size に 0 を指定することは Protocol Error である。
        // 検証を非破壊化するため、ここでは値を適用せずに 0 の検出だけ行う。
        // 実際の適用は成功が確定した後 (`connected()` 以降) にまとめて行う。
        if matches!(params.maximum_packet_size, Some(0)) {
            return Err(ConnackError::InvalidMaximumPacketSize);
        }

        // MQTT v5.0 §3.2.2.3.4:
        // Maximum QoS プロパティの値は 0 または 1 のみであり、それ以外は Protocol Error である。
        // QoS 2 をサポートするサーバーはプロパティ自体を省略する。
        // 検証を非破壊化するため、ここでは値を適用せずに ExactlyOnce の検出だけ行う。
        // 実際の適用は成功が確定した後 (`connected()` 以降) にまとめて行う。
        if matches!(params.maximum_qos, Some(QoS::ExactlyOnce)) {
            return Err(ConnackError::InvalidMaximumQoS);
        }

        // 検証フェーズ最後の fallible 操作。
        // `connack_received()` は検証と `auth_state` 遷移を原子的に行うが、
        // それ以前に他のミューテーションを起こしていないため、
        // ここで失敗しても状態は全て未変更のまま保たれる。
        // MQTT v5.0 §4.12 [MQTT-4.12.0-5] / [MQTT-4.12.0-6]。
        // Authentication Method は MQTT v5.0 のみのプロパティであるため、
        // MQTT v3.1.1 のセッションでは検証しない。
        if self.is_v5() {
            self.auth_state
                .connack_received(params.authentication_method.as_deref())
                .map_err(|err| match err {
                    AuthMethodError::Missing => ConnackError::AuthenticationMethodMissing,
                    AuthMethodError::Mismatch => ConnackError::AuthenticationMethodMismatch,
                    AuthMethodError::Unexpected => ConnackError::UnexpectedAuthenticationMethod,
                })?;
        }

        // ====================================================================
        // 適用フェーズ: ここから先はエラーを返さず、全てのパラメータを適用する。
        // ====================================================================

        self.connected(params.session_present);

        // 検証済みなので Receive Maximum の適用は失敗し得ない。
        // Some(0) は上の検証で弾いており、Some(1..=u16::MAX) は set_receive_maximum で受理される。
        if let Some(max) = params.receive_maximum {
            self.flow_control
                .set_receive_maximum(max)
                .expect("Receive Maximum was validated to be non-zero");
        }

        if let Some(interval) = params.session_expiry_interval {
            self.set_session_expiry_interval(interval);
        }
        // MQTT v5.0 §3.2.2.3.8: Topic Alias Maximum が未指定の場合は 0 とみなす。
        // 0 はエイリアスを使用しないことを意味する。
        if let Some(max) = params.topic_alias_maximum {
            self.topic_alias_manager.set_peer_maximum(max);
        }
        // MQTT v5.0 §3.2.2.3.14: Server Keep Alive が未指定の場合は
        // CONNECT で指定した Keep Alive を使用する。
        if let Some(secs) = params.server_keep_alive {
            self.keep_alive.set_keep_alive(secs);
        }
        // サーバー能力値（MQTT v5.0 §3.2.2.3）。
        // 未指定のものは仕様の既定値（`ServerCapabilities::new()`）のままとする。
        if let Some(size) = params.maximum_packet_size {
            self.server_capabilities.maximum_packet_size = Some(size);
        }
        if let Some(qos) = params.maximum_qos {
            self.server_capabilities.maximum_qos = qos;
        }
        if let Some(available) = params.retain_available {
            self.server_capabilities.retain_available = available;
        }
        if let Some(available) = params.wildcard_subscription_available {
            self.server_capabilities.wildcard_subscription_available = available;
        }
        if let Some(available) = params.subscription_identifiers_available {
            self.server_capabilities.subscription_identifiers_available = available;
        }
        if let Some(available) = params.shared_subscription_available {
            self.server_capabilities.shared_subscription_available = available;
        }
        // MQTT v5.0 §3.2.2.3.7:
        // 空の Client Identifier で接続した場合、サーバーは Assigned Client Identifier を返す。
        // その値を今後の通信で使用するためにセッションの client_id を更新する。
        if let Some(assigned) = params.assigned_client_identifier
            && self.client_id.is_empty()
        {
            self.client_id = assigned;
        }
        Ok(())
    }

    // ======================================================================
    // 接続状態遷移
    // ======================================================================

    /// CONNECT パケットを送信したことを記録する。
    ///
    /// 新しい Network Connection の開始として、接続スコープの状態を
    /// 再初期化する (セッション状態は維持される)。
    pub fn connect_sent(&mut self) {
        self.connection_state = ConnectionState::Connecting;
        self.reset_connection_state();
    }

    /// Authentication Method を指定して CONNECT パケットを送信したことを記録する。
    ///
    /// MQTT v5.0 §3.1.2.11.9 [MQTT-3.1.2-30] に対応する初回認証の開始 API。
    /// v5 セッションでの通常動作は、内部で [`connect_sent`](Self::connect_sent) を
    /// 呼び出したあと、認証状態を `InitialAuthenticating` に遷移させる。順序は必ず
    /// この向きでなければならない。
    ///
    /// 順序の罠: 先に認証状態を `InitialAuthenticating` にしてから `connect_sent()`
    /// を呼び出すと、`connect_sent()` の内部で走る `reset_connection_state()` により
    /// `auth_state.reset()` が実行され、初回認証中の状態が黙って消える。
    /// `AuthStateMachine::connect_with_auth` は `pub(crate)` で外部から直接呼べない
    /// ため、この順序の罠を起こしうるのは同一クレート内のコードだけだが、本メソッドは
    /// 正しい順序を API として保証することで、誤った順序で認証状態が失われる事故を
    /// 根本から防ぐ。
    ///
    /// 呼び出し順序は
    /// [`configure_for_connect`](Self::configure_for_connect) →
    /// `connect_sent_with_auth` の順とする (`connect_sent()` と同じ)。
    ///
    /// # v3.1.1 セッションでの拒否
    ///
    /// 拡張認証パケット AUTH 自体は MQTT v5.0 §3.15 で定義される v5 専用パケットであり、
    /// MQTT v3.1.1 §2.2.1 Table 2.1 で制御パケット type 15 (MQTT v5.0 で AUTH に
    /// 割り当てられている位置) は Reserved / Forbidden。加えて MQTT v3.1.1 §3.1.2 の
    /// CONNECT Variable header は Protocol Name / Protocol Level / Connect Flags /
    /// Keep Alive の 4 フィールドのみで Properties 領域を持たず、Authentication Method
    /// を伝送する場所自体が存在しない。MQTT v5.0 §4.12 [MQTT-4.12.0-7] も Method なし
    /// CONNECT のクライアントによるサーバーへの AUTH 送信を MUST NOT で禁止する。
    /// したがって v3.1.1 セッションでの本メソッドの呼び出しは仕様違反相当であり、
    /// [`AuthError::V5OnlyOperation`] を返して拒否する。
    ///
    /// 拒否時は副作用ゼロ: `is_v5()` の判定を [`connect_sent`](Self::connect_sent) の
    /// 前に置いているため、`Err` 時は `connect_sent()` を呼ばず、`reset_connection_state()`
    /// も `auth_state.connect_with_auth()` も実行されない。結果として `connection_state`
    /// / `auth_state` / `flow_control` / `topic_alias_manager` / `server_capabilities` /
    /// `keep_alive` のいずれも変更されない。
    ///
    /// なお、拒否時に直前に呼び出した [`configure_for_connect`](Self::configure_for_connect)
    /// の副作用 (`keep_alive` / `session_expiry_interval` / `topic_alias_manager` /
    /// `flow_control` / `limits` への書き込み) は本メソッドでは巻き戻さない。
    pub fn connect_sent_with_auth(
        &mut self,
        authentication_method: String,
    ) -> Result<(), AuthError> {
        // 検証フェーズ: 非破壊。is_v5() の判定を connect_sent() より前に置くことで、
        // 拒否時に connect_sent() 経由の reset_connection_state() の副作用と
        // auth_state.connect_with_auth() の遷移が同時にゼロになる。
        if !self.is_v5() {
            return Err(AuthError::V5OnlyOperation);
        }
        // 適用フェーズ: is_v5() が確定した後にのみ副作用を起こす。
        self.connect_sent();
        self.auth_state.connect_with_auth(authentication_method);
        Ok(())
    }

    /// CONNACK パケットを受信して接続が確立したことを記録する。
    ///
    /// `session_present` が true の場合、セッションが継続されていることを示す。
    /// `session_expiry_interval` などの CONNACK パラメータは `apply_connack()`
    /// で適用される。
    pub fn connected(&mut self, session_present: bool) {
        self.connection_state = ConnectionState::Connected;
        if self.clean_start || !session_present {
            // クリーンスタート、またはサーバーに既存セッションが存在しなかった場合、
            // 状態をリセットする。
            // MQTT v5.0 §3.2.2.1.1 [MQTT-3.2.2-5]:
            // Session Present が false の場合、クライアントは以前のセッションが
            // 破棄されたものとして扱う。
            self.reset_session_state();
        }
    }

    /// DISCONNECT パケットを送信したことを記録する。
    ///
    /// 接続状態を `Disconnecting` に遷移させる。Reason Code には依存しない。
    pub fn disconnect_sent(&mut self) {
        self.connection_state = ConnectionState::Disconnecting;
    }

    /// DISCONNECT パケットをサーバーから受信したことを記録する。
    ///
    /// サーバーからクライアントへの DISCONNECT は MQTT v5.0 §3.14 にのみ存在する
    /// （MQTT v3.1.1 §3.14 の DISCONNECT はクライアントからサーバーへの一方向のみ）。
    pub fn disconnect_received(&mut self) {
        self.connection_state = ConnectionState::Disconnected;
    }

    /// 接続が切断されたことを記録する。
    pub fn disconnected(&mut self) {
        self.connection_state = ConnectionState::Disconnected;
    }

    /// 接続が確立されているかどうかを返す。
    pub fn is_connected(&self) -> bool {
        self.connection_state == ConnectionState::Connected
    }

    /// 接続が確立されているか、確立中かどうかを返す。
    pub fn is_active(&self) -> bool {
        matches!(
            self.connection_state,
            ConnectionState::Connected | ConnectionState::Connecting
        )
    }

    // ======================================================================
    // パケット受信ハンドラ
    // ======================================================================

    /// PINGRESP パケットを受信したことを記録する。
    ///
    /// Keep Alive の PINGRESP 応答期限の待ち状態を解除する。
    /// 詳細な挙動（冪等性など）は
    /// [`KeepAlive::pingresp_received`](crate::state::keep_alive::KeepAlive::pingresp_received)
    /// を参照。
    ///
    /// - MQTT v5.0 §3.1.2.10:
    ///   「If a Client does not receive a PINGRESP packet within a reasonable amount
    ///   of time after it has sent a PINGREQ, it SHOULD close the Network Connection
    ///   to the Server」（規範番号なし）。
    /// - MQTT v3.1.1 §3.1.2.10:
    ///   「If a Client does not receive a PINGRESP Packet within a reasonable amount
    ///   of time after it has sent a PINGREQ, it SHOULD close the Network Connection
    ///   to the Server」（規範番号なし）。
    pub fn pingresp_received(&mut self) {
        self.keep_alive.pingresp_received();
    }

    /// PINGREQ パケットを送信したことを記録する。
    ///
    /// Keep Alive の PINGRESP 応答期限の待ち状態を立てる。
    /// `now` は現在時刻（利用者が選択した単位、通常はミリ秒）。
    /// 利用者は PINGREQ を送信するたびに本 API を呼ぶこと。
    /// 呼ばない場合、[`Session::keep_alive`](Self::keep_alive) の
    /// [`has_timed_out`](crate::state::keep_alive::KeepAlive::has_timed_out) は
    /// PINGRESP 待ちを検出できず、Keep Alive のタイムアウト機能そのものが働かない。
    ///
    /// PINGREQ 送信自体が送信タイマー（`last_activity`）の更新も兼ねるため、
    /// 本 API の後に [`Session::activity`](Self::activity) を追加で呼ぶ必要はない。
    ///
    /// - MQTT v5.0 §3.1.2.10 [MQTT-3.1.2-20]:
    ///   Keep Alive が非ゼロで、他の MQTT Control Packet を送信していない場合、
    ///   クライアントは PINGREQ を送信しなければならない。
    /// - MQTT v3.1.1 §3.1.2.10 [MQTT-3.1.2-23]:
    ///   他の Control Packet を送信していない場合、クライアントは PINGREQ を
    ///   送信しなければならない。
    pub fn pingreq_sent(&mut self, now: Timestamp) {
        self.keep_alive.pingreq_sent(now);
    }

    /// PINGREQ 以外の MQTT Control Packet を送信したことを記録する。
    ///
    /// Keep Alive の送信タイマー（`last_activity`）を更新する。
    /// `now` は現在時刻（利用者が選択した単位、通常はミリ秒）。
    /// 内部は [`KeepAlive::activity`](crate::state::keep_alive::KeepAlive::activity) への委譲のみ。
    ///
    /// # 送信間隔の定義と呼び出しタイミング
    ///
    /// Keep Alive は Client が 1 つの MQTT Control Packet の送信完了から次の送信開始までに
    /// 許される最大時間間隔である。Client はその間隔が Keep Alive を超えない責任を負う。
    ///
    /// - MQTT v5.0 §3.1.2.10（規範番号なし）: 最大時間間隔の定義と、間隔を超えない責任文。
    /// - MQTT v3.1.1 §3.1.2.10: 最大時間間隔の定義は規範番号なし。
    ///   Client が間隔を超えない責任文は [MQTT-3.1.2-23] に含まれる。
    ///
    /// 利用者は当該 Control Packet の wire 送信が成功完了したあとに本 API を呼ぶ。
    /// Session の `*_sent` が wire 前か wire 後かは API ごとに異なるが、
    /// 本 API のタイミングは常に wire 成功完了に紐づける。
    ///
    /// # PINGREQ 送信義務との関係
    ///
    /// 次の規範は [`should_send_pingreq`](crate::state::keep_alive::KeepAlive::should_send_pingreq)
    /// 側の義務であり、本 API 追加の直接根拠ではない。
    ///
    /// - MQTT v5.0 §3.1.2.10 [MQTT-3.1.2-20]:
    ///   Keep Alive が非ゼロで他の MQTT Control Packet を送信していない場合、
    ///   Client は PINGREQ を送信しなければならない。
    /// - MQTT v3.1.1 §3.1.2.10 [MQTT-3.1.2-23]:
    ///   他の Control Packet を送信していない場合、Client は PINGREQ を
    ///   送信しなければならない（同規範文は責任文も含む）。
    ///
    /// `activity(now)` は「他の Control Packet を送信した」事実を `last_activity` に記録し、
    /// 上記の “in the absence of sending any other … Control Packets” 条件を満たさなくする
    /// ためのヘルパである。規範自体が本 API の追加を要求しているわけではない。
    ///
    /// # [`Session::pingreq_sent`](Self::pingreq_sent) との使い分け
    ///
    /// PINGREQ 送信では本 API ではなく [`Session::pingreq_sent`](Self::pingreq_sent) を呼ぶ。
    /// `pingreq_sent(now)` は内部で送信タイマー（`last_activity`）も更新するため、
    /// 追加で `activity(now)` を呼ぶ必要はない。同じ `now` で追加呼出ししても
    /// `last_activity` は変わらない。
    ///
    /// # Keep Alive = 0 / PINGRESP 待ち中 / 再接続
    ///
    /// - Keep Alive = 0 でも呼べる（`should_send_pingreq` は常に false）。
    /// - PINGRESP 待ち中でも待ちは解除せず `last_activity` のみ更新する。
    /// - [`connect_sent`](Self::connect_sent) は `last_activity` を触らないため、
    ///   CONNECT 送信完了後にも本 API を呼ぶ。
    pub fn activity(&mut self, now: Timestamp) {
        self.keep_alive.activity(now);
    }

    /// AUTH パケットを受信したことを記録する。
    ///
    /// MQTT v5.0 拡張認証の状態を更新する。`authentication_method` は
    /// 受信 AUTH の Authentication Method プロパティの値。
    /// 判定規則は [`AuthStateMachine::auth_received`] を参照。
    ///
    /// 戻り値の `AuthAction` に従って利用者は応答パケットを送信する。
    /// MQTT v5.0 §4.12 を参照。
    pub fn auth_received(
        &mut self,
        reason_code: u8,
        authentication_method: Option<&str>,
    ) -> AuthAction {
        self.auth_state
            .auth_received(reason_code, authentication_method)
    }

    /// クライアントが AUTH ReAuthenticate (0x19) を送信したことを記録する。
    ///
    /// [`AuthStateMachine::reauthenticate_sent`] への委譲。`Authenticated` 状態でのみ
    /// 成功し、それ以外では状態を変えずに [`AuthError::NotAuthenticated`] を返す。
    /// 拒否の根拠は状態ごとに異なる:
    ///
    /// - `Idle`: MQTT v5.0 §4.12 [MQTT-4.12.0-7] (Method なし CONNECT の送信側違反)。
    /// - `InitialAuthenticating`: MQTT v5.0 §4.12.1 は再認証を CONNACK 受信後に許容する
    ///   と定めており、初回認証の CONNACK 未受信での再認証開始は仕様の前提を欠く。
    /// - `Reauthenticating`: 仕様は進行中の再認証中に新たな再認証を開始することを
    ///   明示的に禁じていない。本状態機械では防御的に拒否する。
    pub fn reauthenticate_sent(&mut self) -> Result<(), AuthError> {
        self.auth_state.reauthenticate_sent()
    }

    /// 認証が進行中 (初回認証中または再認証中) かどうかを返す。
    ///
    /// これらの照会 API は AUTH パケットの往復状態を追跡するもので、Network
    /// Connection の生死とは独立している。[`disconnected`](Self::disconnected) は
    /// `auth_state` を変更しないため、切断後もこの値は残り、次の
    /// [`connect_sent`](Self::connect_sent) または
    /// [`connect_sent_with_auth`](Self::connect_sent_with_auth) が
    /// `reset_connection_state()` 経由で `auth_state.reset()` を呼ぶまでクリアされない。
    pub fn is_authenticating(&self) -> bool {
        self.auth_state.is_authenticating()
    }

    /// 初回認証中かどうかを返す。
    ///
    /// [`is_authenticating`](Self::is_authenticating) と同じく、切断後もそのまま残る。
    pub fn is_initial_authenticating(&self) -> bool {
        self.auth_state.is_initial_authenticating()
    }

    /// 再認証中かどうかを返す。
    ///
    /// [`is_authenticating`](Self::is_authenticating) と同じく、切断後もそのまま残る。
    pub fn is_reauthenticating(&self) -> bool {
        self.auth_state.is_reauthenticating()
    }

    /// 認証が完了しているかどうかを返す。
    ///
    /// [`is_authenticating`](Self::is_authenticating) と同じく、切断後もそのまま残る。
    /// また、`Reauthenticating` 状態 (再認証中) では `false` を返すことに注意。
    /// 再認証中も通常通信は継続できる (MQTT v5.0 §4.12.1) が、状態機械としては
    /// 「初回認証を完了して再認証を開始する前」だけを `Authenticated` とみなす。
    pub fn is_authenticated(&self) -> bool {
        self.auth_state.is_authenticated()
    }

    // ======================================================================
    // 受信パケットの統合ハンドラ
    // ======================================================================

    /// PUBACK パケットを受信したときに呼び出す。
    ///
    /// QoS 1 送信フローを完了させ、未確認カウントを減らし、
    /// パケット識別子を解放する。
    ///
    /// 戻り値:
    /// - `Ok(Some(Action::Complete))`: QoS 1 フローが完了した。
    /// - `Ok(None)`: 該当するフローがない。
    /// - `Err(FlowError::StateMismatch)`: 状態が不一致。
    pub fn handle_puback(&mut self, packet_id: u16) -> Result<Option<Action>, FlowError> {
        let action = self.qos_flow.puback_received(packet_id)?;
        if let Some(action) = action {
            self.flow_control.publish_acked();
            self.packet_id_manager.release(packet_id);
            Ok(Some(action))
        } else {
            Ok(None)
        }
    }

    /// PUBREC パケットを受信したときに呼び出す。
    ///
    /// QoS 2 送信フローを進行させる。
    /// Reason Code が 0x80 以上の場合はフローを中断し、未確認カウントを減らし、
    /// パケット識別子を解放する。
    ///
    /// 戻り値:
    /// - `Ok(Some(Action::SendPubrel))`: PUBREL を送信する必要がある。
    /// - `Ok(Some(Action::Aborted))`: フローが中断された。
    /// - `Ok(None)`: 該当するフローがない。
    /// - `Err(FlowError::StateMismatch)`: 状態が不一致。
    pub fn handle_pubrec(
        &mut self,
        packet_id: u16,
        reason_code: u8,
    ) -> Result<Option<Action>, FlowError> {
        let action = self.qos_flow.pubrec_received(packet_id, reason_code)?;
        if let Some(action) = action {
            if let Action::Aborted { .. } = action {
                // MQTT v5.0 §4.4 [MQTT-4.4.0-2]:
                // Reason Code 0x80 以上の PUBREC を受信した PUBLISH は確認済みとして扱う。
                // 送信クォータを回復し、パケット識別子を解放する。
                self.flow_control.publish_acked();
                self.packet_id_manager.release(packet_id);
            }
            Ok(Some(action))
        } else {
            Ok(None)
        }
    }

    /// PUBREL パケットを受信したときに呼び出す。
    ///
    /// `Action::SendPubcomp` に従って PUBCOMP を送信したら、
    /// `pubcomp_sent()` を呼び出して受信方向の未確認カウントを解放すること。
    ///
    /// 該当する受信フローが存在しない PUBREL（送信済み PUBCOMP の消失により
    /// 相手が PUBREL を再送したケース）には `Action::ResendPubcomp` が返る。
    /// MQTT v5.0 §4.3.3 [MQTT-4.3.3-11] / MQTT v3.1.1 §4.3.3 に従い
    /// PUBCOMP を再送すること。このとき受信枠は既に解放済みのため、
    /// `pubcomp_sent()` を呼び出してはならない。
    ///
    /// 戻り値:
    /// - `Ok(Some(Action::SendPubcomp))`: PUBCOMP を送信する必要がある。
    /// - `Ok(Some(Action::ResendPubcomp))`: 該当フローはないが PUBCOMP を再送する必要がある。
    /// - `Err(FlowError::StateMismatch)`: 状態が不一致。
    pub fn handle_pubrel(&mut self, packet_id: u16) -> Result<Option<Action>, FlowError> {
        self.qos_flow.pubrel_received(packet_id)
    }

    /// PUBCOMP パケットを受信したときに呼び出す。
    ///
    /// QoS 2 送信フローを完了させ、未確認カウントを減らし、
    /// パケット識別子を解放する。
    ///
    /// 戻り値:
    /// - `Ok(Some(Action::Complete))`: QoS 2 フローが完了した。
    /// - `Ok(None)`: 該当するフローがない。
    /// - `Err(FlowError::StateMismatch)`: 状態が不一致。
    pub fn handle_pubcomp(&mut self, packet_id: u16) -> Result<Option<Action>, FlowError> {
        let action = self.qos_flow.pubcomp_received(packet_id)?;
        if let Some(action) = action {
            self.flow_control.publish_acked();
            self.packet_id_manager.release(packet_id);
            Ok(Some(action))
        } else {
            Ok(None)
        }
    }

    /// QoS 1 の PUBLISH を受信したときに呼び出す。
    ///
    /// 受信方向の未確認カウントを増やし、自身の Receive Maximum を超過していないか
    /// 検証する。超過している場合は `Err(FlowControlError::ReceiveMaximumExceeded)` を
    /// 返す。利用者は MQTT v5.0 §4.9 に従い、
    /// Reason Code 0x93 (Receive Maximum exceeded) の DISCONNECT パケットを
    /// 送信して接続を切断しなければならない。
    ///
    /// 戻り値:
    /// - `Ok(Action::SendPuback)`: PUBACK を送信する必要がある。
    /// - `Err(FlowControlError::ReceiveMaximumExceeded)`: Receive Maximum を超過した。
    pub fn handle_publish_qos1(&mut self, packet_id: u16) -> Result<Action, FlowControlError> {
        // MQTT v5.0 §3.3.4 [MQTT-3.3.4-9] / MQTT v5.0 §4.3.2:
        // PUBACK 送信前に同じ Packet Identifier の PUBLISH を再受信した場合、
        // それは同一メッセージの再送であり、新たな未確認 PUBLISH ではない。
        // 受信方向の未確認カウントを増やすとフロー完了時の puback_sent() が
        // 1 回しか減算しないため受信枠が恒久的に減ってしまう。
        // したがって重複時はカウントを増やさず PUBACK の再送だけを指示する。
        if !self.qos_flow.has_incoming_flow(packet_id) {
            self.flow_control.publish_received()?;
        }
        Ok(self.qos_flow.publish_received_qos1(packet_id))
    }

    /// QoS 2 の PUBLISH を受信したときに呼び出す。
    ///
    /// 受信方向の未確認カウントを増やし、自身の Receive Maximum を超過していないか
    /// 検証する。超過している場合は `Err(FlowControlError::ReceiveMaximumExceeded)` を
    /// 返す。利用者は MQTT v5.0 §4.9 に従い、
    /// Reason Code 0x93 (Receive Maximum exceeded) の DISCONNECT パケットを
    /// 送信して接続を切断しなければならない。
    ///
    /// 戻り値:
    /// - `Ok(Action::SendPubrec)`: PUBREC を送信する必要がある。
    ///   `is_duplicate` が true の場合は同一メッセージの再送であり、
    ///   利用者はアプリケーションへの配送を行ってはならない
    ///   （MQTT v5.0 §4.3.3 [MQTT-4.3.3-10]）。
    /// - `Err(FlowControlError::ReceiveMaximumExceeded)`: Receive Maximum を超過した。
    pub fn handle_publish_qos2(&mut self, packet_id: u16) -> Result<Action, FlowControlError> {
        // MQTT v5.0 §4.3.3 [MQTT-4.3.3-10] / MQTT v3.1.1 §4.3.3:
        // PUBREL 受信前に同じ Packet Identifier の PUBLISH を再受信した場合、
        // それは同一メッセージの再送であり、新たな未確認 PUBLISH ではない。
        // 受信方向の未確認カウントを増やすとフロー完了時の pubcomp_sent() が
        // 1 回しか減算しないため受信枠が恒久的に減ってしまう。
        // したがって重複時はカウントを増やさず PUBREC の再送だけを指示する。
        if !self.qos_flow.has_incoming_flow(packet_id) {
            self.flow_control.publish_received()?;
        }
        Ok(self.qos_flow.publish_received_qos2(packet_id))
    }

    /// QoS 1 の PUBLISH に対する PUBACK を送信したときに呼び出す。
    ///
    /// 受信方向の未確認カウントを 1 減少させ、
    /// [`QosFlowManager`] 上の当該 Packet Identifier の受信フローを完了する。
    ///
    /// `packet_id` には送信した PUBACK と同じ Packet Identifier を渡すこと。
    pub fn puback_sent(&mut self, packet_id: u16) {
        self.qos_flow.puback_sent(packet_id);
        self.flow_control.publish_ack_sent();
    }

    /// QoS 2 の PUBLISH に対する PUBREC を送信したときに呼び出す。
    ///
    /// MQTT v5.0 §3.3.4 [MQTT-3.3.4-9]: サーバーは、PUBACK・PUBCOMP・
    /// Reason Code 0x80 以上の PUBREC のいずれかを受信するまで、その PUBLISH を
    /// 未確認として数える。つまり QoS 2 の受信フローは成功 PUBREC の送信では
    /// 完了せず、PUBCOMP の送信まで継続する。
    ///
    /// そのため、成功（Reason Code 0x80 未満）の PUBREC では受信方向の
    /// 未確認カウントを維持し、`pubcomp_sent()` の呼び出しで解放する。
    /// エラー（Reason Code 0x80 以上）の PUBREC では QoS 2 フローが
    /// その時点で終了するため、受信方向の未確認カウントを 1 減少させる。
    pub fn pubrec_sent(&mut self, reason_code: u8) {
        if reason_code >= 0x80 {
            self.flow_control.publish_ack_sent();
        }
    }

    /// QoS 2 の PUBLISH に対する PUBCOMP を送信したときに呼び出す。
    ///
    /// QoS 2 の受信フローが完了するため、受信方向の未確認カウントを
    /// 1 減少させる。MQTT v5.0 §3.3.4 [MQTT-3.3.4-9] を参照。
    pub fn pubcomp_sent(&mut self) {
        self.flow_control.publish_ack_sent();
    }

    // ======================================================================
    // 再送管理
    // ======================================================================

    /// 再送が必要なパケットの一覧を返す。
    ///
    /// 戻り値は `(パケット識別子, 再送アクション)` のリスト。
    /// QoS 1 の未確認 PUBLISH と QoS 2 の未確認 PUBREL が対象となる。
    /// 利用者はこのリストに基づいて該当パケットを再送する。
    ///
    /// 一覧は再送順の規範（MQTT v5.0 §4.6 [MQTT-4.6.0-1] / [MQTT-4.6.0-4]）を
    /// 満たす順で並ぶため、利用者はこのリストの順序どおりに再送すればよい。
    /// 順序の契約の詳細は [`QosFlowManager::pending_packet_ids`] を参照。
    ///
    /// PUBLISH の再送を実際に送信するときは `resend_publish` を呼び出し、
    /// send quota を消費すること。これを怠るとサーバーの Receive Maximum を超過する
    /// プロトコル違反につながる。PUBREL の再送は send quota の対象外であり、
    /// `resend_pubrel` を呼び出して記録する。
    pub fn pending_retransmissions(&self) -> Vec<(u16, Action)> {
        self.qos_flow
            .pending_packet_ids()
            .iter()
            .filter_map(|&id| {
                self.qos_flow
                    .needs_retransmission(id)
                    .map(|action| (id, action))
            })
            .collect()
    }

    /// PUBLISH の再送を送信したことを記録する。
    ///
    /// 指定されたパケット識別子が QoS 1/2 の未確認 PUBLISH で、現在の接続で
    /// まだ send quota を消費していない場合、send quota を 1 消費して
    /// `Action::ResendPublish` を返す。再送対象でない場合は `None` を返す。
    /// send quota が不足している場合は
    /// `Err(FlowControlError::ReceiveMaximumExceeded)` を返す。
    ///
    /// MQTT v5.0 §4.9 [MQTT-4.9.0-2]:
    /// PUBLISH (QoS > 0) の送信のたびに send quota を 1 減らすため、
    /// 再送も send quota を消費する。send quota は Network Connection を
    /// またいで保存されないため、再接続後の再送は新しい接続の quota を消費する。
    /// `pending_retransmissions` で取得した各パケット識別子に対して
    /// 本メソッドを呼び出すことで、Receive Maximum 超過を防ぐ。
    ///
    /// # 同一接続内での呼び出し
    ///
    /// 現在の接続で send quota を消費済みのフロー（初回送信済み、または
    /// 再接続後に再送記録済み）に対する呼び出しはバージョンごとに扱いが異なる。
    ///
    /// - MQTT v5.0: `Ok(None)` を返す。MQTT v5.0 §4.4 [MQTT-4.4.0-1] により
    ///   再送が許されるのは再接続直後のみで、それ以外の時点で再送してはならない。
    /// - MQTT v3.1.1: send quota を消費せずに `Ok(Some(Action::ResendPublish))` を
    ///   返す。MQTT v3.1.1 §4.4 は同一接続内の再送を禁止しておらず
    ///   （同節の Non normative comment はデータ消失が起こる旧来のネットワークでの
    ///   再送に言及している）、未確認のままの同一メッセージの再送で send quota を
    ///   二重に消費すると quota が恒久的にリークするため消費しない。
    pub fn resend_publish(&mut self, packet_id: u16) -> Result<Option<Action>, FlowControlError> {
        let action = self.qos_flow.needs_retransmission(packet_id);
        match action {
            Some(Action::ResendPublish { .. }) => {
                if self.qos_flow.is_quota_charged(packet_id) {
                    if self.is_v5() {
                        // MQTT v5.0 §4.4 [MQTT-4.4.0-1]:
                        // 再接続直後以外の再送は禁止されているため、再送対象として扱わない。
                        return Ok(None);
                    }
                    // MQTT v3.1.1: 同一接続内の再送は禁止されていないが、
                    // send quota は消費済みのため二重に消費しない。
                    return Ok(action);
                }
                self.flow_control.publish_sent()?;
                self.qos_flow.mark_quota_charged(packet_id);
                Ok(action)
            }
            _ => Ok(None),
        }
    }

    /// PUBREL の再送を送信したことを記録する。
    ///
    /// 指定されたパケット識別子が QoS 2 の PUBCOMP 待ちの場合、
    /// `Action::ResendPubrel` を返す。再送対象でない場合は `None` を返す。
    ///
    /// MQTT v5.0 §4.9 [MQTT-4.9.0-2]: send quota の対象は QoS > 0 の
    /// PUBLISH パケットのみであり、PUBREL は send quota を消費しない。
    /// また MQTT v5.0 §4.9 [MQTT-4.9.0-3]: send quota が 0 でも他のすべての MQTT Control Packet
    /// の処理と応答を継続しなければならないため、quota が枯渇した状態でも
    /// PUBREL は再送できる。
    ///
    /// なお QoS 2 の send quota は元の PUBLISH 送信時に消費済みであり、
    /// 回復するのは PUBCOMP 受信時（またはエラー PUBREC 受信時）である。
    pub fn resend_pubrel(&mut self, packet_id: u16) -> Option<Action> {
        let action = self.qos_flow.needs_retransmission(packet_id);
        match action {
            Some(Action::ResendPubrel { .. }) => action,
            _ => None,
        }
    }

    // ======================================================================
    // 送信パケットの統合ハンドラ
    // ======================================================================

    /// SUBSCRIBE / UNSUBSCRIBE / PUBLISH (QoS > 0) 送信用のパケット識別子を割り当てる。
    ///
    /// 現在使用されていないパケット識別子を 1 つ返す。プールが枯渇している場合は
    /// `None` を返す。`None` を受け取った場合、利用者は SUBSCRIBE / UNSUBSCRIBE /
    /// PUBLISH (QoS > 0) を送信してはならない。送信するとサーバー側で使用中の
    /// パケット識別子と衝突し、以下の規範に違反する。
    ///
    /// MQTT v5.0 §2.2.1 [MQTT-2.2.1-3]:
    /// Each time a Client sends a new SUBSCRIBE, UNSUBSCRIBE, or PUBLISH
    /// (where QoS > 0) MQTT Control Packet it MUST assign it a non-zero
    /// Packet Identifier that is currently unused.
    /// MQTT v3.1.1 §2.3.1 [MQTT-2.3.1-1]:
    /// SUBSCRIBE / UNSUBSCRIBE / PUBLISH (QoS > 0) は非ゼロの 16-bit
    /// Packet Identifier を含む MUST。
    pub fn allocate_packet_id(&mut self) -> Option<u16> {
        self.packet_id_manager.allocate()
    }

    /// PUBLISH (QoS > 0) を送信したことを記録する。
    ///
    /// 送信クォータを 1 消費し、成功時のみ `qos_flow` に送信フローを登録する。
    /// [`allocate_packet_id`](Self::allocate_packet_id) で得たパケット識別子と一緒に
    /// 呼ぶこと。
    ///
    /// # 呼び出し順序
    ///
    /// `Session::allocate_packet_id() → Session::publish_sent(qos, id)? → wire 送信`
    /// の順に呼ぶ。wire 送信が成功したら、PUBACK / PUBREC / PUBCOMP 受信で
    /// [`handle_puback`](Self::handle_puback) / [`handle_pubrec`](Self::handle_pubrec)
    /// / [`handle_pubcomp`](Self::handle_pubcomp) を呼ぶ。wire 送信が失敗した場合は
    /// [`abort_publish`](Self::abort_publish) を呼ぶ。
    ///
    /// # 戻り値
    ///
    /// - `Ok(())`: 送信クォータを 1 消費し、`qos_flow` に送信フローを登録した。
    /// - `Err(FlowControlError::ReceiveMaximumExceeded)`: 送信クォータが不足している。
    ///   wire を送信してはならず、[`release_packet_id`](Self::release_packet_id) で
    ///   パケット識別子を解放すること。**このケースでは
    ///   [`abort_publish`](Self::abort_publish) を呼んではならない**
    ///   (内部の `is_active` ガードで実害は防がれるが、明示的な規約違反)。
    /// - `Err(FlowControlError::InvalidQoS)`: `qos == QoS::AtMostOnce` で呼び出された。
    ///   QoS 0 には確認応答フローが存在せず、send quota の対象でもない
    ///   (MQTT v5.0 §4.9 [MQTT-4.9.0-2] の対象は QoS > 0 の PUBLISH のみ)。
    ///   状態は一切変更されない。
    /// - `Err(FlowControlError::InvalidPacketId)`: `packet_id == 0` で呼び出された。
    ///   MQTT v5.0 §2.2.1 [MQTT-2.2.1-3] / MQTT v3.1.1 §2.3.1 [MQTT-2.3.1-1] により
    ///   PUBLISH (QoS > 0) のパケット識別子は非ゼロでなければならない。
    ///   状態は一切変更されない。wire を送信してはならない。
    ///
    /// # 規範
    ///
    /// MQTT v5.0 §3.3.4 [MQTT-3.3.4-7]:
    /// The Client MUST NOT send more than Receive Maximum QoS 1 and QoS 2 PUBLISH
    /// packets for which it has not received PUBACK, PUBCOMP, or PUBREC with a
    /// Reason Code of 128 or greater from the Server.
    /// MQTT v5.0 §4.9 [MQTT-4.9.0-2]:
    /// 送信クォータが 0 に達したら追加の PUBLISH (QoS > 0) を送信してはならない。
    /// MQTT v3.1.1 §4.6 Message ordering:
    /// v3.1.1 では in-flight window は同節末尾の Non normative comment で 1 度言及
    /// されるのみで、規範は存在しない。本ライブラリは `flow_control` の初期値
    /// 65535 で v3.1.1 でも同一の API を通す。
    ///
    /// # 呼び出し禁止条件
    ///
    /// - 同一 `packet_id` を連続で `publish_sent` に渡すこと。クォータ二重消費と
    ///   `qos_flow` silent skip の組み合わせで silent leak が生じる。同じ
    ///   `packet_id` を再利用する場合は [`allocate_packet_id`](Self::allocate_packet_id)
    ///   で新規取得すること。
    pub fn publish_sent(&mut self, qos: QoS, packet_id: u16) -> Result<(), FlowControlError> {
        // QoS 0 と packet_id == 0 は呼び出し契約違反であり、状態を一切変更せずに
        // 明示的な Err で拒否する (silent no-op にすると呼び出し側が成功とみなして
        // 不正な wire 送信をし得るため)。
        if qos == QoS::AtMostOnce {
            return Err(FlowControlError::InvalidQoS);
        }
        if packet_id == 0 {
            return Err(FlowControlError::InvalidPacketId);
        }
        self.flow_control.publish_sent()?;
        // QoS::AtMostOnce は上の early return で除外済み。
        match qos {
            QoS::AtLeastOnce => self.qos_flow.publish_sent_qos1(packet_id),
            QoS::ExactlyOnce => self.qos_flow.publish_sent_qos2(packet_id),
            QoS::AtMostOnce => unreachable!("early return covers QoS::AtMostOnce"),
        }
        Ok(())
    }

    /// PUBLISH (QoS > 0) 送信フローを中断し、リソースを解放する。
    ///
    /// [`publish_sent`](Self::publish_sent) で消費した送信クォータを回復させ、
    /// `qos_flow` の送信フローとパケット識別子を解放する。wire 送信の失敗、または
    /// PUBACK / PUBREC / PUBCOMP 受信のタイムアウト (利用者側で判定) 時に呼ぶ。
    ///
    /// # 呼び出し禁止条件
    ///
    /// - [`publish_sent`](Self::publish_sent) が `Err` を返した後
    /// - [`handle_puback`](Self::handle_puback) / [`handle_pubcomp`](Self::handle_pubcomp)
    ///   が `Some(Action::Complete)` を返した後
    /// - [`handle_pubrec`](Self::handle_pubrec) が `Some(Action::Aborted)` を返した後
    ///
    /// これらのケースでは統合ハンドラ側で既にリソースが解放されているため、本 API を
    /// 呼ぶと二重解放になる。内部の [`QosFlowManager::is_active`] ガードで実害
    /// (`FlowControl::publish_acked` の [`u16::saturating_sub`] による他フロー
    /// クォータ侵食) は防がれるが、規約違反として扱う。
    ///
    /// # 使い分け
    ///
    /// | 状況 | 呼ぶべき API |
    /// |---|---|
    /// | PUBLISH (QoS > 0) 送信フローの中断 (wire 送信失敗 / PUBACK / PUBREC / PUBCOMP 受信タイムアウト / QoS 2 の PUBREL 送信失敗など、`publish_sent` 成功後にフローを終了させる全パス) | `abort_publish` |
    /// | [`publish_sent`](Self::publish_sent) が `Err` を返した後 | [`release_packet_id`](Self::release_packet_id) |
    /// | SUBSCRIBE 送信失敗 | [`abort_subscribe`](Self::abort_subscribe) |
    /// | UNSUBSCRIBE 送信失敗 | [`abort_unsubscribe`](Self::abort_unsubscribe) |
    /// | [`handle_puback`](Self::handle_puback) / [`handle_pubcomp`](Self::handle_pubcomp) が `Some(Complete)` を返した後 | 何もしない (内部で release 済み) |
    /// | [`handle_pubrec`](Self::handle_pubrec) が `Some(Aborted)` を返した後 | 何もしない (内部で release 済み) |
    /// | [`handle_suback`](Self::handle_suback) / [`handle_unsuback`](Self::handle_unsuback) を呼んだ後 | 何もしない (内部で release 済み) |
    ///
    /// # 注意
    ///
    /// [`QosFlowManager::release`] は outgoing / incoming の両方向をクリアする副作用が
    /// あるため、本 API は **PUBLISH 送信パスの release にのみ** 使うこと。
    /// SUBSCRIBE / UNSUBSCRIBE パスでは [`abort_subscribe`](Self::abort_subscribe) /
    /// [`abort_unsubscribe`](Self::abort_unsubscribe) を使う。
    pub fn abort_publish(&mut self, packet_id: u16) {
        // is_active ガードは防御的措置 (詳細は rustdoc 参照)。
        if self.qos_flow.is_active(packet_id) {
            self.flow_control.publish_acked();
        }
        self.qos_flow.release(packet_id);
        self.packet_id_manager.release(packet_id);
    }

    /// パケット識別子のみを解放する。
    ///
    /// wire 未送信の状態からパケット識別子だけを解放するために使う。具体的には
    /// [`publish_sent`](Self::publish_sent) が `Err` を返した後で、`qos_flow` /
    /// `flow_control` / `subscription_manager` を触っていない状態の復旧に用いる。
    ///
    /// # 使い分け
    ///
    /// SUBSCRIBE / UNSUBSCRIBE の allocate 後の送信失敗パスでは
    /// [`abort_subscribe`](Self::abort_subscribe) / [`abort_unsubscribe`](Self::abort_unsubscribe)
    /// を使うこと (`subscription_manager` の pending も明示破棄する必要があるため)。
    ///
    /// 判定表は [`abort_publish`](Self::abort_publish) を参照。
    pub fn release_packet_id(&mut self, packet_id: u16) {
        self.packet_id_manager.release(packet_id);
    }

    /// SUBSCRIBE を送信したことを記録する。
    ///
    /// `subscription_manager` の pending エントリに `subscriptions` を登録する。
    /// SUBACK が返るまで pending 状態のまま保持される。
    ///
    /// # 呼び出し順序
    ///
    /// `Session::allocate_packet_id() → Session::subscribe_sent(id, subscriptions)
    /// → wire 送信` の順に呼ぶ。wire 送信が成功したら SUBACK 受信で
    /// [`handle_suback`](Self::handle_suback) を呼ぶ。wire 送信が失敗した場合は
    /// [`abort_subscribe`](Self::abort_subscribe) を呼ぶ。
    ///
    /// # 規範
    ///
    /// MQTT v5.0 §3.8.3 [MQTT-3.8.3-2]:
    /// SUBSCRIBE Payload には少なくとも 1 つの Topic Filter / Subscription Options
    /// ペアが必要。
    /// MQTT v3.1.1 §3.8.3 [MQTT-3.8.3-3]:
    /// SUBSCRIBE Payload には少なくとも 1 つの Topic Filter / QoS ペアが必要。
    ///
    /// # 呼び出し禁止条件
    ///
    /// - 同一 `packet_id` を連続で呼び出すこと。`pending_subscribes.insert` が
    ///   silent overwrite となり、旧 pending エントリが破棄される。同じ
    ///   `packet_id` を再利用する場合は [`allocate_packet_id`](Self::allocate_packet_id)
    ///   で新規取得すること。
    /// - `packet_id == 0` (MQTT v5.0 §2.2.1 [MQTT-2.2.1-3] / MQTT v3.1.1 §2.3.1
    ///   [MQTT-2.3.1-1] 違反)。underlying が silent no-op となる。
    /// - `subscriptions.is_empty()`。underlying が silent no-op となる。
    ///
    /// 実装は debug ビルドで `debug_assert!` により loud panic する。
    pub fn subscribe_sent(&mut self, packet_id: u16, subscriptions: Vec<SubscriptionEntry>) {
        debug_assert!(
            packet_id != 0,
            "subscribe_sent must be called with a non-zero packet identifier"
        );
        debug_assert!(
            !subscriptions.is_empty(),
            "subscribe_sent must be called with a non-empty subscriptions list"
        );
        self.subscription_manager
            .subscribe_sent(packet_id, subscriptions);
    }

    /// UNSUBSCRIBE を送信したことを記録する。
    ///
    /// `subscription_manager` の pending エントリに `topic_filters` を登録する。
    /// UNSUBACK が返るまで pending 状態のまま保持される。
    ///
    /// # 呼び出し順序
    ///
    /// `Session::allocate_packet_id() → Session::unsubscribe_sent(id, topic_filters)
    /// → wire 送信` の順に呼ぶ。wire 送信が成功したら UNSUBACK 受信で
    /// [`handle_unsuback`](Self::handle_unsuback) を呼ぶ。wire 送信が失敗した場合は
    /// [`abort_unsubscribe`](Self::abort_unsubscribe) を呼ぶ。
    ///
    /// # 規範
    ///
    /// MQTT v5.0 §3.10.3 [MQTT-3.10.3-2]:
    /// UNSUBSCRIBE Payload には少なくとも 1 つの Topic Filter が必要。
    /// MQTT v3.1.1 §3.10.3 [MQTT-3.10.3-2]:
    /// UNSUBSCRIBE Payload には少なくとも 1 つの Topic Filter が必要。
    ///
    /// # 呼び出し禁止条件
    ///
    /// - 同一 `packet_id` を連続で呼び出すこと。`pending_unsubscribes.insert` が
    ///   silent overwrite となり、旧 pending エントリが破棄される。同じ
    ///   `packet_id` を再利用する場合は [`allocate_packet_id`](Self::allocate_packet_id)
    ///   で新規取得すること。
    /// - `packet_id == 0` (MQTT v5.0 §2.2.1 [MQTT-2.2.1-3] / MQTT v3.1.1 §2.3.1
    ///   [MQTT-2.3.1-1] 違反)。underlying が silent no-op となる。
    /// - `topic_filters.is_empty()`。underlying が silent no-op となる。
    ///
    /// 実装は debug ビルドで `debug_assert!` により loud panic する。
    pub fn unsubscribe_sent(&mut self, packet_id: u16, topic_filters: Vec<String>) {
        debug_assert!(
            packet_id != 0,
            "unsubscribe_sent must be called with a non-zero packet identifier"
        );
        debug_assert!(
            !topic_filters.is_empty(),
            "unsubscribe_sent must be called with a non-empty topic_filters list"
        );
        self.subscription_manager
            .unsubscribe_sent(packet_id, topic_filters);
    }

    /// SUBSCRIBE 送信を中断し、pending エントリとパケット識別子を解放する。
    ///
    /// [`subscribe_sent`](Self::subscribe_sent) で登録した pending エントリを破棄し、
    /// パケット識別子を解放する。wire 送信の失敗、または SUBACK 受信のタイムアウト
    /// (利用者側で判定) 時に呼ぶ。
    ///
    /// # 呼び出し禁止条件
    ///
    /// - [`subscribe_sent`](Self::subscribe_sent) 呼び出し前
    /// - [`handle_suback`](Self::handle_suback) 呼び出し後
    ///
    /// `handle_suback` 呼び出し後にはパケット識別子が既に解放されているため、本 API を
    /// 呼ぶと、別用途で再割り当てされたパケット識別子を過剰解放するリスクがある。
    ///
    /// # 戻り値
    ///
    /// 戻り値は `()`。drop された pending エントリを利用者に返さない
    /// (将来必要になれば別 API で追加)。
    ///
    /// 判定表は [`abort_publish`](Self::abort_publish) を参照。
    pub fn abort_subscribe(&mut self, packet_id: u16) {
        self.subscription_manager.drop_pending_subscribe(packet_id);
        self.packet_id_manager.release(packet_id);
    }

    /// UNSUBSCRIBE 送信を中断し、pending エントリとパケット識別子を解放する。
    ///
    /// [`unsubscribe_sent`](Self::unsubscribe_sent) で登録した pending エントリを破棄し、
    /// パケット識別子を解放する。wire 送信の失敗、または UNSUBACK 受信のタイムアウト
    /// (利用者側で判定) 時に呼ぶ。
    ///
    /// # 呼び出し禁止条件
    ///
    /// - [`unsubscribe_sent`](Self::unsubscribe_sent) 呼び出し前
    /// - [`handle_unsuback`](Self::handle_unsuback) 呼び出し後
    ///
    /// `handle_unsuback` 呼び出し後にはパケット識別子が既に解放されているため、本 API を
    /// 呼ぶと、別用途で再割り当てされたパケット識別子を過剰解放するリスクがある。
    ///
    /// # 戻り値
    ///
    /// 戻り値は `()`。drop された pending トピックフィルタ一覧を利用者に返さない
    /// (将来必要になれば別 API で追加)。
    ///
    /// 判定表は [`abort_publish`](Self::abort_publish) を参照。
    pub fn abort_unsubscribe(&mut self, packet_id: u16) {
        self.subscription_manager
            .drop_pending_unsubscribe(packet_id);
        self.packet_id_manager.release(packet_id);
    }

    /// SUBACK パケットを受信したときに呼び出す。
    ///
    /// pending エントリを確認し、成功した購読をアクティブ一覧に反映する。
    /// 戻り値に関わらずパケット識別子は **必ず** 解放される。
    ///
    /// # 戻り値
    ///
    /// - `Ok(Vec<SubscriptionEntry>)`: pending エントリが存在し、
    ///   `reason_codes` 数が pending の要求数と一致した。確認された購読エントリの
    ///   一覧を返す。
    /// - `Err(HandleSubackError::PendingNotFound)`: 以下 2 ケースの吸収。
    ///   1. pending エントリが存在しない (サーバー実装のバグ、または client 側の
    ///      状態破損)。
    ///   2. `reason_codes.len() != pending.len()` (サーバー仕様違反。SUBACK の
    ///      理由コード順序は SUBSCRIBE のトピックフィルタ順序と一致 MUST であり
    ///      (MQTT v5.0 §3.9.3 [MQTT-3.9.3-1] / MQTT v3.1.1 §3.9.3 [MQTT-3.9.3-1])、
    ///      各理由コードは対応するトピックフィルタを指すため、長さの一致はこの順序
    ///      規範の暗黙の帰結として要求される)。このケースでは pending エントリは
    ///      underlying の副作用で silent に破棄され、利用者に返却されない。
    ///
    /// # 規範
    ///
    /// MQTT v5.0 §2.2.1:
    /// Packet Identifier は SUBACK 受信後に再利用可能 (非規範的記述)。
    /// MQTT v3.1.1 §2.3.1 [MQTT-2.3.1-3]:
    /// Packet Identifier は対応する ACK パケット (SUBSCRIBE / UNSUBSCRIBE に
    /// 対しては SUBACK / UNSUBACK) 受信後に再利用可能。
    ///
    /// 本 API は戻り値に関わらずパケット識別子を必ず解放し、上記記述を実装で吸収する。
    ///
    /// # トレードオフ
    ///
    /// 常時解放により Packet Identifier リークは防げる一方、サーバーが SUBACK で
    /// packet_id を取り違えた場合、別用途で使用中のパケット識別子を過剰解放し、
    /// 後続の [`allocate_packet_id`](Self::allocate_packet_id) で同じパケット識別子が
    /// 再取得されて衝突する可能性がある。この耐性を捨てる代わりに、Packet Identifier
    /// リーク防止を優先する。
    pub fn handle_suback(
        &mut self,
        packet_id: u16,
        reason_codes: &[u8],
    ) -> Result<Vec<SubscriptionEntry>, HandleSubackError> {
        let confirmed = self
            .subscription_manager
            .suback_received(packet_id, reason_codes);
        self.packet_id_manager.release(packet_id);
        confirmed.ok_or(HandleSubackError::PendingNotFound)
    }

    /// UNSUBACK パケットを受信したときに呼び出す。
    ///
    /// pending エントリを確認し、成功した購読解除をアクティブ一覧から削除する。
    /// 戻り値に関わらずパケット識別子は **必ず** 解放される。
    ///
    /// # 戻り値
    ///
    /// - `Ok(Vec<String>)`: pending エントリが存在した。ローカルのアクティブ一覧から
    ///   削除されたトピックフィルタ一覧を返す。
    /// - `Err(HandleUnsubackError::PendingNotFound)`: 以下 2 ケースの吸収。
    ///   1. pending エントリが存在しない (サーバー実装のバグ、または client 側の
    ///      状態破損)。
    ///   2. `reason_codes.len() != pending.len()` (サーバー仕様違反。UNSUBACK の
    ///      理由コード順序は UNSUBSCRIBE のトピックフィルタ順序と一致 MUST であり
    ///      (MQTT v5.0 §3.11.3 [MQTT-3.11.3-1])、各理由コードは対応するトピック
    ///      フィルタを指すため、長さの一致はこの順序規範の暗黙の帰結として要求
    ///      される)。このケースでは pending エントリは underlying の副作用で
    ///      silent に破棄され、利用者に返却されない。
    ///
    /// # バージョン依存の挙動
    ///
    /// v3.1.1 の UNSUBACK にはペイロードが無いため、v3.1.1 経路では
    /// `reason_codes` を常に `&[]` で呼ぶこと。`&[]` を渡した場合、
    /// underlying はすべての pending を成功扱いとする。v3.1.1 経路では
    /// length mismatch は原理的に発生せず、`Err(PendingNotFound)` は pending 不在
    /// のみを意味する。
    ///
    /// # 規範
    ///
    /// MQTT v5.0 §2.2.1:
    /// Packet Identifier は UNSUBACK 受信後に再利用可能 (非規範的記述)。
    /// MQTT v3.1.1 §2.3.1 [MQTT-2.3.1-3]:
    /// Packet Identifier は対応する ACK パケット (SUBSCRIBE / UNSUBSCRIBE に
    /// 対しては SUBACK / UNSUBACK) 受信後に再利用可能。
    ///
    /// 本 API は戻り値に関わらずパケット識別子を必ず解放し、上記記述を実装で吸収する。
    /// [`handle_suback`](Self::handle_suback) と同じトレードオフを持つ。
    pub fn handle_unsuback(
        &mut self,
        packet_id: u16,
        reason_codes: &[u8],
    ) -> Result<Vec<String>, HandleUnsubackError> {
        let unsubscribed = self
            .subscription_manager
            .unsuback_received(packet_id, reason_codes);
        self.packet_id_manager.release(packet_id);
        unsubscribed.ok_or(HandleUnsubackError::PendingNotFound)
    }

    // ======================================================================
    // サブ状態機械へのアクセス
    // ======================================================================

    /// パケット識別子プールへの参照を返す。
    pub fn packet_id_manager(&self) -> &PacketIdManager {
        &self.packet_id_manager
    }

    /// パケット識別子プールへの可変参照を返す。
    #[cfg(test)]
    fn packet_id_manager_mut(&mut self) -> &mut PacketIdManager {
        &mut self.packet_id_manager
    }

    /// QoS フロー状態機械への参照を返す。
    pub fn qos_flow(&self) -> &QosFlowManager {
        &self.qos_flow
    }

    /// QoS フロー状態機械への可変参照を返す。
    pub fn qos_flow_mut(&mut self) -> &mut QosFlowManager {
        &mut self.qos_flow
    }

    /// フロー制御への参照を返す。
    pub fn flow_control(&self) -> &FlowControl {
        &self.flow_control
    }

    /// フロー制御への可変参照を返す。
    pub fn flow_control_mut(&mut self) -> &mut FlowControl {
        &mut self.flow_control
    }

    /// サブスクリプション管理への参照を返す。
    pub fn subscription_manager(&self) -> &SubscriptionManager {
        &self.subscription_manager
    }

    /// サブスクリプション管理への可変参照を返す。
    #[cfg(test)]
    fn subscription_manager_mut(&mut self) -> &mut SubscriptionManager {
        &mut self.subscription_manager
    }

    /// トピックエイリアス管理への参照を返す。
    pub fn topic_alias_manager(&self) -> &TopicAliasManager {
        &self.topic_alias_manager
    }

    /// トピックエイリアス管理への可変参照を返す。
    #[cfg(test)]
    fn topic_alias_manager_mut(&mut self) -> &mut TopicAliasManager {
        &mut self.topic_alias_manager
    }

    /// CONNACK で通知されたサーバー能力値への参照を返す。
    pub fn server_capabilities(&self) -> &ServerCapabilities {
        &self.server_capabilities
    }

    /// 送信 PUBLISH がサーバー能力値に適合しているか検証する。
    ///
    /// `server_capabilities().validate_publish` のラッパー。
    pub fn validate_outgoing_publish(
        &self,
        qos: QoS,
        retain: bool,
        packet_size: usize,
    ) -> Result<(), ServerCapabilityError> {
        self.server_capabilities
            .validate_publish(qos, retain, packet_size)
    }

    /// 送信 SUBSCRIBE がサーバー能力値に適合しているか検証する。
    ///
    /// `server_capabilities().validate_subscribe` のラッパー。
    pub fn validate_outgoing_subscribe(
        &self,
        entries: &[SubscriptionEntry],
    ) -> Result<(), ServerCapabilityError> {
        self.server_capabilities.validate_subscribe(entries)
    }

    /// Keep Alive 補助への参照を返す。
    pub fn keep_alive(&self) -> &KeepAlive {
        &self.keep_alive
    }

    /// Keep Alive 補助への可変参照を返す。
    pub fn keep_alive_mut(&mut self) -> &mut KeepAlive {
        &mut self.keep_alive
    }

    /// 認証状態機械への参照を返す。
    pub fn auth_state(&self) -> &AuthStateMachine {
        &self.auth_state
    }

    /// 認証状態機械への可変参照を返す。
    pub fn auth_state_mut(&mut self) -> &mut AuthStateMachine {
        &mut self.auth_state
    }

    // ======================================================================
    // リセット
    // ======================================================================

    /// セッション状態のみをリセットする。
    ///
    /// 対象は Network Connection をまたいで保持されるセッション状態
    /// （QoS フロー・パケット識別子・サブスクリプション）のみ。
    /// 接続スコープの状態は `reset_connection_state` が担う。
    fn reset_session_state(&mut self) {
        self.packet_id_manager.reset();
        self.qos_flow.reset();
        self.subscription_manager.reset();
        // KeepAlive は接続ごとに再設定されるためリセットしない。
    }

    /// 接続スコープの状態を再初期化する。
    ///
    /// 以下はセッション状態ではなく Network Connection ごとの状態であり、
    /// セッション継続の有無に関係なく新しい接続のたびに再初期化する。
    ///
    /// - トピックエイリアスのマッピング。
    ///   MQTT v5.0 §3.3.2.3.4 [MQTT-3.3.2-7]:
    ///   受信者はトピックエイリアスのマッピングを Network Connection を
    ///   またいで持ち越してはならない。
    /// - フロー制御。MQTT v5.0 §4.9:
    ///   send quota と Receive Maximum は Network Connection をまたいで
    ///   保存されず、新しい接続ごとに再初期化される。
    /// - 認証状態とサーバー能力値。
    /// - Keep Alive の PINGRESP 待ち状態。
    ///   MQTT v5.0 §3.1.2.10 のクライアント SHOULD 段落および
    ///   MQTT v3.1.1 §3.1.2.10 のクライアント SHOULD 段落（両バージョンとも規範番号なし）:
    ///   PINGRESP 応答期限はクライアントの死活判定に用いる状態であり、
    ///   前接続の PINGRESP 待ちが新接続に持ち越されると誤タイムアウトが起きる。
    ///
    /// `configure_for_connect` で設定する自身の Topic Alias Maximum は
    /// 次の CONNECT のための設定値であるため維持する。
    fn reset_connection_state(&mut self) {
        self.flow_control.reset(true);
        // MQTT v5.0 §4.9: send quota は Network Connection をまたいで保存されない。
        // flow_control のリセットに合わせて、セッション状態として持ち越される
        // 送信フローの quota 消費状態もクリアし、再送時に新しい接続の
        // send quota を消費できるようにする。
        self.qos_flow.clear_quota_charges();
        self.topic_alias_manager.reset_mappings();
        self.topic_alias_manager.set_peer_maximum(0);
        self.auth_state.reset();
        self.server_capabilities = ServerCapabilities::new();
        // 前接続の PINGRESP 待ちが新接続に持ち越されると誤タイムアウトになるため、
        // pingresp_received を呼んで待ち状態を解除する（同 API は冪等）。
        self.keep_alive.pingresp_received();
    }

    /// すべての状態を完全にリセットする。
    pub fn reset(&mut self) {
        self.connection_state = ConnectionState::Disconnected;
        self.session_expiry_interval = 0;
        self.packet_id_manager.reset();
        self.qos_flow.reset();
        self.flow_control.reset(false);
        self.subscription_manager.reset();
        self.topic_alias_manager.reset();
        self.keep_alive.reset();
        self.auth_state.reset();
        self.server_capabilities = ServerCapabilities::new();
    }
}
