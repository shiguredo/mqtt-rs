//! Tokio ベースの MQTT クライアント example のエラー型。

use std::fmt;

use shiguredo_mqtt::error::{DecodeError, EncodeError};
use shiguredo_mqtt::state::flow_control::FlowControlError;
use shiguredo_mqtt::state::qos_flow::FlowError;
use shiguredo_mqtt::state::session::{
    ConnackError, HandleSubackError, HandleUnsubackError, ServerCapabilityError, SessionError,
};
use shiguredo_mqtt::v5::connack::ConnectReasonCode;
use shiguredo_mqtt::v5::pubrec::PubRecReasonCode;

/// クライアント動作中に発生しうるエラー。
#[derive(Debug)]
pub enum Error {
    /// I/O エラー。
    Io(std::io::Error),
    /// TLS のセットアップまたはハンドシェイクに失敗した。
    Tls(String),
    /// パケットのエンコードに失敗した。
    Encode(EncodeError),
    /// パケットのデコードに失敗した。
    Decode(DecodeError),
    /// セッションの作成または設定に失敗した。
    Session(SessionError),
    /// CONNACK の適用に失敗した。
    Connack(ConnackError),
    /// 接続が拒否された。
    ConnectRefused(ConnectReasonCode),
    /// サーバー能力値に対する送信検証に失敗した。
    ServerCapability(ServerCapabilityError),
    /// SUBACK の処理に失敗した。
    Suback(HandleSubackError),
    /// UNSUBACK の処理に失敗した。
    Unsuback(HandleUnsubackError),
    /// QoS フロー状態機械のエラー。
    Flow(FlowError),
    /// Receive Maximum を超過した。
    FlowControl(FlowControlError),
    /// QoS 2 の PUBLISH がエラー Reason Code の PUBREC で拒否された。
    PublishRejected(PubRecReasonCode),
    /// パケット識別子が枯渇した。
    PacketIdExhausted,
    /// 予期しないパケットを受信した。
    UnexpectedPacket,
    /// 接続が切断された。
    Disconnected,
    /// Keep Alive の PINGRESP 待ちがタイムアウトした。
    KeepAliveTimeout,
    /// 操作がタイムアウトした。
    Timeout,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Tls(e) => write!(f, "TLS error: {e}"),
            Self::Encode(e) => write!(f, "encode error: {e}"),
            Self::Decode(e) => write!(f, "decode error: {e}"),
            Self::Session(e) => write!(f, "session error: {e}"),
            Self::Connack(e) => write!(f, "CONNACK error: {e}"),
            Self::ConnectRefused(code) => write!(f, "connection refused: {code:?}"),
            Self::ServerCapability(e) => write!(f, "server capability error: {e}"),
            Self::Suback(e) => write!(f, "SUBACK error: {e}"),
            Self::Unsuback(e) => write!(f, "UNSUBACK error: {e}"),
            Self::Flow(e) => write!(f, "QoS flow error: {e}"),
            Self::FlowControl(e) => write!(f, "flow control error: {e}"),
            Self::PublishRejected(code) => write!(f, "PUBLISH rejected: {code:?}"),
            Self::PacketIdExhausted => write!(f, "packet identifier exhausted"),
            Self::UnexpectedPacket => write!(f, "unexpected MQTT packet"),
            Self::Disconnected => write!(f, "connection closed"),
            Self::KeepAliveTimeout => write!(f, "PINGRESP keep alive timeout"),
            Self::Timeout => write!(f, "operation timed out"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<EncodeError> for Error {
    fn from(e: EncodeError) -> Self {
        Self::Encode(e)
    }
}

impl From<DecodeError> for Error {
    fn from(e: DecodeError) -> Self {
        Self::Decode(e)
    }
}

impl From<SessionError> for Error {
    fn from(e: SessionError) -> Self {
        Self::Session(e)
    }
}

impl From<ConnackError> for Error {
    fn from(e: ConnackError) -> Self {
        Self::Connack(e)
    }
}

impl From<ServerCapabilityError> for Error {
    fn from(e: ServerCapabilityError) -> Self {
        Self::ServerCapability(e)
    }
}

impl From<HandleSubackError> for Error {
    fn from(e: HandleSubackError) -> Self {
        Self::Suback(e)
    }
}

impl From<HandleUnsubackError> for Error {
    fn from(e: HandleUnsubackError) -> Self {
        Self::Unsuback(e)
    }
}

impl From<FlowError> for Error {
    fn from(e: FlowError) -> Self {
        Self::Flow(e)
    }
}

impl From<FlowControlError> for Error {
    fn from(e: FlowControlError) -> Self {
        Self::FlowControl(e)
    }
}
