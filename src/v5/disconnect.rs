//! MQTT v5.0 DISCONNECT パケット。
//!
//! MQTT v5.0 §3.14 を参照。

use crate::error::{DecodeError, EncodeError, EncodeInvalidField};
use crate::v5::ack_helper;
use crate::v5::property::PacketDirection;
use crate::v5::property::Properties;

/// MQTT v5.0 DISCONNECT パケット。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Disconnect {
    /// 理由コード。
    pub reason_code: DisconnectReasonCode,
    /// DISCONNECT プロパティ。
    pub properties: Properties,
}

/// MQTT v5.0 Disconnect 理由コード。
///
/// MQTT v5.0 §3.14.2.1 を参照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DisconnectReasonCode {
    /// 0x00
    NormalDisconnection = 0x00,
    /// 0x04
    DisconnectWithWillMessage = 0x04,
    /// 0x80
    UnspecifiedError = 0x80,
    /// 0x81
    MalformedPacket = 0x81,
    /// 0x82
    ProtocolError = 0x82,
    /// 0x83
    ImplementationSpecificError = 0x83,
    /// 0x87
    NotAuthorized = 0x87,
    /// 0x89
    ServerBusy = 0x89,
    /// 0x8B
    ServerShuttingDown = 0x8B,
    /// 0x8C
    BadAuthenticationMethod = 0x8C,
    /// 0x8D
    KeepAliveTimeout = 0x8D,
    /// 0x8E
    SessionTakenOver = 0x8E,
    /// 0x8F
    TopicFilterInvalid = 0x8F,
    /// 0x90
    TopicNameInvalid = 0x90,
    /// 0x93
    ReceiveMaximumExceeded = 0x93,
    /// 0x94
    TopicAliasInvalid = 0x94,
    /// 0x95
    PacketTooLarge = 0x95,
    /// 0x96
    MessageRateTooHigh = 0x96,
    /// 0x97
    QuotaExceeded = 0x97,
    /// 0x98
    AdministrativeAction = 0x98,
    /// 0x99
    PayloadFormatInvalid = 0x99,
    /// 0x9A
    RetainNotSupported = 0x9A,
    /// 0x9B
    QoSNotSupported = 0x9B,
    /// 0x9C
    UseAnotherServer = 0x9C,
    /// 0x9D
    ServerMoved = 0x9D,
    /// 0x9E
    SharedSubscriptionsNotSupported = 0x9E,
    /// 0x9F
    ConnectionRateExceeded = 0x9F,
    /// 0xA0
    MaximumConnectTime = 0xA0,
    /// 0xA1
    SubscriptionIdentifiersNotSupported = 0xA1,
    /// 0xA2
    WildcardSubscriptionsNotSupported = 0xA2,
}

impl DisconnectReasonCode {
    /// 数値から `DisconnectReasonCode` を作成する。
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(Self::NormalDisconnection),
            0x04 => Some(Self::DisconnectWithWillMessage),
            0x80 => Some(Self::UnspecifiedError),
            0x81 => Some(Self::MalformedPacket),
            0x82 => Some(Self::ProtocolError),
            0x83 => Some(Self::ImplementationSpecificError),
            0x87 => Some(Self::NotAuthorized),
            0x89 => Some(Self::ServerBusy),
            0x8B => Some(Self::ServerShuttingDown),
            0x8C => Some(Self::BadAuthenticationMethod),
            0x8D => Some(Self::KeepAliveTimeout),
            0x8E => Some(Self::SessionTakenOver),
            0x8F => Some(Self::TopicFilterInvalid),
            0x90 => Some(Self::TopicNameInvalid),
            0x93 => Some(Self::ReceiveMaximumExceeded),
            0x94 => Some(Self::TopicAliasInvalid),
            0x95 => Some(Self::PacketTooLarge),
            0x96 => Some(Self::MessageRateTooHigh),
            0x97 => Some(Self::QuotaExceeded),
            0x98 => Some(Self::AdministrativeAction),
            0x99 => Some(Self::PayloadFormatInvalid),
            0x9A => Some(Self::RetainNotSupported),
            0x9B => Some(Self::QoSNotSupported),
            0x9C => Some(Self::UseAnotherServer),
            0x9D => Some(Self::ServerMoved),
            0x9E => Some(Self::SharedSubscriptionsNotSupported),
            0x9F => Some(Self::ConnectionRateExceeded),
            0xA0 => Some(Self::MaximumConnectTime),
            0xA1 => Some(Self::SubscriptionIdentifiersNotSupported),
            0xA2 => Some(Self::WildcardSubscriptionsNotSupported),
            _ => None,
        }
    }

    /// この理由コードの数値を返す。
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// クライアントが送信できる Reason Code かどうかを返す。
    ///
    /// MQTT v5.0 §3.14.2.1 Table 3-10 の「Sent by」列に基づく。
    /// Client のみ: 0x04。Client or Server: 0x00・0x80〜0x83・0x90・0x93〜0x99。
    /// 0x8C (Bad authentication method) は Table 3-10 に無いが、MQTT v5.0 §2.4 Table 2-6 は
    /// DISCONNECT 用として挙げ、MQTT v5.0 §4.12.1 [MQTT-4.12.1-2] は再認証失敗時に
    /// Client or Server が DISCONNECT を送るとするため、双方向で許容する。
    fn is_sent_by_client(self) -> bool {
        matches!(
            self,
            Self::NormalDisconnection
                | Self::DisconnectWithWillMessage
                | Self::UnspecifiedError
                | Self::MalformedPacket
                | Self::ProtocolError
                | Self::ImplementationSpecificError
                | Self::BadAuthenticationMethod
                | Self::TopicNameInvalid
                | Self::ReceiveMaximumExceeded
                | Self::TopicAliasInvalid
                | Self::PacketTooLarge
                | Self::MessageRateTooHigh
                | Self::QuotaExceeded
                | Self::AdministrativeAction
                | Self::PayloadFormatInvalid
        )
    }

    /// サーバーが送信できる Reason Code かどうかを返す。
    ///
    /// MQTT v5.0 §3.14.2.1 Table 3-10 の「Sent by」列に基づく。
    /// Client のみが送る 0x04 (Disconnect with Will Message) 以外はサーバーも送れる。
    /// 0x8C (Bad authentication method) は `is_sent_by_client` と同じ根拠で双方向許容する。
    fn is_sent_by_server(self) -> bool {
        !matches!(self, Self::DisconnectWithWillMessage)
    }
}

impl Disconnect {
    const PACKET_TYPE: u8 = 0xE0;
    const FLAGS: u8 = 0;

    /// エンコード済みパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        ack_helper::reason_code_only_packet_encoded_len(self.reason_code.as_u8(), &self.properties)
    }

    /// DISCONNECT パケットを `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        self.validate()?;

        ack_helper::encode_reason_code_only_packet(
            buf,
            Self::PACKET_TYPE,
            Self::FLAGS,
            self.reason_code.as_u8(),
            &self.properties,
        )
    }

    fn validate(&self) -> Result<(), EncodeError> {
        // encode は Client → Server 方向のため、クライアントが送れない Reason Code は拒否する。
        // MQTT v5.0 §3.14.2.1 Table 3-10 の「Sent by」列を参照。
        if !self.reason_code.is_sent_by_client() {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::InvalidReasonCode,
            });
        }
        // Client → Server 方向の DISCONNECT では Session Expiry Interval (0x11) を含むことができる。
        // 禁止されるのは Server → Client 方向のみである
        // （MQTT v5.0 §3.14.2.2.2 [MQTT-3.14.2-2]）。
        if self
            .properties
            .validate_for_disconnect_with_direction(PacketDirection::ClientToServer)
            .is_err()
        {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::PropertyValidationFailed,
            });
        }
        if self
            .properties
            .validate_duplicate_identifiers(false)
            .is_err()
        {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::DuplicatePropertyIdentifier,
            });
        }
        Ok(())
    }

    /// `buf` から DISCONNECT パケットをデコードする。
    ///
    /// デコードしたパケットと消費したバイト数を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        // AUTH との対称性メモ: `Auth::decode` は判定順制御のため独自の decode 順序を持つ。
        // ここでは非対称を意図的に揃えていない (詳細は `Auth::decode` のコメント参照)。

        let (reason_code, properties, consumed) =
            ack_helper::decode_reason_code_only_packet(buf, Self::PACKET_TYPE, Self::FLAGS)?;
        let reason_code =
            DisconnectReasonCode::from_u8(reason_code).ok_or(DecodeError::MalformedPacket)?;
        // decode は Server → Client 方向のため、サーバーが送れない Reason Code は拒否する。
        // MQTT v5.0 §3.14.2.1 Table 3-10 の「Sent by」列を参照。
        if !reason_code.is_sent_by_server() {
            return Err(DecodeError::MalformedPacket);
        }
        // Server → Client 方向の DISCONNECT では Session Expiry Interval (0x11) を含んではならない。
        properties.validate_for_disconnect_with_direction(PacketDirection::ServerToClient)?;

        Ok((
            Self {
                reason_code,
                properties,
            },
            consumed,
        ))
    }
}
