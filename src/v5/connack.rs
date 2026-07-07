//! MQTT v5.0 CONNACK パケット。
//!
//! MQTT v5.0 §3.2 を参照。

use crate::codec::variable_byte_integer::VariableByteInteger;
use crate::error::{DecodeError, EncodeError, EncodeInvalidField};
use crate::v5::property::Properties;

/// MQTT v5.0 CONNACK パケット。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnAck {
    /// 既存のセッションを使用しているかどうか。
    pub session_present: bool,
    /// 接続試行の結果を示す理由コード。
    pub reason_code: ConnectReasonCode,
    /// CONNACK プロパティ。
    pub properties: Properties,
}

/// MQTT v5.0 Connect 理由コード。
///
/// MQTT v5.0 §3.2.2.2 [MQTT-3.2.2-8]:
/// CONNACK パケットを送信するサーバーは Connect Reason Code 値の
/// いずれかを使用しなければならない。
/// この enum は同節の Connect Reason Code 表に存在する値のみを定義する。
/// 0x9E / 0xA1 / 0xA2 は SUBACK / DISCONNECT 用の Reason Code であり
/// CONNACK には存在しない（MQTT v5.0 §2.4 Table 2-6 を参照）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnectReasonCode {
    /// 0x00
    Success = 0x00,
    /// 0x80
    UnspecifiedError = 0x80,
    /// 0x81
    MalformedPacket = 0x81,
    /// 0x82
    ProtocolError = 0x82,
    /// 0x83
    ImplementationSpecificError = 0x83,
    /// 0x84
    UnsupportedProtocolVersion = 0x84,
    /// 0x85
    ClientIdentifierNotValid = 0x85,
    /// 0x86
    BadUserNameOrPassword = 0x86,
    /// 0x87
    NotAuthorized = 0x87,
    /// 0x88
    ServerUnavailable = 0x88,
    /// 0x89
    ServerBusy = 0x89,
    /// 0x8A
    Banned = 0x8A,
    /// 0x8C
    BadAuthenticationMethod = 0x8C,
    /// 0x90
    TopicNameInvalid = 0x90,
    /// 0x95
    PacketTooLarge = 0x95,
    /// 0x97
    QuotaExceeded = 0x97,
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
    /// 0x9F
    ConnectionRateExceeded = 0x9F,
}

impl ConnectReasonCode {
    /// 数値から `ConnectReasonCode` を作成する。
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(Self::Success),
            0x80 => Some(Self::UnspecifiedError),
            0x81 => Some(Self::MalformedPacket),
            0x82 => Some(Self::ProtocolError),
            0x83 => Some(Self::ImplementationSpecificError),
            0x84 => Some(Self::UnsupportedProtocolVersion),
            0x85 => Some(Self::ClientIdentifierNotValid),
            0x86 => Some(Self::BadUserNameOrPassword),
            0x87 => Some(Self::NotAuthorized),
            0x88 => Some(Self::ServerUnavailable),
            0x89 => Some(Self::ServerBusy),
            0x8A => Some(Self::Banned),
            0x8C => Some(Self::BadAuthenticationMethod),
            0x90 => Some(Self::TopicNameInvalid),
            0x95 => Some(Self::PacketTooLarge),
            0x97 => Some(Self::QuotaExceeded),
            0x99 => Some(Self::PayloadFormatInvalid),
            0x9A => Some(Self::RetainNotSupported),
            0x9B => Some(Self::QoSNotSupported),
            0x9C => Some(Self::UseAnotherServer),
            0x9D => Some(Self::ServerMoved),
            0x9F => Some(Self::ConnectionRateExceeded),
            _ => None,
        }
    }

    /// この理由コードの数値を返す。
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

impl ConnAck {
    const PACKET_TYPE: u8 = 0x20;

    /// エンコード済みパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        1usize
            .saturating_add(1)
            .saturating_add(self.properties.encoded_len())
    }

    /// CONNACK パケットを `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        self.validate()?;

        let remaining_len = self.encoded_len();
        if remaining_len > VariableByteInteger::MAX as usize {
            return Err(EncodeError::PacketTooLarge {
                size: remaining_len,
                limit: VariableByteInteger::MAX as usize,
            });
        }

        let vbi = VariableByteInteger(remaining_len as u32);
        let total_len = 1usize
            .checked_add(vbi.encoded_len())
            .and_then(|x| x.checked_add(remaining_len))
            .ok_or(EncodeError::PacketTooLarge {
                size: remaining_len,
                limit: VariableByteInteger::MAX as usize,
            })?;
        if buf.len() < total_len {
            return Err(EncodeError::BufferTooSmall);
        }

        buf[0] = Self::PACKET_TYPE;
        let mut offset = 1 + vbi.encode(&mut buf[1..])?;

        // コネクト確認応答フラグ
        buf[offset] = if self.session_present { 0x01 } else { 0x00 };
        offset += 1;

        // 理由コード
        buf[offset] = self.reason_code.as_u8();
        offset += 1;

        // プロパティ
        offset += self.properties.encode(&mut buf[offset..])?;

        Ok(offset)
    }

    fn validate(&self) -> Result<(), EncodeError> {
        // MQTT v5.0 §3.2.2.1.1 [MQTT-3.2.2-3] / MQTT v5.0 §3.2.2.2:
        // Session Present が true の場合、Reason Code は Success でなければならない。
        if self.session_present && self.reason_code != ConnectReasonCode::Success {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::SessionPresentWithNonSuccess,
            });
        }
        if self.properties.validate_for_connack().is_err() {
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

    /// `buf` から CONNACK パケットをデコードする。
    ///
    /// デコードしたパケットと消費したバイト数を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        if buf.is_empty() {
            return Err(DecodeError::InsufficientData);
        }
        if buf[0] & 0xF0 != Self::PACKET_TYPE {
            return Err(DecodeError::InvalidPacketType);
        }
        // MQTT v5.0 §2.1.3 [MQTT-2.1.3-1]: 固定ヘッダーの予約フラグは 0 でなければならない。
        if buf[0] & 0x0F != 0 {
            return Err(DecodeError::InvalidPacketFlags);
        }

        let (remaining_len, vbi_len) = VariableByteInteger::decode(&buf[1..])?;
        let remaining_len = remaining_len.0 as usize;
        let header_len = 1 + vbi_len;
        if buf.len() < header_len + remaining_len {
            return Err(DecodeError::InsufficientData);
        }

        let mut offset = header_len;
        let end = header_len + remaining_len;

        // コネクト確認応答フラグ
        if offset >= end {
            return Err(DecodeError::MalformedPacket);
        }
        let flags = buf[offset];
        offset += 1;

        if flags & 0xFE != 0 {
            return Err(DecodeError::MalformedPacket);
        }
        let session_present = flags & 0x01 != 0;

        // 理由コード
        if offset >= end {
            return Err(DecodeError::MalformedPacket);
        }
        let reason_code =
            ConnectReasonCode::from_u8(buf[offset]).ok_or(DecodeError::MalformedPacket)?;
        offset += 1;

        // プロパティ
        let (properties, n) = Properties::decode(&buf[offset..end])?;
        properties.validate_for_connack()?;
        offset += n;

        // Session Present が true の場合、理由コードは Success でなければならない。
        if reason_code != ConnectReasonCode::Success && session_present {
            return Err(DecodeError::MalformedPacket);
        }

        if offset != end {
            return Err(DecodeError::MalformedPacket);
        }

        Ok((
            Self {
                session_present,
                reason_code,
                properties,
            },
            offset,
        ))
    }
}
