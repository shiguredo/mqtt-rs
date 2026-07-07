//! MQTT v5.0 SUBACK パケット。
//!
//! MQTT v5.0 §3.9 を参照。

use alloc::vec::Vec;

use crate::codec::variable_byte_integer::VariableByteInteger;
use crate::error::{DecodeError, EncodeError, EncodeInvalidField};
use crate::v5::property::Properties;

/// MQTT v5.0 SUBACK パケット。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAck {
    /// パケット識別子。
    pub packet_id: u16,
    /// 各トピックフィルタに対する理由コード。
    pub reason_codes: Vec<SubAckReasonCode>,
    /// SUBACK プロパティ。
    pub properties: Properties,
}

/// MQTT v5.0 Subscribe 理由コード。
///
/// MQTT v5.0 §3.9.3 を参照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SubAckReasonCode {
    /// 0x00
    GrantedQoS0 = 0x00,
    /// 0x01
    GrantedQoS1 = 0x01,
    /// 0x02
    GrantedQoS2 = 0x02,
    /// 0x80
    UnspecifiedError = 0x80,
    /// 0x83
    ImplementationSpecificError = 0x83,
    /// 0x87
    NotAuthorized = 0x87,
    /// 0x8F
    TopicFilterInvalid = 0x8F,
    /// 0x91
    PacketIdentifierInUse = 0x91,
    /// 0x97
    QuotaExceeded = 0x97,
    /// 0x9E
    SharedSubscriptionsNotSupported = 0x9E,
    /// 0xA1
    SubscriptionIdentifiersNotSupported = 0xA1,
    /// 0xA2
    WildcardSubscriptionsNotSupported = 0xA2,
}

impl SubAckReasonCode {
    /// 数値から `SubAckReasonCode` を作成する。
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(Self::GrantedQoS0),
            0x01 => Some(Self::GrantedQoS1),
            0x02 => Some(Self::GrantedQoS2),
            0x80 => Some(Self::UnspecifiedError),
            0x83 => Some(Self::ImplementationSpecificError),
            0x87 => Some(Self::NotAuthorized),
            0x8F => Some(Self::TopicFilterInvalid),
            0x91 => Some(Self::PacketIdentifierInUse),
            0x97 => Some(Self::QuotaExceeded),
            0x9E => Some(Self::SharedSubscriptionsNotSupported),
            0xA1 => Some(Self::SubscriptionIdentifiersNotSupported),
            0xA2 => Some(Self::WildcardSubscriptionsNotSupported),
            _ => None,
        }
    }

    /// この理由コードの数値を返す。
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

impl SubAck {
    const PACKET_TYPE: u8 = 0x90;

    /// エンコード済みパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        2usize
            .saturating_add(self.properties.encoded_len())
            .saturating_add(self.reason_codes.len())
    }

    /// SUBACK パケットを `buf` にエンコードし、書き込んだバイト数を返す。
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

        // パケット識別子
        buf[offset..offset + 2].copy_from_slice(&self.packet_id.to_be_bytes());
        offset += 2;

        // プロパティ
        offset += self.properties.encode(&mut buf[offset..])?;

        // 理由コード
        for code in &self.reason_codes {
            buf[offset] = code.as_u8();
            offset += 1;
        }

        Ok(offset)
    }

    /// `buf` から SUBACK パケットをデコードする。
    ///
    /// デコードしたパケットと消費したバイト数を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        if buf.is_empty() {
            return Err(DecodeError::InsufficientData);
        }
        if buf[0] & 0xF0 != Self::PACKET_TYPE {
            return Err(DecodeError::InvalidPacketType);
        }
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

        if offset + 2 > end {
            return Err(DecodeError::MalformedPacket);
        }
        let packet_id = u16::from_be_bytes([buf[offset], buf[offset + 1]]);
        offset += 2;

        // MQTT v5.0 §2.2.1 [MQTT-2.2.1-3]: Packet Identifier は 0 以外でなければならない。
        if packet_id == 0 {
            return Err(DecodeError::MalformedPacket);
        }

        // プロパティ
        let (properties, n) = Properties::decode(&buf[offset..end])?;
        properties.validate_for_suback()?;
        offset += n;

        // 理由コード
        let mut reason_codes = Vec::new();
        while offset < end {
            let code =
                SubAckReasonCode::from_u8(buf[offset]).ok_or(DecodeError::MalformedPacket)?;
            reason_codes.push(code);
            offset += 1;
        }

        if reason_codes.is_empty() {
            return Err(DecodeError::MalformedPacket);
        }

        if offset != end {
            return Err(DecodeError::MalformedPacket);
        }

        Ok((
            Self {
                packet_id,
                reason_codes,
                properties,
            },
            offset,
        ))
    }

    fn validate(&self) -> Result<(), EncodeError> {
        // MQTT v5.0 §2.2.1 [MQTT-2.2.1-3]: Packet Identifier は 0 以外でなければならない。
        if self.packet_id == 0 {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::ZeroPacketId,
            });
        }
        if self.reason_codes.is_empty() {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::EmptyReasonCodes,
            });
        }
        if self.properties.validate_for_suback().is_err() {
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
}
