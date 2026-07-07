//! MQTT v5.0 PUBCOMP パケット。
//!
//! MQTT v5.0 §3.7 を参照。

use crate::error::{DecodeError, EncodeError, EncodeInvalidField};
use crate::v5::ack_helper;
use crate::v5::property::Properties;

/// MQTT v5.0 PUBCOMP パケット。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubComp {
    /// パケット識別子。
    pub packet_id: u16,
    /// 理由コード。
    pub reason_code: PubCompReasonCode,
    /// PUBCOMP プロパティ。
    pub properties: Properties,
}

/// MQTT v5.0 PUBCOMP 理由コード。
///
/// MQTT v5.0 §3.7.2.1 を参照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PubCompReasonCode {
    /// 0x00
    Success = 0x00,
    /// 0x92
    PacketIdentifierNotFound = 0x92,
}

impl PubCompReasonCode {
    /// 数値から `PubCompReasonCode` を作成する。
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(Self::Success),
            0x92 => Some(Self::PacketIdentifierNotFound),
            _ => None,
        }
    }

    /// この理由コードの数値を返す。
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

impl PubComp {
    const PACKET_TYPE: u8 = 0x70;
    const FLAGS: u8 = 0;

    /// エンコード済みパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        ack_helper::reason_code_packet_encoded_len(self.reason_code.as_u8(), &self.properties)
    }

    /// PUBCOMP パケットを `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        self.validate()?;
        ack_helper::encode_reason_code_packet(
            buf,
            Self::PACKET_TYPE,
            Self::FLAGS,
            self.packet_id,
            self.reason_code.as_u8(),
            &self.properties,
        )
    }

    /// `buf` から PUBCOMP パケットをデコードする。
    ///
    /// デコードしたパケットと消費したバイト数を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        let (packet_id, reason_code, properties, consumed) = ack_helper::decode_reason_code_packet(
            buf,
            Self::PACKET_TYPE,
            Self::FLAGS,
            PubCompReasonCode::Success as u8,
        )?;
        properties.validate_for_pubcomp()?;
        let reason_code =
            PubCompReasonCode::from_u8(reason_code).ok_or(DecodeError::MalformedPacket)?;
        Ok((
            Self {
                packet_id,
                reason_code,
                properties,
            },
            consumed,
        ))
    }

    fn validate(&self) -> Result<(), EncodeError> {
        // MQTT v5.0 §2.2.1 [MQTT-2.2.1-3]: Packet Identifier は 0 以外でなければならない。
        if self.packet_id == 0 {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::ZeroPacketId,
            });
        }
        if self.properties.validate_for_pubcomp().is_err() {
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
