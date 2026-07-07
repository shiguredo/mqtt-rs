//! MQTT v5.0 PUBREC パケット。
//!
//! MQTT v5.0 §3.5 を参照。

use crate::error::{DecodeError, EncodeError, EncodeInvalidField};
use crate::v5::ack_helper;
use crate::v5::property::Properties;

/// MQTT v5.0 PUBREC パケット。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubRec {
    /// パケット識別子。
    pub packet_id: u16,
    /// 理由コード。
    pub reason_code: PubRecReasonCode,
    /// PUBREC プロパティ。
    pub properties: Properties,
}

/// MQTT v5.0 PUBREC 理由コード。
///
/// MQTT v5.0 §3.5.2.1 を参照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PubRecReasonCode {
    /// 0x00
    Success = 0x00,
    /// 0x10
    NoMatchingSubscribers = 0x10,
    /// 0x80
    UnspecifiedError = 0x80,
    /// 0x83
    ImplementationSpecificError = 0x83,
    /// 0x87
    NotAuthorized = 0x87,
    /// 0x90
    TopicNameInvalid = 0x90,
    /// 0x91
    PacketIdentifierInUse = 0x91,
    /// 0x97
    QuotaExceeded = 0x97,
    /// 0x99
    PayloadFormatInvalid = 0x99,
}

impl PubRecReasonCode {
    /// 数値から `PubRecReasonCode` を作成する。
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(Self::Success),
            0x10 => Some(Self::NoMatchingSubscribers),
            0x80 => Some(Self::UnspecifiedError),
            0x83 => Some(Self::ImplementationSpecificError),
            0x87 => Some(Self::NotAuthorized),
            0x90 => Some(Self::TopicNameInvalid),
            0x91 => Some(Self::PacketIdentifierInUse),
            0x97 => Some(Self::QuotaExceeded),
            0x99 => Some(Self::PayloadFormatInvalid),
            _ => None,
        }
    }

    /// この理由コードの数値を返す。
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

impl PubRec {
    const PACKET_TYPE: u8 = 0x50;
    const FLAGS: u8 = 0;

    /// エンコード済みパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        ack_helper::reason_code_packet_encoded_len(self.reason_code.as_u8(), &self.properties)
    }

    /// PUBREC パケットを `buf` にエンコードし、書き込んだバイト数を返す。
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

    /// `buf` から PUBREC パケットをデコードする。
    ///
    /// デコードしたパケットと消費したバイト数を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        let (packet_id, reason_code, properties, consumed) = ack_helper::decode_reason_code_packet(
            buf,
            Self::PACKET_TYPE,
            Self::FLAGS,
            PubRecReasonCode::Success as u8,
        )?;
        properties.validate_for_pubrec()?;
        let reason_code =
            PubRecReasonCode::from_u8(reason_code).ok_or(DecodeError::MalformedPacket)?;
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
        // MQTT v5.0 §3.5.2.1 (PUBREC Reason Code):
        // 0x10 No matching subscribers は「PUBLISH を受理したが購読者がいない」
        // ことを示すもので、サーバーだけが送信し得る。クライアント専用
        // ライブラリとして encode 時には拒否する（decode 側は受理を維持する）。
        if self.reason_code == PubRecReasonCode::NoMatchingSubscribers {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::InvalidReasonCode,
            });
        }
        if self.properties.validate_for_pubrec().is_err() {
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
