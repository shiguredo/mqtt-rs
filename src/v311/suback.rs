//! MQTT v3.1.1 SUBACK パケット。
//!
//! MQTT v3.1.1 §3.9 を参照。

use alloc::vec::Vec;

use crate::codec::variable_byte_integer::VariableByteInteger;
use crate::error::{DecodeError, EncodeError, EncodeInvalidField};

/// MQTT v3.1.1 SUBACK パケット。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAck {
    /// パケット識別子。
    pub packet_id: u16,
    /// 各購読要求に対するリターンコード。
    pub return_codes: Vec<SubscribeReturnCode>,
}

/// MQTT v3.1.1 SUBACK Return Code。
///
/// MQTT v3.1.1 §3.9.3 を参照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SubscribeReturnCode {
    /// 0x00: 成功 - 最大 QoS 0。
    SuccessQoS0 = 0x00,
    /// 0x01: 成功 - 最大 QoS 1。
    SuccessQoS1 = 0x01,
    /// 0x02: 成功 - 最大 QoS 2。
    SuccessQoS2 = 0x02,
    /// 0x80: 失敗。
    Failure = 0x80,
}

impl SubscribeReturnCode {
    /// 数値から `SubscribeReturnCode` を作成する。
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(Self::SuccessQoS0),
            0x01 => Some(Self::SuccessQoS1),
            0x02 => Some(Self::SuccessQoS2),
            0x80 => Some(Self::Failure),
            _ => None,
        }
    }

    /// このリターンコードの数値を返す。
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

impl SubAck {
    const PACKET_TYPE: u8 = 0x90;

    /// エンコード後のパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        // パケット識別子 (2 バイト) + リターンコード
        2 + self.return_codes.len()
    }

    /// SUBACK パケットを `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        if self.packet_id == 0 {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::ZeroPacketId,
            });
        }
        if self.return_codes.is_empty() {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::EmptyReturnCodes,
            });
        }

        let remaining_len = self.encoded_len();
        if remaining_len > VariableByteInteger::MAX as usize {
            return Err(EncodeError::PacketTooLarge {
                size: remaining_len,
                limit: VariableByteInteger::MAX as usize,
            });
        }

        let vbi = VariableByteInteger(remaining_len as u32);
        let total_len = 1 + vbi.encoded_len() + remaining_len;
        if buf.len() < total_len {
            return Err(EncodeError::BufferTooSmall);
        }

        buf[0] = Self::PACKET_TYPE;
        let mut offset = 1 + vbi.encode(&mut buf[1..])?;

        buf[offset..offset + 2].copy_from_slice(&self.packet_id.to_be_bytes());
        offset += 2;

        for code in &self.return_codes {
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

        if packet_id == 0 {
            return Err(DecodeError::MalformedPacket);
        }

        let mut return_codes = Vec::new();
        while offset < end {
            let code =
                SubscribeReturnCode::from_u8(buf[offset]).ok_or(DecodeError::MalformedPacket)?;
            return_codes.push(code);
            offset += 1;
        }

        if return_codes.is_empty() {
            return Err(DecodeError::MalformedPacket);
        }

        Ok((
            Self {
                packet_id,
                return_codes,
            },
            offset,
        ))
    }
}
