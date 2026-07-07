//! MQTT 可変長バイト整数のエンコード／デコード。
//!
//! MQTT v5.0 §1.5.5 を参照。

use crate::error::{DecodeError, EncodeError};

/// MQTT で定義される可変長バイト整数。
///
/// 継続ビット付きの base-128 方式でエンコードされる。表現可能な最大値は 268,435,455 である。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VariableByteInteger(pub u32);

impl VariableByteInteger {
    /// 可変長バイト整数として表現可能な最大値。
    pub const MAX: u32 = 268_435_455;

    /// エンコード後の長さをバイト数で返す。
    pub fn encoded_len(&self) -> usize {
        match self.0 {
            0..=127 => 1,
            128..=16_383 => 2,
            16_384..=2_097_151 => 3,
            _ => 4,
        }
    }

    /// 値を `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        if self.0 > Self::MAX {
            return Err(EncodeError::PacketTooLarge {
                size: self.0 as usize,
                limit: Self::MAX as usize,
            });
        }

        let mut value = self.0;
        let mut i = 0;
        loop {
            if i >= buf.len() {
                return Err(EncodeError::BufferTooSmall);
            }
            let mut byte = (value % 128) as u8;
            value /= 128;
            if value > 0 {
                byte |= 0x80;
            }
            buf[i] = byte;
            i += 1;
            if value == 0 {
                return Ok(i);
            }
        }
    }

    /// `buf` から可変長バイト整数をデコードする。
    ///
    /// デコードした値と消費したバイト数を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        let mut value: u32 = 0;
        let mut multiplier: u32 = 1;
        let mut i = 0;

        loop {
            if i >= 4 {
                return Err(DecodeError::MalformedPacket);
            }
            if i >= buf.len() {
                return Err(DecodeError::InsufficientData);
            }

            let byte = buf[i];
            value = value
                .checked_add(((byte & 0x7F) as u32) * multiplier)
                .ok_or(DecodeError::MalformedPacket)?;
            i += 1;

            if (byte & 0x80) == 0 {
                break;
            }

            multiplier = multiplier
                .checked_mul(128)
                .ok_or(DecodeError::MalformedPacket)?;
        }

        if value > Self::MAX {
            return Err(DecodeError::MalformedPacket);
        }

        // MQTT v5.0 §1.5.5 [MQTT-1.5.5-1]:
        // 過長なエンコーディングを検出する（例: 0 を 0x80 0x00 としてエンコードした場合）。
        let expected_len = Self(value).encoded_len();
        if i != expected_len {
            return Err(DecodeError::MalformedPacket);
        }

        Ok((Self(value), i))
    }
}
