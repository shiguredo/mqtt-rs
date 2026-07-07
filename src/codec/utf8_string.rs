//! MQTT UTF-8 エンコード文字列のエンコード／デコード。
//!
//! MQTT v5.0 §1.5.4 / MQTT v3.1.1 §1.5.3 を参照。

use crate::error::{DecodeError, EncodeError, EncodeInvalidField};
use alloc::string::String;

/// 2 バイトのビッグエンディアン長さが前置された UTF-8 エンコード文字列。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Utf8String(pub String);

impl Utf8String {
    /// 文字列を `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        Self::encode_str(&self.0, buf)
    }

    /// 文字列スライスを `buf` にエンコードし、書き込んだバイト数を返す。
    ///
    /// 検証とフォーマットは [`encode`](Self::encode) と同一である。
    /// 所有権を取らないため、エンコードのためだけに `String` を
    /// clone して `Utf8String` に包む必要がない。
    pub fn encode_str(value: &str, buf: &mut [u8]) -> Result<usize, EncodeError> {
        // MQTT v5.0 §1.5.4 [MQTT-1.5.4-2] / MQTT v3.1.1 §1.5.3 [MQTT-1.5.3-2]:
        // UTF-8 文字列に U+0000 を含めない。
        if value.contains('\0') {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::NullInUtf8String,
            });
        }

        let bytes = value.as_bytes();
        let len = bytes.len();
        if len > u16::MAX as usize {
            return Err(EncodeError::PacketTooLarge {
                size: len,
                limit: u16::MAX as usize,
            });
        }
        if buf.len() < 2 + len {
            return Err(EncodeError::BufferTooSmall);
        }

        buf[..2].copy_from_slice(&(len as u16).to_be_bytes());
        buf[2..2 + len].copy_from_slice(bytes);
        Ok(2 + len)
    }

    /// `buf` から UTF-8 文字列をデコードする。
    ///
    /// デコードした文字列と消費したバイト数を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        if buf.len() < 2 {
            return Err(DecodeError::InsufficientData);
        }

        let len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
        if buf.len() < 2 + len {
            return Err(DecodeError::InsufficientData);
        }

        let bytes = &buf[2..2 + len];
        let s = String::from_utf8(bytes.to_vec()).map_err(|_| DecodeError::InvalidUtf8)?;
        // MQTT v5.0 §1.5.4 [MQTT-1.5.4-2] / MQTT v3.1.1 §1.5.3 [MQTT-1.5.3-2]:
        // UTF-8 文字列に U+0000 を含めない。
        if s.contains('\0') {
            return Err(DecodeError::MalformedPacket);
        }
        Ok((Self(s), 2 + len))
    }
}
