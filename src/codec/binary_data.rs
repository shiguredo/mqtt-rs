//! MQTT バイナリデータのエンコード／デコード。
//!
//! MQTT v5.0 §1.5.6 を参照。

use crate::error::{DecodeError, EncodeError};
use alloc::vec::Vec;

/// 2 バイトのビッグエンディアン長さが前置された任意のバイナリデータ。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BinaryData(pub Vec<u8>);

impl BinaryData {
    /// データを `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        Self::encode_slice(&self.0, buf)
    }

    /// バイトスライスを `buf` にエンコードし、書き込んだバイト数を返す。
    ///
    /// 検証とフォーマットは [`encode`](Self::encode) と同一である。
    /// 所有権を取らないため、エンコードのためだけに `Vec<u8>` を
    /// clone して `BinaryData` に包む必要がない。
    pub fn encode_slice(value: &[u8], buf: &mut [u8]) -> Result<usize, EncodeError> {
        let len = value.len();
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
        buf[2..2 + len].copy_from_slice(value);
        Ok(2 + len)
    }

    /// `buf` からバイナリデータをデコードする。
    ///
    /// デコードしたデータと消費したバイト数を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        if buf.len() < 2 {
            return Err(DecodeError::InsufficientData);
        }

        let len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
        if buf.len() < 2 + len {
            return Err(DecodeError::InsufficientData);
        }

        let data = buf[2..2 + len].to_vec();
        Ok((Self(data), 2 + len))
    }
}
