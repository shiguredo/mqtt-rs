//! MQTT v3.1.1 DISCONNECT パケット。
//!
//! MQTT v3.1.1 §3.14 を参照。

use crate::error::{DecodeError, EncodeError};
use crate::v311::ack_helper;

/// MQTT v3.1.1 DISCONNECT パケット。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Disconnect;

impl Disconnect {
    const PACKET_TYPE: u8 = 0xE0;
    const FLAGS: u8 = 0;

    /// エンコード後のパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        0
    }

    /// DISCONNECT パケットを `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        ack_helper::encode_empty(buf, Self::PACKET_TYPE, Self::FLAGS)
    }

    /// `buf` から DISCONNECT パケットをデコードする。
    ///
    /// デコードしたパケットと消費したバイト数を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        let offset = ack_helper::decode_empty(buf, Self::PACKET_TYPE, Self::FLAGS)?;
        Ok((Self, offset))
    }
}
