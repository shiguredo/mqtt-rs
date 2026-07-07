//! MQTT v5.0 PINGREQ パケット。
//!
//! MQTT v5.0 §3.12 を参照。

use crate::error::{DecodeError, EncodeError};
use crate::v5::ack_helper;

/// MQTT v5.0 PINGREQ パケット。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PingReq;

impl PingReq {
    const PACKET_TYPE: u8 = 0xC0;
    const FLAGS: u8 = 0;

    /// エンコード後のパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        0
    }

    /// PINGREQ パケットを `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        ack_helper::encode_empty(buf, Self::PACKET_TYPE, Self::FLAGS)
    }

    /// `buf` から PINGREQ パケットをデコードする。
    ///
    /// デコードしたパケットと消費したバイト数を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        let offset = ack_helper::decode_empty(buf, Self::PACKET_TYPE, Self::FLAGS)?;
        Ok((Self, offset))
    }
}
