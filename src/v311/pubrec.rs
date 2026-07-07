//! MQTT v3.1.1 PUBREC パケット。
//!
//! MQTT v3.1.1 §3.5 を参照。

use crate::error::{DecodeError, EncodeError};
use crate::v311::ack_helper;

/// MQTT v3.1.1 PUBREC パケット。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PubRec {
    /// 受信対象のパケット識別子。
    pub packet_id: u16,
}

impl PubRec {
    const PACKET_TYPE: u8 = 0x50;
    const FLAGS: u8 = 0;

    /// エンコード後のパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        // パケット識別子 (2 バイト)
        2
    }

    /// PUBREC パケットを `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        ack_helper::encode_simple_ack(buf, Self::PACKET_TYPE, Self::FLAGS, self.packet_id)
    }

    /// `buf` から PUBREC パケットをデコードする。
    ///
    /// デコードしたパケットと消費したバイト数を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        let (packet_id, offset) =
            ack_helper::decode_simple_ack(buf, Self::PACKET_TYPE, Self::FLAGS)?;
        Ok((Self { packet_id }, offset))
    }
}
