//! MQTT v3.1.1 PUBCOMP パケット。
//!
//! MQTT v3.1.1 §3.7 を参照。

use crate::error::{DecodeError, EncodeError};
use crate::v311::ack_helper;

/// MQTT v3.1.1 PUBCOMP パケット。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PubComp {
    /// 完了対象のパケット識別子。
    pub packet_id: u16,
}

impl PubComp {
    const PACKET_TYPE: u8 = 0x70;
    const FLAGS: u8 = 0;

    /// エンコード後のパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        // パケット識別子 (2 バイト)
        2
    }

    /// PUBCOMP パケットを `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        ack_helper::encode_simple_ack(buf, Self::PACKET_TYPE, Self::FLAGS, self.packet_id)
    }

    /// `buf` から PUBCOMP パケットをデコードする。
    ///
    /// デコードしたパケットと消費したバイト数を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        let (packet_id, offset) =
            ack_helper::decode_simple_ack(buf, Self::PACKET_TYPE, Self::FLAGS)?;
        Ok((Self { packet_id }, offset))
    }
}
