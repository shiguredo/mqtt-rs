//! MQTT v3.1.1 SUBSCRIBE パケット。
//!
//! MQTT v3.1.1 §3.8 を参照。

use alloc::string::String;
use alloc::vec::Vec;

use crate::codec::qos::QoS;
use crate::codec::utf8_string::Utf8String;
use crate::codec::variable_byte_integer::VariableByteInteger;
use crate::error::{DecodeError, EncodeError, EncodeInvalidField};

/// MQTT v3.1.1 SUBSCRIBE パケット。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    /// パケット識別子。
    pub packet_id: u16,
    /// トピックフィルタ購読のリスト。
    pub topic_filters: Vec<Subscription>,
}

/// 単一のトピックフィルタ購読。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    /// トピックフィルタ。
    pub topic_filter: String,
    /// 要求する最大 QoS。
    pub qos: QoS,
}

impl Subscribe {
    const PACKET_TYPE: u8 = 0x80;
    const FLAGS: u8 = 0x02;

    /// エンコード後のパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        // パケット識別子 (2 バイト)
        let mut len = 2usize;
        for subscription in &self.topic_filters {
            len = len
                .saturating_add(2)
                .saturating_add(subscription.topic_filter.len());
            len = len.saturating_add(1); // Requested QoS バイト
        }
        len
    }

    /// SUBSCRIBE パケットを `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        if self.packet_id == 0 {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::ZeroPacketId,
            });
        }
        // MQTT v3.1.1 §3.8.3 [MQTT-3.8.3-3]: Topic Filter / QoS の組は 1 つ以上必要。
        if self.topic_filters.is_empty() {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::EmptyTopicFilters,
            });
        }
        // MQTT v3.1.1 §4.7.1 [MQTT-4.7.1-2][MQTT-4.7.1-3] / MQTT v3.1.1 §4.7.3 [MQTT-4.7.3-1]:
        // Topic Filter の構文（ワイルドカード規則と最低 1 文字制限）を検証する。
        for subscription in &self.topic_filters {
            if !crate::topic_filter::is_valid_topic_filter(&subscription.topic_filter) {
                return Err(EncodeError::InvalidField {
                    reason: EncodeInvalidField::InvalidTopicFilter,
                });
            }
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

        buf[0] = Self::PACKET_TYPE | Self::FLAGS;
        let mut offset = 1 + vbi.encode(&mut buf[1..])?;

        buf[offset..offset + 2].copy_from_slice(&self.packet_id.to_be_bytes());
        offset += 2;

        for subscription in &self.topic_filters {
            offset += Utf8String::encode_str(&subscription.topic_filter, &mut buf[offset..])?;
            buf[offset] = subscription.qos.as_u8();
            offset += 1;
        }

        Ok(offset)
    }

    /// `buf` から SUBSCRIBE パケットをデコードする。
    ///
    /// デコードしたパケットと消費したバイト数を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        if buf.is_empty() {
            return Err(DecodeError::InsufficientData);
        }
        if buf[0] != (Self::PACKET_TYPE | Self::FLAGS) {
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

        let mut topic_filters = Vec::new();
        while offset < end {
            let (topic_filter, n) = Utf8String::decode(&buf[offset..end])
                .map_err(crate::error::malformed_if_insufficient)?;
            offset += n;

            // MQTT v3.1.1 §4.7.1 [MQTT-4.7.1-2][MQTT-4.7.1-3] / MQTT v3.1.1 §4.7.3 [MQTT-4.7.3-1]:
            // Topic Filter の構文（ワイルドカード規則と最低 1 文字制限）を検証する。
            if !crate::topic_filter::is_valid_topic_filter(&topic_filter.0) {
                return Err(DecodeError::MalformedPacket);
            }

            if offset >= end {
                return Err(DecodeError::MalformedPacket);
            }
            let qos = QoS::from_u8(buf[offset] & 0x03).ok_or(DecodeError::MalformedPacket)?;
            if buf[offset] & 0xFC != 0 {
                return Err(DecodeError::MalformedPacket);
            }
            offset += 1;

            topic_filters.push(Subscription {
                topic_filter: topic_filter.0,
                qos,
            });
        }

        // MQTT v3.1.1 §3.8.3 [MQTT-3.8.3-3]: Topic Filter / QoS の組は 1 つ以上必要。
        if topic_filters.is_empty() {
            return Err(DecodeError::MalformedPacket);
        }

        Ok((
            Self {
                packet_id,
                topic_filters,
            },
            offset,
        ))
    }
}
