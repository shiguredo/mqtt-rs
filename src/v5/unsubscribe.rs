//! MQTT v5.0 UNSUBSCRIBE パケット。
//!
//! MQTT v5.0 §3.10 を参照。

use alloc::string::String;
use alloc::vec::Vec;

use crate::codec::utf8_string::Utf8String;
use crate::codec::variable_byte_integer::VariableByteInteger;
use crate::error::{DecodeError, EncodeError, EncodeInvalidField};
use crate::v5::property::Properties;

/// MQTT v5.0 UNSUBSCRIBE パケット。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsubscribe {
    /// パケット識別子。
    pub packet_id: u16,
    /// 購読解除するトピックフィルタ。
    pub topic_filters: Vec<String>,
    /// UNSUBSCRIBE プロパティ。
    pub properties: Properties,
}

impl Unsubscribe {
    const PACKET_TYPE: u8 = 0xA0;
    const FLAGS: u8 = 0x02;

    /// エンコード済みパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        let mut len = 2usize; // パケット識別子
        len = len.saturating_add(self.properties.encoded_len());
        for filter in &self.topic_filters {
            len = len.saturating_add(2).saturating_add(filter.len());
        }
        len
    }

    /// UNSUBSCRIBE パケットを `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        self.validate()?;

        let remaining_len = self.encoded_len();
        if remaining_len > VariableByteInteger::MAX as usize {
            return Err(EncodeError::PacketTooLarge {
                size: remaining_len,
                limit: VariableByteInteger::MAX as usize,
            });
        }

        let vbi = VariableByteInteger(remaining_len as u32);
        let total_len = 1usize
            .checked_add(vbi.encoded_len())
            .and_then(|x| x.checked_add(remaining_len))
            .ok_or(EncodeError::PacketTooLarge {
                size: remaining_len,
                limit: VariableByteInteger::MAX as usize,
            })?;
        if buf.len() < total_len {
            return Err(EncodeError::BufferTooSmall);
        }

        buf[0] = Self::PACKET_TYPE | Self::FLAGS;
        let mut offset = 1 + vbi.encode(&mut buf[1..])?;

        // パケット識別子
        buf[offset..offset + 2].copy_from_slice(&self.packet_id.to_be_bytes());
        offset += 2;

        // プロパティ
        offset += self.properties.encode(&mut buf[offset..])?;

        // トピックフィルタ
        for filter in &self.topic_filters {
            offset += Utf8String::encode_str(filter, &mut buf[offset..])?;
        }

        Ok(offset)
    }

    /// `buf` から UNSUBSCRIBE パケットをデコードする。
    ///
    /// デコードしたパケットと消費したバイト数を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        if buf.is_empty() {
            return Err(DecodeError::InsufficientData);
        }
        if buf[0] & 0xF0 != Self::PACKET_TYPE {
            return Err(DecodeError::InvalidPacketType);
        }
        if buf[0] & 0x0F != Self::FLAGS {
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

        // MQTT v5.0 §2.2.1 [MQTT-2.2.1-3]: Packet Identifier は 0 以外でなければならない。
        if packet_id == 0 {
            return Err(DecodeError::MalformedPacket);
        }

        // プロパティ
        let (properties, n) = Properties::decode(&buf[offset..end])?;
        properties.validate_for_unsubscribe()?;
        offset += n;

        // トピックフィルタ
        let mut topic_filters = Vec::new();
        while offset < end {
            // remaining length 内に次の Topic Filter が不完全に含まれている場合は Malformed Packet とする。
            if offset + 2 > end {
                return Err(DecodeError::MalformedPacket);
            }
            let filter_len = u16::from_be_bytes([buf[offset], buf[offset + 1]]) as usize;
            if offset + 2 + filter_len > end {
                return Err(DecodeError::MalformedPacket);
            }
            let (filter, n) = Utf8String::decode(&buf[offset..end])?;
            offset += n;
            // MQTT v5.0 §4.7.1 / MQTT v5.0 §4.8.2: Topic Filter の構文を検証する。
            if !crate::topic_filter::is_valid_v5_topic_filter(&filter.0) {
                return Err(DecodeError::MalformedPacket);
            }
            topic_filters.push(filter.0);
        }

        // MQTT v5.0 §3.10.3 [MQTT-3.10.3-2]:
        // UNSUBSCRIBE パケットの Payload は最低 1 つの Topic Filter を
        // 含まなければならない。
        if topic_filters.is_empty() {
            return Err(DecodeError::MalformedPacket);
        }

        if offset != end {
            return Err(DecodeError::MalformedPacket);
        }

        Ok((
            Self {
                packet_id,
                topic_filters,
                properties,
            },
            offset,
        ))
    }

    fn validate(&self) -> Result<(), EncodeError> {
        // MQTT v5.0 §2.2.1 [MQTT-2.2.1-3]: Packet Identifier は 0 以外でなければならない。
        if self.packet_id == 0 {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::ZeroPacketId,
            });
        }
        // MQTT v5.0 §3.10.3 [MQTT-3.10.3-2]:
        // UNSUBSCRIBE パケットの Payload は最低 1 つの Topic Filter を
        // 含まなければならない。
        if self.topic_filters.is_empty() {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::EmptyTopicFilters,
            });
        }
        // MQTT v5.0 §4.7.1 / MQTT v5.0 §4.8.2: Topic Filter の構文を検証する。
        for filter in &self.topic_filters {
            if !crate::topic_filter::is_valid_v5_topic_filter(filter) {
                return Err(EncodeError::InvalidField {
                    reason: EncodeInvalidField::InvalidTopicFilter,
                });
            }
        }
        // プロパティの許可リストを検証する。
        if self.properties.validate_for_unsubscribe().is_err() {
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
