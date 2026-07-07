//! MQTT v5.0 PUBLISH パケット。
//!
//! MQTT v5.0 §3.3 を参照。

use alloc::string::String;
use alloc::vec::Vec;

use crate::codec::qos::QoS;
use crate::codec::utf8_string::Utf8String;
use crate::codec::variable_byte_integer::VariableByteInteger;
use crate::error::{DecodeError, EncodeError, EncodeInvalidField};
use crate::v5::property::PacketDirection;
use crate::v5::property::Properties;

/// MQTT v5.0 PUBLISH パケット。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    /// 重複配送かどうか。
    pub dup: bool,
    /// QoS レベル。
    pub qos: QoS,
    /// メッセージを保持するかどうか。
    pub retain: bool,
    /// トピック名。
    pub topic: String,
    /// パケット識別子。QoS が 0 より大きい場合に存在する。
    pub packet_id: Option<u16>,
    /// PUBLISH プロパティ。
    pub properties: Properties,
    /// アプリケーションペイロード。
    pub payload: Vec<u8>,
}

impl Publish {
    const PACKET_TYPE: u8 = 0x30;

    /// エンコード済みパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        let mut len = 2usize.saturating_add(self.topic.len());
        if self.qos != QoS::AtMostOnce {
            len = len.saturating_add(2);
        }
        len = len.saturating_add(self.properties.encoded_len());
        len = len.saturating_add(self.payload.len());
        len
    }

    /// PUBLISH パケットを `buf` にエンコードし、書き込んだバイト数を返す。
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

        let mut flags = self.qos.as_u8() << 1;
        if self.dup {
            flags |= 0x08;
        }
        if self.retain {
            flags |= 0x01;
        }

        buf[0] = Self::PACKET_TYPE | flags;
        let mut offset = 1 + vbi.encode(&mut buf[1..])?;

        // トピック名
        offset += Utf8String::encode_str(&self.topic, &mut buf[offset..])?;

        // パケット識別子
        if let Some(packet_id) = self.packet_id {
            buf[offset..offset + 2].copy_from_slice(&packet_id.to_be_bytes());
            offset += 2;
        }

        // プロパティ
        offset += self.properties.encode(&mut buf[offset..])?;

        // ペイロード
        buf[offset..offset + self.payload.len()].copy_from_slice(&self.payload);
        offset += self.payload.len();

        Ok(offset)
    }

    /// `buf` から PUBLISH パケットをデコードする。
    ///
    /// デコードしたパケットと消費したバイト数を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        if buf.is_empty() {
            return Err(DecodeError::InsufficientData);
        }
        if buf[0] & 0xF0 != Self::PACKET_TYPE {
            return Err(DecodeError::InvalidPacketType);
        }

        let flags = buf[0] & 0x0F;
        // MQTT v5.0 §3.3.1.2 [MQTT-3.3.1-4]: QoS ビットが両方 1 の PUBLISH は Malformed Packet。
        let qos = QoS::from_u8((flags >> 1) & 0x03).ok_or(DecodeError::InvalidPacketFlags)?;
        let dup = flags & 0x08 != 0;
        let retain = flags & 0x01 != 0;

        // MQTT v5.0 §3.3.1.1 [MQTT-3.3.1-2]: QoS 0 では DUP は 0 でなければならない。
        if qos == QoS::AtMostOnce && dup {
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

        // トピック名
        let (topic, n) = Utf8String::decode(&buf[offset..end])
            .map_err(crate::error::malformed_if_insufficient)?;
        offset += n;

        // パケット識別子
        // MQTT v5.0 §3.3.2.2:
        // QoS 0 の PUBLISH には Packet Identifier が存在しない。
        let packet_id = if qos != QoS::AtMostOnce {
            if offset + 2 > end {
                return Err(DecodeError::MalformedPacket);
            }
            let id = u16::from_be_bytes([buf[offset], buf[offset + 1]]);
            // MQTT v5.0 §2.2.1 [MQTT-2.2.1-3]: Packet Identifier は 0 以外でなければならない。
            if id == 0 {
                return Err(DecodeError::MalformedPacket);
            }
            offset += 2;
            Some(id)
        } else {
            None
        };

        // プロパティ
        let (properties, n) = Properties::decode_for_publish(&buf[offset..end])?;
        offset += n;

        // MQTT v5.0 §3.3.2.3.4 (Topic Alias):
        // Topic Alias が設定済みの場合は空のトピック名が許可される。
        let topic_name = topic.0;
        let has_topic_alias = properties
            .iter()
            .any(|p| matches!(p, crate::v5::property::Property::TopicAlias(_)));
        if topic_name.is_empty() && !has_topic_alias {
            return Err(DecodeError::MalformedPacket);
        }
        // MQTT v5.0 §3.3.2.1 [MQTT-3.3.2-2]:
        // PUBLISH の Topic Name にワイルドカード文字を含めてはならない。
        if topic_name.contains('+') || topic_name.contains('#') {
            return Err(DecodeError::MalformedPacket);
        }

        // Server → Client 方向のプロパティ制約を検証する。
        properties.validate_for_publish_with_direction(PacketDirection::ServerToClient)?;

        // ペイロード
        let payload = buf[offset..end].to_vec();
        offset = end;

        Ok((
            Self {
                dup,
                qos,
                retain,
                topic: topic_name,
                packet_id,
                properties,
                payload,
            },
            offset,
        ))
    }

    fn validate(&self) -> Result<(), EncodeError> {
        // MQTT v5.0 §3.3.2.1:
        // Topic Name が空文字列かつ Topic Alias なしの場合は Protocol Error。
        if self.topic.is_empty()
            && !self
                .properties
                .iter()
                .any(|p| matches!(p, crate::v5::property::Property::TopicAlias(_)))
        {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::EmptyTopicName,
            });
        }
        // MQTT v5.0 §3.3.2.1 [MQTT-3.3.2-2]:
        // PUBLISH の Topic Name にワイルドカード文字を含めてはならない。
        if self.topic.contains('+') || self.topic.contains('#') {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::WildcardInTopicName,
            });
        }
        if self.qos != QoS::AtMostOnce && self.packet_id.is_none() {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::MissingPacketId,
            });
        }
        // MQTT v5.0 §3.3.2.2:
        // QoS 0 の PUBLISH には Packet Identifier が存在しない。
        if self.qos == QoS::AtMostOnce && self.packet_id.is_some() {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::UnexpectedPacketId,
            });
        }
        // MQTT v5.0 §2.2.1 [MQTT-2.2.1-3]: Packet Identifier は 0 以外でなければならない。
        if let Some(0) = self.packet_id {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::ZeroPacketId,
            });
        }
        // MQTT v5.0 §3.3.1.1 [MQTT-3.3.1-2]: QoS 0 では DUP は 0 でなければならない。
        if self.qos == QoS::AtMostOnce && self.dup {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::DupWithQos0,
            });
        }
        // プロパティの許可リストを検証する。
        // Client → Server 方向では Subscription Identifier (0x0B) を含んではならない。
        if self
            .properties
            .validate_for_publish_with_direction(PacketDirection::ClientToServer)
            .is_err()
        {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::PropertyValidationFailed,
            });
        }
        if self
            .properties
            .validate_duplicate_identifiers(true)
            .is_err()
        {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::DuplicatePropertyIdentifier,
            });
        }
        // MQTT v5.0 §3.3.2.3.2 (Payload Format Indicator):
        // Payload Format Indicator が 1 の場合、Payload は Unicode 仕様
        // （RFC 3629 で再定義）の well-formed UTF-8 でなければならない。
        // 対象は UTF-8 Encoded String（MQTT v5.0 §1.5.4）ではなく生の UTF-8 データのため、
        // `core::str::from_utf8` による well-formed 検証のみを行う。
        if self
            .properties
            .iter()
            .any(|p| matches!(p, crate::v5::property::Property::PayloadFormatIndicator(1)))
            && core::str::from_utf8(&self.payload).is_err()
        {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::InvalidPayloadUtf8,
            });
        }
        Ok(())
    }
}
