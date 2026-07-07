//! MQTT v3.1.1 PUBLISH パケット。
//!
//! MQTT v3.1.1 §3.3 を参照。

use alloc::string::String;
use alloc::vec::Vec;

use crate::codec::qos::QoS;
use crate::codec::utf8_string::Utf8String;
use crate::codec::variable_byte_integer::VariableByteInteger;
use crate::error::{DecodeError, EncodeError, EncodeInvalidField};

/// MQTT v3.1.1 PUBLISH パケット。
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
    /// アプリケーションペイロード。
    pub payload: Vec<u8>,
}

impl Publish {
    const PACKET_TYPE: u8 = 0x30;

    /// エンコード後のパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        // トピック名長プリフィックス (2 バイト) + トピックバイト
        let mut len = 2usize.saturating_add(self.topic.len());
        if self.qos != QoS::AtMostOnce {
            len = len.saturating_add(2);
        }
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
        let total_len = 1 + vbi.encoded_len() + remaining_len;
        if buf.len() < total_len {
            return Err(EncodeError::BufferTooSmall);
        }

        buf[0] = Self::PACKET_TYPE | self.flags();
        let mut offset = 1 + vbi.encode(&mut buf[1..])?;

        // トピック名
        offset += Utf8String::encode_str(&self.topic, &mut buf[offset..])?;

        // パケット識別子
        if self.qos != QoS::AtMostOnce {
            if let Some(packet_id) = self.packet_id {
                buf[offset..offset + 2].copy_from_slice(&packet_id.to_be_bytes());
                offset += 2;
            } else {
                // validate() で必ず拒否されるため、ここに到達することはない。
                return Err(EncodeError::InvalidField {
                    reason: EncodeInvalidField::MissingPacketId,
                });
            }
        }

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
        let packet_type = buf[0] & 0xF0;
        if packet_type != Self::PACKET_TYPE {
            return Err(DecodeError::InvalidPacketType);
        }
        let flags = buf[0] & 0x0F;

        let dup = flags & 0x08 != 0;
        let qos = QoS::from_u8((flags >> 1) & 0x03).ok_or(DecodeError::InvalidPacketFlags)?;
        let retain = flags & 0x01 != 0;

        // フラグの組み合わせを検証する。
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

        // MQTT v3.1.1 §4.7.3 [MQTT-4.7.3-1]: トピック名は 1 文字以上でなければならない。
        if topic.0.is_empty() {
            return Err(DecodeError::MalformedPacket);
        }
        // MQTT v3.1.1 §3.3.2.1 [MQTT-3.3.2-2]: PUBLISH のトピック名にワイルドカード文字は使用できない。
        if topic.0.contains('+') || topic.0.contains('#') {
            return Err(DecodeError::MalformedPacket);
        }

        // パケット識別子
        // MQTT v3.1.1 §3.3.2.2:
        // QoS 0 の PUBLISH には Packet Identifier が存在しない。
        let packet_id = if qos != QoS::AtMostOnce {
            if offset + 2 > end {
                return Err(DecodeError::MalformedPacket);
            }
            let packet_id = u16::from_be_bytes([buf[offset], buf[offset + 1]]);
            offset += 2;
            if packet_id == 0 {
                return Err(DecodeError::MalformedPacket);
            }
            Some(packet_id)
        } else {
            None
        };

        // ペイロード
        let payload = buf[offset..end].to_vec();
        offset = end;

        Ok((
            Self {
                dup,
                qos,
                retain,
                topic: topic.0,
                packet_id,
                payload,
            },
            offset,
        ))
    }

    fn validate(&self) -> Result<(), EncodeError> {
        // MQTT v3.1.1 §4.7.3 [MQTT-4.7.3-1]: トピック名は 1 文字以上でなければならない。
        if self.topic.is_empty() {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::EmptyTopicName,
            });
        }
        // MQTT v3.1.1 §3.3.2.1 [MQTT-3.3.2-2]: PUBLISH のトピック名にワイルドカード文字は使用できない。
        if self.topic.contains('+') || self.topic.contains('#') {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::WildcardInTopicName,
            });
        }
        if self.qos != QoS::AtMostOnce {
            match self.packet_id {
                Some(0) => {
                    return Err(EncodeError::InvalidField {
                        reason: EncodeInvalidField::ZeroPacketId,
                    });
                }
                None => {
                    return Err(EncodeError::InvalidField {
                        reason: EncodeInvalidField::MissingPacketId,
                    });
                }
                _ => {}
            }
        }
        // MQTT v3.1.1 §3.3.2.2:
        // QoS 0 の PUBLISH には Packet Identifier が存在しない。
        if self.qos == QoS::AtMostOnce && self.packet_id.is_some() {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::UnexpectedPacketId,
            });
        }
        if self.qos == QoS::AtMostOnce && self.dup {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::DupWithQos0,
            });
        }
        Ok(())
    }

    fn flags(&self) -> u8 {
        let mut flags = 0u8;
        if self.dup {
            flags |= 0x08;
        }
        flags |= self.qos.as_u8() << 1;
        if self.retain {
            flags |= 0x01;
        }
        flags
    }
}
