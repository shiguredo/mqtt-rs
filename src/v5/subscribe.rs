//! MQTT v5.0 SUBSCRIBE パケット。
//!
//! MQTT v5.0 §3.8 を参照。

use alloc::string::String;
use alloc::vec::Vec;

use crate::codec::qos::QoS;
use crate::codec::utf8_string::Utf8String;
use crate::codec::variable_byte_integer::VariableByteInteger;
use crate::error::{DecodeError, EncodeError, EncodeInvalidField};
use crate::v5::property::Properties;

/// MQTT v5.0 SUBSCRIBE パケット。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    /// パケット識別子。
    pub packet_id: u16,
    /// サブスクリプション一覧。
    pub subscriptions: Vec<Subscription>,
    /// SUBSCRIBE プロパティ。
    pub properties: Properties,
}

/// 保持メッセージの扱いオプション。
///
/// MQTT v5.0 §3.8.3.1 に定義された値に対応する。
/// 既定値は `SendRetained`（値 0）で、
/// MQTT v5.0 §3.3.1.3 [MQTT-3.3.1-9] が定める Retain Handling = 0 の挙動
/// （サブスクリプション確立時に保持メッセージを送信する）に対応する。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RetainHandling {
    /// サブスクリプションが確立したときに保持メッセージを送信する（0）。
    #[default]
    SendRetained = 0,
    /// サブスクリプションが存在しない場合にのみ保持メッセージを送信する（1）。
    SendRetainedIfNotExists = 1,
    /// サブスクリプション確立時に保持メッセージを送信しない（2）。
    DoNotSendRetained = 2,
}

impl RetainHandling {
    /// 数値から `RetainHandling` を返す。無効な値の場合は `None` を返す。
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::SendRetained),
            1 => Some(Self::SendRetainedIfNotExists),
            2 => Some(Self::DoNotSendRetained),
            _ => None,
        }
    }

    /// `RetainHandling` を対応する数値に変換する。
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// SUBSCRIBE パケット内の 1 つのサブスクリプション項目。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    /// トピックフィルタ。
    pub topic_filter: String,
    /// 要求する最大 QoS。
    pub qos: QoS,
    /// No Local サブスクリプションかどうか。
    pub no_local: bool,
    /// 保持メッセージを RETAIN フラグ付きで送信するかどうか。
    pub retain_as_published: bool,
    /// 保持メッセージの扱いオプション。
    pub retain_handling: RetainHandling,
}

impl Subscribe {
    const PACKET_TYPE: u8 = 0x80;
    const FLAGS: u8 = 0x02;

    /// エンコード済みパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        let mut len = 2usize; // パケット識別子
        len = len.saturating_add(self.properties.encoded_len());
        for sub in &self.subscriptions {
            len = len.saturating_add(2).saturating_add(sub.topic_filter.len());
            len = len.saturating_add(1); // サブスクリプションオプション
        }
        len
    }

    /// SUBSCRIBE パケットを `buf` にエンコードし、書き込んだバイト数を返す。
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

        // サブスクリプション
        for sub in &self.subscriptions {
            offset += Utf8String::encode_str(&sub.topic_filter, &mut buf[offset..])?;
            buf[offset] = sub.options_byte();
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
        properties.validate_for_subscribe()?;
        offset += n;

        // サブスクリプション
        let mut subscriptions = Vec::new();
        while offset < end {
            let (topic_filter, n) = Utf8String::decode(&buf[offset..end])
                .map_err(crate::error::malformed_if_insufficient)?;
            offset += n;

            // MQTT v5.0 §4.7.1 / MQTT v5.0 §4.8.2: Topic Filter の構文を検証する。
            if !crate::topic_filter::is_valid_v5_topic_filter(&topic_filter.0) {
                return Err(DecodeError::MalformedPacket);
            }

            if offset >= end {
                return Err(DecodeError::MalformedPacket);
            }
            let options = buf[offset];
            offset += 1;

            let qos = QoS::from_u8(options & 0x03).ok_or(DecodeError::MalformedPacket)?;
            let no_local = options & 0x04 != 0;
            let retain_as_published = options & 0x08 != 0;
            let retain_handling = RetainHandling::from_u8((options >> 4) & 0x03)
                .ok_or(DecodeError::MalformedPacket)?;

            // MQTT v5.0 §3.8.3.1 [MQTT-3.8.3-5]:
            // 予約ビット（6、7 ビット目）が立っている場合は Malformed Packet である。
            if options & 0xC0 != 0 {
                return Err(DecodeError::MalformedPacket);
            }

            // MQTT v5.0 §3.8.3.1 [MQTT-3.8.3-4]:
            // 共有サブスクリプションで No Local ビットを 1 にすることは Protocol Error である。
            if no_local && topic_filter.0.starts_with("$share/") {
                return Err(DecodeError::MalformedPacket);
            }

            subscriptions.push(Subscription {
                topic_filter: topic_filter.0,
                qos,
                no_local,
                retain_as_published,
                retain_handling,
            });
        }

        // MQTT v5.0 §3.8.3 [MQTT-3.8.3-2]:
        // Payload は最低 1 組の Topic Filter と Subscription Options の
        // ペアを含まなければならない。
        if subscriptions.is_empty() {
            return Err(DecodeError::MalformedPacket);
        }

        if offset != end {
            return Err(DecodeError::MalformedPacket);
        }

        Ok((
            Self {
                packet_id,
                subscriptions,
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
        // MQTT v5.0 §3.8.3 [MQTT-3.8.3-2]:
        // Payload は最低 1 組の Topic Filter と Subscription Options の
        // ペアを含まなければならない。
        if self.subscriptions.is_empty() {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::EmptySubscriptions,
            });
        }
        for sub in &self.subscriptions {
            // MQTT v5.0 §4.7.1 / MQTT v5.0 §4.8.2: Topic Filter の構文を検証する。
            if !crate::topic_filter::is_valid_v5_topic_filter(&sub.topic_filter) {
                return Err(EncodeError::InvalidField {
                    reason: EncodeInvalidField::InvalidTopicFilter,
                });
            }
            // MQTT v5.0 §3.8.3.1 [MQTT-3.8.3-4]:
            // 共有サブスクリプションで No Local ビットを 1 にすることは Protocol Error である。
            if sub.no_local && sub.topic_filter.starts_with("$share/") {
                return Err(EncodeError::InvalidField {
                    reason: EncodeInvalidField::SharedSubscriptionNoLocal,
                });
            }
        }
        // プロパティの許可リストを検証する。
        if self.properties.validate_for_subscribe().is_err() {
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

impl Subscription {
    fn options_byte(&self) -> u8 {
        let mut options = self.qos.as_u8();
        if self.no_local {
            options |= 0x04;
        }
        if self.retain_as_published {
            options |= 0x08;
        }
        options |= self.retain_handling.as_u8() << 4;
        options
    }
}

impl From<crate::state::subscribe::SubscriptionEntry> for Subscription {
    fn from(entry: crate::state::subscribe::SubscriptionEntry) -> Self {
        Self {
            topic_filter: entry.topic_filter,
            qos: entry.requested_qos,
            no_local: entry.no_local,
            retain_as_published: entry.retain_as_published,
            retain_handling: entry.retain_handling,
        }
    }
}
