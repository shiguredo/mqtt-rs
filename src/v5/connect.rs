//! MQTT v5.0 CONNECT パケット。
//!
//! MQTT v5.0 §3.1 を参照。

use alloc::string::String;
use alloc::vec::Vec;

use crate::codec::binary_data::BinaryData;
use crate::codec::qos::QoS;
use crate::codec::utf8_string::Utf8String;
use crate::codec::variable_byte_integer::VariableByteInteger;
use crate::error::{DecodeError, EncodeError, EncodeInvalidField};
use crate::v5::property::Properties;

/// MQTT v5.0 CONNECT パケット。
#[derive(Clone, PartialEq, Eq)]
pub struct Connect {
    /// クライアント識別子。
    pub client_id: String,
    /// クリーンセッションで開始するかどうか。
    pub clean_start: bool,
    /// キープアライブ間隔（秒）。
    pub keep_alive: u16,
    /// Connect プロパティ。
    pub properties: Properties,
    /// オプションの Will メッセージ。
    pub will: Option<Will>,
    /// オプションのユーザー名。
    pub username: Option<String>,
    /// オプションのパスワード。
    pub password: Option<Vec<u8>>,
}

impl core::fmt::Debug for Connect {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Connect")
            .field("client_id", &self.client_id)
            .field("clean_start", &self.clean_start)
            .field("keep_alive", &self.keep_alive)
            .field("properties", &self.properties)
            .field("will", &self.will)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// MQTT v5.0 Will メッセージ。
#[derive(Clone, PartialEq, Eq)]
pub struct Will {
    /// Will トピック名。
    pub topic: String,
    /// Will ペイロード。
    pub payload: Vec<u8>,
    /// Will QoS レベル。
    pub qos: QoS,
    /// Will メッセージを保持するかどうか。
    pub retain: bool,
    /// Will プロパティ。
    pub properties: Properties,
}

impl core::fmt::Debug for Will {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Will")
            .field("topic", &self.topic)
            .field("payload", &"<redacted>")
            .field("qos", &self.qos)
            .field("retain", &self.retain)
            .field("properties", &self.properties)
            .finish()
    }
}

impl Connect {
    const PACKET_TYPE: u8 = 0x10;

    /// エンコード済みパケットの残り長さ（可変ヘッダー + ペイロード）を返す。
    pub fn encoded_len(&self) -> usize {
        // プロトコル名長（2）+ "MQTT"（4）+ プロトコルレベル（1）
        // + コネクトフラグ（1）+ キープアライブ（2）
        let mut len = 2usize + 4 + 1 + 1 + 2;
        len = len.saturating_add(self.properties.encoded_len());
        len = len.saturating_add(2).saturating_add(self.client_id.len());
        if let Some(will) = &self.will {
            len = len.saturating_add(will.properties.encoded_len());
            len = len.saturating_add(2).saturating_add(will.topic.len());
            len = len.saturating_add(2).saturating_add(will.payload.len());
        }
        if let Some(username) = &self.username {
            len = len.saturating_add(2).saturating_add(username.len());
        }
        if let Some(password) = &self.password {
            len = len.saturating_add(2).saturating_add(password.len());
        }
        len
    }

    /// CONNECT パケットを `buf` にエンコードし、書き込んだバイト数を返す。
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

        buf[0] = Self::PACKET_TYPE;
        let mut offset = 1 + vbi.encode(&mut buf[1..])?;

        // プロトコル名
        offset += Utf8String::encode_str("MQTT", &mut buf[offset..])?;
        // プロトコルレベル
        buf[offset] = 0x05;
        offset += 1;
        // コネクトフラグ
        buf[offset] = self.connect_flags();
        offset += 1;
        // キープアライブ
        buf[offset..offset + 2].copy_from_slice(&self.keep_alive.to_be_bytes());
        offset += 2;
        // プロパティ
        offset += self.properties.encode(&mut buf[offset..])?;
        // クライアント識別子
        offset += Utf8String::encode_str(&self.client_id, &mut buf[offset..])?;
        // Will メッセージ
        if let Some(will) = &self.will {
            offset += will.properties.encode(&mut buf[offset..])?;
            offset += Utf8String::encode_str(&will.topic, &mut buf[offset..])?;
            offset += BinaryData::encode_slice(&will.payload, &mut buf[offset..])?;
        }
        // ユーザー名
        if let Some(username) = &self.username {
            offset += Utf8String::encode_str(username, &mut buf[offset..])?;
        }
        // パスワード
        if let Some(password) = &self.password {
            offset += BinaryData::encode_slice(password, &mut buf[offset..])?;
        }

        Ok(offset)
    }

    /// `buf` から CONNECT パケットをデコードする。
    ///
    /// デコードしたパケットと消費したバイト数を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        if buf.is_empty() {
            return Err(DecodeError::InsufficientData);
        }
        if buf[0] & 0xF0 != Self::PACKET_TYPE {
            return Err(DecodeError::InvalidPacketType);
        }
        // MQTT v5.0 §2.1.3 [MQTT-2.1.3-1]: 固定ヘッダーの予約フラグは 0 でなければならない。
        if buf[0] & 0x0F != 0 {
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

        // プロトコル名
        let (protocol_name, n) = Utf8String::decode(&buf[offset..end])
            .map_err(crate::error::malformed_if_insufficient)?;
        offset += n;
        if protocol_name.0 != "MQTT" {
            return Err(DecodeError::MalformedPacket);
        }

        // プロトコルレベル
        if offset >= end || buf[offset] != 0x05 {
            return Err(DecodeError::MalformedPacket);
        }
        offset += 1;

        // コネクトフラグ
        if offset >= end {
            return Err(DecodeError::MalformedPacket);
        }
        let flags = buf[offset];
        offset += 1;

        // キープアライブ
        if offset + 2 > end {
            return Err(DecodeError::MalformedPacket);
        }
        let keep_alive = u16::from_be_bytes([buf[offset], buf[offset + 1]]);
        offset += 2;

        // プロパティ
        let (properties, n) = Properties::decode(&buf[offset..end])?;
        properties.validate_for_connect()?;
        offset += n;

        // クライアント識別子
        let (client_id, n) = Utf8String::decode(&buf[offset..end])
            .map_err(crate::error::malformed_if_insufficient)?;
        offset += n;

        // Will メッセージ
        let will = if flags & 0x04 != 0 {
            let (will_properties, n) = Properties::decode(&buf[offset..end])?;
            will_properties.validate_for_will()?;
            offset += n;
            let (topic, n) = Utf8String::decode(&buf[offset..end])
                .map_err(crate::error::malformed_if_insufficient)?;
            offset += n;
            // MQTT v5.0 §3.3.2.1 [MQTT-3.3.2-2]:
            // Will Topic にワイルドカード文字は使用できない。
            // Will メッセージは PUBLISH として発行されるため、
            // PUBLISH のトピック名の制約が適用される。
            if topic.0.contains('+') || topic.0.contains('#') {
                return Err(DecodeError::MalformedPacket);
            }
            // MQTT v5.0 §3.3.2.1:
            // Topic Name が空文字列かつ Topic Alias なしの場合は Protocol Error。
            // Will Topic にも PUBLISH のトピック名制約が適用される。
            if topic.0.is_empty() {
                return Err(DecodeError::MalformedPacket);
            }
            let (payload, n) = BinaryData::decode(&buf[offset..end])
                .map_err(crate::error::malformed_if_insufficient)?;
            offset += n;
            Some(Will {
                topic: topic.0,
                payload: payload.0,
                qos: QoS::from_u8((flags >> 3) & 0x03).ok_or(DecodeError::MalformedPacket)?,
                retain: flags & 0x20 != 0,
                properties: will_properties,
            })
        } else {
            None
        };

        // ユーザー名
        let username = if flags & 0x80 != 0 {
            let (username, n) = Utf8String::decode(&buf[offset..end])
                .map_err(crate::error::malformed_if_insufficient)?;
            offset += n;
            Some(username.0)
        } else {
            None
        };

        // パスワード
        let password = if flags & 0x40 != 0 {
            let (password, n) = BinaryData::decode(&buf[offset..end])
                .map_err(crate::error::malformed_if_insufficient)?;
            offset += n;
            Some(password.0)
        } else {
            None
        };

        // コネクトフラグを検証する。
        // MQTT v5.0 §3.1.2.3 [MQTT-3.1.2-3]: コネクトフラグの予約ビット（bit 0）は 0 でなければならない。
        if flags & 0x01 != 0 {
            return Err(DecodeError::MalformedPacket);
        }
        // MQTT v5.0 §3.1.2.6 [MQTT-3.1.2-11] / MQTT v5.0 §3.1.2.7 [MQTT-3.1.2-13]:
        // Will Flag=0 の場合、Will QoS と Will Retain は 0 でなければならない。
        if will.is_none() && (flags & 0x38) != 0 {
            return Err(DecodeError::MalformedPacket);
        }
        // MQTT v5.0 §3.1.3.1 [MQTT-3.1.3-6]:
        // サーバーは長さ 0 バイトの Client ID の指定を許容してよく、その場合は特別扱いとして
        // 一意な Client ID をそのクライアントに割り当てなければならない
        // （MQTT v3.1.1 と異なり、空 Client ID に Clean Start を要求する規範は MQTT v5.0 にはない）。
        // 空 Client ID を受け入れるかはサーバーの裁量であるため、本ライブラリは
        // Sans-I/O のコーデックとして空 Client ID を形式エラーとせず、可否は上位層に委ねる。

        if offset != end {
            return Err(DecodeError::MalformedPacket);
        }

        Ok((
            Self {
                client_id: client_id.0,
                clean_start: flags & 0x02 != 0,
                keep_alive,
                properties,
                will,
                username,
                password,
            },
            offset,
        ))
    }

    fn validate(&self) -> Result<(), EncodeError> {
        // MQTT v5.0 §3.1.3.1 [MQTT-3.1.3-6]:
        // サーバーは長さ 0 バイトの Client ID の指定を許容してよく、その場合は特別扱いとして
        // 一意な Client ID をそのクライアントに割り当てなければならない
        // （MQTT v3.1.1 と異なり、空 Client ID に Clean Start を要求する規範は MQTT v5.0 にはない）。
        // 空 Client ID を受け入れるかはサーバーの裁量であるため、本ライブラリは
        // Sans-I/O のコーデックとして空 Client ID を形式エラーとせず、可否は上位層に委ねる。

        // MQTT v5.0 §3.3.2.1: Topic Name は空文字列にできない（Topic Alias なしの場合）。
        // Will Topic にも PUBLISH のトピック名制約が適用される。
        if let Some(will) = &self.will
            && will.topic.is_empty()
        {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::EmptyTopicName,
            });
        }
        // MQTT v5.0 §3.1.3.3 / MQTT v5.0 §3.3.2.1 [MQTT-3.3.2-2]:
        // Will Topic Name にワイルドカード文字は使用できない。
        if let Some(will) = &self.will
            && (will.topic.contains('+') || will.topic.contains('#'))
        {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::WildcardInTopicName,
            });
        }
        // プロパティの許可リストを検証する。
        if self.properties.validate_for_connect().is_err() {
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
        if let Some(will) = &self.will
            && will.properties.validate_for_will().is_err()
        {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::PropertyValidationFailed,
            });
        }
        if let Some(will) = &self.will
            && will
                .properties
                .validate_duplicate_identifiers(false)
                .is_err()
        {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::DuplicatePropertyIdentifier,
            });
        }
        // MQTT v5.0 §3.1.3.2.3 (Payload Format Indicator):
        // Will の Payload Format Indicator が 1 の場合、Will Payload は
        // Unicode 仕様（RFC 3629 で再定義）の well-formed UTF-8 で
        // なければならない。対象は UTF-8 Encoded String（MQTT v5.0 §1.5.4）ではなく
        // 生の UTF-8 データのため、`core::str::from_utf8` による
        // well-formed 検証のみを行う。
        if let Some(will) = &self.will
            && will
                .properties
                .iter()
                .any(|p| matches!(p, crate::v5::property::Property::PayloadFormatIndicator(1)))
            && core::str::from_utf8(&will.payload).is_err()
        {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::InvalidPayloadUtf8,
            });
        }
        Ok(())
    }

    fn connect_flags(&self) -> u8 {
        let mut flags = 0u8;
        if self.clean_start {
            flags |= 0x02;
        }
        if let Some(will) = &self.will {
            flags |= 0x04;
            flags |= will.qos.as_u8() << 3;
            if will.retain {
                flags |= 0x20;
            }
        }
        if self.password.is_some() {
            flags |= 0x40;
        }
        if self.username.is_some() {
            flags |= 0x80;
        }
        flags
    }
}
