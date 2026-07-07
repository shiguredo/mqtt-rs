//! MQTT v3.1.1 CONNECT パケット。
//!
//! MQTT v3.1.1 §3.1 を参照。

use alloc::string::String;
use alloc::vec::Vec;

use crate::codec::binary_data::BinaryData;
use crate::codec::qos::QoS;
use crate::codec::utf8_string::Utf8String;
use crate::codec::variable_byte_integer::VariableByteInteger;
use crate::error::{DecodeError, EncodeError, EncodeInvalidField};

/// MQTT v3.1.1 CONNECT パケット。
#[derive(Clone, PartialEq, Eq)]
pub struct Connect {
    /// クライアント識別子。
    pub client_id: String,
    /// セッションをクリーンに開始するかどうか。
    pub clean_session: bool,
    /// キープアライブ間隔（秒）。
    pub keep_alive: u16,
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
            .field("clean_session", &self.clean_session)
            .field("keep_alive", &self.keep_alive)
            .field("will", &self.will)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// MQTT v3.1.1 Will メッセージ。
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
}

impl core::fmt::Debug for Will {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Will")
            .field("topic", &self.topic)
            .field("payload", &"<redacted>")
            .field("qos", &self.qos)
            .field("retain", &self.retain)
            .finish()
    }
}

impl Connect {
    const PACKET_TYPE: u8 = 0x10;

    /// エンコード後のパケットの残り長さを返す（可変ヘッダー + ペイロード）。
    pub fn encoded_len(&self) -> usize {
        // プロトコル名長 (2 バイト) + "MQTT" (4 バイト) + プロトコルレベル (1 バイト)
        // + Connect Flags (1 バイト) + Keep Alive (2 バイト)
        let mut len = 2usize + 4 + 1 + 1 + 2;
        len = len.saturating_add(2).saturating_add(self.client_id.len());
        if let Some(will) = &self.will {
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
        let total_len = 1 + vbi.encoded_len() + remaining_len;
        if buf.len() < total_len {
            return Err(EncodeError::BufferTooSmall);
        }

        buf[0] = Self::PACKET_TYPE;
        let mut offset = 1 + vbi.encode(&mut buf[1..])?;

        // プロトコル名
        offset += Utf8String::encode_str("MQTT", &mut buf[offset..])?;
        // プロトコルレベル
        buf[offset] = 0x04;
        offset += 1;
        // 接続フラグ
        buf[offset] = self.connect_flags();
        offset += 1;
        // キープアライブ
        buf[offset..offset + 2].copy_from_slice(&self.keep_alive.to_be_bytes());
        offset += 2;
        // クライアント識別子
        offset += Utf8String::encode_str(&self.client_id, &mut buf[offset..])?;
        // Will
        if let Some(will) = &self.will {
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
        if offset >= end || buf[offset] != 0x04 {
            return Err(DecodeError::MalformedPacket);
        }
        offset += 1;

        // 接続フラグ
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

        // クライアント識別子
        let (client_id, n) = Utf8String::decode(&buf[offset..end])
            .map_err(crate::error::malformed_if_insufficient)?;
        offset += n;

        // 予約フラグを先に検証し、最小限のパケットにも適用されるようにする。
        if flags & 0x01 != 0 {
            return Err(DecodeError::MalformedPacket);
        }

        // Will
        let will = if flags & 0x04 != 0 {
            let (topic, n) = Utf8String::decode(&buf[offset..end])
                .map_err(crate::error::malformed_if_insufficient)?;
            offset += n;
            // MQTT v3.1.1 §3.1.2.5 [MQTT-3.1.2-8] / MQTT v3.1.1 §3.1.3.2 / MQTT v3.1.1 §3.3.2.1 [MQTT-3.3.2-2] / MQTT v3.1.1 §4.7.3 [MQTT-4.7.3-1]:
            // Will Message は PUBLISH として発行されるため、Will Topic にも
            // PUBLISH の Topic Name と同じくワイルドカード文字を含めてはならない。
            if topic.0.contains('+') || topic.0.contains('#') {
                return Err(DecodeError::MalformedPacket);
            }
            // Will Topic は空にできない。
            // PUBLISH のトピック名制約と一致させる。
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

        // フラグの整合性を検証する。
        if password.is_some() && username.is_none() {
            return Err(DecodeError::MalformedPacket);
        }
        if will.is_none() && (flags & 0x38) != 0 {
            return Err(DecodeError::MalformedPacket);
        }
        // MQTT v3.1.1 §3.1.3.1 [MQTT-3.1.3-7]:
        // 長さ 0 の ClientId を供給する場合、CleanSession を 1 に設定しなければならない。
        // 違反時の規定処置はサーバーが CONNACK 0x02 (Identifier rejected) を返して
        // 接続を閉じること（MQTT v3.1.1 §3.1.3.1 [MQTT-3.1.3-8]）だが、本ライブラリはクライアント専用で
        // CONNECT の decode はサーバー用途に使われないため、専用のエラー種別は設けず
        // MalformedPacket として拒否する。
        if client_id.0.is_empty() && flags & 0x02 == 0 {
            return Err(DecodeError::MalformedPacket);
        }

        // 残り長さ分を全て消費したことを検証する。
        if offset != end {
            return Err(DecodeError::MalformedPacket);
        }

        Ok((
            Self {
                client_id: client_id.0,
                clean_session: flags & 0x02 != 0,
                keep_alive,
                will,
                username,
                password,
            },
            offset,
        ))
    }

    fn validate(&self) -> Result<(), EncodeError> {
        if self.password.is_some() && self.username.is_none() {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::PasswordWithoutUsername,
            });
        }
        // MQTT v3.1.1 §3.1.3.1 [MQTT-3.1.3-7]:
        // 長さ 0 の ClientId を供給する場合、CleanSession を 1 に設定しなければならない。
        // クライアントに対する無条件の MUST であり、この組み合わせはプロトコル違反である
        // （v5.0 には相当する規定が無く、割り当てた識別子を CONNACK の
        // Assigned Client Identifier で通知できるため許容される）。
        if self.client_id.is_empty() && !self.clean_session {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::EmptyClientIdWithoutCleanSession,
            });
        }
        // MQTT v3.1.1 §3.1.2.5 [MQTT-3.1.2-8] / MQTT v3.1.1 §3.1.3.2 / MQTT v3.1.1 §3.3.2.1 [MQTT-3.3.2-2] / MQTT v3.1.1 §4.7.3 [MQTT-4.7.3-1]:
        // Will Topic は PUBLISH の Topic Name 制約を受け、ワイルドカード文字を含めず、1 文字以上でなければならない。
        // 空とワイルドカードは異なる違反として区別するため条件を分割する。
        if let Some(will) = &self.will {
            if will.topic.is_empty() {
                return Err(EncodeError::InvalidField {
                    reason: EncodeInvalidField::EmptyTopicName,
                });
            }
            if will.topic.contains('+') || will.topic.contains('#') {
                return Err(EncodeError::InvalidField {
                    reason: EncodeInvalidField::WildcardInTopicName,
                });
            }
        }
        Ok(())
    }

    fn connect_flags(&self) -> u8 {
        let mut flags = 0u8;
        if self.clean_session {
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
