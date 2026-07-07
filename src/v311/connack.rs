//! MQTT v3.1.1 CONNACK パケット。
//!
//! MQTT v3.1.1 §3.2 を参照。

use crate::codec::variable_byte_integer::VariableByteInteger;
use crate::error::{DecodeError, EncodeError, EncodeInvalidField};

/// MQTT v3.1.1 CONNACK パケット。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnAck {
    /// サーバーが既存のセッションを使用しているかどうか。
    pub session_present: bool,
    /// 接続試行の結果を示すリターンコード。
    pub return_code: ConnectReturnCode,
}

/// MQTT v3.1.1 Connect Return Code。
///
/// MQTT v3.1.1 §3.2.2.3 を参照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnectReturnCode {
    /// 0x00: 接続を受け入れた。
    Accepted = 0x00,
    /// 0x01: 接続を拒否した。プロトコルバージョンが受け入れられない。
    UnacceptableProtocolVersion = 0x01,
    /// 0x02: 接続を拒否した。識別子が拒否された。
    IdentifierRejected = 0x02,
    /// 0x03: 接続を拒否した。サーバーが利用できない。
    ServerUnavailable = 0x03,
    /// 0x04: 接続を拒否した。ユーザー名またはパスワードが不正。
    BadUserNameOrPassword = 0x04,
    /// 0x05: 接続を拒否した。認可されていない。
    NotAuthorized = 0x05,
}

impl ConnectReturnCode {
    /// 数値から `ConnectReturnCode` を作成する。
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(Self::Accepted),
            0x01 => Some(Self::UnacceptableProtocolVersion),
            0x02 => Some(Self::IdentifierRejected),
            0x03 => Some(Self::ServerUnavailable),
            0x04 => Some(Self::BadUserNameOrPassword),
            0x05 => Some(Self::NotAuthorized),
            _ => None,
        }
    }

    /// このリターンコードの数値を返す。
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

impl ConnAck {
    const PACKET_TYPE: u8 = 0x20;

    /// エンコード後のパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        // Connect Acknowledge Flags (1 バイト) + Connect Return Code (1 バイト)
        2
    }

    /// CONNACK パケットを `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        // MQTT v3.1.1 §3.2.2.2 [MQTT-3.2.2-4]:
        // 非ゼロのリターンコードを返す場合、Session Present は 0 でなければならない。
        // したがって Session Present が true の場合、Return Code は Accepted でなければならない。
        if self.session_present && self.return_code != ConnectReturnCode::Accepted {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::SessionPresentWithNonSuccess,
            });
        }

        // MQTT v3.1.1 の CONNACK の Remaining Length は 2 バイト固定のため、
        // サイズ超過は起こり得ない。
        let remaining_len = self.encoded_len();
        let vbi = VariableByteInteger(remaining_len as u32);
        let total_len = 1 + vbi.encoded_len() + remaining_len;
        if buf.len() < total_len {
            return Err(EncodeError::BufferTooSmall);
        }

        buf[0] = Self::PACKET_TYPE;
        let mut offset = 1 + vbi.encode(&mut buf[1..])?;

        // 接続確認応答フラグ
        buf[offset] = if self.session_present { 0x01 } else { 0x00 };
        offset += 1;

        // 接続リターンコード
        buf[offset] = self.return_code.as_u8();
        offset += 1;

        Ok(offset)
    }

    /// `buf` から CONNACK パケットをデコードする。
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

        // 接続確認応答フラグ
        if offset >= end {
            return Err(DecodeError::MalformedPacket);
        }
        let flags = buf[offset];
        offset += 1;

        if flags & 0xFE != 0 {
            return Err(DecodeError::MalformedPacket);
        }
        let session_present = flags & 0x01 != 0;

        // 接続リターンコード
        if offset >= end {
            return Err(DecodeError::MalformedPacket);
        }
        let return_code =
            ConnectReturnCode::from_u8(buf[offset]).ok_or(DecodeError::MalformedPacket)?;
        offset += 1;

        // MQTT v3.1.1 §3.2.2.2 [MQTT-3.2.2-4]:
        // Session Present が true の場合、リターンコードは Accepted でなければならない。
        if return_code != ConnectReturnCode::Accepted && session_present {
            return Err(DecodeError::MalformedPacket);
        }

        // 残り長さ分を全て消費したことを検証する。
        if offset != end {
            return Err(DecodeError::MalformedPacket);
        }

        Ok((
            Self {
                session_present,
                return_code,
            },
            offset,
        ))
    }
}
