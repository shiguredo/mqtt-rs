//! MQTT v5.0 AUTH パケット。
//!
//! MQTT v5.0 §3.15 を参照。

use crate::error::{DecodeError, EncodeError, EncodeInvalidField};
use crate::v5::ack_helper;
use crate::v5::property::{Properties, Property};

/// MQTT v5.0 AUTH パケット。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Auth {
    /// 理由コード。
    pub reason_code: AuthReasonCode,
    /// AUTH プロパティ。
    pub properties: Properties,
}

/// MQTT v5.0 AUTH 理由コード。
///
/// MQTT v5.0 §3.15.2.1 を参照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AuthReasonCode {
    /// 0x00
    Success = 0x00,
    /// 0x18
    ContinueAuthentication = 0x18,
    /// 0x19
    ReAuthenticate = 0x19,
}

impl AuthReasonCode {
    /// 数値から `AuthReasonCode` を作成する。
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(Self::Success),
            0x18 => Some(Self::ContinueAuthentication),
            0x19 => Some(Self::ReAuthenticate),
            _ => None,
        }
    }

    /// この理由コードの数値を返す。
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// クライアントが送信できる Reason Code かどうかを返す。
    ///
    /// MQTT v5.0 §3.15.2.1 Table 3-11 の「Sent by」列に基づく。
    /// 0x00 (Success) は Server のみ、0x18 (Continue authentication) は Client or Server、
    /// 0x19 (Re-authenticate) は Client のみ。
    fn is_sent_by_client(self) -> bool {
        matches!(self, Self::ContinueAuthentication | Self::ReAuthenticate)
    }

    /// サーバーが送信できる Reason Code かどうかを返す。
    ///
    /// MQTT v5.0 §3.15.2.1 Table 3-11 の「Sent by」列に基づく。
    fn is_sent_by_server(self) -> bool {
        matches!(self, Self::Success | Self::ContinueAuthentication)
    }
}

impl Auth {
    const PACKET_TYPE: u8 = 0xF0;
    const FLAGS: u8 = 0;

    /// エンコード済みパケットの残り長さを返す。
    pub fn encoded_len(&self) -> usize {
        ack_helper::reason_code_only_packet_encoded_len(self.reason_code.as_u8(), &self.properties)
    }

    /// Authentication Method プロパティの値を返す。
    ///
    /// MQTT v5.0 §3.15.2.2.2 の Authentication Method (プロパティ識別子 0x15) を
    /// `Properties` から取り出す。見つからない場合は `None` を返す。
    /// `Session::auth_received` に渡すことで、認証方式の一致検証
    /// (MQTT v5.0 §4.12 [MQTT-4.12.0-5]) を行える。
    pub fn authentication_method(&self) -> Option<&str> {
        self.properties.iter().find_map(|prop| match prop {
            Property::AuthenticationMethod(method) => Some(method.as_str()),
            _ => None,
        })
    }

    /// AUTH パケットを `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        self.validate()?;

        ack_helper::encode_reason_code_only_packet(
            buf,
            Self::PACKET_TYPE,
            Self::FLAGS,
            self.reason_code.as_u8(),
            &self.properties,
        )
    }

    fn validate(&self) -> Result<(), EncodeError> {
        // encode は Client → Server 方向のため、クライアントが送れない Reason Code は拒否する。
        // MQTT v5.0 §3.15.2.1 Table 3-11 の「Sent by」列を参照。
        if !self.reason_code.is_sent_by_client() {
            return Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::InvalidReasonCode,
            });
        }
        if self.properties.validate_for_auth().is_err() {
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

    /// `buf` から AUTH パケットをデコードする。
    ///
    /// デコードしたパケットと消費したバイト数を返す。
    /// Server → Client 方向 (クライアント受信) を前提とする。
    ///
    /// MQTT v5.0 §3.15.2.1 末尾に従い Remaining Length 0 の省略形
    /// (Reason Code Success・プロパティなし) を受理する。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        // 共通ヘルパー `ack_helper::decode_reason_code_only_packet` を経由すると
        // 省略形と中間形の判定順を制御しにくいため、固定ヘッダー検証だけを共通化し
        // 以降は自前でデコードする。
        //
        // 自前デコードの目的は省略形受理と中間形拒否の分岐を先に済ませることである。
        // 方向検証を Properties::decode より前に置く短絡効果は、後続の方向検証コメントを参照。
        let (remaining_len, header_len, end) =
            ack_helper::decode_fixed_header(buf, Self::PACKET_TYPE, Self::FLAGS)?;

        // MQTT v5.0 §3.15.2.1 末尾は Reason Code と Property Length の並列主語で
        // 「両者を同時に省略した場合のみ Remaining Length 0」を許容する。
        // 省略形はプロパティ集合が物理的に存在しないため MQTT v5.0 §2.2.2.1 [MQTT-2.2.2-1]
        // (空である旨は Property Length 0 で示す) の適用外となり、
        // 同節の連鎖として MQTT v5.0 §3.15.2.2.2 の Authentication Method 必須も適用外と解釈する。
        // Reason Code 0x00 Success の Sent by は Server (MQTT v5.0 §3.15.2.1 Table 3-11) のため
        // 省略形の合成値は方向検証を通過することが自明で、方向検証を省略しても安全。
        if remaining_len == 0 {
            debug_assert!(AuthReasonCode::Success.is_sent_by_server());
            return Ok((
                Self {
                    reason_code: AuthReasonCode::Success,
                    properties: Properties::new(),
                },
                end,
            ));
        }

        // `decode_fixed_header` が `buf.len() >= end` を保証するため境界チェックは不要。
        let reason_code =
            AuthReasonCode::from_u8(buf[header_len]).ok_or(DecodeError::MalformedPacket)?;

        // decode は Server → Client 方向のため、サーバーが送れない Reason Code は拒否する。
        // MQTT v5.0 §3.15.2.1 Table 3-11 の「Sent by」列を参照。
        // 方向検証を `Properties::decode` より前に置くことで、方向不正の Reason Code に対する
        // Properties デコードを短絡させる。
        if !reason_code.is_sent_by_server() {
            return Err(DecodeError::MalformedPacket);
        }

        // 中間形 (Reason Code のみで Property Length フィールドを持たない形) は
        // MQTT v5.0 §3.15.2.1 末尾の許容対象外だが、プロパティ空とみなして
        // 直後の `validate_for_auth()` で Method 欠落として拒否する 2 段構成にする。
        // `Properties::decode(&[])` は空スライスを `MalformedPacket` として返すため、
        // 中間形ではヘルパーを呼ばず自前で空 Properties を組み立てる。
        let properties_offset = header_len + 1;
        let (properties, n) = if properties_offset == end {
            (Properties::new(), 0)
        } else {
            Properties::decode(&buf[properties_offset..end])?
        };

        // 末尾に余分バイトが残る場合は破損として拒否する。
        if properties_offset + n != end {
            return Err(DecodeError::MalformedPacket);
        }

        properties.validate_for_auth()?;

        Ok((
            Self {
                reason_code,
                properties,
            },
            end,
        ))
    }
}
