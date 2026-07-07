//! MQTT v5.0 プロパティのエンコードとデコード。
//!
//! MQTT v5.0 §2.2.2 を参照。

use alloc::collections::BTreeSet;
use alloc::string::String;
use alloc::vec::Vec;

use crate::codec::binary_data::BinaryData;
use crate::codec::utf8_string::Utf8String;
use crate::codec::variable_byte_integer::VariableByteInteger;
use crate::error::{DecodeError, EncodeError, EncodeInvalidField};

mod validate;

/// 1 つの MQTT v5.0 プロパティ。
#[derive(Clone, PartialEq, Eq)]
pub enum Property {
    /// 0x01: ペイロード形式指示子。
    PayloadFormatIndicator(u8),
    /// 0x02: メッセージ有効期限間隔。
    MessageExpiryInterval(u32),
    /// 0x03: コンテンツタイプ。
    ContentType(String),
    /// 0x08: 応答トピック。
    ResponseTopic(String),
    /// 0x09: 相関データ。
    CorrelationData(Vec<u8>),
    /// 0x0B: サブスクリプション識別子。
    SubscriptionIdentifier(VariableByteInteger),
    /// 0x11: セッション有効期限間隔。
    SessionExpiryInterval(u32),
    /// 0x12: 割り当てられたクライアント識別子。
    AssignedClientIdentifier(String),
    /// 0x13: サーバー キープアライブ。
    ServerKeepAlive(u16),
    /// 0x15: 認証方式。
    AuthenticationMethod(String),
    /// 0x16: 認証データ。
    AuthenticationData(Vec<u8>),
    /// 0x17: 問題情報の要求。
    RequestProblemInformation(u8),
    /// 0x18: Will 遅延間隔。
    WillDelayInterval(u32),
    /// 0x19: 応答情報の要求。
    RequestResponseInformation(u8),
    /// 0x1A: 応答情報。
    ResponseInformation(String),
    /// 0x1C: サーバー参照。
    ServerReference(String),
    /// 0x1F: 理由文字列。
    ReasonString(String),
    /// 0x21: 受信最大値。
    ReceiveMaximum(u16),
    /// 0x22: トピックエイリアス最大値。
    TopicAliasMaximum(u16),
    /// 0x23: トピックエイリアス。
    TopicAlias(u16),
    /// 0x24: 最大 QoS。
    MaximumQoS(u8),
    /// 0x25: 保持メッセージの利用可否。
    RetainAvailable(u8),
    /// 0x26: ユーザープロパティ。
    UserProperty(String, String),
    /// 0x27: 最大パケットサイズ。
    MaximumPacketSize(u32),
    /// 0x28: ワイルドカードサブスクリプションの利用可否。
    WildcardSubscriptionAvailable(u8),
    /// 0x29: サブスクリプション識別子の利用可否。
    SubscriptionIdentifierAvailable(u8),
    /// 0x2A: 共有サブスクリプションの利用可否。
    SharedSubscriptionAvailable(u8),
}

impl core::fmt::Debug for Property {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            // Authentication Data は SCRAM 等の証明材料を含み得るため平文を出さない。
            // Connect の password / Will payload と同じ方針。
            Property::AuthenticationData(_) => f
                .debug_tuple("AuthenticationData")
                .field(&"<redacted>")
                .finish(),
            Property::PayloadFormatIndicator(v) => {
                f.debug_tuple("PayloadFormatIndicator").field(v).finish()
            }
            Property::MessageExpiryInterval(v) => {
                f.debug_tuple("MessageExpiryInterval").field(v).finish()
            }
            Property::ContentType(v) => f.debug_tuple("ContentType").field(v).finish(),
            Property::ResponseTopic(v) => f.debug_tuple("ResponseTopic").field(v).finish(),
            Property::CorrelationData(v) => f.debug_tuple("CorrelationData").field(v).finish(),
            Property::SubscriptionIdentifier(v) => {
                f.debug_tuple("SubscriptionIdentifier").field(v).finish()
            }
            Property::SessionExpiryInterval(v) => {
                f.debug_tuple("SessionExpiryInterval").field(v).finish()
            }
            Property::AssignedClientIdentifier(v) => {
                f.debug_tuple("AssignedClientIdentifier").field(v).finish()
            }
            Property::ServerKeepAlive(v) => f.debug_tuple("ServerKeepAlive").field(v).finish(),
            Property::AuthenticationMethod(v) => {
                f.debug_tuple("AuthenticationMethod").field(v).finish()
            }
            Property::RequestProblemInformation(v) => {
                f.debug_tuple("RequestProblemInformation").field(v).finish()
            }
            Property::WillDelayInterval(v) => f.debug_tuple("WillDelayInterval").field(v).finish(),
            Property::RequestResponseInformation(v) => f
                .debug_tuple("RequestResponseInformation")
                .field(v)
                .finish(),
            Property::ResponseInformation(v) => {
                f.debug_tuple("ResponseInformation").field(v).finish()
            }
            Property::ServerReference(v) => f.debug_tuple("ServerReference").field(v).finish(),
            Property::ReasonString(v) => f.debug_tuple("ReasonString").field(v).finish(),
            Property::ReceiveMaximum(v) => f.debug_tuple("ReceiveMaximum").field(v).finish(),
            Property::TopicAliasMaximum(v) => f.debug_tuple("TopicAliasMaximum").field(v).finish(),
            Property::TopicAlias(v) => f.debug_tuple("TopicAlias").field(v).finish(),
            Property::MaximumQoS(v) => f.debug_tuple("MaximumQoS").field(v).finish(),
            Property::RetainAvailable(v) => f.debug_tuple("RetainAvailable").field(v).finish(),
            Property::UserProperty(k, v) => {
                f.debug_tuple("UserProperty").field(k).field(v).finish()
            }
            Property::MaximumPacketSize(v) => f.debug_tuple("MaximumPacketSize").field(v).finish(),
            Property::WildcardSubscriptionAvailable(v) => f
                .debug_tuple("WildcardSubscriptionAvailable")
                .field(v)
                .finish(),
            Property::SubscriptionIdentifierAvailable(v) => f
                .debug_tuple("SubscriptionIdentifierAvailable")
                .field(v)
                .finish(),
            Property::SharedSubscriptionAvailable(v) => f
                .debug_tuple("SharedSubscriptionAvailable")
                .field(v)
                .finish(),
        }
    }
}

impl Property {
    /// このプロパティの識別子を返す。
    pub fn identifier(&self) -> u8 {
        match self {
            Property::PayloadFormatIndicator(_) => 0x01,
            Property::MessageExpiryInterval(_) => 0x02,
            Property::ContentType(_) => 0x03,
            Property::ResponseTopic(_) => 0x08,
            Property::CorrelationData(_) => 0x09,
            Property::SubscriptionIdentifier(_) => 0x0B,
            Property::SessionExpiryInterval(_) => 0x11,
            Property::AssignedClientIdentifier(_) => 0x12,
            Property::ServerKeepAlive(_) => 0x13,
            Property::AuthenticationMethod(_) => 0x15,
            Property::AuthenticationData(_) => 0x16,
            Property::RequestProblemInformation(_) => 0x17,
            Property::WillDelayInterval(_) => 0x18,
            Property::RequestResponseInformation(_) => 0x19,
            Property::ResponseInformation(_) => 0x1A,
            Property::ServerReference(_) => 0x1C,
            Property::ReasonString(_) => 0x1F,
            Property::ReceiveMaximum(_) => 0x21,
            Property::TopicAliasMaximum(_) => 0x22,
            Property::TopicAlias(_) => 0x23,
            Property::MaximumQoS(_) => 0x24,
            Property::RetainAvailable(_) => 0x25,
            Property::UserProperty(_, _) => 0x26,
            Property::MaximumPacketSize(_) => 0x27,
            Property::WildcardSubscriptionAvailable(_) => 0x28,
            Property::SubscriptionIdentifierAvailable(_) => 0x29,
            Property::SharedSubscriptionAvailable(_) => 0x2A,
        }
    }

    /// 識別子を含むこのプロパティのエンコード後の長さを返す。
    pub fn encoded_len(&self) -> usize {
        let value_len = match self {
            Property::PayloadFormatIndicator(_)
            | Property::RequestProblemInformation(_)
            | Property::RequestResponseInformation(_)
            | Property::MaximumQoS(_)
            | Property::RetainAvailable(_)
            | Property::WildcardSubscriptionAvailable(_)
            | Property::SubscriptionIdentifierAvailable(_)
            | Property::SharedSubscriptionAvailable(_) => 1,
            Property::ServerKeepAlive(_)
            | Property::ReceiveMaximum(_)
            | Property::TopicAliasMaximum(_)
            | Property::TopicAlias(_) => 2,
            Property::MessageExpiryInterval(_)
            | Property::SessionExpiryInterval(_)
            | Property::WillDelayInterval(_)
            | Property::MaximumPacketSize(_) => 4,
            Property::ContentType(s)
            | Property::ResponseTopic(s)
            | Property::AssignedClientIdentifier(s)
            | Property::AuthenticationMethod(s)
            | Property::ResponseInformation(s)
            | Property::ServerReference(s)
            | Property::ReasonString(s) => 2usize.saturating_add(s.len()),
            Property::CorrelationData(d) | Property::AuthenticationData(d) => {
                2usize.saturating_add(d.len())
            }
            Property::SubscriptionIdentifier(v) => v.encoded_len(),
            Property::UserProperty(k, v) => 2usize
                .saturating_add(k.len())
                .saturating_add(2)
                .saturating_add(v.len()),
        };
        1usize.saturating_add(value_len)
    }

    /// プロパティを `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        self.validate_value()?;

        if buf.len() < self.encoded_len() {
            return Err(EncodeError::BufferTooSmall);
        }

        // 値をバイト 1 から書き込む。これにより識別子を先頭バイトに配置できる。
        // エンコード済みの値を上書きせずに済む。
        let (identifier, written) = match self {
            Property::PayloadFormatIndicator(v) => (0x01, encode_u8(*v, &mut buf[1..])?),
            Property::MessageExpiryInterval(v) => (0x02, encode_u32(*v, &mut buf[1..])?),
            Property::ContentType(v) => (0x03, encode_string(v, &mut buf[1..])?),
            Property::ResponseTopic(v) => (0x08, encode_string(v, &mut buf[1..])?),
            Property::CorrelationData(v) => (0x09, encode_binary(v, &mut buf[1..])?),
            Property::SubscriptionIdentifier(v) => (0x0B, v.encode(&mut buf[1..])?),
            Property::SessionExpiryInterval(v) => (0x11, encode_u32(*v, &mut buf[1..])?),
            Property::AssignedClientIdentifier(v) => (0x12, encode_string(v, &mut buf[1..])?),
            Property::ServerKeepAlive(v) => (0x13, encode_u16(*v, &mut buf[1..])?),
            Property::AuthenticationMethod(v) => (0x15, encode_string(v, &mut buf[1..])?),
            Property::AuthenticationData(v) => (0x16, encode_binary(v, &mut buf[1..])?),
            Property::RequestProblemInformation(v) => (0x17, encode_u8(*v, &mut buf[1..])?),
            Property::WillDelayInterval(v) => (0x18, encode_u32(*v, &mut buf[1..])?),
            Property::RequestResponseInformation(v) => (0x19, encode_u8(*v, &mut buf[1..])?),
            Property::ResponseInformation(v) => (0x1A, encode_string(v, &mut buf[1..])?),
            Property::ServerReference(v) => (0x1C, encode_string(v, &mut buf[1..])?),
            Property::ReasonString(v) => (0x1F, encode_string(v, &mut buf[1..])?),
            Property::ReceiveMaximum(v) => (0x21, encode_u16(*v, &mut buf[1..])?),
            Property::TopicAliasMaximum(v) => (0x22, encode_u16(*v, &mut buf[1..])?),
            Property::TopicAlias(v) => (0x23, encode_u16(*v, &mut buf[1..])?),
            Property::MaximumQoS(v) => (0x24, encode_u8(*v, &mut buf[1..])?),
            Property::RetainAvailable(v) => (0x25, encode_u8(*v, &mut buf[1..])?),
            Property::UserProperty(k, v) => (0x26, encode_user_property(k, v, &mut buf[1..])?),
            Property::MaximumPacketSize(v) => (0x27, encode_u32(*v, &mut buf[1..])?),
            Property::WildcardSubscriptionAvailable(v) => (0x28, encode_u8(*v, &mut buf[1..])?),
            Property::SubscriptionIdentifierAvailable(v) => (0x29, encode_u8(*v, &mut buf[1..])?),
            Property::SharedSubscriptionAvailable(v) => (0x2A, encode_u8(*v, &mut buf[1..])?),
        };

        buf[0] = identifier;
        Ok(1 + written)
    }

    /// `buf` からプロパティをデコードする。
    ///
    /// デコードしたプロパティと消費したバイト数を返す。
    /// プロパティ識別子は可変長バイト整数としてデコードする。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        let (identifier, id_len) = VariableByteInteger::decode(buf)?;
        // 現行仕様で定義されているプロパティ識別子は 0x2A までである。
        // それを超える値は不正な識別子として拒否する。
        if identifier.0 > 0x2A {
            return Err(DecodeError::MalformedPacket);
        }
        let identifier = identifier.0 as u8;
        let (property, consumed) = Self::decode_value(identifier, &buf[id_len..])?;
        Ok((property, id_len + consumed))
    }

    /// このプロパティの値が仕様で定められた値域および内容を満たしているか検証する。
    fn validate_value(&self) -> Result<(), EncodeError> {
        match self {
            Property::PayloadFormatIndicator(v)
            | Property::RequestProblemInformation(v)
            | Property::RequestResponseInformation(v)
            | Property::RetainAvailable(v)
            | Property::WildcardSubscriptionAvailable(v)
            | Property::SubscriptionIdentifierAvailable(v)
            | Property::SharedSubscriptionAvailable(v) => {
                // これらのプロパティは 0 または 1 のみ有効。
                if *v > 1 {
                    return Err(EncodeError::InvalidField {
                        reason: EncodeInvalidField::InvalidPropertyValue,
                    });
                }
            }
            Property::MaximumQoS(v) => {
                // MQTT v5.0 §3.2.2.3.4 (Maximum QoS):
                // 0 または 1 のみ有効。Maximum QoS プロパティが無い場合、Client は QoS 2 として扱う。
                if *v > 1 {
                    return Err(EncodeError::InvalidField {
                        reason: EncodeInvalidField::InvalidPropertyValue,
                    });
                }
            }
            Property::ReceiveMaximum(v) | Property::TopicAlias(v) => {
                // これらのプロパティは 0 より大きくなければならない。
                if *v == 0 {
                    return Err(EncodeError::InvalidField {
                        reason: EncodeInvalidField::InvalidPropertyValue,
                    });
                }
            }
            Property::TopicAliasMaximum(_) => {
                // トピックエイリアス最大値 (Topic Alias Maximum) は 0 も有効値である。
            }
            Property::MaximumPacketSize(v) => {
                // 最大パケットサイズ (Maximum Packet Size) は 0 より大きくなければならない。
                if *v == 0 {
                    return Err(EncodeError::InvalidField {
                        reason: EncodeInvalidField::InvalidPropertyValue,
                    });
                }
            }
            Property::SubscriptionIdentifier(v) => {
                // サブスクリプション識別子 (Subscription Identifier) は 0 より大きくなければならない。
                if v.0 == 0 {
                    return Err(EncodeError::InvalidField {
                        reason: EncodeInvalidField::InvalidPropertyValue,
                    });
                }
            }
            Property::ResponseTopic(topic) => {
                // MQTT v5.0 §4.7.3 [MQTT-4.7.3-1]:
                // すべての Topic Name と Topic Filter は最低 1 文字でなければならない。
                if topic.is_empty() {
                    return Err(EncodeError::InvalidField {
                        reason: EncodeInvalidField::EmptyTopicName,
                    });
                }
                // MQTT v5.0 §3.3.2.3.5 [MQTT-3.3.2-14]:
                // Response Topic にワイルドカード文字を含めてはならない。
                if topic.contains('+') || topic.contains('#') {
                    return Err(EncodeError::InvalidField {
                        reason: EncodeInvalidField::WildcardInTopicName,
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn decode_value(identifier: u8, buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        let (property, consumed) = match identifier {
            0x01 => {
                let (v, n) = decode_u8(buf)?;
                (Property::PayloadFormatIndicator(v), n)
            }
            0x02 => {
                let (v, n) = decode_u32(buf)?;
                (Property::MessageExpiryInterval(v), n)
            }
            0x03 => {
                let (v, n) = decode_string(buf)?;
                (Property::ContentType(v.0), n)
            }
            0x08 => {
                let (v, n) = decode_string(buf)?;
                (Property::ResponseTopic(v.0), n)
            }
            0x09 => {
                let (v, n) = decode_binary(buf)?;
                (Property::CorrelationData(v.0), n)
            }
            0x0B => {
                let (v, n) = VariableByteInteger::decode(buf)?;
                (Property::SubscriptionIdentifier(v), n)
            }
            0x11 => {
                let (v, n) = decode_u32(buf)?;
                (Property::SessionExpiryInterval(v), n)
            }
            0x12 => {
                let (v, n) = decode_string(buf)?;
                (Property::AssignedClientIdentifier(v.0), n)
            }
            0x13 => {
                let (v, n) = decode_u16(buf)?;
                (Property::ServerKeepAlive(v), n)
            }
            0x15 => {
                let (v, n) = decode_string(buf)?;
                (Property::AuthenticationMethod(v.0), n)
            }
            0x16 => {
                let (v, n) = decode_binary(buf)?;
                (Property::AuthenticationData(v.0), n)
            }
            0x17 => {
                let (v, n) = decode_u8(buf)?;
                (Property::RequestProblemInformation(v), n)
            }
            0x18 => {
                let (v, n) = decode_u32(buf)?;
                (Property::WillDelayInterval(v), n)
            }
            0x19 => {
                let (v, n) = decode_u8(buf)?;
                (Property::RequestResponseInformation(v), n)
            }
            0x1A => {
                let (v, n) = decode_string(buf)?;
                (Property::ResponseInformation(v.0), n)
            }
            0x1C => {
                let (v, n) = decode_string(buf)?;
                (Property::ServerReference(v.0), n)
            }
            0x1F => {
                let (v, n) = decode_string(buf)?;
                (Property::ReasonString(v.0), n)
            }
            0x21 => {
                let (v, n) = decode_u16(buf)?;
                (Property::ReceiveMaximum(v), n)
            }
            0x22 => {
                let (v, n) = decode_u16(buf)?;
                (Property::TopicAliasMaximum(v), n)
            }
            0x23 => {
                let (v, n) = decode_u16(buf)?;
                (Property::TopicAlias(v), n)
            }
            0x24 => {
                let (v, n) = decode_u8(buf)?;
                (Property::MaximumQoS(v), n)
            }
            0x25 => {
                let (v, n) = decode_u8(buf)?;
                (Property::RetainAvailable(v), n)
            }
            0x26 => {
                let (k, n1) = decode_string(buf)?;
                let (v, n2) = decode_string(&buf[n1..])?;
                (Property::UserProperty(k.0, v.0), n1 + n2)
            }
            0x27 => {
                let (v, n) = decode_u32(buf)?;
                (Property::MaximumPacketSize(v), n)
            }
            0x28 => {
                let (v, n) = decode_u8(buf)?;
                (Property::WildcardSubscriptionAvailable(v), n)
            }
            0x29 => {
                let (v, n) = decode_u8(buf)?;
                (Property::SubscriptionIdentifierAvailable(v), n)
            }
            0x2A => {
                let (v, n) = decode_u8(buf)?;
                (Property::SharedSubscriptionAvailable(v), n)
            }
            _ => return Err(DecodeError::MalformedPacket),
        };

        // デコード時にも値域を検証する。
        property
            .validate_value()
            .map_err(|_| DecodeError::MalformedPacket)?;
        Ok((property, consumed))
    }
}

/// MQTT v5.0 パケットの送信方向。
///
/// プロパティの許可リストは Server → Client / Client → Server で異なる箇所がある。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PacketDirection {
    /// Client から Server への送信。
    ClientToServer,
    /// Server から Client への送信。
    ServerToClient,
}

/// MQTT v5.0 プロパティの集合。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Properties(Vec<Property>);

impl Properties {
    /// 空のプロパティ一覧を作成する。
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// プロパティを追加する。
    pub fn push(&mut self, property: Property) {
        self.0.push(property);
    }

    /// 含まれるプロパティを順に返すイテレータを返す。
    pub fn iter(&self) -> impl Iterator<Item = &Property> {
        self.0.iter()
    }

    /// 指定した識別子のプロパティが含まれているかどうかを返す。
    pub fn has_identifier(&self, identifier: u8) -> bool {
        self.0.iter().any(|p| p.identifier() == identifier)
    }

    /// 各プロパティのエンコード後の長さの合計を返す。
    fn property_payload_len(&self) -> usize {
        self.0
            .iter()
            .map(|p| p.encoded_len())
            .fold(0usize, |acc, len| acc.saturating_add(len))
    }

    /// 長さプレフィックスを含むすべてのプロパティのエンコード後の長さを返す。
    pub fn encoded_len(&self) -> usize {
        let properties_len = self.property_payload_len();
        if properties_len > VariableByteInteger::MAX as usize {
            return usize::MAX;
        }
        let vbi_len = VariableByteInteger(properties_len as u32).encoded_len();
        vbi_len.saturating_add(properties_len)
    }

    /// プロパティを `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        let properties_len = self.property_payload_len();
        if properties_len > VariableByteInteger::MAX as usize {
            return Err(EncodeError::PacketTooLarge {
                size: properties_len,
                limit: VariableByteInteger::MAX as usize,
            });
        }
        let vbi = VariableByteInteger(properties_len as u32);

        if buf.len() < self.encoded_len() {
            return Err(EncodeError::BufferTooSmall);
        }

        let mut offset = vbi.encode(buf)?;
        for property in &self.0 {
            offset += property.encode(&mut buf[offset..])?;
        }
        Ok(offset)
    }

    /// `buf` からプロパティ一覧をデコードする。
    ///
    /// デコードしたプロパティと消費したバイト数を返す。
    ///
    /// フレーム境界で切ったスライスを渡すことを前提とする。プロパティ長の
    /// フィールドや領域がスライスを超える場合は、追加入力では解決しない破損として
    /// [`DecodeError::MalformedPacket`] を返す。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        Self::decode_with_options(buf, false)
    }

    /// PUBLISH パケット用にプロパティ一覧をデコードする。
    ///
    /// PUBLISH では複数の Subscription Identifier (0x0B) が許可されるため、
    /// それ以外の重複のみを拒否する。
    /// 入力の前提とエラーの扱いは [`Properties::decode`] と同じ。
    pub fn decode_for_publish(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        Self::decode_with_options(buf, true)
    }

    fn decode_with_options(
        buf: &[u8],
        allow_duplicate_subscription_id: bool,
    ) -> Result<(Self, usize), DecodeError> {
        // 呼び出し元はフレーム境界で切ったスライスを渡すため、プロパティ長の
        // フィールド自体が途切れている場合や、プロパティ長がスライスの残量を
        // 超える場合は、追加入力では解決しない破損として扱う。
        let (properties_len, vbi_len) =
            VariableByteInteger::decode(buf).map_err(crate::error::malformed_if_insufficient)?;
        let properties_len = properties_len.0 as usize;
        let mut offset = vbi_len;
        let end = offset + properties_len;

        if buf.len() < end {
            return Err(DecodeError::MalformedPacket);
        }

        let mut properties = Vec::new();
        while offset < end {
            // プロパティ領域の長さは確定しているので、内部でデータ不足が発生した場合は
            // パケットが壊れているものとして扱う。
            let (property, consumed) = match Property::decode(&buf[offset..end]) {
                Ok(v) => v,
                Err(DecodeError::InsufficientData) => {
                    return Err(DecodeError::MalformedPacket);
                }
                Err(e) => return Err(e),
            };

            properties.push(property);
            offset += consumed;
        }

        // ユーザープロパティ (User Property) 以外のプロパティ識別子の重複は許可されない。
        // PUBLISH のみ、複数の Subscription Identifier (0x0B) が許可される。
        let properties = Self(properties);
        properties
            .validate_duplicate_identifiers(allow_duplicate_subscription_id)
            .map_err(|_| DecodeError::MalformedPacket)?;

        Ok((properties, offset))
    }

    /// プロパティ識別子の重複を検証する。
    ///
    /// User Property (0x26) は複数回出現できる。
    /// `allow_duplicate_subscription_id` が `true` の場合、PUBLISH において
    /// Subscription Identifier (0x0B) の重複も許容する。
    pub(crate) fn validate_duplicate_identifiers(
        &self,
        allow_duplicate_subscription_id: bool,
    ) -> Result<(), EncodeError> {
        let mut seen_identifiers = BTreeSet::new();
        for property in &self.0 {
            let identifier = property.identifier();
            if identifier != 0x26
                && !(allow_duplicate_subscription_id && identifier == 0x0B)
                && !seen_identifiers.insert(identifier)
            {
                return Err(EncodeError::InvalidField {
                    reason: EncodeInvalidField::DuplicatePropertyIdentifier,
                });
            }
        }
        Ok(())
    }
}

fn encode_u8(value: u8, buf: &mut [u8]) -> Result<usize, EncodeError> {
    if buf.is_empty() {
        return Err(EncodeError::BufferTooSmall);
    }
    buf[0] = value;
    Ok(1)
}

fn encode_u16(value: u16, buf: &mut [u8]) -> Result<usize, EncodeError> {
    if buf.len() < 2 {
        return Err(EncodeError::BufferTooSmall);
    }
    buf[..2].copy_from_slice(&value.to_be_bytes());
    Ok(2)
}

fn encode_u32(value: u32, buf: &mut [u8]) -> Result<usize, EncodeError> {
    if buf.len() < 4 {
        return Err(EncodeError::BufferTooSmall);
    }
    buf[..4].copy_from_slice(&value.to_be_bytes());
    Ok(4)
}

fn encode_string(value: &str, buf: &mut [u8]) -> Result<usize, EncodeError> {
    Utf8String::encode_str(value, buf)
}

fn encode_binary(value: &[u8], buf: &mut [u8]) -> Result<usize, EncodeError> {
    BinaryData::encode_slice(value, buf)
}

fn encode_user_property(key: &str, value: &str, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let key_len = key.len();
    let value_len = value.len();
    if buf.len() < 2 + key_len + 2 + value_len {
        return Err(EncodeError::BufferTooSmall);
    }
    let mut offset = encode_string(key, buf)?;
    offset += encode_string(value, &mut buf[offset..])?;
    Ok(offset)
}

fn decode_u8(buf: &[u8]) -> Result<(u8, usize), DecodeError> {
    if buf.is_empty() {
        return Err(DecodeError::InsufficientData);
    }
    Ok((buf[0], 1))
}

fn decode_u16(buf: &[u8]) -> Result<(u16, usize), DecodeError> {
    if buf.len() < 2 {
        return Err(DecodeError::InsufficientData);
    }
    Ok((u16::from_be_bytes([buf[0], buf[1]]), 2))
}

fn decode_u32(buf: &[u8]) -> Result<(u32, usize), DecodeError> {
    if buf.len() < 4 {
        return Err(DecodeError::InsufficientData);
    }
    Ok((u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]), 4))
}

fn decode_string(buf: &[u8]) -> Result<(Utf8String, usize), DecodeError> {
    Utf8String::decode(buf)
}

fn decode_binary(buf: &[u8]) -> Result<(BinaryData, usize), DecodeError> {
    BinaryData::decode(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_specific_property_validation() {
        // PUBLISH プロパティに Receive Maximum を含めることはできない。
        let mut props = Properties::new();
        props.push(Property::ReceiveMaximum(10));
        assert_eq!(
            props.validate_for_publish(),
            Err(DecodeError::MalformedPacket)
        );

        // CONNECT プロパティに Maximum QoS を含めることはできない。
        let mut props = Properties::new();
        props.push(Property::MaximumQoS(1));
        assert_eq!(
            props.validate_for_connect(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn validate_duplicate_identifiers_rejects_non_user_property_duplicates() {
        // User Property 以外のプロパティ識別子の重複は拒否される。
        let mut props = Properties::new();
        props.push(Property::SessionExpiryInterval(60));
        props.push(Property::SessionExpiryInterval(120));
        assert_eq!(
            props.validate_duplicate_identifiers(false),
            Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::DuplicatePropertyIdentifier,
            })
        );
    }

    #[test]
    fn validate_duplicate_identifiers_allows_user_property_duplicates() {
        // User Property は同じ識別子でも複数回出現できる。
        let mut props = Properties::new();
        props.push(Property::UserProperty("a".to_string(), "1".to_string()));
        props.push(Property::UserProperty("b".to_string(), "2".to_string()));
        assert!(props.validate_duplicate_identifiers(false).is_ok());
    }

    #[test]
    fn validate_duplicate_identifiers_rejects_assigned_client_identifier_duplicates() {
        // CONNACK などで Assigned Client Identifier (0x12) が重複すると拒否される。
        let mut props = Properties::new();
        props.push(Property::AssignedClientIdentifier("a".to_string()));
        props.push(Property::AssignedClientIdentifier("b".to_string()));
        assert_eq!(
            props.validate_duplicate_identifiers(false),
            Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::DuplicatePropertyIdentifier,
            })
        );
    }

    #[test]
    fn validate_duplicate_identifiers_allows_subscription_id_duplicates_for_publish() {
        // PUBLISH では Subscription Identifier の重複が許可される。
        let mut props = Properties::new();
        props.push(Property::SubscriptionIdentifier(VariableByteInteger(1)));
        props.push(Property::SubscriptionIdentifier(VariableByteInteger(2)));
        assert!(props.validate_duplicate_identifiers(true).is_ok());
    }

    #[test]
    fn validate_duplicate_identifiers_rejects_subscription_id_duplicates_for_others() {
        // PUBLISH 以外では Subscription Identifier の重複は拒否される。
        let mut props = Properties::new();
        props.push(Property::SubscriptionIdentifier(VariableByteInteger(1)));
        props.push(Property::SubscriptionIdentifier(VariableByteInteger(2)));
        assert_eq!(
            props.validate_duplicate_identifiers(false),
            Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::DuplicatePropertyIdentifier,
            })
        );
    }

    #[test]
    fn validate_for_publish_rejects_subscription_identifier_for_client_to_server() {
        // Client → Server の PUBLISH では Subscription Identifier (0x0B) を含められない。
        let mut props = Properties::new();
        props.push(Property::SubscriptionIdentifier(VariableByteInteger(1)));
        assert_eq!(
            props.validate_for_publish_with_direction(PacketDirection::ClientToServer),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn validate_for_publish_allows_subscription_identifier_for_server_to_client() {
        // Server → Client の PUBLISH では Subscription Identifier (0x0B) が許可される。
        let mut props = Properties::new();
        props.push(Property::SubscriptionIdentifier(VariableByteInteger(1)));
        assert!(
            props
                .validate_for_publish_with_direction(PacketDirection::ServerToClient)
                .is_ok()
        );
    }

    #[test]
    fn validate_for_disconnect_rejects_session_expiry_interval_for_server_to_client() {
        // Server → Client の DISCONNECT では Session Expiry Interval (0x11) を含められない。
        let mut props = Properties::new();
        props.push(Property::SessionExpiryInterval(60));
        assert_eq!(
            props.validate_for_disconnect_with_direction(PacketDirection::ServerToClient),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn validate_for_disconnect_allows_session_expiry_interval_for_client_to_server() {
        // Client → Server の DISCONNECT では Session Expiry Interval (0x11) が許可される。
        let mut props = Properties::new();
        props.push(Property::SessionExpiryInterval(60));
        assert!(
            props
                .validate_for_disconnect_with_direction(PacketDirection::ClientToServer)
                .is_ok()
        );
    }
}
