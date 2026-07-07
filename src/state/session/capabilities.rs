use alloc::fmt;

use crate::codec::qos::QoS;
use crate::state::subscribe::SubscriptionEntry;

/// CONNACK でサーバーから通知された能力値。
///
/// MQTT v5.0 §3.2.2.3 の各プロパティに対応する。
/// CONNACK に含まれなかったプロパティは仕様の既定値を保持する。
/// クライアントはこれらの値を超えるパケットをサーバーに送信してはならない
/// （例: MQTT v5.0 §3.2.2.3.4 [MQTT-3.2.2-11] Maximum QoS 超過の PUBLISH 送信禁止、
/// MQTT v5.0 §3.2.2.3.5 [MQTT-3.2.2-14] Retain Available=0 での RETAIN=1 送信禁止、
/// MQTT v5.0 §3.2.2.3.6 [MQTT-3.2.2-15] Maximum Packet Size 超過パケットの送信禁止）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerCapabilities {
    /// サーバーが受け入れる最大パケットサイズ（MQTT v5.0 §3.2.2.3.6）。
    /// `None` は制限なしを表す。
    pub maximum_packet_size: Option<u32>,
    /// サーバーがサポートする最大 QoS（MQTT v5.0 §3.2.2.3.4）。
    /// 既定値は QoS 2。
    pub maximum_qos: QoS,
    /// Retain メッセージのサポート有無（MQTT v5.0 §3.2.2.3.5）。
    /// 既定値は true。
    pub retain_available: bool,
    /// ワイルドカードサブスクリプションのサポート有無
    /// （MQTT v5.0 §3.2.2.3.11）。既定値は true。
    pub wildcard_subscription_available: bool,
    /// サブスクリプション識別子のサポート有無
    /// （MQTT v5.0 §3.2.2.3.12）。既定値は true。
    pub subscription_identifiers_available: bool,
    /// 共有サブスクリプションのサポート有無
    /// （MQTT v5.0 §3.2.2.3.13）。既定値は true。
    pub shared_subscription_available: bool,
}

impl ServerCapabilities {
    /// 仕様の既定値でサーバー能力値を新規作成する。
    pub fn new() -> Self {
        Self {
            maximum_packet_size: None,
            maximum_qos: QoS::ExactlyOnce,
            retain_available: true,
            wildcard_subscription_available: true,
            subscription_identifiers_available: true,
            shared_subscription_available: true,
        }
    }
}

impl Default for ServerCapabilities {
    fn default() -> Self {
        Self::new()
    }
}

/// サーバー能力値違反で発生するエラー。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerCapabilityError {
    /// 送信 PUBLISH の QoS がサーバーの Maximum QoS を超えた。
    MaximumQoSExceeded,
    /// サーバーが Retain をサポートしていないのに retain=true の PUBLISH を
    /// 送信しようとした。
    RetainNotAvailable,
    /// 送信パケットサイズがサーバーの Maximum Packet Size を超えた。
    MaximumPacketSizeExceeded {
        /// 実際のパケットサイズ。
        size: u32,
        /// サーバーが宣言した最大パケットサイズ。
        limit: u32,
    },
    /// サーバーがワイルドカードサブスクリプションをサポートしていないのに
    /// ワイルドカードを含む SUBSCRIBE を送信しようとした。
    WildcardSubscriptionNotAvailable,
    /// サーバーがサブスクリプション識別子をサポートしていないのに
    /// Subscription Identifier を含む SUBSCRIBE を送信しようとした。
    SubscriptionIdentifierNotAvailable,
    /// サーバーが共有サブスクリプションをサポートしていないのに
    /// 共有サブスクリプションを含む SUBSCRIBE を送信しようとした。
    SharedSubscriptionNotAvailable,
}

impl fmt::Display for ServerCapabilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MaximumQoSExceeded => {
                write!(f, "outgoing PUBLISH QoS exceeds server Maximum QoS")
            }
            Self::RetainNotAvailable => {
                write!(
                    f,
                    "outgoing PUBLISH has RETAIN=1 but Retain Available is false"
                )
            }
            Self::MaximumPacketSizeExceeded { size, limit } => {
                write!(
                    f,
                    "outgoing packet size {size} exceeds server Maximum Packet Size {limit}"
                )
            }
            Self::WildcardSubscriptionNotAvailable => {
                write!(
                    f,
                    "outgoing SUBSCRIBE contains wildcard but Wildcard Subscription Available is false"
                )
            }
            Self::SubscriptionIdentifierNotAvailable => {
                write!(
                    f,
                    "outgoing SUBSCRIBE contains Subscription Identifier but Subscription Identifiers Available is false"
                )
            }
            Self::SharedSubscriptionNotAvailable => {
                write!(
                    f,
                    "outgoing SUBSCRIBE contains shared subscription but Shared Subscription Available is false"
                )
            }
        }
    }
}

impl core::error::Error for ServerCapabilityError {}

impl ServerCapabilities {
    /// 送信 PUBLISH がサーバー能力値に適合しているか検証する。
    ///
    /// 検証項目:
    /// - QoS が `maximum_qos` を超えていないこと
    /// - `retain=true` の場合、`retain_available` が true であること
    /// - `packet_size` が `maximum_packet_size` を超えていないこと
    ///
    /// MQTT v5.0 §3.2.2.3、MQTT v5.0 §3.2.2.3.4 [MQTT-3.2.2-11]、
    /// MQTT v5.0 §3.2.2.3.5 [MQTT-3.2.2-14]、MQTT v5.0 §3.2.2.3.6 [MQTT-3.2.2-15] を参照。
    pub fn validate_publish(
        &self,
        qos: QoS,
        retain: bool,
        packet_size: usize,
    ) -> Result<(), ServerCapabilityError> {
        if qos as u8 > self.maximum_qos as u8 {
            return Err(ServerCapabilityError::MaximumQoSExceeded);
        }
        if retain && !self.retain_available {
            return Err(ServerCapabilityError::RetainNotAvailable);
        }
        if let Some(limit) = self.maximum_packet_size
            && packet_size > limit as usize
        {
            return Err(ServerCapabilityError::MaximumPacketSizeExceeded {
                size: packet_size as u32,
                limit,
            });
        }
        Ok(())
    }

    /// 送信 SUBSCRIBE がサーバー能力値に適合しているか検証する。
    ///
    /// 検証項目:
    /// - ワイルドカードを含むトピックフィルタがある場合、
    ///   `wildcard_subscription_available` が true であること
    ///   （MQTT v5.0 §3.2.2.3.11）
    /// - `subscription_identifier` を含むエントリがある場合、
    ///   `subscription_identifiers_available` が true であること
    ///   （MQTT v5.0 §3.2.2.3.12）
    /// - 共有サブスクリプション (`$share/...`) がある場合、
    ///   `shared_subscription_available` が true であること
    ///   （MQTT v5.0 §3.2.2.3.13）
    ///
    /// Requested QoS は検証しない。MQTT v5.0 §3.2.2.3.4 [MQTT-3.2.2-10]:
    /// QoS 1 や QoS 2 の PUBLISH をサポートしないサーバーであっても、
    /// Requested QoS 0 / 1 / 2 を含む SUBSCRIBE パケットを受理しなければ
    /// ならない。Maximum QoS がクライアントに課す制限は送信 PUBLISH の QoS
    /// （MQTT v5.0 §3.2.2.3.4 [MQTT-3.2.2-11]、`validate_publish` で検証）だけである。
    pub fn validate_subscribe(
        &self,
        entries: &[SubscriptionEntry],
    ) -> Result<(), ServerCapabilityError> {
        for entry in entries {
            if !self.wildcard_subscription_available
                && (entry.topic_filter.contains('+') || entry.topic_filter.contains('#'))
            {
                return Err(ServerCapabilityError::WildcardSubscriptionNotAvailable);
            }
            if !self.subscription_identifiers_available && entry.subscription_identifier.is_some() {
                return Err(ServerCapabilityError::SubscriptionIdentifierNotAvailable);
            }
            if !self.shared_subscription_available && entry.topic_filter.starts_with("$share/") {
                return Err(ServerCapabilityError::SharedSubscriptionNotAvailable);
            }
        }
        Ok(())
    }
}
