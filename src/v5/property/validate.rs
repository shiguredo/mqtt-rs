//! MQTT v5.0 プロパティのパケット種別ごとの許可リスト検証。

use crate::error::DecodeError;
use crate::v5::property::{PacketDirection, Properties};

impl Properties {
    /// このプロパティ一覧が CONNECT パケットで許可されるものか検証する。
    pub(crate) fn validate_for_connect(&self) -> Result<(), DecodeError> {
        const ALLOWED: &[u8] = &[
            0x11, // セッション有効期限間隔 (Session Expiry Interval)
            0x21, // 受信最大値 (Receive Maximum)
            0x27, // 最大パケットサイズ (Maximum Packet Size)
            0x22, // トピックエイリアス最大値 (Topic Alias Maximum)
            0x19, // 応答情報の要求 (Request Response Information)
            0x17, // 問題情報の要求 (Request Problem Information)
            0x26, // ユーザープロパティ (User Property)
            0x15, // 認証方式 (Authentication Method)
            0x16, // 認証データ (Authentication Data)
        ];
        self.validate_against(ALLOWED)?;
        // MQTT v5.0 §3.1.2.11.10: Authentication Data は Authentication Method と共にのみ送信可能。
        if self.has_identifier(0x16) && !self.has_identifier(0x15) {
            return Err(DecodeError::MalformedPacket);
        }
        Ok(())
    }

    /// このプロパティ一覧が CONNACK パケットで許可されるものか検証する。
    pub(crate) fn validate_for_connack(&self) -> Result<(), DecodeError> {
        const ALLOWED: &[u8] = &[
            0x11, // セッション有効期限間隔 (Session Expiry Interval)
            0x21, // 受信最大値 (Receive Maximum)
            0x24, // 最大 QoS (Maximum QoS)
            0x25, // 保持メッセージの利用可否 (Retain Available)
            0x27, // 最大パケットサイズ (Maximum Packet Size)
            0x12, // 割り当てられたクライアント識別子 (Assigned Client Identifier)
            0x22, // トピックエイリアス最大値 (Topic Alias Maximum)
            0x1F, // 理由文字列 (Reason String)
            0x26, // ユーザープロパティ (User Property)
            0x28, // ワイルドカードサブスクリプションの利用可否 (Wildcard Subscription Available)
            0x29, // サブスクリプション識別子の利用可否 (Subscription Identifier Available)
            0x2A, // 共有サブスクリプションの利用可否 (Shared Subscription Available)
            0x13, // サーバー キープアライブ (Server Keep Alive)
            0x1A, // 応答情報 (Response Information)
            0x1C, // サーバー参照 (Server Reference)
            0x15, // 認証方式 (Authentication Method)
            0x16, // 認証データ (Authentication Data)
        ];
        self.validate_against(ALLOWED)?;
        // CONNECT 側の MQTT v5.0 §3.1.2.11.10 と異なり、CONNACK 側の
        // MQTT v5.0 §3.2.2.3.18 には「Authentication Method なしの
        // Authentication Data は Protocol Error」とする規範文がない
        // （重複のみが Protocol Error）。仕様にない拒否は行わず、
        // 拡張認証の文脈依存の検証（MQTT v5.0 §4.12 [MQTT-4.12.0-5] /
        // [MQTT-4.12.0-6]）は `crate::state::auth` が担う。
        Ok(())
    }

    /// このプロパティ一覧が PUBLISH パケットで許可されるものか検証する。
    ///
    /// 両方向で許可されるプロパティの和集合を許可する。
    /// 方向ごとの厳密な検証は `validate_for_publish_with_direction` を使用する。
    pub(crate) fn validate_for_publish(&self) -> Result<(), DecodeError> {
        const ALLOWED: &[u8] = &[
            0x01, // ペイロード形式指示子 (Payload Format Indicator)
            0x02, // メッセージ有効期限間隔 (Message Expiry Interval)
            0x03, // コンテンツタイプ (Content Type)
            0x08, // 応答トピック (Response Topic)
            0x09, // 相関データ (Correlation Data)
            0x0B, // サブスクリプション識別子 (Subscription Identifier)
            0x23, // トピックエイリアス (Topic Alias)
            0x26, // ユーザープロパティ (User Property)
        ];
        self.validate_against(ALLOWED)
    }

    /// 送信方向を考慮して、このプロパティ一覧が PUBLISH パケットで許可されるものか検証する。
    ///
    /// MQTT v5.0 §3.3.2.3.8 / MQTT v5.0 §3.3.4 [MQTT-3.3.4-6]:
    /// Subscription Identifier (0x0B) は Server から Client への PUBLISH のみに含まれる。
    pub(crate) fn validate_for_publish_with_direction(
        &self,
        direction: PacketDirection,
    ) -> Result<(), DecodeError> {
        self.validate_for_publish()?;
        if direction == PacketDirection::ClientToServer && self.has_identifier(0x0B) {
            return Err(DecodeError::MalformedPacket);
        }
        Ok(())
    }

    /// このプロパティ一覧が PUBACK パケットで許可されるものか検証する。
    pub(crate) fn validate_for_puback(&self) -> Result<(), DecodeError> {
        const ALLOWED: &[u8] = &[
            0x1F, // 理由文字列 (Reason String)
            0x26, // ユーザープロパティ (User Property)
        ];
        self.validate_against(ALLOWED)
    }

    /// このプロパティ一覧が PUBREC パケットで許可されるものか検証する。
    pub(crate) fn validate_for_pubrec(&self) -> Result<(), DecodeError> {
        const ALLOWED: &[u8] = &[
            0x1F, // 理由文字列 (Reason String)
            0x26, // ユーザープロパティ (User Property)
        ];
        self.validate_against(ALLOWED)
    }

    /// このプロパティ一覧が PUBREL パケットで許可されるものか検証する。
    pub(crate) fn validate_for_pubrel(&self) -> Result<(), DecodeError> {
        const ALLOWED: &[u8] = &[
            0x1F, // 理由文字列 (Reason String)
            0x26, // ユーザープロパティ (User Property)
        ];
        self.validate_against(ALLOWED)
    }

    /// このプロパティ一覧が PUBCOMP パケットで許可されるものか検証する。
    pub(crate) fn validate_for_pubcomp(&self) -> Result<(), DecodeError> {
        const ALLOWED: &[u8] = &[
            0x1F, // 理由文字列 (Reason String)
            0x26, // ユーザープロパティ (User Property)
        ];
        self.validate_against(ALLOWED)
    }

    /// このプロパティ一覧が SUBSCRIBE パケットで許可されるものか検証する。
    pub(crate) fn validate_for_subscribe(&self) -> Result<(), DecodeError> {
        const ALLOWED: &[u8] = &[
            0x0B, // サブスクリプション識別子 (Subscription Identifier)
            0x26, // ユーザープロパティ (User Property)
        ];
        self.validate_against(ALLOWED)
    }

    /// このプロパティ一覧が SUBACK パケットで許可されるものか検証する。
    pub(crate) fn validate_for_suback(&self) -> Result<(), DecodeError> {
        const ALLOWED: &[u8] = &[
            0x1F, // 理由文字列 (Reason String)
            0x26, // ユーザープロパティ (User Property)
        ];
        self.validate_against(ALLOWED)
    }

    /// このプロパティ一覧が UNSUBSCRIBE パケットで許可されるものか検証する。
    pub(crate) fn validate_for_unsubscribe(&self) -> Result<(), DecodeError> {
        const ALLOWED: &[u8] = &[
            0x26, // ユーザープロパティ (User Property)
        ];
        self.validate_against(ALLOWED)
    }

    /// このプロパティ一覧が UNSUBACK パケットで許可されるものか検証する。
    pub(crate) fn validate_for_unsuback(&self) -> Result<(), DecodeError> {
        const ALLOWED: &[u8] = &[
            0x1F, // 理由文字列 (Reason String)
            0x26, // ユーザープロパティ (User Property)
        ];
        self.validate_against(ALLOWED)
    }

    /// このプロパティ一覧が DISCONNECT パケットで許可されるものか検証する。
    ///
    /// 両方向で許可されるプロパティの和集合を許可する。
    /// 方向ごとの厳密な検証は `validate_for_disconnect_with_direction` を使用する。
    pub(crate) fn validate_for_disconnect(&self) -> Result<(), DecodeError> {
        const ALLOWED: &[u8] = &[
            0x11, // セッション有効期限間隔 (Session Expiry Interval)
            0x1C, // サーバー参照 (Server Reference)
            0x1F, // 理由文字列 (Reason String)
            0x26, // ユーザープロパティ (User Property)
        ];
        self.validate_against(ALLOWED)
    }

    /// 送信方向を考慮して、このプロパティ一覧が DISCONNECT パケットで許可されるものか検証する。
    ///
    /// MQTT v5.0 §3.14.2.2.2 [MQTT-3.14.2-2]:
    /// Server から Client への DISCONNECT では Session Expiry Interval (0x11) を含んではならない。
    pub(crate) fn validate_for_disconnect_with_direction(
        &self,
        direction: PacketDirection,
    ) -> Result<(), DecodeError> {
        self.validate_for_disconnect()?;
        if direction == PacketDirection::ServerToClient && self.has_identifier(0x11) {
            return Err(DecodeError::MalformedPacket);
        }
        Ok(())
    }

    /// このプロパティ一覧が AUTH パケットで許可されるものか検証する。
    pub(crate) fn validate_for_auth(&self) -> Result<(), DecodeError> {
        const ALLOWED: &[u8] = &[
            0x15, // 認証方式 (Authentication Method)
            0x16, // 認証データ (Authentication Data)
            0x1F, // 理由文字列 (Reason String)
            0x26, // ユーザープロパティ (User Property)
        ];
        self.validate_against(ALLOWED)?;
        // MQTT v5.0 §3.15.2.2.2: AUTH パケットには Authentication Method が必須。
        if !self.has_identifier(0x15) {
            return Err(DecodeError::MalformedPacket);
        }
        Ok(())
    }

    /// このプロパティ一覧が CONNECT の Will 部分で許可されるものか検証する。
    pub(crate) fn validate_for_will(&self) -> Result<(), DecodeError> {
        const ALLOWED: &[u8] = &[
            0x18, // Will 遅延間隔 (Will Delay Interval)
            0x01, // ペイロード形式指示子 (Payload Format Indicator)
            0x02, // メッセージ有効期限間隔 (Message Expiry Interval)
            0x03, // コンテンツタイプ (Content Type)
            0x08, // 応答トピック (Response Topic)
            0x09, // 相関データ (Correlation Data)
            0x26, // ユーザープロパティ (User Property)
        ];
        self.validate_against(ALLOWED)
    }

    /// 許可されたプロパティ識別子の集合に対して検証する。
    fn validate_against(&self, allowed: &[u8]) -> Result<(), DecodeError> {
        for property in &self.0 {
            let identifier = property.identifier();
            if !allowed.contains(&identifier) {
                return Err(DecodeError::MalformedPacket);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::variable_byte_integer::VariableByteInteger;
    use crate::v5::property::Property;

    /// 指定したプロパティ 1 つを含む `Properties` を作成する。
    fn properties_with(property: Property) -> Properties {
        let mut properties = Properties::new();
        properties.push(property);
        properties
    }

    // 空のプロパティ一覧は、AUTH を除く全パケット種別で許可される。
    // AUTH には Authentication Method (0x15) が必須。

    #[test]
    fn empty_properties_are_valid_for_connect() {
        assert!(Properties::new().validate_for_connect().is_ok());
    }

    #[test]
    fn empty_properties_are_valid_for_connack() {
        assert!(Properties::new().validate_for_connack().is_ok());
    }

    #[test]
    fn empty_properties_are_valid_for_publish() {
        assert!(Properties::new().validate_for_publish().is_ok());
    }

    #[test]
    fn empty_properties_are_valid_for_puback() {
        assert!(Properties::new().validate_for_puback().is_ok());
    }

    #[test]
    fn empty_properties_are_valid_for_pubrec() {
        assert!(Properties::new().validate_for_pubrec().is_ok());
    }

    #[test]
    fn empty_properties_are_valid_for_pubrel() {
        assert!(Properties::new().validate_for_pubrel().is_ok());
    }

    #[test]
    fn empty_properties_are_valid_for_pubcomp() {
        assert!(Properties::new().validate_for_pubcomp().is_ok());
    }

    #[test]
    fn empty_properties_are_valid_for_subscribe() {
        assert!(Properties::new().validate_for_subscribe().is_ok());
    }

    #[test]
    fn empty_properties_are_valid_for_suback() {
        assert!(Properties::new().validate_for_suback().is_ok());
    }

    #[test]
    fn empty_properties_are_valid_for_unsubscribe() {
        assert!(Properties::new().validate_for_unsubscribe().is_ok());
    }

    #[test]
    fn empty_properties_are_valid_for_unsuback() {
        assert!(Properties::new().validate_for_unsuback().is_ok());
    }

    #[test]
    fn empty_properties_are_valid_for_disconnect() {
        assert!(Properties::new().validate_for_disconnect().is_ok());
    }

    #[test]
    fn empty_properties_are_valid_for_will() {
        assert!(Properties::new().validate_for_will().is_ok());
    }

    #[test]
    fn empty_properties_are_invalid_for_auth() {
        // AUTH には Authentication Method (0x15) が必須。
        assert_eq!(
            Properties::new().validate_for_auth(),
            Err(DecodeError::MalformedPacket)
        );
    }

    // 許可されたプロパティは受け入れられる。

    #[test]
    fn connect_accepts_session_expiry_interval() {
        // Session Expiry Interval (0x11) は CONNECT で許可されている。
        let properties = properties_with(Property::SessionExpiryInterval(60));
        assert!(properties.validate_for_connect().is_ok());
    }

    #[test]
    fn connect_accepts_authentication_method() {
        // Authentication Method (0x15) 単独は CONNECT で許可されている。
        let properties = properties_with(Property::AuthenticationMethod("method".to_string()));
        assert!(properties.validate_for_connect().is_ok());
    }

    #[test]
    fn connect_accepts_authentication_method_and_data() {
        // Authentication Method (0x15) と Authentication Data (0x16) の組み合わせは許可される。
        let mut properties = Properties::new();
        properties.push(Property::AuthenticationMethod("method".to_string()));
        properties.push(Property::AuthenticationData(vec![0x01]));
        assert!(properties.validate_for_connect().is_ok());
    }

    #[test]
    fn connack_accepts_receive_maximum() {
        // Receive Maximum (0x21) は CONNACK で許可されている。
        let properties = properties_with(Property::ReceiveMaximum(10));
        assert!(properties.validate_for_connack().is_ok());
    }

    #[test]
    fn connack_accepts_authentication_method() {
        // Authentication Method (0x15) 単独は CONNACK で許可されている。
        let properties = properties_with(Property::AuthenticationMethod("method".to_string()));
        assert!(properties.validate_for_connack().is_ok());
    }

    #[test]
    fn connack_accepts_authentication_method_and_data() {
        // Authentication Method (0x15) と Authentication Data (0x16) の組み合わせは許可される。
        let mut properties = Properties::new();
        properties.push(Property::AuthenticationMethod("method".to_string()));
        properties.push(Property::AuthenticationData(vec![0x01]));
        assert!(properties.validate_for_connack().is_ok());
    }

    #[test]
    fn publish_accepts_payload_format_indicator() {
        // Payload Format Indicator (0x01) は PUBLISH で許可されている。
        let properties = properties_with(Property::PayloadFormatIndicator(0));
        assert!(properties.validate_for_publish().is_ok());
    }

    #[test]
    fn puback_accepts_reason_string() {
        // Reason String (0x1F) は PUBACK で許可されている。
        let properties = properties_with(Property::ReasonString("reason".to_string()));
        assert!(properties.validate_for_puback().is_ok());
    }

    #[test]
    fn pubrec_accepts_reason_string() {
        // Reason String (0x1F) は PUBREC で許可されている。
        let properties = properties_with(Property::ReasonString("reason".to_string()));
        assert!(properties.validate_for_pubrec().is_ok());
    }

    #[test]
    fn pubrel_accepts_reason_string() {
        // Reason String (0x1F) は PUBREL で許可されている。
        let properties = properties_with(Property::ReasonString("reason".to_string()));
        assert!(properties.validate_for_pubrel().is_ok());
    }

    #[test]
    fn pubcomp_accepts_reason_string() {
        // Reason String (0x1F) は PUBCOMP で許可されている。
        let properties = properties_with(Property::ReasonString("reason".to_string()));
        assert!(properties.validate_for_pubcomp().is_ok());
    }

    #[test]
    fn subscribe_accepts_subscription_identifier() {
        // Subscription Identifier (0x0B) は SUBSCRIBE で許可されている。
        let properties = properties_with(Property::SubscriptionIdentifier(VariableByteInteger(1)));
        assert!(properties.validate_for_subscribe().is_ok());
    }

    #[test]
    fn suback_accepts_reason_string() {
        // Reason String (0x1F) は SUBACK で許可されている。
        let properties = properties_with(Property::ReasonString("reason".to_string()));
        assert!(properties.validate_for_suback().is_ok());
    }

    #[test]
    fn unsubscribe_accepts_user_property() {
        // User Property (0x26) は UNSUBSCRIBE で許可されている。
        let properties = properties_with(Property::UserProperty("k".to_string(), "v".to_string()));
        assert!(properties.validate_for_unsubscribe().is_ok());
    }

    #[test]
    fn unsuback_accepts_reason_string() {
        // Reason String (0x1F) は UNSUBACK で許可されている。
        let properties = properties_with(Property::ReasonString("reason".to_string()));
        assert!(properties.validate_for_unsuback().is_ok());
    }

    #[test]
    fn disconnect_accepts_session_expiry_interval() {
        // Session Expiry Interval (0x11) は DISCONNECT の許可リストに含まれる。
        let properties = properties_with(Property::SessionExpiryInterval(60));
        assert!(properties.validate_for_disconnect().is_ok());
    }

    #[test]
    fn auth_accepts_authentication_method() {
        // Authentication Method (0x15) を含む AUTH は正常系。
        let properties = properties_with(Property::AuthenticationMethod("method".to_string()));
        assert!(properties.validate_for_auth().is_ok());
    }

    #[test]
    fn auth_accepts_authentication_method_and_data() {
        // Authentication Method (0x15) と Authentication Data (0x16) の組み合わせは許可される。
        let mut properties = Properties::new();
        properties.push(Property::AuthenticationMethod("method".to_string()));
        properties.push(Property::AuthenticationData(vec![0x01]));
        assert!(properties.validate_for_auth().is_ok());
    }

    #[test]
    fn will_accepts_will_delay_interval() {
        // Will Delay Interval (0x18) は Will 部分で許可されている。
        let properties = properties_with(Property::WillDelayInterval(60));
        assert!(properties.validate_for_will().is_ok());
    }

    // 許可されていないプロパティは拒否される。

    #[test]
    fn connect_rejects_payload_format_indicator() {
        // Payload Format Indicator (0x01) は CONNECT で許可されていない。
        let properties = properties_with(Property::PayloadFormatIndicator(0));
        assert_eq!(
            properties.validate_for_connect(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn connect_rejects_authentication_data_without_method() {
        // Authentication Data (0x16) は Authentication Method (0x15) と共にのみ送信可能。
        let properties = properties_with(Property::AuthenticationData(vec![0x01]));
        assert_eq!(
            properties.validate_for_connect(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn connack_rejects_payload_format_indicator() {
        // Payload Format Indicator (0x01) は CONNACK で許可されていない。
        let properties = properties_with(Property::PayloadFormatIndicator(0));
        assert_eq!(
            properties.validate_for_connack(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn connack_accepts_authentication_data_without_method() {
        // CONNECT 側の MQTT v5.0 §3.1.2.11.10 と異なり、CONNACK 側の
        // MQTT v5.0 §3.2.2.3.18 には「Authentication Method なしの
        // Authentication Data は Protocol Error」とする規範文がない
        // （重複のみが Protocol Error）ため、デコーダーでは拒否しない。
        let properties = properties_with(Property::AuthenticationData(vec![0x01]));
        assert!(properties.validate_for_connack().is_ok());
    }

    #[test]
    fn publish_rejects_session_expiry_interval() {
        // Session Expiry Interval (0x11) は PUBLISH で許可されていない。
        let properties = properties_with(Property::SessionExpiryInterval(60));
        assert_eq!(
            properties.validate_for_publish(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn puback_rejects_topic_alias() {
        // Topic Alias (0x23) は PUBACK で許可されていない。
        let properties = properties_with(Property::TopicAlias(1));
        assert_eq!(
            properties.validate_for_puback(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn pubrec_rejects_topic_alias() {
        // Topic Alias (0x23) は PUBREC で許可されていない。
        let properties = properties_with(Property::TopicAlias(1));
        assert_eq!(
            properties.validate_for_pubrec(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn pubrel_rejects_topic_alias() {
        // Topic Alias (0x23) は PUBREL で許可されていない。
        let properties = properties_with(Property::TopicAlias(1));
        assert_eq!(
            properties.validate_for_pubrel(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn pubcomp_rejects_topic_alias() {
        // Topic Alias (0x23) は PUBCOMP で許可されていない。
        let properties = properties_with(Property::TopicAlias(1));
        assert_eq!(
            properties.validate_for_pubcomp(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn subscribe_rejects_payload_format_indicator() {
        // Payload Format Indicator (0x01) は SUBSCRIBE で許可されていない。
        let properties = properties_with(Property::PayloadFormatIndicator(0));
        assert_eq!(
            properties.validate_for_subscribe(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn suback_rejects_payload_format_indicator() {
        // Payload Format Indicator (0x01) は SUBACK で許可されていない。
        let properties = properties_with(Property::PayloadFormatIndicator(0));
        assert_eq!(
            properties.validate_for_suback(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn unsubscribe_rejects_payload_format_indicator() {
        // Payload Format Indicator (0x01) は UNSUBSCRIBE で許可されていない。
        let properties = properties_with(Property::PayloadFormatIndicator(0));
        assert_eq!(
            properties.validate_for_unsubscribe(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn unsuback_rejects_payload_format_indicator() {
        // Payload Format Indicator (0x01) は UNSUBACK で許可されていない。
        let properties = properties_with(Property::PayloadFormatIndicator(0));
        assert_eq!(
            properties.validate_for_unsuback(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn disconnect_rejects_payload_format_indicator() {
        // Payload Format Indicator (0x01) は DISCONNECT で許可されていない。
        let properties = properties_with(Property::PayloadFormatIndicator(0));
        assert_eq!(
            properties.validate_for_disconnect(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn auth_rejects_payload_format_indicator() {
        // Payload Format Indicator (0x01) は AUTH で許可されていない。
        let properties = properties_with(Property::PayloadFormatIndicator(0));
        assert_eq!(
            properties.validate_for_auth(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn auth_rejects_authentication_data_without_method() {
        // Authentication Data (0x16) 単独では Authentication Method (0x15) が必須。
        let properties = properties_with(Property::AuthenticationData(vec![0x01]));
        assert_eq!(
            properties.validate_for_auth(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn auth_rejects_missing_authentication_method() {
        // AUTH パケットには Authentication Method (0x15) が必須。
        let properties = properties_with(Property::ReasonString("reason".to_string()));
        assert_eq!(
            properties.validate_for_auth(),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn will_rejects_session_expiry_interval() {
        // Session Expiry Interval (0x11) は Will 部分で許可されていない。
        let properties = properties_with(Property::SessionExpiryInterval(60));
        assert_eq!(
            properties.validate_for_will(),
            Err(DecodeError::MalformedPacket)
        );
    }
}
