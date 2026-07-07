//! MQTT v5.0 DISCONNECT パケットの単体テスト。

use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v5::disconnect::{Disconnect, DisconnectReasonCode};
use shiguredo_mqtt::v5::property::Properties;

#[test]
fn invalid_flags_are_rejected() {
    let buf = [0xE1, 0x00];
    assert_eq!(
        Disconnect::decode(&buf),
        Err(DecodeError::InvalidPacketFlags)
    );
}

#[test]
fn omitted_form_is_accepted() {
    // MQTT v5.0 §3.14.2.1: Remaining Length が 0 のとき Reason Code は 0x00
    // （Normal disconnection）が使われ、Properties は空として扱う。
    let (disconnect, consumed) =
        Disconnect::decode(&[0xE0, 0x00]).expect("省略形はデコードに成功すること");
    assert_eq!(
        disconnect,
        Disconnect {
            reason_code: DisconnectReasonCode::NormalDisconnection,
            properties: Properties::new(),
        }
    );
    assert_eq!(consumed, 2);
}

#[test]
fn invalid_reason_code_is_rejected() {
    let buf = [0xE0, 0x01, 0x03];
    assert_eq!(Disconnect::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn extra_bytes_after_disconnect_are_rejected() {
    // 残り長さを偽装して余計なバイトを追加する。
    let buf = [0xE0, 0x03, 0x00, 0x00, 0x00];
    assert_eq!(Disconnect::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn reason_code_direction_matrix_is_enforced() {
    // MQTT v5.0 §3.14.2.1 Table 3-10 の「Sent by」列に基づく方向別検証。
    // encode（Client → Server）は Client が送れる値のみ許可し、
    // decode（Server → Client）は Server が送れる値のみ許可する。
    // (reason_code, Client が送れるか, Server が送れるか)
    let cases: &[(DisconnectReasonCode, bool, bool)] = &[
        (DisconnectReasonCode::NormalDisconnection, true, true),
        (DisconnectReasonCode::DisconnectWithWillMessage, true, false),
        (DisconnectReasonCode::UnspecifiedError, true, true),
        (DisconnectReasonCode::MalformedPacket, true, true),
        (DisconnectReasonCode::ProtocolError, true, true),
        (
            DisconnectReasonCode::ImplementationSpecificError,
            true,
            true,
        ),
        (DisconnectReasonCode::NotAuthorized, false, true),
        (DisconnectReasonCode::ServerBusy, false, true),
        (DisconnectReasonCode::ServerShuttingDown, false, true),
        (DisconnectReasonCode::BadAuthenticationMethod, true, true),
        (DisconnectReasonCode::KeepAliveTimeout, false, true),
        (DisconnectReasonCode::SessionTakenOver, false, true),
        (DisconnectReasonCode::TopicFilterInvalid, false, true),
        (DisconnectReasonCode::TopicNameInvalid, true, true),
        (DisconnectReasonCode::ReceiveMaximumExceeded, true, true),
        (DisconnectReasonCode::TopicAliasInvalid, true, true),
        (DisconnectReasonCode::PacketTooLarge, true, true),
        (DisconnectReasonCode::MessageRateTooHigh, true, true),
        (DisconnectReasonCode::QuotaExceeded, true, true),
        (DisconnectReasonCode::AdministrativeAction, true, true),
        (DisconnectReasonCode::PayloadFormatInvalid, true, true),
        (DisconnectReasonCode::RetainNotSupported, false, true),
        (DisconnectReasonCode::QoSNotSupported, false, true),
        (DisconnectReasonCode::UseAnotherServer, false, true),
        (DisconnectReasonCode::ServerMoved, false, true),
        (
            DisconnectReasonCode::SharedSubscriptionsNotSupported,
            false,
            true,
        ),
        (DisconnectReasonCode::ConnectionRateExceeded, false, true),
        (DisconnectReasonCode::MaximumConnectTime, false, true),
        (
            DisconnectReasonCode::SubscriptionIdentifiersNotSupported,
            false,
            true,
        ),
        (
            DisconnectReasonCode::WildcardSubscriptionsNotSupported,
            false,
            true,
        ),
    ];
    for &(reason_code, client_can_send, server_can_send) in cases {
        let disconnect = Disconnect {
            reason_code,
            properties: Properties::new(),
        };
        let mut buf = [0u8; 32];
        // encode = Client → Server。
        let encoded = disconnect.encode(&mut buf);
        if client_can_send {
            let len = encoded.expect("クライアントが送れる Reason Code はエンコードできること");
            // decode = Server → Client。
            let decoded = Disconnect::decode(&buf[..len]);
            if server_can_send {
                assert_eq!(
                    decoded,
                    Ok((disconnect.clone(), len)),
                    "サーバーが送れる Reason Code はデコードできること: {reason_code:?}"
                );
            } else {
                assert_eq!(
                    decoded,
                    Err(DecodeError::MalformedPacket),
                    "サーバーが送れない Reason Code はデコードを拒否すること: {reason_code:?}"
                );
            }
        } else {
            assert_eq!(
                encoded,
                Err(EncodeError::InvalidField {
                    reason: EncodeInvalidField::InvalidReasonCode,
                }),
                "クライアントが送れない Reason Code はエンコードを拒否すること: {reason_code:?}"
            );
            // decode のみ検証するため、手組みのバイト列でサーバー方向を確認する。
            let raw = [0xE0, 0x01, reason_code.as_u8()];
            let decoded = Disconnect::decode(&raw);
            if server_can_send {
                let (packet, _) =
                    decoded.expect("サーバーが送れる Reason Code はデコードできること");
                assert_eq!(packet.reason_code, reason_code);
            } else {
                assert_eq!(decoded, Err(DecodeError::MalformedPacket));
            }
        }
    }
}

#[test]
fn invalid_property_is_rejected_on_encode() {
    // DISCONNECT で許可されていない Topic Alias (0x23) を含むプロパティを追加する。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::TopicAlias(1));
    let disconnect = Disconnect {
        reason_code: DisconnectReasonCode::NormalDisconnection,
        properties,
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        disconnect.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::PropertyValidationFailed,
        })
    );
}

#[test]
fn duplicate_disconnect_property_identifiers_are_rejected_on_encode() {
    // DISCONNECT のプロパティに Reason String (0x1F) が重複している場合、エンコードを拒否する。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::ReasonString(
        "a".to_string(),
    ));
    properties.push(shiguredo_mqtt::v5::property::Property::ReasonString(
        "b".to_string(),
    ));
    let disconnect = Disconnect {
        reason_code: DisconnectReasonCode::NormalDisconnection,
        properties,
    };
    let mut buf = [0u8; 32];
    assert_eq!(
        disconnect.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::DuplicatePropertyIdentifier,
        })
    );
}

#[test]
fn session_expiry_interval_in_client_to_server_disconnect_is_allowed_on_encode() {
    // Client → Server 方向の DISCONNECT では Session Expiry Interval (0x11) が許可される。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::SessionExpiryInterval(60));
    let disconnect = Disconnect {
        reason_code: DisconnectReasonCode::NormalDisconnection,
        properties,
    };
    let mut buf = [0u8; 32];
    assert!(disconnect.encode(&mut buf).is_ok());
}

#[test]
fn session_expiry_interval_in_server_to_client_disconnect_is_rejected_on_decode() {
    // Server → Client 方向の DISCONNECT に Session Expiry Interval (0x11) を含めることは
    // MQTT v5.0 §3.14.2.2.2 [MQTT-3.14.2-2] で禁止されている。
    // 0xE0 = DISCONNECT, 0x07 = Remaining Length, 0x00 = Normal Disconnection,
    // 0x05 = Properties Length, 0x11 0x00 0x00 0x00 0x3C = Session Expiry Interval (60)。
    let buf = [0xE0, 0x07, 0x00, 0x05, 0x11, 0x00, 0x00, 0x00, 0x3C];
    assert_eq!(Disconnect::decode(&buf), Err(DecodeError::MalformedPacket));
}
