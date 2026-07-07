//! MQTT v5.0 パケット列挙型の単体テスト。
//!
//! `codec::Packet` と方向分離型 (`IncomingPacket` / `OutgoingPacket`) をパスで区別する。

use shiguredo_mqtt::codec::limits::Limits;
use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::codec::variable_byte_integer::VariableByteInteger;
use shiguredo_mqtt::decoder::{Decoder, VersionedIncomingPacket};
use shiguredo_mqtt::error::{DecodeError, EncodeError, EncodeInvalidField};
use shiguredo_mqtt::v5::auth::{Auth, AuthReasonCode};
use shiguredo_mqtt::v5::connack::ConnAck;
use shiguredo_mqtt::v5::connect::Connect;
use shiguredo_mqtt::v5::disconnect::{Disconnect, DisconnectReasonCode};
use shiguredo_mqtt::v5::packet::codec::Packet;
use shiguredo_mqtt::v5::packet::{IncomingPacket, OutgoingPacket};
use shiguredo_mqtt::v5::pingreq::PingReq;
use shiguredo_mqtt::v5::pingresp::PingResp;
use shiguredo_mqtt::v5::property::{Properties, Property};
use shiguredo_mqtt::v5::puback::{PubAck, PubAckReasonCode};
use shiguredo_mqtt::v5::pubcomp::PubComp;
use shiguredo_mqtt::v5::publish::Publish;
use shiguredo_mqtt::v5::pubrec::{PubRec, PubRecReasonCode};
use shiguredo_mqtt::v5::pubrel::PubRel;
use shiguredo_mqtt::v5::suback::SubAck;
use shiguredo_mqtt::v5::subscribe::{Subscribe, Subscription};
use shiguredo_mqtt::v5::unsuback::UnsubAck;
use shiguredo_mqtt::v5::unsubscribe::Unsubscribe;

#[test]
fn packet_roundtrip_publish() {
    let publish = Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "t".to_string(),
        packet_id: Some(1),
        properties: Properties::new(),
        payload: vec![0x01],
    };
    let packet = Packet::Publish(publish.clone());
    let mut buf = [0u8; 32];
    let len = packet.encode(&mut buf).expect("エンコードに成功すること");
    let (decoded, consumed) = Packet::decode(&buf[..len]).expect("デコードに成功すること");
    assert_eq!(decoded, packet);
    assert_eq!(consumed, len);
}

#[test]
fn packet_roundtrip_puback() {
    let puback = PubAck {
        packet_id: 5,
        reason_code: shiguredo_mqtt::v5::puback::PubAckReasonCode::Success,
        properties: Properties::new(),
    };
    let packet = Packet::PubAck(puback);
    let mut buf = [0u8; 16];
    let len = packet.encode(&mut buf).expect("エンコードに成功すること");
    let (decoded, consumed) = Packet::decode(&buf[..len]).expect("デコードに成功すること");
    assert_eq!(decoded, packet);
    assert_eq!(consumed, len);
}

#[test]
fn invalid_packet_type_is_rejected() {
    let buf = [0x00, 0x00];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::InvalidPacketType));
}

#[test]
fn empty_buffer_is_rejected() {
    let buf: &[u8] = &[];
    assert_eq!(Packet::decode(buf), Err(DecodeError::InsufficientData));
}

#[test]
fn encode_to_vec_matches_encode() {
    let packet = Packet::PingReq(PingReq);
    let vec = packet
        .encode_to_vec()
        .expect("Vec へのエンコードに成功すること");
    let mut buf = [0u8; 16];
    let len = packet.encode(&mut buf).expect("エンコードに成功すること");
    assert_eq!(vec, &buf[..len]);
}

#[test]
fn encode_to_vec_rejects_packet_too_large() {
    // VariableByteInteger::MAX を超える残り長さを持つパケットを作り、
    // 巨大な Vec を確保する前に PacketTooLarge が返ることを確認する。
    let topic_filter = "a".repeat(65535);
    let n = 4097;
    let mut subscriptions = Vec::with_capacity(n);
    for _ in 0..n {
        subscriptions.push(Subscription {
            topic_filter: topic_filter.clone(),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: shiguredo_mqtt::v5::subscribe::RetainHandling::SendRetained,
        });
    }
    let packet = Packet::Subscribe(Subscribe {
        packet_id: 1,
        subscriptions,
        properties: Properties::new(),
    });
    assert_eq!(
        packet.encode_to_vec(),
        Err(EncodeError::PacketTooLarge {
            size: packet.encoded_len(),
            limit: VariableByteInteger::MAX as usize,
        })
    );
}

/// 有効なパケットをエンコードし、固定ヘッダーフラグを書き換えてディスパッチャーで
/// 拒否されることを確認するヘルパー。
fn assert_invalid_flags_rejected(packet: Packet, invalid_flags: u8) {
    let mut buf = packet
        .encode_to_vec()
        .expect("テスト用パケットのエンコードに成功すること");
    buf[0] = (buf[0] & 0xF0) | invalid_flags;
    assert!(
        Packet::decode(&buf).is_err(),
        "不正なフラグ 0x{invalid_flags:01X} を拒否すること"
    );
}

#[test]
fn invalid_connect_flags_are_rejected() {
    assert_invalid_flags_rejected(
        Packet::Connect(Connect {
            client_id: "c".to_string(),
            clean_start: true,
            keep_alive: 60,
            properties: Properties::new(),
            will: None,
            username: None,
            password: None,
        }),
        0x01,
    );
}

#[test]
fn invalid_connack_flags_are_rejected() {
    assert_invalid_flags_rejected(
        Packet::ConnAck(ConnAck {
            session_present: false,
            reason_code: shiguredo_mqtt::v5::connack::ConnectReasonCode::Success,
            properties: Properties::new(),
        }),
        0x01,
    );
}

#[test]
fn invalid_publish_flags_are_rejected() {
    // QoS 0 の PUBLISH を作り、DUP ビットを立てると QoS 0 では DUP は 0 でなければならない。
    assert_invalid_flags_rejected(
        Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "t".to_string(),
            packet_id: None,
            properties: Properties::new(),
            payload: vec![],
        }),
        0x08,
    );
}

#[test]
fn invalid_puback_flags_are_rejected() {
    assert_invalid_flags_rejected(
        Packet::PubAck(PubAck {
            packet_id: 1,
            reason_code: shiguredo_mqtt::v5::puback::PubAckReasonCode::Success,
            properties: Properties::new(),
        }),
        0x01,
    );
}

#[test]
fn invalid_pubrec_flags_are_rejected() {
    assert_invalid_flags_rejected(
        Packet::PubRec(PubRec {
            packet_id: 1,
            reason_code: shiguredo_mqtt::v5::pubrec::PubRecReasonCode::Success,
            properties: Properties::new(),
        }),
        0x01,
    );
}

#[test]
fn invalid_pubrel_flags_are_rejected() {
    // PUBREL の正しいフラグは 0x02 なので、0x00 で検証する。
    assert_invalid_flags_rejected(
        Packet::PubRel(PubRel {
            packet_id: 1,
            reason_code: shiguredo_mqtt::v5::pubrel::PubRelReasonCode::Success,
            properties: Properties::new(),
        }),
        0x00,
    );
}

#[test]
fn invalid_pubcomp_flags_are_rejected() {
    assert_invalid_flags_rejected(
        Packet::PubComp(PubComp {
            packet_id: 1,
            reason_code: shiguredo_mqtt::v5::pubcomp::PubCompReasonCode::Success,
            properties: Properties::new(),
        }),
        0x01,
    );
}

#[test]
fn invalid_subscribe_flags_are_rejected() {
    // SUBSCRIBE の正しいフラグは 0x02 なので、0x00 で検証する。
    assert_invalid_flags_rejected(
        Packet::Subscribe(Subscribe {
            packet_id: 1,
            subscriptions: vec![Subscription {
                topic_filter: "t".to_string(),
                qos: QoS::AtMostOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: shiguredo_mqtt::v5::subscribe::RetainHandling::SendRetained,
            }],
            properties: Properties::new(),
        }),
        0x00,
    );
}

#[test]
fn invalid_suback_flags_are_rejected() {
    assert_invalid_flags_rejected(
        Packet::SubAck(SubAck {
            packet_id: 1,
            reason_codes: vec![shiguredo_mqtt::v5::suback::SubAckReasonCode::GrantedQoS0],
            properties: Properties::new(),
        }),
        0x01,
    );
}

#[test]
fn invalid_unsubscribe_flags_are_rejected() {
    // UNSUBSCRIBE の正しいフラグは 0x02 なので、0x00 で検証する。
    assert_invalid_flags_rejected(
        Packet::Unsubscribe(Unsubscribe {
            packet_id: 1,
            topic_filters: vec!["t".to_string()],
            properties: Properties::new(),
        }),
        0x00,
    );
}

#[test]
fn invalid_unsuback_flags_are_rejected() {
    assert_invalid_flags_rejected(
        Packet::UnsubAck(UnsubAck {
            packet_id: 1,
            reason_codes: vec![shiguredo_mqtt::v5::unsuback::UnsubAckReasonCode::Success],
            properties: Properties::new(),
        }),
        0x01,
    );
}

#[test]
fn invalid_pingreq_flags_are_rejected() {
    assert_invalid_flags_rejected(Packet::PingReq(PingReq), 0x01);
}

#[test]
fn invalid_pingresp_flags_are_rejected() {
    assert_invalid_flags_rejected(Packet::PingResp(PingResp), 0x01);
}

#[test]
fn invalid_disconnect_flags_are_rejected() {
    assert_invalid_flags_rejected(
        Packet::Disconnect(Disconnect {
            reason_code: shiguredo_mqtt::v5::disconnect::DisconnectReasonCode::NormalDisconnection,
            properties: Properties::new(),
        }),
        0x01,
    );
}

#[test]
fn invalid_auth_flags_are_rejected() {
    // AUTH パケットは Authentication Method プロパティが必須。
    let mut properties = Properties::new();
    properties
        .push(shiguredo_mqtt::v5::property::Property::AuthenticationMethod("method".to_string()));
    assert_invalid_flags_rejected(
        Packet::Auth(Auth {
            reason_code: shiguredo_mqtt::v5::auth::AuthReasonCode::ContinueAuthentication,
            properties,
        }),
        0x01,
    );
}

/// 完全な 1 フレームを v5 デコーダーに供給してデコードする。
fn decode_v5(buf: &[u8]) -> Result<Option<VersionedIncomingPacket>, DecodeError> {
    let mut decoder = Decoder::new_v5(Limits::new());
    decoder.feed(buf).expect("バイト列の供給に成功すること");
    decoder.decode()
}

/// 双方向種別の encode 側検証が `OutgoingPacket` 経由でも Client → Server 方向に
/// 焼き付いていることを検証する。
#[test]
fn outgoing_publish_rejects_subscription_identifier() {
    // Client → Server 方向の PUBLISH に Subscription Identifier (0x0B) は許可されない。
    // MQTT v5.0 §3.3.4 [MQTT-3.3.4-6] を参照。
    let mut properties = Properties::new();
    properties.push(Property::SubscriptionIdentifier(VariableByteInteger(1)));
    let packet = OutgoingPacket::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "t".to_string(),
        packet_id: None,
        properties,
        payload: vec![],
    });
    let mut buf = [0u8; 64];
    assert_eq!(
        packet.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::PropertyValidationFailed,
        })
    );
}

#[test]
fn incoming_publish_accepts_subscription_identifier() {
    // Server → Client 方向の PUBLISH では Subscription Identifier (0x0B) を許容する。
    // MQTT v5.0 §3.3.2.3.8 を参照。
    let buf = [
        0x30, 0x07, // PUBLISH, Remaining Length = 7
        0x00, 0x01, b't', // トピック名
        0x02, // Properties Length = 2
        0x0B, 0x01, // Subscription Identifier = 1
        b'x', // ペイロード
    ];
    match decode_v5(&buf) {
        Ok(Some(VersionedIncomingPacket::V5(IncomingPacket::Publish(publish)))) => {
            assert_eq!(publish.topic, "t");
            assert_eq!(publish.properties.iter().count(), 1);
        }
        other => panic!("Subscription Identifier 付き PUBLISH のデコードに失敗: {other:?}"),
    }
}

#[test]
fn outgoing_disconnect_accepts_session_expiry_interval() {
    // Client → Server 方向の DISCONNECT では Session Expiry Interval (0x11) を許容する。
    let mut properties = Properties::new();
    properties.push(Property::SessionExpiryInterval(60));
    let packet = OutgoingPacket::Disconnect(Disconnect {
        reason_code: DisconnectReasonCode::NormalDisconnection,
        properties,
    });
    let mut buf = [0u8; 64];
    packet.encode(&mut buf).expect("エンコードに成功すること");
}

#[test]
fn outgoing_disconnect_rejects_server_only_reason_code() {
    // Server 側専用の Reason Code（0x8E Session taken over）を持つ DISCONNECT は
    // Client → Server 方向としてエンコードできない。
    // MQTT v5.0 §3.14.2.1 Table 3-10 の「Sent by」列を参照。
    let packet = OutgoingPacket::Disconnect(Disconnect {
        reason_code: DisconnectReasonCode::SessionTakenOver,
        properties: Properties::new(),
    });
    let mut buf = [0u8; 64];
    assert_eq!(
        packet.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::InvalidReasonCode,
        })
    );
}

#[test]
fn outgoing_puback_rejects_no_matching_subscribers() {
    // 0x10 No matching subscribers は Server 側専用のため、
    // Client → Server 方向の PUBACK では拒否する。
    // MQTT v5.0 §3.4.2.1 を参照。
    let packet = OutgoingPacket::PubAck(PubAck {
        packet_id: 1,
        reason_code: PubAckReasonCode::NoMatchingSubscribers,
        properties: Properties::new(),
    });
    let mut buf = [0u8; 64];
    assert_eq!(
        packet.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::InvalidReasonCode,
        })
    );
}

#[test]
fn outgoing_pubrec_rejects_no_matching_subscribers() {
    // 0x10 No matching subscribers は Server 側専用のため、
    // Client → Server 方向の PUBREC では拒否する。
    // MQTT v5.0 §3.5.2.1 を参照。
    let packet = OutgoingPacket::PubRec(PubRec {
        packet_id: 1,
        reason_code: PubRecReasonCode::NoMatchingSubscribers,
        properties: Properties::new(),
    });
    let mut buf = [0u8; 64];
    assert_eq!(
        packet.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::InvalidReasonCode,
        })
    );
}

#[test]
fn incoming_disconnect_rejects_session_expiry_interval() {
    // MQTT v5.0 §3.14.2.2.2 [MQTT-3.14.2-2]: Server → Client 方向の DISCONNECT に
    // Session Expiry Interval (0x11) は禁止。
    let buf = [0xE0, 0x07, 0x00, 0x05, 0x11, 0x00, 0x00, 0x00, 0x3C];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn outgoing_auth_rejects_success_reason_code() {
    // 0x00 Success は Server 側専用のため、Client → Server 方向の AUTH では拒否する。
    // MQTT v5.0 §3.15.2.1 Table 3-11 の「Sent by」列を参照。
    let mut properties = Properties::new();
    properties.push(Property::AuthenticationMethod("method".to_string()));
    let packet = OutgoingPacket::Auth(Auth {
        reason_code: AuthReasonCode::Success,
        properties,
    });
    let mut buf = [0u8; 64];
    assert_eq!(
        packet.encode(&mut buf),
        Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::InvalidReasonCode,
        })
    );
}

#[test]
fn outgoing_auth_accepts_client_and_bidirectional_reason_codes() {
    // 0x19 Re-authenticate（Client 側専用）と 0x18 Continue authentication（双方向）は
    // Client → Server 方向の AUTH としてエンコードできる。
    // MQTT v5.0 §3.15.2.1 Table 3-11 の「Sent by」列を参照。
    for reason_code in [
        AuthReasonCode::ReAuthenticate,
        AuthReasonCode::ContinueAuthentication,
    ] {
        let mut properties = Properties::new();
        properties.push(Property::AuthenticationMethod("method".to_string()));
        let packet = OutgoingPacket::Auth(Auth {
            reason_code,
            properties,
        });
        let mut buf = [0u8; 64];
        packet.encode(&mut buf).expect("エンコードに成功すること");
    }
}

#[test]
fn incoming_auth_rejects_reauthenticate_reason_code() {
    // 0x19 Re-authenticate は Client 側専用のため、Server → Client 方向の AUTH では拒否する。
    // MQTT v5.0 §3.15.2.1 Table 3-11 の「Sent by」列を参照。
    let buf = [
        0xF0, 0x01, // AUTH, Remaining Length = 1
        0x19, // Reason Code = Re-authenticate
    ];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn incoming_connack_with_invalid_flags_is_rejected() {
    // Server → Client 種別（CONNACK）のフラグ違反は、Decoder 経由でも
    // 個別 struct のフラグ検証が動き InvalidPacketFlags になる。
    let buf = [0x2F, 0x03, 0x00, 0x00, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::InvalidPacketFlags));
}
