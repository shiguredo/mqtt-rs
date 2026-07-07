//! MQTT v5.0 パケットのプロパティベース・ラウンドトリップ・テスト。

mod helpers;

use proptest::prelude::*;
use proptest::sample::select;
use shiguredo_mqtt::codec::limits::Limits;
use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::codec::variable_byte_integer::VariableByteInteger;
use shiguredo_mqtt::decoder::Decoder;
use shiguredo_mqtt::decoder::VersionedIncomingPacket;
use shiguredo_mqtt::error::DecodeError;
use shiguredo_mqtt::error::EncodeError;
use shiguredo_mqtt::error::EncodeInvalidField;
use shiguredo_mqtt::v5;
use shiguredo_mqtt::v5::packet::IncomingPacket;
use shiguredo_mqtt::v5::packet::OutgoingPacket;
use shiguredo_mqtt::v5::packet::codec;
use shiguredo_mqtt::v5::property::{Properties, Property};
use shiguredo_mqtt::v5::subscribe::RetainHandling;

use helpers::{binary_strategy, qos_strategy, string_strategy};

fn user_property_strategy() -> impl Strategy<Value = Property> {
    (string_strategy(), string_strategy())
        .prop_map(|(key, value)| Property::UserProperty(key, value))
}

fn property_strategy() -> impl Strategy<Value = Property> {
    prop_oneof![
        (0u8..=1u8).prop_map(Property::PayloadFormatIndicator),
        any::<u32>().prop_map(Property::MessageExpiryInterval),
        string_strategy().prop_map(Property::ContentType),
        string_strategy()
            .prop_filter("Response Topic は空にできない", |s| !s.is_empty())
            .prop_map(Property::ResponseTopic),
        binary_strategy().prop_map(Property::CorrelationData),
        (1u32..=VariableByteInteger::MAX)
            .prop_map(|v| Property::SubscriptionIdentifier(VariableByteInteger(v))),
        any::<u32>().prop_map(Property::SessionExpiryInterval),
        string_strategy().prop_map(Property::AssignedClientIdentifier),
        any::<u16>().prop_map(Property::ServerKeepAlive),
        string_strategy().prop_map(Property::AuthenticationMethod),
        binary_strategy().prop_map(Property::AuthenticationData),
        (0u8..=1u8).prop_map(Property::RequestProblemInformation),
        any::<u32>().prop_map(Property::WillDelayInterval),
        (0u8..=1u8).prop_map(Property::RequestResponseInformation),
        string_strategy().prop_map(Property::ResponseInformation),
        string_strategy().prop_map(Property::ServerReference),
        string_strategy().prop_map(Property::ReasonString),
        (1u16..=u16::MAX).prop_map(Property::ReceiveMaximum),
        any::<u16>().prop_map(Property::TopicAliasMaximum),
        (1u16..=u16::MAX).prop_map(Property::TopicAlias),
        (0u8..=1u8).prop_map(Property::MaximumQoS),
        (0u8..=1u8).prop_map(Property::RetainAvailable),
        user_property_strategy(),
        (1u32..=u32::MAX).prop_map(Property::MaximumPacketSize),
        (0u8..=1u8).prop_map(Property::WildcardSubscriptionAvailable),
        (0u8..=1u8).prop_map(Property::SubscriptionIdentifierAvailable),
        (0u8..=1u8).prop_map(Property::SharedSubscriptionAvailable),
    ]
}

fn connect_properties_strategy() -> impl Strategy<Value = Properties> {
    (
        proptest::option::of(any::<u32>().prop_map(Property::SessionExpiryInterval)),
        proptest::option::of((1..=u16::MAX).prop_map(Property::ReceiveMaximum)),
        proptest::option::of((1..=u32::MAX).prop_map(Property::MaximumPacketSize)),
        proptest::option::of(any::<u16>().prop_map(Property::TopicAliasMaximum)),
        proptest::option::of((0u8..=1u8).prop_map(Property::RequestProblemInformation)),
        proptest::option::of((0u8..=1u8).prop_map(Property::RequestResponseInformation)),
        proptest::collection::vec(user_property_strategy(), 0..3),
    )
        .prop_map(
            |(
                session_expiry,
                receive_maximum,
                maximum_packet_size,
                topic_alias_maximum,
                request_problem_information,
                request_response_information,
                user_properties,
            )| {
                let mut props = Properties::new();
                if let Some(p) = session_expiry {
                    props.push(p);
                }
                if let Some(p) = receive_maximum {
                    props.push(p);
                }
                if let Some(p) = maximum_packet_size {
                    props.push(p);
                }
                if let Some(p) = topic_alias_maximum {
                    props.push(p);
                }
                if let Some(p) = request_problem_information {
                    props.push(p);
                }
                if let Some(p) = request_response_information {
                    props.push(p);
                }
                for p in user_properties {
                    props.push(p);
                }
                props
            },
        )
}

fn connack_properties_strategy() -> impl Strategy<Value = Properties> {
    (
        (
            proptest::option::of(any::<u32>().prop_map(Property::SessionExpiryInterval)),
            proptest::option::of((0u8..=1u8).prop_map(Property::MaximumQoS)),
            proptest::option::of((1..=u16::MAX).prop_map(Property::ReceiveMaximum)),
            proptest::option::of((1..=u32::MAX).prop_map(Property::MaximumPacketSize)),
            proptest::option::of(any::<u16>().prop_map(Property::TopicAliasMaximum)),
        ),
        (
            proptest::option::of((0u8..=1u8).prop_map(Property::RetainAvailable)),
            proptest::option::of((0u8..=1u8).prop_map(Property::WildcardSubscriptionAvailable)),
            proptest::option::of((0u8..=1u8).prop_map(Property::SubscriptionIdentifierAvailable)),
            proptest::option::of((0u8..=1u8).prop_map(Property::SharedSubscriptionAvailable)),
        ),
        (
            proptest::option::of(any::<u16>().prop_map(Property::ServerKeepAlive)),
            proptest::option::of(string_strategy().prop_map(Property::AssignedClientIdentifier)),
            proptest::option::of(string_strategy().prop_map(Property::ResponseInformation)),
            proptest::option::of(string_strategy().prop_map(Property::ServerReference)),
            proptest::option::of(string_strategy().prop_map(Property::ReasonString)),
        ),
        proptest::collection::vec(user_property_strategy(), 0..3),
    )
        .prop_map(
            |(
                (
                    session_expiry,
                    maximum_qos,
                    receive_maximum,
                    maximum_packet_size,
                    topic_alias_maximum,
                ),
                (
                    retain_available,
                    wildcard_subscription_available,
                    subscription_identifier_available,
                    shared_subscription_available,
                ),
                (
                    server_keep_alive,
                    assigned_client_identifier,
                    response_information,
                    server_reference,
                    reason_string,
                ),
                user_properties,
            )| {
                let mut props = Properties::new();
                if let Some(p) = session_expiry {
                    props.push(p);
                }
                if let Some(p) = maximum_qos {
                    props.push(p);
                }
                if let Some(p) = receive_maximum {
                    props.push(p);
                }
                if let Some(p) = maximum_packet_size {
                    props.push(p);
                }
                if let Some(p) = topic_alias_maximum {
                    props.push(p);
                }
                if let Some(p) = retain_available {
                    props.push(p);
                }
                if let Some(p) = wildcard_subscription_available {
                    props.push(p);
                }
                if let Some(p) = subscription_identifier_available {
                    props.push(p);
                }
                if let Some(p) = shared_subscription_available {
                    props.push(p);
                }
                if let Some(p) = server_keep_alive {
                    props.push(p);
                }
                if let Some(p) = assigned_client_identifier {
                    props.push(p);
                }
                if let Some(p) = response_information {
                    props.push(p);
                }
                if let Some(p) = server_reference {
                    props.push(p);
                }
                if let Some(p) = reason_string {
                    props.push(p);
                }
                for p in user_properties {
                    props.push(p);
                }
                props
            },
        )
}

fn publish_properties_strategy() -> impl Strategy<Value = Properties> {
    // MQTT v5.0 §3.3.4 [MQTT-3.3.4-6]: Client → Server 方向の PUBLISH では Subscription Identifier (0x0B) は許可されない。
    (
        proptest::option::of((0u8..=1u8).prop_map(Property::PayloadFormatIndicator)),
        proptest::option::of(any::<u32>().prop_map(Property::MessageExpiryInterval)),
        proptest::option::of(string_strategy().prop_map(Property::ContentType)),
        proptest::option::of(
            string_strategy()
                .prop_filter("Response Topic は空にできない", |s| !s.is_empty())
                .prop_map(Property::ResponseTopic),
        ),
        proptest::option::of(binary_strategy().prop_map(Property::CorrelationData)),
        proptest::option::of((1u16..=u16::MAX).prop_map(Property::TopicAlias)),
        proptest::collection::vec(user_property_strategy(), 0..3),
    )
        .prop_map(
            |(
                payload_format,
                message_expiry,
                content_type,
                response_topic,
                correlation_data,
                topic_alias,
                user_properties,
            )| {
                let mut props = Properties::new();
                if let Some(p) = payload_format {
                    props.push(p);
                }
                if let Some(p) = message_expiry {
                    props.push(p);
                }
                if let Some(p) = content_type {
                    props.push(p);
                }
                if let Some(p) = response_topic {
                    props.push(p);
                }
                if let Some(p) = correlation_data {
                    props.push(p);
                }
                if let Some(p) = topic_alias {
                    props.push(p);
                }
                for p in user_properties {
                    props.push(p);
                }
                props
            },
        )
}

fn subscribe_properties_strategy() -> impl Strategy<Value = Properties> {
    (
        proptest::option::of(
            (1u32..=VariableByteInteger::MAX)
                .prop_map(|v| Property::SubscriptionIdentifier(VariableByteInteger(v))),
        ),
        proptest::collection::vec(user_property_strategy(), 0..3),
    )
        .prop_map(|(subscription_id, user_properties)| {
            let mut props = Properties::new();
            if let Some(p) = subscription_id {
                props.push(p);
            }
            for p in user_properties {
                props.push(p);
            }
            props
        })
}

fn will_properties_strategy() -> impl Strategy<Value = Properties> {
    (
        proptest::option::of(any::<u32>().prop_map(Property::WillDelayInterval)),
        proptest::option::of((0u8..=1u8).prop_map(Property::PayloadFormatIndicator)),
        proptest::option::of(any::<u32>().prop_map(Property::MessageExpiryInterval)),
        proptest::option::of(string_strategy().prop_map(Property::ContentType)),
        proptest::option::of(
            string_strategy()
                .prop_filter("Response Topic は空にできない", |s| !s.is_empty())
                .prop_map(Property::ResponseTopic),
        ),
        proptest::option::of(binary_strategy().prop_map(Property::CorrelationData)),
        proptest::collection::vec(user_property_strategy(), 0..3),
    )
        .prop_map(
            |(
                will_delay_interval,
                payload_format,
                message_expiry,
                content_type,
                response_topic,
                correlation_data,
                user_properties,
            )| {
                let mut props = Properties::new();
                if let Some(p) = will_delay_interval {
                    props.push(p);
                }
                if let Some(p) = payload_format {
                    props.push(p);
                }
                if let Some(p) = message_expiry {
                    props.push(p);
                }
                if let Some(p) = content_type {
                    props.push(p);
                }
                if let Some(p) = response_topic {
                    props.push(p);
                }
                if let Some(p) = correlation_data {
                    props.push(p);
                }
                for p in user_properties {
                    props.push(p);
                }
                props
            },
        )
}

fn disconnect_properties_strategy() -> impl Strategy<Value = Properties> {
    // Client → Server 方向の DISCONNECT では Session Expiry Interval (0x11) が許可されるが、
    // PBT の roundtrip は encode → decode なので、decode 側の Server → Client 制約（MQTT v5.0 §3.14.2.2.2 [MQTT-3.14.2-2]）に引っかかる。
    // ここでは許可される Reason String と User Property のみを生成する。
    (
        proptest::option::of(string_strategy().prop_map(Property::ReasonString)),
        proptest::collection::vec(user_property_strategy(), 0..3),
    )
        .prop_map(|(reason_string, user_properties)| {
            let mut props = Properties::new();
            if let Some(p) = reason_string {
                props.push(p);
            }
            for p in user_properties {
                props.push(p);
            }
            props
        })
}

fn auth_properties_strategy() -> impl Strategy<Value = Properties> {
    (
        string_strategy().prop_map(Property::AuthenticationMethod),
        proptest::collection::vec(user_property_strategy(), 0..3),
    )
        .prop_map(|(auth_method, user_properties)| {
            let mut props = Properties::new();
            // MQTT v5.0 §3.15.2.2.2: AUTH パケットには Authentication Method が必須。
            props.push(auth_method);
            for p in user_properties {
                props.push(p);
            }
            props
        })
}

fn reason_code_properties_strategy() -> impl Strategy<Value = Properties> {
    (
        proptest::option::of(string_strategy().prop_map(Property::ReasonString)),
        proptest::collection::vec(user_property_strategy(), 0..3),
    )
        .prop_map(|(reason_string, user_properties)| {
            let mut props = Properties::new();
            if let Some(p) = reason_string {
                props.push(p);
            }
            for p in user_properties {
                props.push(p);
            }
            props
        })
}

fn empty_properties_strategy() -> impl Strategy<Value = Properties> {
    Just(Properties::new())
}

fn will_strategy() -> impl Strategy<Value = v5::connect::Will> {
    (
        string_strategy(),
        qos_strategy(),
        any::<bool>(),
        binary_strategy(),
        will_properties_strategy(),
    )
        .prop_filter("Will Topic は空にできない", |(topic, _, _, _, _)| {
            !topic.is_empty()
        })
        .prop_filter(
            "Will Topic にワイルドカード文字は使用できない",
            |(topic, _, _, _, _)| !topic.contains('+') && !topic.contains('#'),
        )
        .prop_map(|(topic, qos, retain, payload, properties)| {
            // MQTT v5.0 §3.1.3.2.3: Payload Format Indicator が 1 の場合、
            // Will Payload は well-formed UTF-8 でなければならないため、
            // 生成した任意バイト列を有効な UTF-8 に変換する。
            let payload = if properties
                .iter()
                .any(|p| matches!(p, Property::PayloadFormatIndicator(1)))
            {
                String::from_utf8_lossy(&payload).into_owned().into_bytes()
            } else {
                payload
            };
            v5::connect::Will {
                topic,
                qos,
                retain,
                payload,
                properties,
            }
        })
}

fn connect_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        string_strategy(),
        any::<bool>(),
        any::<u16>(),
        connect_properties_strategy(),
        proptest::option::of(will_strategy()),
        proptest::option::of(string_strategy()),
        proptest::option::of(binary_strategy()),
    )
        .prop_map(
            |(client_id, clean_start, keep_alive, properties, will, username, password)| {
                codec::Packet::Connect(v5::connect::Connect {
                    client_id,
                    clean_start,
                    keep_alive,
                    properties,
                    will,
                    username,
                    password,
                })
            },
        )
}

fn connack_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        any::<bool>(),
        select(&[
            v5::connack::ConnectReasonCode::Success,
            v5::connack::ConnectReasonCode::NotAuthorized,
            v5::connack::ConnectReasonCode::ServerUnavailable,
        ]),
        connack_properties_strategy(),
    )
        .prop_filter(
            "session_present は Success の場合のみ true",
            |(session_present, reason_code, _)| {
                !(*session_present && *reason_code != v5::connack::ConnectReasonCode::Success)
            },
        )
        .prop_map(|(session_present, reason_code, properties)| {
            codec::Packet::ConnAck(v5::connack::ConnAck {
                session_present,
                reason_code,
                properties,
            })
        })
}

fn publish_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        string_strategy(),
        qos_strategy(),
        any::<u16>(),
        any::<bool>(),
        any::<bool>(),
        binary_strategy(),
        publish_properties_strategy(),
    )
        .prop_filter(
            "有効な PUBLISH パケット識別子",
            |(_, qos, packet_id, dup, _, _, _)| {
                if *qos == QoS::AtMostOnce {
                    *packet_id == 0 && !dup
                } else {
                    *packet_id != 0
                }
            },
        )
        .prop_filter(
            "PUBLISH のトピック名にワイルドカード文字を含めない",
            |(topic, _, _, _, _, _, _)| !topic.contains('#') && !topic.contains('+'),
        )
        .prop_filter(
            "PUBLISH のトピック名は空にできない（TopicAlias なしの場合）",
            |(topic, _, _, _, _, _, properties)| {
                !topic.is_empty()
                    || properties
                        .iter()
                        .any(|p| matches!(p, Property::TopicAlias(_)))
            },
        )
        .prop_map(
            |(topic, qos, packet_id, dup, retain, payload, properties)| {
                let id = if qos == QoS::AtMostOnce {
                    None
                } else {
                    Some(packet_id)
                };
                // MQTT v5.0 §3.3.2.3.2: Payload Format Indicator が 1 の場合、
                // Payload は well-formed UTF-8 でなければならないため、
                // 生成した任意バイト列を有効な UTF-8 に変換する。
                let payload = if properties
                    .iter()
                    .any(|p| matches!(p, Property::PayloadFormatIndicator(1)))
                {
                    String::from_utf8_lossy(&payload).into_owned().into_bytes()
                } else {
                    payload
                };
                codec::Packet::Publish(v5::publish::Publish {
                    dup,
                    qos,
                    retain,
                    topic,
                    packet_id: id,
                    properties,
                    payload,
                })
            },
        )
}

fn puback_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        1..=u16::MAX,
        // MQTT v5.0 §3.4.2.1: 0x10 No matching subscribers はサーバー専用のため、
        // クライアント送信（encode → decode roundtrip）の対象から除外する。
        select(&[
            v5::puback::PubAckReasonCode::Success,
            v5::puback::PubAckReasonCode::QuotaExceeded,
            v5::puback::PubAckReasonCode::NotAuthorized,
        ]),
        reason_code_properties_strategy(),
    )
        .prop_map(|(packet_id, reason_code, properties)| {
            codec::Packet::PubAck(v5::puback::PubAck {
                packet_id,
                reason_code,
                properties,
            })
        })
}

fn pubrec_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        1..=u16::MAX,
        // MQTT v5.0 §3.5.2.1: 0x10 No matching subscribers はサーバー専用のため、
        // クライアント送信（encode → decode roundtrip）の対象から除外する。
        select(&[
            v5::pubrec::PubRecReasonCode::Success,
            v5::pubrec::PubRecReasonCode::QuotaExceeded,
            v5::pubrec::PubRecReasonCode::NotAuthorized,
        ]),
        reason_code_properties_strategy(),
    )
        .prop_map(|(packet_id, reason_code, properties)| {
            codec::Packet::PubRec(v5::pubrec::PubRec {
                packet_id,
                reason_code,
                properties,
            })
        })
}

fn pubrel_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        1..=u16::MAX,
        select(&[
            v5::pubrel::PubRelReasonCode::Success,
            v5::pubrel::PubRelReasonCode::PacketIdentifierNotFound,
        ]),
        reason_code_properties_strategy(),
    )
        .prop_map(|(packet_id, reason_code, properties)| {
            codec::Packet::PubRel(v5::pubrel::PubRel {
                packet_id,
                reason_code,
                properties,
            })
        })
}

fn pubcomp_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        1..=u16::MAX,
        select(&[
            v5::pubcomp::PubCompReasonCode::Success,
            v5::pubcomp::PubCompReasonCode::PacketIdentifierNotFound,
        ]),
        reason_code_properties_strategy(),
    )
        .prop_map(|(packet_id, reason_code, properties)| {
            codec::Packet::PubComp(v5::pubcomp::PubComp {
                packet_id,
                reason_code,
                properties,
            })
        })
}

fn normal_topic_filter_strategy() -> impl Strategy<Value = (String, bool)> {
    helpers::topic_filter_strategy().prop_map(|s| (s, false))
}

fn shared_topic_filter_strategy() -> impl Strategy<Value = (String, bool)> {
    (
        proptest::collection::vec(
            any::<char>().prop_filter("ShareName に /, +, #, NUL は使用できない", |c| {
                *c != '\0' && *c != '/' && *c != '+' && *c != '#'
            }),
            1..20,
        )
        .prop_map(|chars| chars.into_iter().collect::<String>()),
        helpers::topic_filter_strategy(),
    )
        .prop_map(|(share_name, filter)| (format!("$share/{}/{}", share_name, filter), true))
}

fn topic_filter_strategy() -> impl Strategy<Value = (String, bool)> {
    prop_oneof![
        normal_topic_filter_strategy(),
        shared_topic_filter_strategy(),
    ]
}

fn subscription_strategy() -> impl Strategy<Value = v5::subscribe::Subscription> {
    (
        topic_filter_strategy(),
        qos_strategy(),
        any::<bool>(),
        any::<bool>(),
        select(&[
            RetainHandling::SendRetained,
            RetainHandling::SendRetainedIfNotExists,
            RetainHandling::DoNotSendRetained,
        ]),
    )
        .prop_filter(
            // MQTT v5.0 §3.8.3.1 [MQTT-3.8.3-4]:
            // 共有サブスクリプションで No Local ビットを 1 にすることは Protocol Error である。
            "共有サブスクリプションでは no_local は false でなければならない",
            |((_, is_shared), _, no_local, _, _)| !is_shared || !no_local,
        )
        .prop_map(
            |((topic_filter, _is_shared), qos, no_local, retain_as_published, retain_handling)| {
                v5::subscribe::Subscription {
                    topic_filter,
                    qos,
                    no_local,
                    retain_as_published,
                    retain_handling,
                }
            },
        )
}

fn subscribe_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        1..=u16::MAX,
        proptest::collection::vec(subscription_strategy(), 1..5),
        subscribe_properties_strategy(),
    )
        .prop_map(|(packet_id, subscriptions, properties)| {
            codec::Packet::Subscribe(v5::subscribe::Subscribe {
                packet_id,
                subscriptions,
                properties,
            })
        })
}

fn suback_reason_code_strategy() -> impl Strategy<Value = v5::suback::SubAckReasonCode> {
    select(&[
        v5::suback::SubAckReasonCode::GrantedQoS0,
        v5::suback::SubAckReasonCode::GrantedQoS1,
        v5::suback::SubAckReasonCode::GrantedQoS2,
        v5::suback::SubAckReasonCode::NotAuthorized,
    ])
}

fn suback_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        1..=u16::MAX,
        proptest::collection::vec(suback_reason_code_strategy(), 1..5),
        reason_code_properties_strategy(),
    )
        .prop_map(|(packet_id, reason_codes, properties)| {
            codec::Packet::SubAck(v5::suback::SubAck {
                packet_id,
                reason_codes,
                properties,
            })
        })
}

fn unsubscribe_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        1..=u16::MAX,
        proptest::collection::vec(helpers::topic_filter_strategy(), 1..5),
        empty_properties_strategy(),
    )
        .prop_map(|(packet_id, topic_filters, properties)| {
            codec::Packet::Unsubscribe(v5::unsubscribe::Unsubscribe {
                packet_id,
                topic_filters,
                properties,
            })
        })
}

fn unsuback_reason_code_strategy() -> impl Strategy<Value = v5::unsuback::UnsubAckReasonCode> {
    select(&[
        v5::unsuback::UnsubAckReasonCode::Success,
        v5::unsuback::UnsubAckReasonCode::NoSubscriptionExisted,
        v5::unsuback::UnsubAckReasonCode::NotAuthorized,
    ])
}

fn unsuback_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        1..=u16::MAX,
        proptest::collection::vec(unsuback_reason_code_strategy(), 1..5),
        reason_code_properties_strategy(),
    )
        .prop_map(|(packet_id, reason_codes, properties)| {
            codec::Packet::UnsubAck(v5::unsuback::UnsubAck {
                packet_id,
                reason_codes,
                properties,
            })
        })
}

fn disconnect_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        // packet_roundtrip は encode -> decode を検証するため、
        // Client → Server と Server → Client の両方向で合法な Reason Code のみを使う。
        select(&[
            v5::disconnect::DisconnectReasonCode::NormalDisconnection,
            v5::disconnect::DisconnectReasonCode::UnspecifiedError,
            v5::disconnect::DisconnectReasonCode::BadAuthenticationMethod,
        ]),
        disconnect_properties_strategy(),
    )
        .prop_map(|(reason_code, properties)| {
            codec::Packet::Disconnect(v5::disconnect::Disconnect {
                reason_code,
                properties,
            })
        })
}

fn auth_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        // packet_roundtrip は encode -> decode を検証するため、
        // 両方向で合法な Reason Code（0x18）のみを使う。0x00 は encode 不可、
        // 0x19 は decode 不可のため片方向専用テスト側で扱う。
        select(&[v5::auth::AuthReasonCode::ContinueAuthentication]),
        auth_properties_strategy(),
    )
        .prop_map(|(reason_code, properties)| {
            codec::Packet::Auth(v5::auth::Auth {
                reason_code,
                properties,
            })
        })
}

// 全種別の `codec::Packet` を生成する戦略（低水準 codec のラウンドトリップ検証用）。
fn codec_packet_strategy() -> BoxedStrategy<codec::Packet> {
    prop_oneof![
        connect_strategy().boxed(),
        connack_strategy().boxed(),
        publish_strategy().boxed(),
        puback_strategy().boxed(),
        pubrec_strategy().boxed(),
        pubrel_strategy().boxed(),
        pubcomp_strategy().boxed(),
        subscribe_strategy().boxed(),
        suback_strategy().boxed(),
        unsubscribe_strategy().boxed(),
        unsuback_strategy().boxed(),
        Just(codec::Packet::PingReq(v5::pingreq::PingReq)).boxed(),
        Just(codec::Packet::PingResp(v5::pingresp::PingResp)).boxed(),
        disconnect_strategy().boxed(),
        auth_strategy().boxed(),
    ]
    .boxed()
}

// Server → Client 方向の種別のみを生成する戦略（Decoder 経由のラウンドトリップ検証用）。
// 双方向種別（PUBLISH / PUBACK 系 / DISCONNECT / AUTH）は両方向で合法な
// Reason Code とプロパティのみを生成するため、encode（Client → Server 検証）と
// decode（Server → Client 検証）の両方を通る。
fn incoming_packet_strategy() -> BoxedStrategy<codec::Packet> {
    prop_oneof![
        connack_strategy().boxed(),
        publish_strategy().boxed(),
        puback_strategy().boxed(),
        pubrec_strategy().boxed(),
        pubrel_strategy().boxed(),
        pubcomp_strategy().boxed(),
        suback_strategy().boxed(),
        unsuback_strategy().boxed(),
        Just(codec::Packet::PingResp(v5::pingresp::PingResp)).boxed(),
        disconnect_strategy().boxed(),
        auth_strategy().boxed(),
    ]
    .boxed()
}

// Client → Server 専用種別のみを生成する戦略（Decoder の方向拒否検証用）。
fn outgoing_only_packet_strategy() -> BoxedStrategy<codec::Packet> {
    prop_oneof![
        connect_strategy().boxed(),
        subscribe_strategy().boxed(),
        unsubscribe_strategy().boxed(),
        Just(codec::Packet::PingReq(v5::pingreq::PingReq)).boxed(),
    ]
    .boxed()
}

// Server → Client 側専用の Reason Code やプロパティを含む OutgoingPacket を
// 生成する戦略。encode が Err(EncodeError::InvalidField { .. }) を返すことを検証する。
fn reverse_direction_outgoing_strategy() -> BoxedStrategy<OutgoingPacket> {
    // Success (0x00) は Server 側専用の Reason Code（MQTT v5.0 §3.15.2.1 Table 3-11）
    let auth = auth_properties_strategy().prop_map(|properties| {
        OutgoingPacket::Auth(v5::auth::Auth {
            reason_code: v5::auth::AuthReasonCode::Success,
            properties,
        })
    });
    // Server 側専用の Reason Code（MQTT v5.0 §3.14.2.1 Table 3-10 の「Sent by」列）
    let disconnect = (
        select(&[
            v5::disconnect::DisconnectReasonCode::ServerShuttingDown,
            v5::disconnect::DisconnectReasonCode::SessionTakenOver,
            v5::disconnect::DisconnectReasonCode::ServerMoved,
        ]),
        reason_code_properties_strategy(),
    )
        .prop_map(|(reason_code, properties)| {
            OutgoingPacket::Disconnect(v5::disconnect::Disconnect {
                reason_code,
                properties,
            })
        });
    // Subscription Identifier (0x0B) は Client → Server 方向の PUBLISH では許可されない
    // （MQTT v5.0 §3.3.4 [MQTT-3.3.4-6]）
    let publish = (
        string_strategy(),
        qos_strategy(),
        any::<u16>(),
        binary_strategy(),
        (1u32..=VariableByteInteger::MAX).prop_map(VariableByteInteger),
    )
        .prop_filter(
            "PUBLISH のトピック名は空にできない",
            |(topic, ..)| !topic.is_empty(),
        )
        .prop_filter(
            "有効な PUBLISH パケット識別子",
            |(_, qos, packet_id, ..)| *qos == QoS::AtMostOnce || *packet_id != 0,
        )
        .prop_map(|(topic, qos, packet_id, payload, subscription_id)| {
            let id = if qos == QoS::AtMostOnce {
                None
            } else {
                Some(packet_id)
            };
            let mut properties = Properties::new();
            properties.push(Property::SubscriptionIdentifier(subscription_id));
            OutgoingPacket::Publish(v5::publish::Publish {
                dup: false,
                qos,
                retain: false,
                topic,
                packet_id: id,
                properties,
                payload,
            })
        });
    // 0x10 No matching subscribers は Server 側専用（MQTT v5.0 §3.4.2.1 / MQTT v5.0 §3.5.2.1）
    let puback =
        (1..=u16::MAX, reason_code_properties_strategy()).prop_map(|(packet_id, properties)| {
            OutgoingPacket::PubAck(v5::puback::PubAck {
                packet_id,
                reason_code: v5::puback::PubAckReasonCode::NoMatchingSubscribers,
                properties,
            })
        });
    let pubrec =
        (1..=u16::MAX, reason_code_properties_strategy()).prop_map(|(packet_id, properties)| {
            OutgoingPacket::PubRec(v5::pubrec::PubRec {
                packet_id,
                reason_code: v5::pubrec::PubRecReasonCode::NoMatchingSubscribers,
                properties,
            })
        });
    prop_oneof![
        auth.boxed(),
        disconnect.boxed(),
        publish.boxed(),
        puback.boxed(),
        pubrec.boxed(),
    ]
    .boxed()
}

proptest! {
    #[test]
    fn property_roundtrip(property in property_strategy()) {
        let mut buf = vec![0u8; 65536];
        let len = property.encode(&mut buf).expect("エンコードに成功すること");
        let (decoded, consumed) = Property::decode(&buf[..len]).expect("デコードに成功すること");
        prop_assert_eq!(decoded, property);
        prop_assert_eq!(consumed, len);
    }

    #[test]
    fn packet_roundtrip(packet in codec_packet_strategy()) {
        let mut buf = vec![0u8; 65536];
        let len = packet.encode(&mut buf).expect("エンコードに成功すること");
        let (decoded, consumed) = codec::Packet::decode(&buf[..len]).expect("デコードに成功すること");
        prop_assert_eq!(decoded.clone(), packet);
        prop_assert_eq!(consumed, len);

        let mut buf2 = vec![0u8; 65536];
        let len2 = decoded.encode(&mut buf2).expect("再エンコードに成功すること");
        prop_assert_eq!(&buf[..len], &buf2[..len2]);
    }

    #[test]
    fn packet_encode_exact_buffer(packet in codec_packet_strategy()) {
        // 合計長ちょうどのバッファで encode() が Ok(合計長) を返すことを検証する。
        // 戻り値の一致まで検証し、encoded_len() の過大見積もりも検出する。
        // 過小見積もりの場合はバッファ書き込み時の panic または
        // 内部エンコーダからの予期しない Err(BufferTooSmall) として検出される。
        let encoded = packet.encode_to_vec().expect("エンコード可能なパケットであること");
        let total_len = encoded.len();
        let mut buf = vec![0u8; total_len];
        let len = packet
            .encode(&mut buf)
            .expect("合計長ちょうどのバッファでエンコードに成功すること");
        prop_assert_eq!(len, total_len);
        prop_assert_eq!(&buf[..len], &encoded[..]);
    }

    #[test]
    fn packet_encode_short_buffer_fails(packet in codec_packet_strategy()) {
        // 合計長 - 1 のバッファで encode() が Err(BufferTooSmall) を返すことを検証する。
        let encoded = packet.encode_to_vec().expect("エンコード可能なパケットであること");
        let total_len = encoded.len();
        let mut buf = vec![0u8; total_len - 1];
        let result = packet.encode(&mut buf);
        prop_assert_eq!(result, Err(EncodeError::BufferTooSmall));
    }

    #[test]
    fn incoming_roundtrip(packet in incoming_packet_strategy()) {
        // Server → Client 方向のパケットを低水準 codec でエンコードし、
        // Decoder 経由で IncomingPacket としてデコードして内部 struct が一致することを検証する。
        let encoded = packet.encode_to_vec().expect("エンコードに成功すること");
        let mut decoder = Decoder::new_v5(Limits::new());
        decoder.feed(&encoded).expect("feed に成功すること");
        let decoded = decoder
            .decode()
            .expect("デコードに成功すること")
            .expect("パケットが得られること");
        match (decoded, &packet) {
            (
                VersionedIncomingPacket::V5(IncomingPacket::ConnAck(decoded)),
                codec::Packet::ConnAck(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::Publish(decoded)),
                codec::Packet::Publish(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::PubAck(decoded)),
                codec::Packet::PubAck(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::PubRec(decoded)),
                codec::Packet::PubRec(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::PubRel(decoded)),
                codec::Packet::PubRel(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::PubComp(decoded)),
                codec::Packet::PubComp(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::SubAck(decoded)),
                codec::Packet::SubAck(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::UnsubAck(decoded)),
                codec::Packet::UnsubAck(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::PingResp(decoded)),
                codec::Packet::PingResp(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::Disconnect(decoded)),
                codec::Packet::Disconnect(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::Auth(decoded)),
                codec::Packet::Auth(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            _ => prop_assert!(false, "デコード結果の種別が元のパケットと一致しない"),
        }
    }

    #[test]
    fn decoder_rejects_reverse_direction(
        outgoing in outgoing_only_packet_strategy(),
        incoming in incoming_packet_strategy(),
    ) {
        // Client → Server 専用種別を feed すると Decoder は UnexpectedPacket で拒否する。
        let encoded = outgoing.encode_to_vec().expect("エンコードに成功すること");
        let mut decoder = Decoder::new_v5(Limits::new());
        decoder.feed(&encoded).expect("feed に成功すること");
        match decoder.decode() {
            Err(DecodeError::UnexpectedPacket { packet_type }) => {
                prop_assert_eq!(packet_type, encoded[0] & 0xF0);
            }
            _ => prop_assert!(false, "UnexpectedPacket エラーが返ること"),
        }
        // 1 フレーム消費・後続維持: 拒否後に正しい方向のパケットを続けてデコードできる。
        let incoming_encoded = incoming.encode_to_vec().expect("エンコードに成功すること");
        decoder
            .feed(&incoming_encoded)
            .expect("feed に成功すること");
        let decoded = decoder.decode().expect("デコードに成功すること");
        prop_assert!(matches!(decoded, Some(VersionedIncomingPacket::V5(_))));
    }

    #[test]
    fn outgoing_encode_rejects_reverse_direction(
        packet in reverse_direction_outgoing_strategy(),
    ) {
        // Server → Client 側専用の Reason Code やプロパティを含む OutgoingPacket の
        // エンコードは Err(EncodeError::InvalidField { .. }) で拒否される。
        // 拒否原因は複数あり得るため、原因を問わず検証エラーであることのみを見る。
        // `matches!` の波括弧が prop_assert! 内部の concat! と衝突するため、
        // 判定結果を変数に取り出してから渡す。
        let is_invalid_field = matches!(packet.encode_to_vec(), Err(EncodeError::InvalidField { .. }));
        prop_assert!(is_invalid_field);
    }
}

proptest! {
    // Just 固定入力のため、デフォルト 256 ケースで同一内容が繰り返されるのを防ぐ。
    #![proptest_config(ProptestConfig::with_cases(1))]

    #[test]
    fn remaining_length_boundary(_input in Just(())) {
        // 残り長さの符号化長が変化する境界（127・128・16,383・16,384）を
        // 1 ケース内で確定的に生成して検証する。
        //
        // MQTT v5.0 §1.5.5 [MQTT-1.5.5-1]:
        // 残り長さは最小バイト数でエンコードする必要がある。
        // VBI の符号化長そのものの検証は codec 層の単体テストに委ね、
        // パケットレベルでは encoded_len() の一致と戻り値の一致で間接的に検出する。
        //
        // PUBLISH QoS 0・トピック "t"（1 バイト）・空プロパティをベースにする。
        // QoS 0 の PUBLISH は dup: true や packet_id: Some(_) だと
        // validate() が Err(InvalidField) を返すため、dup: false・packet_id: None で構築する。
        // プロパティを空に固定してもプロパティ長 VBI の 1 バイトが入るため、
        // v5.0 の固定オーバーヘッド: 2（トピック長）+ 1（トピック "t"）+ 1（プロパティ長 VBI）。
        let overhead = 2 + 1 + 1;
        for &target in helpers::REMAINING_LENGTH_BOUNDARIES {
            let payload_len = helpers::payload_len_for_remaining_length(target, overhead);
            let packet = codec::Packet::Publish(v5::publish::Publish {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: false,
                topic: "t".to_string(),
                packet_id: None,
                properties: Properties::new(),
                payload: vec![0u8; payload_len],
            });
            prop_assert_eq!(packet.encoded_len(), target);

            let encoded = packet.encode_to_vec().expect("エンコードに成功すること");
            let total_len = encoded.len();

            let mut buf = vec![0u8; total_len];
            let len = packet
                .encode(&mut buf)
                .expect("合計長ちょうどでエンコードに成功すること");
            prop_assert_eq!(len, total_len);

            let mut short_buf = vec![0u8; total_len - 1];
            let result = packet.encode(&mut short_buf);
            prop_assert_eq!(result, Err(EncodeError::BufferTooSmall));
        }
    }
}

// ======================================================================
// プロパティ許可リスト検証 (src/v5/property/validate.rs) の PBT
// ======================================================================

/// MQTT v5.0 に存在する全プロパティ識別子。
///
/// MQTT v5.0 §2.2.2.2 Table 2-4 を出典とする。
const ALL_PROPERTY_IDENTIFIERS: &[u8] = &[
    0x01, 0x02, 0x03, 0x08, 0x09, 0x0B, 0x11, 0x12, 0x13, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1C,
    0x1F, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A,
];

/// CONNECT の Will プロパティで許可される識別子。
///
/// MQTT v5.0 §3.1.3.2 (Will Properties) を出典とする。
const WILL_ALLOWED_IDENTIFIERS: &[u8] = &[0x18, 0x01, 0x02, 0x03, 0x08, 0x09, 0x26];

/// 指定した識別子の代表プロパティ (単体では有効な値) を返す。
///
/// 許可リスト検証だけが失敗要因になるよう、値そのものの検証
/// (0/1 制約や非ゼロ制約など) は通る値を選ぶ。
fn sample_property(identifier: u8) -> Property {
    match identifier {
        0x01 => Property::PayloadFormatIndicator(1),
        0x02 => Property::MessageExpiryInterval(60),
        0x03 => Property::ContentType("text/plain".to_string()),
        0x08 => Property::ResponseTopic("resp/topic".to_string()),
        0x09 => Property::CorrelationData(vec![0x01]),
        0x0B => Property::SubscriptionIdentifier(VariableByteInteger(1)),
        0x11 => Property::SessionExpiryInterval(60),
        0x12 => Property::AssignedClientIdentifier("client-1".to_string()),
        0x13 => Property::ServerKeepAlive(30),
        0x15 => Property::AuthenticationMethod("method".to_string()),
        0x16 => Property::AuthenticationData(vec![0x01]),
        0x17 => Property::RequestProblemInformation(1),
        0x18 => Property::WillDelayInterval(5),
        0x19 => Property::RequestResponseInformation(1),
        0x1A => Property::ResponseInformation("info".to_string()),
        0x1C => Property::ServerReference("server".to_string()),
        0x1F => Property::ReasonString("reason".to_string()),
        0x21 => Property::ReceiveMaximum(10),
        0x22 => Property::TopicAliasMaximum(5),
        0x23 => Property::TopicAlias(1),
        0x24 => Property::MaximumQoS(1),
        0x25 => Property::RetainAvailable(1),
        0x26 => Property::UserProperty("key".to_string(), "value".to_string()),
        0x27 => Property::MaximumPacketSize(1024),
        0x28 => Property::WildcardSubscriptionAvailable(1),
        0x29 => Property::SubscriptionIdentifierAvailable(1),
        0x2A => Property::SharedSubscriptionAvailable(1),
        _ => unreachable!("MQTT v5.0 に存在しないプロパティ識別子"),
    }
}

/// パケットのエンコード時に許可されるプロパティ識別子を返す。
///
/// MQTT v5.0 §2.2.2.2 Table 2-4 を出典とし、方向依存のものは
/// クライアント送信 (エンコード) の方向で解釈する。
///
/// - PUBLISH: Subscription Identifier (0x0B) は Server → Client のみ
///   (MQTT v5.0 §3.3.4 [MQTT-3.3.4-6])。
///
/// PINGREQ / PINGRESP はプロパティを持たないため None を返す。
fn encode_allowed_identifiers(packet: &codec::Packet) -> Option<&'static [u8]> {
    match packet {
        codec::Packet::Connect(_) => Some(&[0x11, 0x21, 0x27, 0x22, 0x19, 0x17, 0x26, 0x15, 0x16]),
        codec::Packet::ConnAck(_) => Some(&[
            0x11, 0x21, 0x24, 0x25, 0x27, 0x12, 0x22, 0x1F, 0x26, 0x28, 0x29, 0x2A, 0x13, 0x1A,
            0x1C, 0x15, 0x16,
        ]),
        codec::Packet::Publish(_) => Some(&[0x01, 0x02, 0x03, 0x08, 0x09, 0x23, 0x26]),
        codec::Packet::PubAck(_)
        | codec::Packet::PubRec(_)
        | codec::Packet::PubRel(_)
        | codec::Packet::PubComp(_)
        | codec::Packet::SubAck(_)
        | codec::Packet::UnsubAck(_) => Some(&[0x1F, 0x26]),
        codec::Packet::Subscribe(_) => Some(&[0x0B, 0x26]),
        codec::Packet::Unsubscribe(_) => Some(&[0x26]),
        codec::Packet::Disconnect(_) => Some(&[0x11, 0x1C, 0x1F, 0x26]),
        codec::Packet::Auth(_) => Some(&[0x15, 0x16, 0x1F, 0x26]),
        codec::Packet::PingReq(_) | codec::Packet::PingResp(_) => None,
    }
}

/// パケットのプロパティ一覧への可変参照を返す。PINGREQ / PINGRESP は None。
fn packet_properties_mut(packet: &mut codec::Packet) -> Option<&mut Properties> {
    match packet {
        codec::Packet::Connect(p) => Some(&mut p.properties),
        codec::Packet::ConnAck(p) => Some(&mut p.properties),
        codec::Packet::Publish(p) => Some(&mut p.properties),
        codec::Packet::PubAck(p) => Some(&mut p.properties),
        codec::Packet::PubRec(p) => Some(&mut p.properties),
        codec::Packet::PubRel(p) => Some(&mut p.properties),
        codec::Packet::PubComp(p) => Some(&mut p.properties),
        codec::Packet::Subscribe(p) => Some(&mut p.properties),
        codec::Packet::SubAck(p) => Some(&mut p.properties),
        codec::Packet::Unsubscribe(p) => Some(&mut p.properties),
        codec::Packet::UnsubAck(p) => Some(&mut p.properties),
        codec::Packet::Disconnect(p) => Some(&mut p.properties),
        codec::Packet::Auth(p) => Some(&mut p.properties),
        codec::Packet::PingReq(_) | codec::Packet::PingResp(_) => None,
    }
}

/// 有効なパケットと、そのパケットでは許可されないプロパティの組を生成する。
fn packet_and_disallowed_property_strategy() -> impl Strategy<Value = (codec::Packet, Property)> {
    codec_packet_strategy()
        .prop_filter(
            "プロパティを持つパケットのみを対象にする",
            |packet| encode_allowed_identifiers(packet).is_some(),
        )
        .prop_flat_map(|packet| {
            let allowed =
                encode_allowed_identifiers(&packet).expect("プロパティを持つパケットであること");
            let disallowed: Vec<u8> = ALL_PROPERTY_IDENTIFIERS
                .iter()
                .copied()
                .filter(|identifier| !allowed.contains(identifier))
                .collect();
            (Just(packet), select(disallowed).prop_map(sample_property))
        })
}

proptest! {
    #[test]
    fn packet_with_disallowed_property_is_rejected_on_encode(
        (mut packet, property) in packet_and_disallowed_property_strategy()
    ) {
        // MQTT v5.0 §2.2.2.2 Table 2-4:
        // プロパティはパケット種別ごとに使用できるものが定められている。
        // 許可されない識別子を 1 つ挿入した有効パケットは、
        // エンコード時の許可リスト検証で必ず拒否される。
        packet_properties_mut(&mut packet)
            .expect("プロパティを持つパケットであること")
            .push(property);
        let mut buf = vec![0u8; 65536];
        prop_assert_eq!(
            packet.encode(&mut buf),
            Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::PropertyValidationFailed,
            })
        );
    }

    #[test]
    fn connect_will_with_disallowed_property_is_rejected_on_encode(
        packet in connect_strategy(),
        will in will_strategy(),
        identifier in select(
            ALL_PROPERTY_IDENTIFIERS
                .iter()
                .copied()
                .filter(|identifier| !WILL_ALLOWED_IDENTIFIERS.contains(identifier))
                .collect::<Vec<u8>>()
        )
    ) {
        // MQTT v5.0 §3.1.3.2:
        // Will プロパティにも許可リストがあり、許可されない識別子を
        // 挿入した Will 付き CONNECT はエンコード時に拒否される。
        let codec::Packet::Connect(mut connect) = packet else {
            unreachable!("connect_strategy は CONNECT パケットのみを生成する");
        };
        let mut will = will;
        will.properties.push(sample_property(identifier));
        connect.will = Some(will);
        let mut buf = vec![0u8; 65536];
        prop_assert_eq!(
            codec::Packet::Connect(connect).encode(&mut buf),
            Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::PropertyValidationFailed,
            })
        );
    }

    #[test]
    fn connect_authentication_data_without_method_is_rejected_on_encode(
        packet in connect_strategy()
    ) {
        // MQTT v5.0 §3.1.2.11.10:
        // Authentication Method なしで Authentication Data を含む CONNECT は
        // Protocol Error であり、エンコード時に拒否される。
        let codec::Packet::Connect(mut connect) = packet else {
            unreachable!("connect_strategy は CONNECT パケットのみを生成する");
        };
        let mut properties = Properties::new();
        for property in connect.properties.iter() {
            if property.identifier() != 0x15 && property.identifier() != 0x16 {
                properties.push(property.clone());
            }
        }
        properties.push(Property::AuthenticationData(vec![0x01]));
        connect.properties = properties;
        let mut buf = vec![0u8; 65536];
        prop_assert_eq!(
            codec::Packet::Connect(connect).encode(&mut buf),
            Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::PropertyValidationFailed,
            })
        );
    }
}
