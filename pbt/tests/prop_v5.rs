//! MQTT v5.0 パケットのプロパティベース・ラウンドトリップ・テスト。

mod helpers;

use noprop::TestCaseContext;
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

/// 0..=1 の値を持つプロパティ値サンプラ。
fn sample_zero_or_one(ctx: &mut TestCaseContext) -> u8 {
    noprop::sample_usize_in(ctx, 0..=1) as u8
}

/// 非ゼロ u16 サンプラ（1..=65535）。
fn sample_nonzero_u16(ctx: &mut TestCaseContext) -> u16 {
    noprop::sample_usize_in(ctx, 1..=u16::MAX as usize) as u16
}

/// 非ゼロ u32 サンプラ（1..=u32::MAX）。
fn sample_nonzero_u32(ctx: &mut TestCaseContext) -> u32 {
    noprop::sample_usize_in(ctx, 1..=u32::MAX as usize) as u32
}

/// VBI 値サンプラ（1..=VariableByteInteger::MAX）。
///
/// 可変長バイト整数は 4 バイトで表現できる上限までしかエンコードできないため、
/// Subscription Identifier などの VBI 値はこの上限で制約する。
fn sample_vbi_value(ctx: &mut TestCaseContext) -> u32 {
    noprop::sample_usize_in(ctx, 1..=VariableByteInteger::MAX as usize) as u32
}

fn sample_user_property(ctx: &mut TestCaseContext) -> Property {
    let key = helpers::sample_string(ctx);
    let value = helpers::sample_string(ctx);
    Property::UserProperty(key, value)
}

fn sample_random_property(ctx: &mut TestCaseContext) -> Property {
    match noprop::sample_usize_in(ctx, 0..28) {
        0 => Property::PayloadFormatIndicator(sample_zero_or_one(ctx)),
        1 => Property::MessageExpiryInterval(noprop::sample_u32(ctx)),
        2 => Property::ContentType(helpers::sample_string(ctx)),
        3 => Property::ResponseTopic(helpers::sample_string_in(ctx, 1, 100)),
        4 => Property::CorrelationData(helpers::sample_binary(ctx)),
        5 => Property::SubscriptionIdentifier(VariableByteInteger(sample_vbi_value(ctx))),
        6 => Property::SessionExpiryInterval(noprop::sample_u32(ctx)),
        7 => Property::AssignedClientIdentifier(helpers::sample_string(ctx)),
        8 => Property::ServerKeepAlive(noprop::sample_u16(ctx)),
        9 => Property::AuthenticationMethod(helpers::sample_string(ctx)),
        10 => Property::AuthenticationData(helpers::sample_binary(ctx)),
        11 => Property::RequestProblemInformation(sample_zero_or_one(ctx)),
        12 => Property::WillDelayInterval(noprop::sample_u32(ctx)),
        13 => Property::RequestResponseInformation(sample_zero_or_one(ctx)),
        14 => Property::ResponseInformation(helpers::sample_string(ctx)),
        15 => Property::ServerReference(helpers::sample_string(ctx)),
        16 => Property::ReasonString(helpers::sample_string(ctx)),
        17 => Property::ReceiveMaximum(sample_nonzero_u16(ctx)),
        18 => Property::TopicAliasMaximum(noprop::sample_u16(ctx)),
        19 => Property::TopicAlias(sample_nonzero_u16(ctx)),
        20 => Property::MaximumQoS(sample_zero_or_one(ctx)),
        21 => Property::RetainAvailable(sample_zero_or_one(ctx)),
        22 => sample_user_property(ctx),
        23 => Property::MaximumPacketSize(sample_nonzero_u32(ctx)),
        24 => Property::WildcardSubscriptionAvailable(sample_zero_or_one(ctx)),
        25 => Property::SubscriptionIdentifierAvailable(sample_zero_or_one(ctx)),
        _ => Property::SharedSubscriptionAvailable(sample_zero_or_one(ctx)),
    }
}

fn sample_connect_properties(ctx: &mut TestCaseContext) -> Properties {
    let mut props = Properties::new();
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::SessionExpiryInterval(noprop::sample_u32(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) =
        helpers::sample_option(ctx, |ctx| Property::ReceiveMaximum(sample_nonzero_u16(ctx)))
    {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::MaximumPacketSize(sample_nonzero_u32(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::TopicAliasMaximum(noprop::sample_u16(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::RequestProblemInformation(sample_zero_or_one(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::RequestResponseInformation(sample_zero_or_one(ctx))
    }) {
        props.push(p);
    }
    for _ in 0..noprop::sample_usize_in(ctx, 0..=3) {
        props.push(sample_user_property(ctx));
    }
    props
}

fn sample_connack_properties(ctx: &mut TestCaseContext) -> Properties {
    let mut props = Properties::new();
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::SessionExpiryInterval(noprop::sample_u32(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) =
        helpers::sample_option(ctx, |ctx| Property::MaximumQoS(sample_zero_or_one(ctx)))
    {
        props.push(p);
    }
    if let Some(p) =
        helpers::sample_option(ctx, |ctx| Property::ReceiveMaximum(sample_nonzero_u16(ctx)))
    {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::MaximumPacketSize(sample_nonzero_u32(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::TopicAliasMaximum(noprop::sample_u16(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::RetainAvailable(sample_zero_or_one(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::WildcardSubscriptionAvailable(sample_zero_or_one(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::SubscriptionIdentifierAvailable(sample_zero_or_one(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::SharedSubscriptionAvailable(sample_zero_or_one(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::ServerKeepAlive(noprop::sample_u16(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::AssignedClientIdentifier(helpers::sample_string(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::ResponseInformation(helpers::sample_string(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::ServerReference(helpers::sample_string(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::ReasonString(helpers::sample_string(ctx))
    }) {
        props.push(p);
    }
    for _ in 0..noprop::sample_usize_in(ctx, 0..=3) {
        props.push(sample_user_property(ctx));
    }
    props
}

fn sample_publish_properties(ctx: &mut TestCaseContext) -> Properties {
    let mut props = Properties::new();
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::PayloadFormatIndicator(sample_zero_or_one(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::MessageExpiryInterval(noprop::sample_u32(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::ContentType(helpers::sample_string(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::ResponseTopic(helpers::sample_string_in(ctx, 1, 100))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::CorrelationData(helpers::sample_binary(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) =
        helpers::sample_option(ctx, |ctx| Property::TopicAlias(sample_nonzero_u16(ctx)))
    {
        props.push(p);
    }
    for _ in 0..noprop::sample_usize_in(ctx, 0..=3) {
        props.push(sample_user_property(ctx));
    }
    props
}

fn sample_subscribe_properties(ctx: &mut TestCaseContext) -> Properties {
    let mut props = Properties::new();
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::SubscriptionIdentifier(VariableByteInteger(sample_vbi_value(ctx)))
    }) {
        props.push(p);
    }
    for _ in 0..noprop::sample_usize_in(ctx, 0..=3) {
        props.push(sample_user_property(ctx));
    }
    props
}

fn sample_will_properties(ctx: &mut TestCaseContext) -> Properties {
    let mut props = Properties::new();
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::WillDelayInterval(noprop::sample_u32(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::PayloadFormatIndicator(sample_zero_or_one(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::MessageExpiryInterval(noprop::sample_u32(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::ContentType(helpers::sample_string(ctx))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::ResponseTopic(helpers::sample_string_in(ctx, 1, 100))
    }) {
        props.push(p);
    }
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::CorrelationData(helpers::sample_binary(ctx))
    }) {
        props.push(p);
    }
    for _ in 0..noprop::sample_usize_in(ctx, 0..=3) {
        props.push(sample_user_property(ctx));
    }
    props
}

fn sample_disconnect_properties(ctx: &mut TestCaseContext) -> Properties {
    let mut props = Properties::new();
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::ReasonString(helpers::sample_string(ctx))
    }) {
        props.push(p);
    }
    for _ in 0..noprop::sample_usize_in(ctx, 0..=3) {
        props.push(sample_user_property(ctx));
    }
    props
}

fn sample_auth_properties(ctx: &mut TestCaseContext) -> Properties {
    let mut props = Properties::new();
    // MQTT v5.0 §3.15.2.2.2: AUTH パケットには Authentication Method が必須。
    props.push(Property::AuthenticationMethod(helpers::sample_string(ctx)));
    for _ in 0..noprop::sample_usize_in(ctx, 0..=3) {
        props.push(sample_user_property(ctx));
    }
    props
}

fn sample_reason_code_properties(ctx: &mut TestCaseContext) -> Properties {
    let mut props = Properties::new();
    if let Some(p) = helpers::sample_option(ctx, |ctx| {
        Property::ReasonString(helpers::sample_string(ctx))
    }) {
        props.push(p);
    }
    for _ in 0..noprop::sample_usize_in(ctx, 0..=3) {
        props.push(sample_user_property(ctx));
    }
    props
}

fn sample_will(ctx: &mut TestCaseContext) -> v5::connect::Will {
    // Will Topic は空にできず、ワイルドカード文字も使用できない。
    // 文字列サンプラがワイルドカードを除外済みのため、空を避けるだけでよい。
    let topic = helpers::sample_string_in(ctx, 1, 100);
    let qos = helpers::sample_qos(ctx);
    let retain = noprop::sample_bool(ctx);
    let payload = helpers::sample_binary(ctx);
    let properties = sample_will_properties(ctx);
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
}

fn sample_connect(ctx: &mut TestCaseContext) -> codec::Packet {
    let client_id = helpers::sample_string(ctx);
    let clean_start = noprop::sample_bool(ctx);
    let keep_alive = noprop::sample_u16(ctx);
    let properties = sample_connect_properties(ctx);
    let will = helpers::sample_option(ctx, sample_will);
    let username = helpers::sample_option(ctx, helpers::sample_string);
    let password = helpers::sample_option(ctx, helpers::sample_binary);

    codec::Packet::Connect(v5::connect::Connect {
        client_id,
        clean_start,
        keep_alive,
        properties,
        will,
        username,
        password,
    })
}

fn sample_connack(ctx: &mut TestCaseContext) -> codec::Packet {
    let reason_code = noprop::sample_choice(
        ctx,
        &[
            v5::connack::ConnectReasonCode::Success,
            v5::connack::ConnectReasonCode::NotAuthorized,
            v5::connack::ConnectReasonCode::ServerUnavailable,
        ],
    );
    // session_present は Success の場合のみ true（valid-by-construction）。
    let session_present =
        reason_code == v5::connack::ConnectReasonCode::Success && noprop::sample_bool(ctx);
    let properties = sample_connack_properties(ctx);

    codec::Packet::ConnAck(v5::connack::ConnAck {
        session_present,
        reason_code,
        properties,
    })
}

fn sample_publish(ctx: &mut TestCaseContext) -> codec::Packet {
    let qos = helpers::sample_qos(ctx);
    let properties = sample_publish_properties(ctx);
    let has_topic_alias = properties
        .iter()
        .any(|p| matches!(p, Property::TopicAlias(_)));
    // PUBLISH のトピック名は空にできない（TopicAlias ありの場合は空を許容）。
    // TopicAlias の有無を先に決めてから、トピック長を valid-by-construction で制約する。
    let topic = if has_topic_alias {
        helpers::sample_string(ctx)
    } else {
        helpers::sample_string_in(ctx, 1, 100)
    };
    // QoS 0 のときは packet_id なし (0) かつ dup: false でなければならない。
    // （MQTT v5.0 §3.3.1.2 / MQTT v5.0 §3.3.1.1 [MQTT-3.3.1-2]）
    let packet_id = if qos == QoS::AtMostOnce {
        0
    } else {
        noprop::sample_usize_in(ctx, 1..=u16::MAX as usize) as u16
    };
    let dup = qos != QoS::AtMostOnce && noprop::sample_bool(ctx);
    let retain = noprop::sample_bool(ctx);
    let payload = helpers::sample_binary(ctx);
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
        packet_id: (packet_id != 0).then_some(packet_id),
        properties,
        payload,
    })
}

fn sample_puback(ctx: &mut TestCaseContext) -> codec::Packet {
    let packet_id = sample_nonzero_u16(ctx);
    // MQTT v5.0 §3.4.2.1: 0x10 No matching subscribers はサーバー専用のため、
    // クライアント送信（encode → decode roundtrip）の対象から除外する。
    let reason_code = noprop::sample_choice(
        ctx,
        &[
            v5::puback::PubAckReasonCode::Success,
            v5::puback::PubAckReasonCode::QuotaExceeded,
            v5::puback::PubAckReasonCode::NotAuthorized,
        ],
    );
    let properties = sample_reason_code_properties(ctx);
    codec::Packet::PubAck(v5::puback::PubAck {
        packet_id,
        reason_code,
        properties,
    })
}

fn sample_pubrec(ctx: &mut TestCaseContext) -> codec::Packet {
    let packet_id = sample_nonzero_u16(ctx);
    // MQTT v5.0 §3.5.2.1: 0x10 No matching subscribers はサーバー専用のため、
    // クライアント送信（encode → decode roundtrip）の対象から除外する。
    let reason_code = noprop::sample_choice(
        ctx,
        &[
            v5::pubrec::PubRecReasonCode::Success,
            v5::pubrec::PubRecReasonCode::QuotaExceeded,
            v5::pubrec::PubRecReasonCode::NotAuthorized,
        ],
    );
    let properties = sample_reason_code_properties(ctx);
    codec::Packet::PubRec(v5::pubrec::PubRec {
        packet_id,
        reason_code,
        properties,
    })
}

fn sample_pubrel(ctx: &mut TestCaseContext) -> codec::Packet {
    let packet_id = sample_nonzero_u16(ctx);
    let reason_code = noprop::sample_choice(
        ctx,
        &[
            v5::pubrel::PubRelReasonCode::Success,
            v5::pubrel::PubRelReasonCode::PacketIdentifierNotFound,
        ],
    );
    let properties = sample_reason_code_properties(ctx);
    codec::Packet::PubRel(v5::pubrel::PubRel {
        packet_id,
        reason_code,
        properties,
    })
}

fn sample_pubcomp(ctx: &mut TestCaseContext) -> codec::Packet {
    let packet_id = sample_nonzero_u16(ctx);
    let reason_code = noprop::sample_choice(
        ctx,
        &[
            v5::pubcomp::PubCompReasonCode::Success,
            v5::pubcomp::PubCompReasonCode::PacketIdentifierNotFound,
        ],
    );
    let properties = sample_reason_code_properties(ctx);
    codec::Packet::PubComp(v5::pubcomp::PubComp {
        packet_id,
        reason_code,
        properties,
    })
}

/// 通常トピックフィルターサンプラ。`(フィルター, false)` を返す。
fn sample_normal_topic_filter(ctx: &mut TestCaseContext) -> (String, bool) {
    (helpers::sample_topic_filter(ctx), false)
}

/// 共有トピックフィルターサンプラ。`(フィルター, true)` を返す。
fn sample_shared_topic_filter(ctx: &mut TestCaseContext) -> (String, bool) {
    // ShareName に /, +, #, NUL は使用できない（valid-by-construction）。
    let share_len = noprop::sample_usize_in(ctx, 1..=20);
    let mut share_name = String::with_capacity(share_len);
    for _ in 0..share_len {
        // 除外文字は 4 種のみであり、受容率は 0.999996 を超える。
        let c = noprop::sample_with_rejection(ctx, 8, |ctx| {
            let c = noprop::sample_char(ctx);
            (c != '\0' && c != '/' && c != '+' && c != '#').then_some(c)
        });
        share_name.push(c);
    }
    let filter = helpers::sample_topic_filter(ctx);
    (format!("$share/{}/{}", share_name, filter), true)
}

/// トピックフィルターサンプラ（通常・共有のどちらか）。
fn sample_v5_topic_filter(ctx: &mut TestCaseContext) -> (String, bool) {
    match noprop::sample_usize_in(ctx, 0..2) {
        0 => sample_normal_topic_filter(ctx),
        _ => sample_shared_topic_filter(ctx),
    }
}

fn sample_subscription(ctx: &mut TestCaseContext) -> v5::subscribe::Subscription {
    let (topic_filter, is_shared) = sample_v5_topic_filter(ctx);
    let qos = helpers::sample_qos(ctx);
    // MQTT v5.0 §3.8.3.1 [MQTT-3.8.3-4]:
    // 共有サブスクリプションで No Local ビットを 1 にすることは Protocol Error である。
    let no_local = !is_shared && noprop::sample_bool(ctx);
    let retain_as_published = noprop::sample_bool(ctx);
    let retain_handling = noprop::sample_choice(
        ctx,
        &[
            RetainHandling::SendRetained,
            RetainHandling::SendRetainedIfNotExists,
            RetainHandling::DoNotSendRetained,
        ],
    );
    v5::subscribe::Subscription {
        topic_filter,
        qos,
        no_local,
        retain_as_published,
        retain_handling,
    }
}

fn sample_subscribe(ctx: &mut TestCaseContext) -> codec::Packet {
    let packet_id = sample_nonzero_u16(ctx);
    let count = noprop::sample_usize_in(ctx, 1..=4);
    let mut subscriptions = Vec::with_capacity(count);
    for _ in 0..count {
        subscriptions.push(sample_subscription(ctx));
    }
    let properties = sample_subscribe_properties(ctx);
    codec::Packet::Subscribe(v5::subscribe::Subscribe {
        packet_id,
        subscriptions,
        properties,
    })
}

fn sample_suback_reason_code(ctx: &mut TestCaseContext) -> v5::suback::SubAckReasonCode {
    noprop::sample_choice(
        ctx,
        &[
            v5::suback::SubAckReasonCode::GrantedQoS0,
            v5::suback::SubAckReasonCode::GrantedQoS1,
            v5::suback::SubAckReasonCode::GrantedQoS2,
            v5::suback::SubAckReasonCode::NotAuthorized,
        ],
    )
}

fn sample_suback(ctx: &mut TestCaseContext) -> codec::Packet {
    let packet_id = sample_nonzero_u16(ctx);
    let count = noprop::sample_usize_in(ctx, 1..=4);
    let mut reason_codes = Vec::with_capacity(count);
    for _ in 0..count {
        reason_codes.push(sample_suback_reason_code(ctx));
    }
    let properties = sample_reason_code_properties(ctx);
    codec::Packet::SubAck(v5::suback::SubAck {
        packet_id,
        reason_codes,
        properties,
    })
}

fn sample_unsubscribe(ctx: &mut TestCaseContext) -> codec::Packet {
    let packet_id = sample_nonzero_u16(ctx);
    let count = noprop::sample_usize_in(ctx, 1..=4);
    let mut topic_filters = Vec::with_capacity(count);
    for _ in 0..count {
        topic_filters.push(helpers::sample_topic_filter(ctx));
    }
    codec::Packet::Unsubscribe(v5::unsubscribe::Unsubscribe {
        packet_id,
        topic_filters,
        properties: Properties::new(),
    })
}

fn sample_unsuback_reason_code(ctx: &mut TestCaseContext) -> v5::unsuback::UnsubAckReasonCode {
    noprop::sample_choice(
        ctx,
        &[
            v5::unsuback::UnsubAckReasonCode::Success,
            v5::unsuback::UnsubAckReasonCode::NoSubscriptionExisted,
            v5::unsuback::UnsubAckReasonCode::NotAuthorized,
        ],
    )
}

fn sample_unsuback(ctx: &mut TestCaseContext) -> codec::Packet {
    let packet_id = sample_nonzero_u16(ctx);
    let count = noprop::sample_usize_in(ctx, 1..=4);
    let mut reason_codes = Vec::with_capacity(count);
    for _ in 0..count {
        reason_codes.push(sample_unsuback_reason_code(ctx));
    }
    let properties = sample_reason_code_properties(ctx);
    codec::Packet::UnsubAck(v5::unsuback::UnsubAck {
        packet_id,
        reason_codes,
        properties,
    })
}

fn sample_disconnect(ctx: &mut TestCaseContext) -> codec::Packet {
    // packet_roundtrip は encode -> decode を検証するため、
    // Client → Server と Server → Client の両方向で合法な Reason Code のみを使う。
    let reason_code = noprop::sample_choice(
        ctx,
        &[
            v5::disconnect::DisconnectReasonCode::NormalDisconnection,
            v5::disconnect::DisconnectReasonCode::UnspecifiedError,
            v5::disconnect::DisconnectReasonCode::BadAuthenticationMethod,
        ],
    );
    let properties = sample_disconnect_properties(ctx);
    codec::Packet::Disconnect(v5::disconnect::Disconnect {
        reason_code,
        properties,
    })
}

fn sample_auth(ctx: &mut TestCaseContext) -> codec::Packet {
    // packet_roundtrip は encode -> decode を検証するため、
    // 両方向で合法な Reason Code（0x18）のみを使う。0x00 は encode 不可、
    // 0x19 は decode 不可のため片方向専用テスト側で扱う。
    let properties = sample_auth_properties(ctx);
    codec::Packet::Auth(v5::auth::Auth {
        reason_code: v5::auth::AuthReasonCode::ContinueAuthentication,
        properties,
    })
}

/// 全種別の `codec::Packet` を生成するサンプラ（低水準 codec のラウンドトリップ検証用）。
fn sample_codec_packet(ctx: &mut TestCaseContext) -> codec::Packet {
    match noprop::sample_usize_in(ctx, 0..15) {
        0 => sample_connect(ctx),
        1 => sample_connack(ctx),
        2 => sample_publish(ctx),
        3 => sample_puback(ctx),
        4 => sample_pubrec(ctx),
        5 => sample_pubrel(ctx),
        6 => sample_pubcomp(ctx),
        7 => sample_subscribe(ctx),
        8 => sample_suback(ctx),
        9 => sample_unsubscribe(ctx),
        10 => sample_unsuback(ctx),
        11 => codec::Packet::PingReq(v5::pingreq::PingReq),
        12 => codec::Packet::PingResp(v5::pingresp::PingResp),
        13 => sample_disconnect(ctx),
        _ => sample_auth(ctx),
    }
}

/// Server → Client 方向の種別のみを生成するサンプラ（Decoder 経由のラウンドトリップ検証用）。
/// 双方向種別（PUBLISH / PUBACK 系 / DISCONNECT / AUTH）は両方向で合法な
/// Reason Code とプロパティのみを生成するため、encode（Client → Server 検証）と
/// decode（Server → Client 検証）の両方を通る。
fn sample_incoming_packet(ctx: &mut TestCaseContext) -> codec::Packet {
    match noprop::sample_usize_in(ctx, 0..11) {
        0 => sample_connack(ctx),
        1 => sample_publish(ctx),
        2 => sample_puback(ctx),
        3 => sample_pubrec(ctx),
        4 => sample_pubrel(ctx),
        5 => sample_pubcomp(ctx),
        6 => sample_suback(ctx),
        7 => sample_unsuback(ctx),
        8 => codec::Packet::PingResp(v5::pingresp::PingResp),
        9 => sample_disconnect(ctx),
        _ => sample_auth(ctx),
    }
}

/// Client → Server 専用種別のみを生成するサンプラ（Decoder の方向拒否検証用）。
fn sample_outgoing_only_packet(ctx: &mut TestCaseContext) -> codec::Packet {
    match noprop::sample_usize_in(ctx, 0..4) {
        0 => sample_connect(ctx),
        1 => sample_subscribe(ctx),
        2 => sample_unsubscribe(ctx),
        _ => codec::Packet::PingReq(v5::pingreq::PingReq),
    }
}

/// Server → Client 側専用の Reason Code やプロパティを含む OutgoingPacket を
/// 生成するサンプラ。encode が Err(EncodeError::InvalidField { .. }) を返すことを検証する。
fn sample_reverse_direction_outgoing(ctx: &mut TestCaseContext) -> OutgoingPacket {
    match noprop::sample_usize_in(ctx, 0..5) {
        0 => {
            // Success (0x00) は Server 側専用の Reason Code（MQTT v5.0 §3.15.2.1 Table 3-11）
            let properties = sample_auth_properties(ctx);
            OutgoingPacket::Auth(v5::auth::Auth {
                reason_code: v5::auth::AuthReasonCode::Success,
                properties,
            })
        }
        1 => {
            // Server 側専用の Reason Code（MQTT v5.0 §3.14.2.1 Table 3-10 の「Sent by」列）
            let reason_code = noprop::sample_choice(
                ctx,
                &[
                    v5::disconnect::DisconnectReasonCode::ServerShuttingDown,
                    v5::disconnect::DisconnectReasonCode::SessionTakenOver,
                    v5::disconnect::DisconnectReasonCode::ServerMoved,
                ],
            );
            let properties = sample_reason_code_properties(ctx);
            OutgoingPacket::Disconnect(v5::disconnect::Disconnect {
                reason_code,
                properties,
            })
        }
        2 => {
            // Subscription Identifier (0x0B) は Client → Server 方向の PUBLISH では許可されない
            // （MQTT v5.0 §3.3.4 [MQTT-3.3.4-6]）
            let topic = helpers::sample_string_in(ctx, 1, 100);
            let qos = helpers::sample_qos(ctx);
            // QoS 0 のときは packet_id なし（valid-by-construction）。
            let packet_id = if qos == QoS::AtMostOnce {
                0
            } else {
                noprop::sample_usize_in(ctx, 1..=u16::MAX as usize) as u16
            };
            let payload = helpers::sample_binary(ctx);
            let subscription_id = VariableByteInteger(sample_vbi_value(ctx));
            let mut properties = Properties::new();
            properties.push(Property::SubscriptionIdentifier(subscription_id));
            OutgoingPacket::Publish(v5::publish::Publish {
                dup: false,
                qos,
                retain: false,
                topic,
                packet_id: (packet_id != 0).then_some(packet_id),
                properties,
                payload,
            })
        }
        3 => {
            // 0x10 No matching subscribers は Server 側専用（MQTT v5.0 §3.4.2.1）
            let packet_id = sample_nonzero_u16(ctx);
            let properties = sample_reason_code_properties(ctx);
            OutgoingPacket::PubAck(v5::puback::PubAck {
                packet_id,
                reason_code: v5::puback::PubAckReasonCode::NoMatchingSubscribers,
                properties,
            })
        }
        _ => {
            // 0x10 No matching subscribers は Server 側専用（MQTT v5.0 §3.5.2.1）
            let packet_id = sample_nonzero_u16(ctx);
            let properties = sample_reason_code_properties(ctx);
            OutgoingPacket::PubRec(v5::pubrec::PubRec {
                packet_id,
                reason_code: v5::pubrec::PubRecReasonCode::NoMatchingSubscribers,
                properties,
            })
        }
    }
}

#[test]
fn property_roundtrip() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let property = sample_random_property(ctx);
        let mut buf = vec![0u8; 65536];
        let len = property.encode(&mut buf).expect("エンコードに成功すること");
        let (decoded, consumed) = Property::decode(&buf[..len]).expect("デコードに成功すること");
        assert_eq!(decoded, property);
        assert_eq!(consumed, len);
        Ok(())
    })?;

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}

#[test]
fn packet_roundtrip() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet = sample_codec_packet(ctx);
        let mut buf = vec![0u8; 65536];
        let len = packet.encode(&mut buf).expect("エンコードに成功すること");
        let (decoded, consumed) =
            codec::Packet::decode(&buf[..len]).expect("デコードに成功すること");
        assert_eq!(decoded.clone(), packet);
        assert_eq!(consumed, len);

        let mut buf2 = vec![0u8; 65536];
        let len2 = decoded
            .encode(&mut buf2)
            .expect("再エンコードに成功すること");
        assert_eq!(&buf[..len], &buf2[..len2]);
        Ok(())
    })?;

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}

#[test]
fn packet_encode_exact_buffer() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet = sample_codec_packet(ctx);
        // 合計長ちょうどのバッファで encode() が Ok(合計長) を返すことを検証する。
        // 戻り値の一致まで検証し、encoded_len() の過大見積もりも検出する。
        // 過小見積もりの場合はバッファ書き込み時の panic または
        // 内部エンコーダからの予期しない Err(BufferTooSmall) として検出される。
        let encoded = packet
            .encode_to_vec()
            .expect("エンコード可能なパケットであること");
        let total_len = encoded.len();
        let mut buf = vec![0u8; total_len];
        let len = packet
            .encode(&mut buf)
            .expect("合計長ちょうどのバッファでエンコードに成功すること");
        assert_eq!(len, total_len);
        assert_eq!(&buf[..len], &encoded[..]);
        Ok(())
    })?;

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}

#[test]
fn packet_encode_short_buffer_fails() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet = sample_codec_packet(ctx);
        // 合計長 - 1 のバッファで encode() が Err(BufferTooSmall) を返すことを検証する。
        let encoded = packet
            .encode_to_vec()
            .expect("エンコード可能なパケットであること");
        let total_len = encoded.len();
        let mut buf = vec![0u8; total_len - 1];
        let result = packet.encode(&mut buf);
        assert_eq!(result, Err(EncodeError::BufferTooSmall));
        Ok(())
    })?;

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}

#[test]
fn incoming_roundtrip() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet = sample_incoming_packet(ctx);
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
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::Publish(decoded)),
                codec::Packet::Publish(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::PubAck(decoded)),
                codec::Packet::PubAck(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::PubRec(decoded)),
                codec::Packet::PubRec(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::PubRel(decoded)),
                codec::Packet::PubRel(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::PubComp(decoded)),
                codec::Packet::PubComp(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::SubAck(decoded)),
                codec::Packet::SubAck(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::UnsubAck(decoded)),
                codec::Packet::UnsubAck(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::PingResp(decoded)),
                codec::Packet::PingResp(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::Disconnect(decoded)),
                codec::Packet::Disconnect(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V5(IncomingPacket::Auth(decoded)),
                codec::Packet::Auth(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            _ => panic!("デコード結果の種別が元のパケットと一致しない"),
        }
        Ok(())
    })?;

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}

#[test]
fn decoder_rejects_reverse_direction() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let outgoing = sample_outgoing_only_packet(ctx);
        let incoming = sample_incoming_packet(ctx);
        // Client → Server 専用種別を feed すると Decoder は UnexpectedPacket で拒否する。
        let encoded = outgoing.encode_to_vec().expect("エンコードに成功すること");
        let mut decoder = Decoder::new_v5(Limits::new());
        decoder.feed(&encoded).expect("feed に成功すること");
        match decoder.decode() {
            Err(DecodeError::UnexpectedPacket { packet_type }) => {
                assert_eq!(packet_type, encoded[0] & 0xF0);
            }
            _ => panic!("UnexpectedPacket エラーが返ること"),
        }
        // 1 フレーム消費・後続維持: 拒否後に正しい方向のパケットを続けてデコードできる。
        let incoming_encoded = incoming.encode_to_vec().expect("エンコードに成功すること");
        decoder
            .feed(&incoming_encoded)
            .expect("feed に成功すること");
        let decoded = decoder.decode().expect("デコードに成功すること");
        assert!(matches!(decoded, Some(VersionedIncomingPacket::V5(_))));
        Ok(())
    })?;

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}

#[test]
fn outgoing_encode_rejects_reverse_direction() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet = sample_reverse_direction_outgoing(ctx);
        // Server → Client 側専用の Reason Code やプロパティを含む OutgoingPacket の
        // エンコードは Err(EncodeError::InvalidField { .. }) で拒否される。
        // 拒否原因は複数あり得るため、原因を問わず検証エラーであることのみを見る。
        let is_invalid_field = matches!(
            packet.encode_to_vec(),
            Err(EncodeError::InvalidField { .. })
        );
        assert!(is_invalid_field);
        Ok(())
    })?;

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}

/// 残り長さの符号化長が変化する境界（127・128・16,383・16,384）を
/// 1 テスト内で確定的に生成して検証する。
///
/// MQTT v5.0 §1.5.5 [MQTT-1.5.5-1]:
/// 残り長さは最小バイト数でエンコードする必要がある。
/// VBI の符号化長そのものの検証は codec 層の単体テストに委ね、
/// パケットレベルでは encoded_len() の一致と戻り値の一致で間接的に検出する。
///
/// PUBLISH QoS 0・トピック "t"（1 バイト）・空プロパティをベースにする。
/// QoS 0 の PUBLISH は dup: true や packet_id: Some(_) だと
/// validate() が Err(InvalidField) を返すため、dup: false・packet_id: None で構築する。
/// プロパティを空に固定してもプロパティ長 VBI の 1 バイトが入るため、
/// v5.0 の固定オーバーヘッド: 2（トピック長）+ 1（トピック "t"）+ 1（プロパティ長 VBI）。
#[test]
fn remaining_length_boundary() {
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
        assert_eq!(packet.encoded_len(), target);

        let encoded = packet.encode_to_vec().expect("エンコードに成功すること");
        let total_len = encoded.len();

        let mut buf = vec![0u8; total_len];
        let len = packet
            .encode(&mut buf)
            .expect("合計長ちょうどでエンコードに成功すること");
        assert_eq!(len, total_len);

        let mut short_buf = vec![0u8; total_len - 1];
        let result = packet.encode(&mut short_buf);
        assert_eq!(result, Err(EncodeError::BufferTooSmall));
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
fn sample_packet_and_disallowed_property(ctx: &mut TestCaseContext) -> (codec::Packet, Property) {
    // プロパティを持つパケットのみを対象にする。codec_packet の 15 種のうち
    // PingReq / PingResp の 2 種はプロパティを持たないため、受容率は 13/15。
    // max_attempts=8 で枯渇する確率は (2/15)^8 ≈ 4e-7 と実質ゼロである。
    let packet = noprop::sample_with_rejection(ctx, 8, |ctx| {
        let packet = sample_codec_packet(ctx);
        encode_allowed_identifiers(&packet)
            .is_some()
            .then_some(packet)
    });
    let allowed = encode_allowed_identifiers(&packet).expect("プロパティを持つパケットであること");
    let disallowed: Vec<u8> = ALL_PROPERTY_IDENTIFIERS
        .iter()
        .copied()
        .filter(|identifier| !allowed.contains(identifier))
        .collect();
    let identifier = noprop::sample_choice(ctx, &disallowed);
    (packet, sample_property(identifier))
}

#[test]
fn packet_with_disallowed_property_is_rejected_on_encode() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let (mut packet, property) = sample_packet_and_disallowed_property(ctx);
        // MQTT v5.0 §2.2.2.2 Table 2-4:
        // プロパティはパケット種別ごとに使用できるものが定められている。
        // 許可されない識別子を 1 つ挿入した有効パケットは、
        // エンコード時の許可リスト検証で必ず拒否される。
        packet_properties_mut(&mut packet)
            .expect("プロパティを持つパケットであること")
            .push(property);
        let mut buf = vec![0u8; 65536];
        assert_eq!(
            packet.encode(&mut buf),
            Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::PropertyValidationFailed,
            })
        );
        Ok(())
    })?;

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}

#[test]
fn connect_will_with_disallowed_property_is_rejected_on_encode() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet = sample_connect(ctx);
        let mut will = sample_will(ctx);
        // Will プロパティで許可されない識別子を 1 つ選ぶ。
        let disallowed: Vec<u8> = ALL_PROPERTY_IDENTIFIERS
            .iter()
            .copied()
            .filter(|identifier| !WILL_ALLOWED_IDENTIFIERS.contains(identifier))
            .collect();
        let identifier = noprop::sample_choice(ctx, &disallowed);
        will.properties.push(sample_property(identifier));

        // MQTT v5.0 §3.1.3.2:
        // Will プロパティにも許可リストがあり、許可されない識別子を
        // 挿入した Will 付き CONNECT はエンコード時に拒否される。
        let codec::Packet::Connect(mut connect) = packet else {
            unreachable!("sample_connect は CONNECT パケットのみを生成する");
        };
        connect.will = Some(will);
        let mut buf = vec![0u8; 65536];
        assert_eq!(
            codec::Packet::Connect(connect).encode(&mut buf),
            Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::PropertyValidationFailed,
            })
        );
        Ok(())
    })?;

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}

#[test]
fn connect_authentication_data_without_method_is_rejected_on_encode() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet = sample_connect(ctx);
        // MQTT v5.0 §3.1.2.11.10:
        // Authentication Method なしで Authentication Data を含む CONNECT は
        // Protocol Error であり、エンコード時に拒否される。
        let codec::Packet::Connect(mut connect) = packet else {
            unreachable!("sample_connect は CONNECT パケットのみを生成する");
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
        assert_eq!(
            codec::Packet::Connect(connect).encode(&mut buf),
            Err(EncodeError::InvalidField {
                reason: EncodeInvalidField::PropertyValidationFailed,
            })
        );
        Ok(())
    })?;

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}
