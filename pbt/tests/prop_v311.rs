//! MQTT v3.1.1 パケットのプロパティベースラウンドトリップテスト。

mod helpers;

use proptest::prelude::*;
use proptest::sample::select;
use shiguredo_mqtt::codec::limits::Limits;
use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::decoder::Decoder;
use shiguredo_mqtt::decoder::VersionedIncomingPacket;
use shiguredo_mqtt::error::DecodeError;
use shiguredo_mqtt::error::EncodeError;
use shiguredo_mqtt::v311;
use shiguredo_mqtt::v311::packet::IncomingPacket;
use shiguredo_mqtt::v311::packet::codec;

use helpers::{binary_strategy, qos_strategy, string_strategy, topic_filter_strategy};

fn will_strategy() -> impl Strategy<Value = v311::connect::Will> {
    (
        string_strategy(),
        qos_strategy(),
        any::<bool>(),
        binary_strategy(),
    )
        .prop_filter("Will Topic は空にできない", |(topic, _, _, _)| {
            !topic.is_empty()
        })
        .prop_filter(
            "Will Topic にワイルドカード文字は使用できない",
            |(topic, _, _, _)| !topic.contains('+') && !topic.contains('#'),
        )
        .prop_map(|(topic, qos, retain, payload)| v311::connect::Will {
            topic,
            qos,
            retain,
            payload,
        })
}

fn connect_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        string_strategy(),
        any::<bool>(),
        any::<u16>(),
        proptest::option::of(will_strategy()),
        proptest::option::of(string_strategy()),
        proptest::option::of(binary_strategy()),
    )
        .prop_filter(
            "Clean Start=false の場合、Client ID は空にできない",
            |(client_id, clean_session, _, _, _, _)| *clean_session || !client_id.is_empty(),
        )
        .prop_filter(
            "Password のみの送信は v3.1.1 では許可されない",
            |(_, _, _, _, username, password)| password.is_none() || username.is_some(),
        )
        .prop_map(
            |(client_id, clean_session, keep_alive, will, username, password)| {
                codec::Packet::Connect(v311::connect::Connect {
                    client_id,
                    clean_session,
                    keep_alive,
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
            v311::connack::ConnectReturnCode::Accepted,
            v311::connack::ConnectReturnCode::NotAuthorized,
            v311::connack::ConnectReturnCode::ServerUnavailable,
        ]),
    )
        .prop_filter(
            "session_present は Accepted の場合のみ true",
            |(session_present, return_code)| {
                !(*session_present && *return_code != v311::connack::ConnectReturnCode::Accepted)
            },
        )
        .prop_map(|(session_present, return_code)| {
            codec::Packet::ConnAck(v311::connack::ConnAck {
                session_present,
                return_code,
            })
        })
}

fn publish_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        any::<bool>(),
        qos_strategy(),
        any::<bool>(),
        string_strategy(),
        any::<u16>(),
        binary_strategy(),
    )
        .prop_filter(
            "有効な PUBLISH パケット識別子",
            |(_, qos, _, _, packet_id, _)| (*qos == QoS::AtMostOnce) == (*packet_id == 0),
        )
        .prop_filter(
            "PUBLISH のトピック名にワイルドカード文字を含めない",
            |(_, _, _, topic, _, _)| !topic.contains('#') && !topic.contains('+'),
        )
        .prop_filter(
            "PUBLISH のトピック名は空にできない",
            |(_, _, _, topic, _, _)| !topic.is_empty(),
        )
        .prop_map(|(dup, qos, retain, topic, packet_id, payload)| {
            let id = if qos == QoS::AtMostOnce {
                None
            } else {
                Some(packet_id)
            };
            // MQTT v3.1.1 §3.3.1.1 [MQTT-3.3.1-2]: QoS 0 の場合、DUP は false でなければならない。
            let dup = if qos == QoS::AtMostOnce { false } else { dup };
            codec::Packet::Publish(v311::publish::Publish {
                dup,
                qos,
                retain,
                topic,
                packet_id: id,
                payload,
            })
        })
}

fn puback_strategy() -> impl Strategy<Value = codec::Packet> {
    (1..=u16::MAX).prop_map(|packet_id| codec::Packet::PubAck(v311::puback::PubAck { packet_id }))
}

fn pubrec_strategy() -> impl Strategy<Value = codec::Packet> {
    (1..=u16::MAX).prop_map(|packet_id| codec::Packet::PubRec(v311::pubrec::PubRec { packet_id }))
}

fn pubrel_strategy() -> impl Strategy<Value = codec::Packet> {
    (1..=u16::MAX).prop_map(|packet_id| codec::Packet::PubRel(v311::pubrel::PubRel { packet_id }))
}

fn pubcomp_strategy() -> impl Strategy<Value = codec::Packet> {
    (1..=u16::MAX)
        .prop_map(|packet_id| codec::Packet::PubComp(v311::pubcomp::PubComp { packet_id }))
}

fn subscription_strategy() -> impl Strategy<Value = v311::subscribe::Subscription> {
    (topic_filter_strategy(), qos_strategy())
        .prop_map(|(topic_filter, qos)| v311::subscribe::Subscription { topic_filter, qos })
}

fn subscribe_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        1..=u16::MAX,
        proptest::collection::vec(subscription_strategy(), 1..5),
    )
        .prop_map(|(packet_id, topic_filters)| {
            codec::Packet::Subscribe(v311::subscribe::Subscribe {
                packet_id,
                topic_filters,
            })
        })
}

fn suback_return_code_strategy() -> impl Strategy<Value = v311::suback::SubscribeReturnCode> {
    select(&[
        v311::suback::SubscribeReturnCode::SuccessQoS0,
        v311::suback::SubscribeReturnCode::SuccessQoS1,
        v311::suback::SubscribeReturnCode::SuccessQoS2,
        v311::suback::SubscribeReturnCode::Failure,
    ])
}

fn suback_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        1..=u16::MAX,
        proptest::collection::vec(suback_return_code_strategy(), 1..5),
    )
        .prop_map(|(packet_id, return_codes)| {
            codec::Packet::SubAck(v311::suback::SubAck {
                packet_id,
                return_codes,
            })
        })
}

fn unsubscribe_strategy() -> impl Strategy<Value = codec::Packet> {
    (
        1..=u16::MAX,
        proptest::collection::vec(topic_filter_strategy(), 1..5),
    )
        .prop_map(|(packet_id, topic_filters)| {
            codec::Packet::Unsubscribe(v311::unsubscribe::Unsubscribe {
                packet_id,
                topic_filters,
            })
        })
}

fn unsuback_strategy() -> impl Strategy<Value = codec::Packet> {
    (1..=u16::MAX)
        .prop_map(|packet_id| codec::Packet::UnsubAck(v311::unsuback::UnsubAck { packet_id }))
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
        Just(codec::Packet::PingReq(v311::pingreq::PingReq)).boxed(),
        Just(codec::Packet::PingResp(v311::pingresp::PingResp)).boxed(),
        Just(codec::Packet::Disconnect(v311::disconnect::Disconnect)).boxed(),
    ]
    .boxed()
}

// Server → Client 方向の種別のみを生成する戦略（Decoder 経由のラウンドトリップ検証用）。
// v3.1.1 の DISCONNECT は Client → Server 専用のため含めない
// （MQTT v3.1.1 §2.2.1 Table 2.1）。
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
        Just(codec::Packet::PingResp(v311::pingresp::PingResp)).boxed(),
    ]
    .boxed()
}

// Client → Server 専用種別のみを生成する戦略（Decoder の方向拒否検証用）。
// v3.1.1 では DISCONNECT も Client → Server 専用（MQTT v3.1.1 §2.2.1 Table 2.1）。
fn outgoing_only_packet_strategy() -> BoxedStrategy<codec::Packet> {
    prop_oneof![
        connect_strategy().boxed(),
        subscribe_strategy().boxed(),
        unsubscribe_strategy().boxed(),
        Just(codec::Packet::PingReq(v311::pingreq::PingReq)).boxed(),
        Just(codec::Packet::Disconnect(v311::disconnect::Disconnect)).boxed(),
    ]
    .boxed()
}

proptest! {
    #[test]
    fn packet_roundtrip(packet in codec_packet_strategy()) {
        let mut buf = vec![0u8; 65536];
        let len = packet.encode(&mut buf).expect("パケットのエンコードに失敗");
        let (decoded, consumed) = codec::Packet::decode(&buf[..len]).expect("パケットのデコードに失敗");
        prop_assert_eq!(decoded.clone(), packet);
        prop_assert_eq!(consumed, len);

        let mut buf2 = vec![0u8; 65536];
        let len2 = decoded.encode(&mut buf2).expect("パケットの再エンコードに失敗");
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
        let mut decoder = Decoder::new_v311(Limits::new());
        decoder.feed(&encoded).expect("feed に成功すること");
        let decoded = decoder
            .decode()
            .expect("デコードに成功すること")
            .expect("パケットが得られること");
        match (decoded, &packet) {
            (
                VersionedIncomingPacket::V311(IncomingPacket::ConnAck(decoded)),
                codec::Packet::ConnAck(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V311(IncomingPacket::Publish(decoded)),
                codec::Packet::Publish(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V311(IncomingPacket::PubAck(decoded)),
                codec::Packet::PubAck(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V311(IncomingPacket::PubRec(decoded)),
                codec::Packet::PubRec(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V311(IncomingPacket::PubRel(decoded)),
                codec::Packet::PubRel(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V311(IncomingPacket::PubComp(decoded)),
                codec::Packet::PubComp(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V311(IncomingPacket::SubAck(decoded)),
                codec::Packet::SubAck(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V311(IncomingPacket::UnsubAck(decoded)),
                codec::Packet::UnsubAck(packet),
            ) => {
                prop_assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V311(IncomingPacket::PingResp(decoded)),
                codec::Packet::PingResp(packet),
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
        let mut decoder = Decoder::new_v311(Limits::new());
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
        prop_assert!(matches!(decoded, Some(VersionedIncomingPacket::V311(_))));
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
        // MQTT v3.1.1 §2.2.3 Table 2.4:
        // 境界値は符号化方式のサイズ対応に基づく。
        // VBI の符号化長そのものの検証は codec 層の単体テストに委ね、
        // パケットレベルでは encoded_len() の一致と戻り値の一致で間接的に検出する。
        //
        // PUBLISH QoS 0・トピック "t"（1 バイト）をベースにする。
        // QoS 0 の PUBLISH は dup: true や packet_id: Some(_) だと
        // validate() が Err(InvalidField) を返すため、dup: false・packet_id: None で構築する。
        // v3.1.1 の固定オーバーヘッド: 2（トピック長）+ 1（トピック "t"）。
        let overhead = 2 + 1;
        for &target in helpers::REMAINING_LENGTH_BOUNDARIES {
            let payload_len = helpers::payload_len_for_remaining_length(target, overhead);
            let packet = codec::Packet::Publish(v311::publish::Publish {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: false,
                topic: "t".to_string(),
                packet_id: None,
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
