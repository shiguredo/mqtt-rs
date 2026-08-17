//! MQTT v3.1.1 パケットのプロパティベースラウンドトリップテスト。

mod helpers;

use noprop::TestCaseContext;
use shiguredo_mqtt::codec::limits::Limits;
use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::decoder::Decoder;
use shiguredo_mqtt::decoder::VersionedIncomingPacket;
use shiguredo_mqtt::error::DecodeError;
use shiguredo_mqtt::error::EncodeError;
use shiguredo_mqtt::v311;
use shiguredo_mqtt::v311::packet::IncomingPacket;
use shiguredo_mqtt::v311::packet::codec;

/// Will サンプラ。
///
/// Will Topic は空にできず、ワイルドカード文字も使用できない。
/// 文字列サンプラがワイルドカードを除外済みのため、空を避けるだけでよい。
fn sample_will(ctx: &mut TestCaseContext) -> v311::connect::Will {
    let topic = helpers::sample_string_in(ctx, 1, 100);
    let qos = helpers::sample_qos(ctx);
    let retain = noprop::sample_bool(ctx);
    let payload = helpers::sample_binary(ctx);
    v311::connect::Will {
        topic,
        qos,
        retain,
        payload,
    }
}

fn sample_connect(ctx: &mut TestCaseContext) -> codec::Packet {
    let clean_session = noprop::sample_bool(ctx);
    // Clean Session=false の場合、Client ID は空にできない（valid-by-construction）。
    let client_id = if clean_session {
        helpers::sample_string(ctx)
    } else {
        helpers::sample_string_in(ctx, 1, 100)
    };
    let keep_alive = noprop::sample_u16(ctx);
    let will = helpers::sample_option(ctx, sample_will);
    // Password のみの送信は v3.1.1 では許可されないため、
    // password が Some なら username も必ず Some にする（valid-by-construction）。
    let password = helpers::sample_option(ctx, helpers::sample_binary);
    let username = if password.is_some() {
        Some(helpers::sample_string(ctx))
    } else {
        helpers::sample_option(ctx, helpers::sample_string)
    };

    codec::Packet::Connect(v311::connect::Connect {
        client_id,
        clean_session,
        keep_alive,
        will,
        username,
        password,
    })
}

fn sample_connack(ctx: &mut TestCaseContext) -> codec::Packet {
    let return_code = noprop::sample_choice(
        ctx,
        &[
            v311::connack::ConnectReturnCode::Accepted,
            v311::connack::ConnectReturnCode::NotAuthorized,
            v311::connack::ConnectReturnCode::ServerUnavailable,
        ],
    );
    // session_present は Accepted の場合のみ true（valid-by-construction）。
    let session_present =
        return_code == v311::connack::ConnectReturnCode::Accepted && noprop::sample_bool(ctx);

    codec::Packet::ConnAck(v311::connack::ConnAck {
        session_present,
        return_code,
    })
}

fn sample_publish(ctx: &mut TestCaseContext) -> codec::Packet {
    let qos = helpers::sample_qos(ctx);
    // QoS 0 のときは packet_id なし (0) かつ dup: false でなければならない
    // （MQTT v3.1.1 §3.3.1.1 [MQTT-3.3.1-2]）。valid-by-construction で制約する。
    let packet_id = if qos == QoS::AtMostOnce {
        0
    } else {
        noprop::sample_usize_in(ctx, 1..=u16::MAX as usize) as u16
    };
    let dup = qos != QoS::AtMostOnce && noprop::sample_bool(ctx);
    let retain = noprop::sample_bool(ctx);
    // PUBLISH のトピック名は空にできない。文字列サンプラはワイルドカード除外済み。
    let topic = helpers::sample_string_in(ctx, 1, 100);
    let payload = helpers::sample_binary(ctx);

    codec::Packet::Publish(v311::publish::Publish {
        dup,
        qos,
        retain,
        topic,
        packet_id: (packet_id != 0).then_some(packet_id),
        payload,
    })
}

fn sample_puback(ctx: &mut TestCaseContext) -> codec::Packet {
    let packet_id = noprop::sample_usize_in(ctx, 1..=u16::MAX as usize) as u16;
    codec::Packet::PubAck(v311::puback::PubAck { packet_id })
}

fn sample_pubrec(ctx: &mut TestCaseContext) -> codec::Packet {
    let packet_id = noprop::sample_usize_in(ctx, 1..=u16::MAX as usize) as u16;
    codec::Packet::PubRec(v311::pubrec::PubRec { packet_id })
}

fn sample_pubrel(ctx: &mut TestCaseContext) -> codec::Packet {
    let packet_id = noprop::sample_usize_in(ctx, 1..=u16::MAX as usize) as u16;
    codec::Packet::PubRel(v311::pubrel::PubRel { packet_id })
}

fn sample_pubcomp(ctx: &mut TestCaseContext) -> codec::Packet {
    let packet_id = noprop::sample_usize_in(ctx, 1..=u16::MAX as usize) as u16;
    codec::Packet::PubComp(v311::pubcomp::PubComp { packet_id })
}

fn sample_subscription(ctx: &mut TestCaseContext) -> v311::subscribe::Subscription {
    let topic_filter = helpers::sample_topic_filter(ctx);
    let qos = helpers::sample_qos(ctx);
    v311::subscribe::Subscription { topic_filter, qos }
}

fn sample_subscribe(ctx: &mut TestCaseContext) -> codec::Packet {
    let packet_id = noprop::sample_usize_in(ctx, 1..=u16::MAX as usize) as u16;
    let count = noprop::sample_usize_in(ctx, 1..=4);
    let mut topic_filters = Vec::with_capacity(count);
    for _ in 0..count {
        topic_filters.push(sample_subscription(ctx));
    }
    codec::Packet::Subscribe(v311::subscribe::Subscribe {
        packet_id,
        topic_filters,
    })
}

fn sample_suback_return_code(ctx: &mut TestCaseContext) -> v311::suback::SubscribeReturnCode {
    noprop::sample_choice(
        ctx,
        &[
            v311::suback::SubscribeReturnCode::SuccessQoS0,
            v311::suback::SubscribeReturnCode::SuccessQoS1,
            v311::suback::SubscribeReturnCode::SuccessQoS2,
            v311::suback::SubscribeReturnCode::Failure,
        ],
    )
}

fn sample_suback(ctx: &mut TestCaseContext) -> codec::Packet {
    let packet_id = noprop::sample_usize_in(ctx, 1..=u16::MAX as usize) as u16;
    let count = noprop::sample_usize_in(ctx, 1..=4);
    let mut return_codes = Vec::with_capacity(count);
    for _ in 0..count {
        return_codes.push(sample_suback_return_code(ctx));
    }
    codec::Packet::SubAck(v311::suback::SubAck {
        packet_id,
        return_codes,
    })
}

fn sample_unsubscribe(ctx: &mut TestCaseContext) -> codec::Packet {
    let packet_id = noprop::sample_usize_in(ctx, 1..=u16::MAX as usize) as u16;
    let count = noprop::sample_usize_in(ctx, 1..=4);
    let mut topic_filters = Vec::with_capacity(count);
    for _ in 0..count {
        topic_filters.push(helpers::sample_topic_filter(ctx));
    }
    codec::Packet::Unsubscribe(v311::unsubscribe::Unsubscribe {
        packet_id,
        topic_filters,
    })
}

fn sample_unsuback(ctx: &mut TestCaseContext) -> codec::Packet {
    let packet_id = noprop::sample_usize_in(ctx, 1..=u16::MAX as usize) as u16;
    codec::Packet::UnsubAck(v311::unsuback::UnsubAck { packet_id })
}

/// 全種別の `codec::Packet` を生成するサンプラ（低水準 codec のラウンドトリップ検証用）。
fn sample_codec_packet(ctx: &mut TestCaseContext) -> codec::Packet {
    match noprop::sample_usize_in(ctx, 0..14) {
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
        11 => codec::Packet::PingReq(v311::pingreq::PingReq),
        12 => codec::Packet::PingResp(v311::pingresp::PingResp),
        _ => codec::Packet::Disconnect(v311::disconnect::Disconnect),
    }
}

/// Server → Client 方向の種別のみを生成するサンプラ（Decoder 経由のラウンドトリップ検証用）。
/// v3.1.1 の DISCONNECT は Client → Server 専用のため含めない
/// （MQTT v3.1.1 §2.2.1 Table 2.1）。
fn sample_incoming_packet(ctx: &mut TestCaseContext) -> codec::Packet {
    match noprop::sample_usize_in(ctx, 0..9) {
        0 => sample_connack(ctx),
        1 => sample_publish(ctx),
        2 => sample_puback(ctx),
        3 => sample_pubrec(ctx),
        4 => sample_pubrel(ctx),
        5 => sample_pubcomp(ctx),
        6 => sample_suback(ctx),
        7 => sample_unsuback(ctx),
        _ => codec::Packet::PingResp(v311::pingresp::PingResp),
    }
}

/// Client → Server 専用種別のみを生成するサンプラ（Decoder の方向拒否検証用）。
/// v3.1.1 では DISCONNECT も Client → Server 専用（MQTT v3.1.1 §2.2.1 Table 2.1）。
fn sample_outgoing_only_packet(ctx: &mut TestCaseContext) -> codec::Packet {
    match noprop::sample_usize_in(ctx, 0..5) {
        0 => sample_connect(ctx),
        1 => sample_subscribe(ctx),
        2 => sample_unsubscribe(ctx),
        3 => codec::Packet::PingReq(v311::pingreq::PingReq),
        _ => codec::Packet::Disconnect(v311::disconnect::Disconnect),
    }
}

#[test]
fn packet_roundtrip() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet = sample_codec_packet(ctx);
        let mut buf = vec![0u8; 65536];
        let len = packet.encode(&mut buf).expect("パケットのエンコードに失敗");
        let (decoded, consumed) =
            codec::Packet::decode(&buf[..len]).expect("パケットのデコードに失敗");
        assert_eq!(decoded.clone(), packet);
        assert_eq!(consumed, len);

        let mut buf2 = vec![0u8; 65536];
        let len2 = decoded
            .encode(&mut buf2)
            .expect("パケットの再エンコードに失敗");
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
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V311(IncomingPacket::Publish(decoded)),
                codec::Packet::Publish(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V311(IncomingPacket::PubAck(decoded)),
                codec::Packet::PubAck(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V311(IncomingPacket::PubRec(decoded)),
                codec::Packet::PubRec(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V311(IncomingPacket::PubRel(decoded)),
                codec::Packet::PubRel(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V311(IncomingPacket::PubComp(decoded)),
                codec::Packet::PubComp(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V311(IncomingPacket::SubAck(decoded)),
                codec::Packet::SubAck(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V311(IncomingPacket::UnsubAck(decoded)),
                codec::Packet::UnsubAck(packet),
            ) => {
                assert_eq!(&decoded, packet);
            }
            (
                VersionedIncomingPacket::V311(IncomingPacket::PingResp(decoded)),
                codec::Packet::PingResp(packet),
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
        let mut decoder = Decoder::new_v311(Limits::new());
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
        assert!(matches!(decoded, Some(VersionedIncomingPacket::V311(_))));
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
/// MQTT v3.1.1 §2.2.3 Table 2.4:
/// 境界値は符号化方式のサイズ対応に基づく。
/// VBI の符号化長そのものの検証は codec 層の単体テストに委ね、
/// パケットレベルでは encoded_len() の一致と戻り値の一致で間接的に検出する。
///
/// PUBLISH QoS 0・トピック "t"（1 バイト）をベースにする。
/// QoS 0 の PUBLISH は dup: true や packet_id: Some(_) だと
/// validate() が Err(InvalidField) を返すため、dup: false・packet_id: None で構築する。
/// v3.1.1 の固定オーバーヘッド: 2（トピック長）+ 1（トピック "t"）。
#[test]
fn remaining_length_boundary() {
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
