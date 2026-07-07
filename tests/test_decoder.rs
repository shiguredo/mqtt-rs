//! ストリーミング MQTT デコーダーの統合テスト。

use shiguredo_mqtt::codec::limits::Limits;
use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::decoder::{Decoder, VersionedIncomingPacket};
use shiguredo_mqtt::error::{DecodeError, EncodeError};
use shiguredo_mqtt::v5;
use shiguredo_mqtt::v5::packet::IncomingPacket as V5IncomingPacket;
use shiguredo_mqtt::v5::packet::OutgoingPacket as V5OutgoingPacket;
use shiguredo_mqtt::v5::pingresp::PingResp as V5PingResp;
use shiguredo_mqtt::v5::property::Properties;
use shiguredo_mqtt::v5::publish::Publish as V5Publish;
use shiguredo_mqtt::v5::subscribe::Subscription as V5Subscription;
use shiguredo_mqtt::v311;
use shiguredo_mqtt::v311::packet::IncomingPacket as V311IncomingPacket;
use shiguredo_mqtt::v311::packet::OutgoingPacket as V311OutgoingPacket;
use shiguredo_mqtt::v311::pingresp::PingResp as V311PingResp;
use shiguredo_mqtt::v311::subscribe::Subscription as V311Subscription;

/// `encode` クロージャで個別 struct のパケットをエンコードし、
/// 書き込み済みの長さに切り詰めたバイト列を返す。
fn encode_packet(encode: impl FnOnce(&mut [u8]) -> Result<usize, EncodeError>) -> Vec<u8> {
    let mut buf = vec![0u8; 65536];
    let len = encode(&mut buf).expect("パケットのエンコードに失敗した");
    buf.truncate(len);
    buf
}

#[test]
fn fragmented_v5_publish_byte_by_byte() {
    let publish = v5::publish::Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: true,
        topic: "test/topic".to_string(),
        packet_id: Some(42),
        properties: v5::property::Properties::new(),
        payload: vec![0x01, 0x02, 0x03],
    };
    let data = encode_packet(|buf| publish.encode(buf));

    let mut decoder = Decoder::new_v5(Limits::new());
    for (i, byte) in data.iter().enumerate() {
        decoder.feed(&[*byte]).expect("バイト列の供給に失敗した");
        if i < data.len() - 1 {
            assert_eq!(
                decoder.decode().expect("パケットのデコードに失敗した"),
                None
            );
        }
    }
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        Some(VersionedIncomingPacket::V5(
            v5::packet::IncomingPacket::Publish(publish)
        ))
    );
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        None
    );
}

#[test]
fn fragmented_v5_auth_omitted_form() {
    // MQTT v5.0 §3.15.2.1 末尾: Reason Code と Property Length を同時に省略した
    // Remaining Length 0 の AUTH は Success・プロパティなしとして受理される。
    // encode は Success を拒否し省略形も生成しないため、手組みバイト列を使う。
    let data = [0xF0, 0x00];

    let mut decoder = Decoder::new_v5(Limits::new());
    for (i, byte) in data.iter().enumerate() {
        decoder.feed(&[*byte]).expect("バイト列の供給に失敗した");
        if i < data.len() - 1 {
            assert_eq!(
                decoder.decode().expect("パケットのデコードに失敗した"),
                None
            );
        }
    }
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        Some(VersionedIncomingPacket::V5(
            v5::packet::IncomingPacket::Auth(v5::auth::Auth {
                reason_code: v5::auth::AuthReasonCode::Success,
                properties: v5::property::Properties::new(),
            })
        ))
    );
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        None
    );
}

#[test]
fn fragmented_v311_connack_byte_by_byte() {
    // 2 バイト固定の PINGRESP ではなく、より長いフレームの CONNACK（4 バイト）を用い、
    // 複数バイトフレームの分割受信カバレッジを維持する。
    let connack = v311::connack::ConnAck {
        session_present: false,
        return_code: v311::connack::ConnectReturnCode::Accepted,
    };
    let data = encode_packet(|buf| connack.encode(buf));

    let mut decoder = Decoder::new_v311(Limits::new());
    let chunk_count = data.chunks(3).len();
    for (i, chunk) in data.chunks(3).enumerate() {
        decoder.feed(chunk).expect("バイト列の供給に失敗した");
        if i < chunk_count - 1 {
            assert_eq!(
                decoder.decode().expect("パケットのデコードに失敗した"),
                None
            );
        }
    }
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        Some(VersionedIncomingPacket::V311(
            v311::packet::IncomingPacket::ConnAck(connack)
        ))
    );
}

#[test]
fn multiple_v5_packets_back_to_back() {
    let pingresp = v5::pingresp::PingResp;
    let disconnect = v5::disconnect::Disconnect {
        reason_code: v5::disconnect::DisconnectReasonCode::NormalDisconnection,
        properties: v5::property::Properties::new(),
    };

    let mut data = Vec::new();
    data.extend_from_slice(&encode_packet(|buf| pingresp.encode(buf)));
    data.extend_from_slice(&encode_packet(|buf| pingresp.encode(buf)));
    data.extend_from_slice(&encode_packet(|buf| disconnect.encode(buf)));

    let mut decoder = Decoder::new_v5(Limits::new());
    decoder.feed(&data).expect("バイト列の供給に失敗した");
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        Some(VersionedIncomingPacket::V5(
            v5::packet::IncomingPacket::PingResp(pingresp)
        ))
    );
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        Some(VersionedIncomingPacket::V5(
            v5::packet::IncomingPacket::PingResp(pingresp)
        ))
    );
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        Some(VersionedIncomingPacket::V5(
            v5::packet::IncomingPacket::Disconnect(disconnect)
        ))
    );
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        None
    );
}

#[test]
fn multiple_v311_packets_back_to_back() {
    let connack = v311::connack::ConnAck {
        session_present: false,
        return_code: v311::connack::ConnectReturnCode::Accepted,
    };
    let pingresp = v311::pingresp::PingResp;
    let puback = v311::puback::PubAck { packet_id: 1 };

    let mut data = Vec::new();
    data.extend_from_slice(&encode_packet(|buf| connack.encode(buf)));
    data.extend_from_slice(&encode_packet(|buf| pingresp.encode(buf)));
    data.extend_from_slice(&encode_packet(|buf| puback.encode(buf)));

    let mut decoder = Decoder::new_v311(Limits::new());
    decoder.feed(&data).expect("バイト列の供給に失敗した");
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        Some(VersionedIncomingPacket::V311(
            v311::packet::IncomingPacket::ConnAck(connack)
        ))
    );
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        Some(VersionedIncomingPacket::V311(
            v311::packet::IncomingPacket::PingResp(pingresp)
        ))
    );
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        Some(VersionedIncomingPacket::V311(
            v311::packet::IncomingPacket::PubAck(puback)
        ))
    );
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        None
    );
}

#[test]
fn packet_size_limit_is_enforced() {
    let limits = Limits::new().with_max_packet_size(16);
    let mut decoder = Decoder::new_v5(limits);

    let publish = v5::publish::Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "a/very/long/topic/name".to_string(),
        packet_id: None,
        properties: v5::property::Properties::new(),
        payload: vec![],
    };
    let data = encode_packet(|buf| publish.encode(buf));

    // 空バッファに対して max_packet_size を超える入力を一度に供給すると、
    // feed 時点で PacketTooLarge が返る。
    // limit には 1 パケット分の固定ヘッダーオーバーヘッド（5 バイト）が加算される。
    assert_eq!(
        decoder.feed(&data),
        Err(DecodeError::PacketTooLarge {
            size: data.len(),
            limit: 21,
        })
    );
}

#[test]
fn decoder_recovers_after_malformed_packet() {
    let mut decoder = Decoder::new_v5(Limits::new());

    // 無効な理由コードを含む PUBACK と、その後に続く有効な PINGRESP を同時に供給する。
    let mut data = Vec::new();
    data.extend_from_slice(&[0x40, 0x03, 0x00, 0x01, 0xFF]);
    data.extend_from_slice(&encode_packet(|buf| v5::pingresp::PingResp.encode(buf)));
    decoder.feed(&data).expect("バイト列の供給に失敗した");

    // Payload フェーズでの malformed パケットは 1 パケット分だけ drain され、
    // 後続の有効なパケットが再 feed なしで救える。
    assert_eq!(decoder.decode(), Err(DecodeError::MalformedPacket));
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        Some(VersionedIncomingPacket::V5(
            v5::packet::IncomingPacket::PingResp(v5::pingresp::PingResp)
        ))
    );
}

#[test]
fn oversized_single_feed_is_rejected_when_buffer_is_empty() {
    let limits = Limits::new().with_max_packet_size(16);
    let mut decoder = Decoder::new_v5(limits);

    let data = vec![0xC0; 32];
    assert_eq!(
        decoder.feed(&data),
        Err(DecodeError::PacketTooLarge {
            size: 32,
            limit: 21,
        })
    );
}

#[test]
fn consecutive_packets_within_limit_are_accepted() {
    // max_packet_size を超える合計サイズのパケット列でも、各パケットをデコードしながら
    // 供給すれば連続してデコードできる。
    let limits = Limits::new().with_max_packet_size(16);
    let mut decoder = Decoder::new_v5(limits);

    let publish1 = v5::publish::Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "12345".to_string(),
        packet_id: None,
        properties: v5::property::Properties::new(),
        payload: vec![0x01],
    };
    let publish2 = publish1.clone();
    let publish3 = publish1.clone();

    // 各パケットは個別に制限内だが、デコードせずに貯めると累積サイズが制限を超える。
    // ここでは 1 つずつデコードしながら供給し、通常の連続利用が維持されることを検証する。
    decoder
        .feed(&encode_packet(|buf| publish1.encode(buf)))
        .expect("バイト列の供給に失敗した");
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        Some(VersionedIncomingPacket::V5(
            v5::packet::IncomingPacket::Publish(publish1.clone())
        ))
    );
    decoder
        .feed(&encode_packet(|buf| publish2.encode(buf)))
        .expect("バイト列の供給に失敗した");
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        Some(VersionedIncomingPacket::V5(
            v5::packet::IncomingPacket::Publish(publish2.clone())
        ))
    );
    decoder
        .feed(&encode_packet(|buf| publish3.encode(buf)))
        .expect("バイト列の供給に失敗した");
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        Some(VersionedIncomingPacket::V5(
            v5::packet::IncomingPacket::Publish(publish3)
        ))
    );
    assert_eq!(
        decoder.decode().expect("パケットのデコードに失敗した"),
        None
    );
}

#[test]
fn accumulated_buffer_size_is_limited() {
    // 未処理のバッファが残っている状態で追加入力を供給すると、
    // 累積サイズが max_packet_size を超えた時点で PacketTooLarge が返る。
    let limits = Limits::new().with_max_packet_size(16);
    let mut decoder = Decoder::new_v5(limits);

    let publish = v5::publish::Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "12345".to_string(),
        packet_id: None,
        properties: v5::property::Properties::new(),
        payload: vec![0x01],
    };
    let data = encode_packet(|buf| publish.encode(buf));

    // 1 パケット分をまるごと feed するが、decode せずにバッファに残す。
    decoder.feed(&data).expect("バイト列の供給に失敗した");

    // 同じサイズのデータをさらに供給しようとすると、累積サイズが制限を超える。
    // limit には 1 パケット分の固定ヘッダーオーバーヘッド（5 バイト）が加算される。
    assert_eq!(
        decoder.feed(&data),
        Err(DecodeError::PacketTooLarge {
            size: data.len() * 2,
            limit: 21,
        })
    );
}

#[test]
fn small_packets_chunk_within_overhead_is_accepted() {
    // max_packet_size を小さく設定しても、1 パケット分の固定ヘッダーオーバーヘッド
    // 分だけ余裕があるため、max_packet_size 自体をわずかに超える未消費バッファを
    // 含む 1 チャンクが偽陽性で拒否されない。
    let limits = Limits::new().with_max_packet_size(16);
    let mut decoder = Decoder::new_v5(limits);

    let pingresp = v5::pingresp::PingResp;
    let encoded = encode_packet(|buf| pingresp.encode(buf));
    // PINGRESP は 2 バイト。10 個で 20 バイトあり、max_packet_size (16) を超えるが
    // 未消費バッファ + オーバーヘッド (21) 以内なら受理される。
    let mut data = Vec::new();
    for _ in 0..10 {
        data.extend_from_slice(&encoded);
    }

    decoder
        .feed(&data)
        .expect("オーバーヘッド内のチャンクは受理されること");

    for _ in 0..10 {
        assert_eq!(
            decoder.decode().expect("パケットのデコードに失敗した"),
            Some(VersionedIncomingPacket::V5(
                v5::packet::IncomingPacket::PingResp(pingresp)
            ))
        );
    }
    assert_eq!(decoder.decode().expect("デコードの終了確認"), None);
}

#[test]
fn fragmented_v5_pingresp() {
    let mut decoder = Decoder::new_v5(Limits::new());
    let packet = V5IncomingPacket::PingResp(V5PingResp);
    let data = encode_packet(|buf| V5PingResp.encode(buf));
    assert_eq!(data.len(), 2);

    for (i, byte) in data.iter().enumerate() {
        assert_eq!(decoder.decode().expect("デコードに失敗しました"), None);
        decoder
            .feed(&[*byte])
            .expect("バイト列の供給に失敗しました");
        if i < data.len() - 1 {
            assert_eq!(decoder.decode().expect("デコードに失敗しました"), None);
        }
    }
    let decoded = decoder
        .decode()
        .expect("PingResp パケットのデコードに失敗しました");
    assert_eq!(decoded, Some(VersionedIncomingPacket::V5(packet)));
}

#[test]
fn fragmented_remaining_length_is_retained() {
    // Remaining Length = 128 の v5 PUBLISH (QoS 0) を生成する。
    // トピック "a" (1 バイト) + プロパティ長 0 (1 バイト) + ペイロード 124 バイト。
    let expected = V5IncomingPacket::Publish(V5Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "a".to_string(),
        packet_id: None,
        properties: Properties::new(),
        payload: vec![0x00; 124],
    });
    let mut packet_bytes = Vec::new();
    packet_bytes.push(0x30); // PUBLISH, QoS 0
    packet_bytes.push(0x80); // Remaining Length = 128 の下位バイト
    packet_bytes.push(0x01); // Remaining Length = 128 の上位バイト
    packet_bytes.extend_from_slice(&[0x00, 0x01, 0x61]); // トピック長 1 + "a"
    packet_bytes.push(0x00); // プロパティ長 0
    packet_bytes.resize(packet_bytes.len() + 124, 0x00); // ペイロード 124 バイト

    let mut decoder = Decoder::new_v5(Limits::new());

    // 1 フィード目: 固定ヘッダーのみ。
    decoder
        .feed(&packet_bytes[..1])
        .expect("供給に失敗しました");
    assert_eq!(decoder.decode().expect("デコードに失敗しました"), None);

    // 2 フィード目: Remaining Length の途中まで。
    decoder
        .feed(&packet_bytes[1..2])
        .expect("供給に失敗しました");
    assert_eq!(decoder.decode().expect("デコードに失敗しました"), None);

    // 3 フィード目: 残りのデータ。
    decoder
        .feed(&packet_bytes[2..])
        .expect("供給に失敗しました");
    let decoded = decoder.decode().expect("デコードに失敗しました");
    assert_eq!(decoded, Some(VersionedIncomingPacket::V5(expected)));
}

#[test]
fn malformed_remaining_length_still_resets_buffer() {
    // 4 バイトを超える可変長バイト整数（継続ビットが 4 バイト目まで立っている）。
    let mut decoder = Decoder::new_v5(Limits::new());
    decoder
        .feed(&[0x10, 0x80, 0x80, 0x80, 0x80, 0x01])
        .expect("供給に失敗しました");
    assert_eq!(decoder.decode(), Err(DecodeError::MalformedPacket));
    // バッファがクリアされているため、後続の正常なデータは新しいパケットとして受理される。
    let packet = V5IncomingPacket::PingResp(V5PingResp);
    let data = encode_packet(|buf| V5PingResp.encode(buf));
    decoder.feed(&data).expect("供給に失敗しました");
    let decoded = decoder.decode().expect("デコードに失敗しました");
    assert_eq!(decoded, Some(VersionedIncomingPacket::V5(packet)));
}

#[test]
fn remaining_length_exceeding_max_is_rejected() {
    // VariableByteInteger::MAX + 1 を表す 5 バイト列。
    let buf = [0x10, 0x80, 0x80, 0x80, 0x80, 0x01];
    let mut decoder = Decoder::new_v311(Limits::new());
    decoder.feed(&buf).expect("バイト列の供給に失敗しました");
    assert_eq!(decoder.decode(), Err(DecodeError::MalformedPacket));
}

#[test]
fn feed_exceeding_max_packet_size_is_rejected() {
    // 空バッファ状態で max_packet_size + オーバーヘッドを超えるバイト列を
    // 供給すると拒否される。
    let limits = Limits::new().with_max_packet_size(4);
    let mut decoder = Decoder::new_v311(limits);
    // allowed = 4 + 5 = 9 バイト。10 バイト供給すると拒否される。
    let data = [0x10, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
    assert_eq!(
        decoder.feed(&data),
        Err(DecodeError::PacketTooLarge { size: 10, limit: 9 })
    );
}

#[test]
fn v5_connect_into_v311_decoder_is_rejected() {
    // v3.1.1 デコーダーに v5 CONNECT を投入すると、種別 0x10 が Client → Server 専用のため
    // Protocol Level の検証に到達する前に UnexpectedPacket として拒否される。
    // Protocol Level の検証自体は低水準 codec 経由のテストが担保する。
    let packet = V5OutgoingPacket::Connect(v5::connect::Connect {
        client_id: "client-1".to_string(),
        clean_start: true,
        keep_alive: 60,
        properties: Properties::new(),
        will: None,
        username: None,
        password: None,
    });
    let buf = encode_packet(|buf| packet.encode(buf));

    let mut decoder = Decoder::new_v311(Limits::new());
    decoder.feed(&buf).expect("バイト列の供給に失敗しました");
    assert_eq!(
        decoder.decode(),
        Err(DecodeError::UnexpectedPacket { packet_type: 0x10 })
    );
}

#[test]
fn v5_client_to_server_packets_are_rejected() {
    // CONNECT / SUBSCRIBE / UNSUBSCRIBE / PINGREQ は Client → Server 専用種別であり
    // （MQTT v5.0 §2.1.2 Table 2-1）、クライアントの受信方向には存在してはならない。
    // 先頭バイト上位 4 ビットを packet_type として UnexpectedPacket を返す。
    let cases: &[(&str, V5OutgoingPacket, u8)] = &[
        (
            "CONNECT",
            V5OutgoingPacket::Connect(v5::connect::Connect {
                client_id: "client-1".to_string(),
                clean_start: true,
                keep_alive: 60,
                properties: Properties::new(),
                will: None,
                username: None,
                password: None,
            }),
            0x10,
        ),
        (
            "SUBSCRIBE",
            V5OutgoingPacket::Subscribe(v5::subscribe::Subscribe {
                packet_id: 1,
                subscriptions: vec![V5Subscription {
                    topic_filter: "a/b".to_string(),
                    qos: QoS::AtMostOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: v5::subscribe::RetainHandling::SendRetained,
                }],
                properties: Properties::new(),
            }),
            0x80,
        ),
        (
            "UNSUBSCRIBE",
            V5OutgoingPacket::Unsubscribe(v5::unsubscribe::Unsubscribe {
                packet_id: 1,
                topic_filters: vec!["a/b".to_string()],
                properties: Properties::new(),
            }),
            0xA0,
        ),
        (
            "PINGREQ",
            V5OutgoingPacket::PingReq(v5::pingreq::PingReq),
            0xC0,
        ),
    ];
    for (name, packet, packet_type) in cases {
        let bytes = encode_packet(|buf| packet.encode(buf));
        let mut decoder = Decoder::new_v5(Limits::new());
        decoder.feed(&bytes).expect("バイト列の供給に失敗しました");
        assert_eq!(
            decoder.decode(),
            Err(DecodeError::UnexpectedPacket {
                packet_type: *packet_type
            }),
            "{name}"
        );
        // 1 フレーム消費・後続維持: 方向拒否の後も再 feed なしで
        // 後続の正当な受信パケット（PINGRESP）をデコードできる。
        decoder.feed(&[0xD0, 0x00]).expect("供給に失敗しました");
        assert_eq!(
            decoder.decode(),
            Ok(Some(VersionedIncomingPacket::V5(
                V5IncomingPacket::PingResp(V5PingResp)
            ))),
            "{name}"
        );
    }
}

#[test]
fn v5_reserved_packet_type_is_rejected() {
    // 0x00 は Reserved 種別のため、方向拒否ではなく InvalidPacketType になる。
    let mut decoder = Decoder::new_v5(Limits::new());
    decoder.feed(&[0x00, 0x00]).expect("供給に失敗しました");
    assert_eq!(decoder.decode(), Err(DecodeError::InvalidPacketType));
}

#[test]
fn v311_client_to_server_packets_are_rejected() {
    // CONNECT / SUBSCRIBE / UNSUBSCRIBE / PINGREQ / DISCONNECT は Client → Server 専用種別であり
    // （MQTT v3.1.1 §2.2.1 Table 2.1 / MQTT v3.1.1 §3.14）、
    // クライアントの受信方向には存在してはならない。
    // 先頭バイト上位 4 ビットを packet_type として UnexpectedPacket を返す。
    let cases: &[(&str, V311OutgoingPacket, u8)] = &[
        (
            "CONNECT",
            V311OutgoingPacket::Connect(v311::connect::Connect {
                client_id: "client-1".to_string(),
                clean_session: true,
                keep_alive: 60,
                will: None,
                username: None,
                password: None,
            }),
            0x10,
        ),
        (
            "SUBSCRIBE",
            V311OutgoingPacket::Subscribe(v311::subscribe::Subscribe {
                packet_id: 1,
                topic_filters: vec![V311Subscription {
                    topic_filter: "a/b".to_string(),
                    qos: QoS::AtMostOnce,
                }],
            }),
            0x80,
        ),
        (
            "UNSUBSCRIBE",
            V311OutgoingPacket::Unsubscribe(v311::unsubscribe::Unsubscribe {
                packet_id: 1,
                topic_filters: vec!["a/b".to_string()],
            }),
            0xA0,
        ),
        (
            "PINGREQ",
            V311OutgoingPacket::PingReq(v311::pingreq::PingReq),
            0xC0,
        ),
        (
            "DISCONNECT",
            V311OutgoingPacket::Disconnect(v311::disconnect::Disconnect),
            0xE0,
        ),
    ];
    for (name, packet, packet_type) in cases {
        let bytes = encode_packet(|buf| packet.encode(buf));
        let mut decoder = Decoder::new_v311(Limits::new());
        decoder.feed(&bytes).expect("バイト列の供給に失敗しました");
        assert_eq!(
            decoder.decode(),
            Err(DecodeError::UnexpectedPacket {
                packet_type: *packet_type
            }),
            "{name}"
        );
        // 1 フレーム消費・後続維持: 方向拒否の後も再 feed なしで
        // 後続の正当な受信パケット（PINGRESP）をデコードできる。
        decoder.feed(&[0xD0, 0x00]).expect("供給に失敗しました");
        assert_eq!(
            decoder.decode(),
            Ok(Some(VersionedIncomingPacket::V311(
                V311IncomingPacket::PingResp(V311PingResp)
            ))),
            "{name}"
        );
    }
}

#[test]
fn v311_reserved_packet_types_are_rejected() {
    // 0x00 と 0xF0 は MQTT v3.1.1 では Reserved 種別のため
    // （MQTT v3.1.1 §2.2.1 Table 2.1）、方向拒否ではなく InvalidPacketType になる。
    for first_byte in [0x00u8, 0xF0] {
        let mut decoder = Decoder::new_v311(Limits::new());
        decoder
            .feed(&[first_byte, 0x00])
            .expect("供給に失敗しました");
        assert_eq!(decoder.decode(), Err(DecodeError::InvalidPacketType));
    }
}

#[test]
fn set_limits_preserves_buffered_packets() {
    // QoS 0 の v5 PUBLISH を 2 つ連続で feed し、1 つ目を decode した後に
    // set_limits で制限を差し替えても、バッファ済みの 2 つ目が失われずに
    // decode できることを検証する。
    // 永続セッション再開時に CONNACK と offline メッセージが一度に到着し、
    // CONNACK デコード後に受信制限を反映する場面を想定する。
    let make_publish = |topic: &str, payload: &[u8]| V5Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: topic.to_string(),
        packet_id: None,
        properties: Properties::new(),
        payload: payload.to_vec(),
    };
    let p1 = make_publish("t/a", b"one");
    let p2 = make_publish("t/b", b"two");
    let data1 = encode_packet(|buf| p1.encode(buf));
    let data2 = encode_packet(|buf| p2.encode(buf));

    let mut decoder = Decoder::new_v5(Limits::new());
    decoder.feed(&data1).expect("バイト列の供給に成功すること");
    decoder.feed(&data2).expect("バイト列の供給に成功すること");

    let first = decoder.decode().expect("デコードに成功すること");
    assert_eq!(
        first,
        Some(VersionedIncomingPacket::V5(V5IncomingPacket::Publish(p1)))
    );

    // CONNACK 受信後の制限差し替えを模擬する。バッファは保持される。
    decoder.set_limits(Limits::new().with_max_packet_size(4096));

    let second = decoder.decode().expect("デコードに成功すること");
    assert_eq!(
        second,
        Some(VersionedIncomingPacket::V5(V5IncomingPacket::Publish(p2)))
    );
}
