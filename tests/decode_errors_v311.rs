//! MQTT v3.1.1 各パケットの decode エラーパスを網羅する統合テスト。
//!
//! 経路の振り分け:
//! - Server → Client 種別・双方向種別・方向に中立なエラー系は、実利用経路である
//!   `Decoder::decode` 経由で検証する。
//! - Client → Server 専用種別（CONNECT / SUBSCRIBE / UNSUBSCRIBE / PINGREQ / DISCONNECT）の
//!   エラー系は `Decoder` 経由では方向拒否で先に弾かれるため、低水準 `codec::Packet` 経由で検証する。

use shiguredo_mqtt::codec::limits::Limits;
use shiguredo_mqtt::decoder::{Decoder, VersionedIncomingPacket};
use shiguredo_mqtt::error::DecodeError;
use shiguredo_mqtt::v311::packet::codec::Packet;

/// 完全な 1 フレームを v3.1.1 デコーダーに供給してデコードする。
fn decode_v311(buf: &[u8]) -> Result<Option<VersionedIncomingPacket>, DecodeError> {
    let mut decoder = Decoder::new_v311(Limits::new());
    decoder.feed(buf).expect("バイト列の供給に成功すること");
    decoder.decode()
}

#[test]
fn invalid_packet_type_is_rejected() {
    // MQTT v3.1.1 §2.2.1 Table 2-1: 0xF0 系のパケット種別は定義されていない（Reserved / Forbidden）。
    // 方向に中立なエラーのため Decoder 経由で検証する。
    let buf = [0xF0, 0x00];
    assert_eq!(decode_v311(&buf), Err(DecodeError::InvalidPacketType));
}

#[test]
fn client_to_server_packets_are_rejected_by_decoder() {
    // Client → Server 専用種別（CONNECT / SUBSCRIBE / UNSUBSCRIBE / PINGREQ / DISCONNECT）は
    // クライアントの受信方向には存在してはならない（MQTT v3.1.1 §2.2.1 Table 2.1 / MQTT v3.1.1 §3.14）。
    // Decoder は先頭バイト上位 4 ビットを packet_type として UnexpectedPacket を返す。
    // 種別判定は内容検証より先に行われるため、Remaining Length 0 の最小フレームで検証する。
    let cases: &[(&str, &[u8], u8)] = &[
        ("CONNECT", &[0x10, 0x00], 0x10),
        ("SUBSCRIBE", &[0x82, 0x00], 0x80),
        ("UNSUBSCRIBE", &[0xA2, 0x00], 0xA0),
        ("PINGREQ", &[0xC0, 0x00], 0xC0),
        ("DISCONNECT", &[0xE0, 0x00], 0xE0),
    ];
    for (name, buf, packet_type) in cases {
        assert_eq!(
            decode_v311(buf),
            Err(DecodeError::UnexpectedPacket {
                packet_type: *packet_type
            }),
            "{name}"
        );
    }
}

#[test]
fn client_to_server_packet_with_invalid_flags_is_rejected_as_unexpected_packet() {
    // 逆方向種別かつ不正フラグの入力は、種別判定が先に行われるため
    // InvalidPacketFlags ではなく UnexpectedPacket が優先される。
    // 0x11 はフラグ 0x01 の CONNECT、0xEF はフラグ 0x0F の DISCONNECT。
    // CONNECT / DISCONNECT は Client → Server 専用種別であり、クライアントの受信方向には
    // 存在してはならない（MQTT v3.1.1 §2.2.1 Table 2.1 / MQTT v3.1.1 §3.14）。
    let cases: &[(&str, &[u8], u8)] = &[
        ("CONNECT", &[0x11, 0x00], 0x10),
        ("DISCONNECT", &[0xEF, 0x00], 0xE0),
    ];
    for (name, buf, packet_type) in cases {
        assert_eq!(
            decode_v311(buf),
            Err(DecodeError::UnexpectedPacket {
                packet_type: *packet_type
            }),
            "{name}"
        );
    }
}

#[test]
fn connect_with_reserved_header_flags_is_rejected() {
    // MQTT v3.1.1 §2.2.2 [MQTT-2.2.2-1]: CONNECT の固定ヘッダー下位 4 ビットは 0 でなければならない。
    let buf = [
        0x1F, // CONNECT | 全フラグ
        0x13, // Remaining Length = 19
        0x00, 0x04, b'M', b'Q', b'T', b'T', // Protocol Name
        0x04, // Protocol Level
        0x02, // Connect Flags
        0x00, 0x3C, // Keep Alive
        0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1', // Client ID
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn connect_with_bad_protocol_name_is_rejected() {
    let buf = [
        0x10, 0x13, // CONNECT, Remaining Length = 19
        0x00, 0x04, b'M', b'Q', b'T', b'X', // Protocol Name = "MQTX"
        0x04, // Protocol Level
        0x02, // Connect Flags
        0x00, 0x3C, // Keep Alive
        0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1', // Client ID
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connect_with_bad_protocol_level_is_rejected() {
    // Protocol Level 0x05 の検出は低水準 codec 経由で担保する。
    // Decoder 経由では種別 0x10 の方向拒否が先に発動し Protocol Level 検証に到達しない。
    let buf = [
        0x10, 0x13, // CONNECT, Remaining Length = 19
        0x00, 0x04, b'M', b'Q', b'T', b'T', // Protocol Name
        0x05, // Protocol Level = 5 (v3.1.1 では不正)
        0x02, // Connect Flags
        0x00, 0x3C, // Keep Alive
        0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1', // Client ID
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connect_with_reserved_connect_flag_is_rejected() {
    let buf = [
        0x10, 0x13, // CONNECT, Remaining Length = 19
        0x00, 0x04, b'M', b'Q', b'T', b'T', // Protocol Name
        0x04, // Protocol Level
        0x03, // Connect Flags | reserved bit 0
        0x00, 0x3C, // Keep Alive
        0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1', // Client ID
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connect_with_password_only_is_rejected() {
    // MQTT v3.1.1 §3.1.2.9 [MQTT-3.1.2-22]: User Name なしで Password ありの CONNECT はプロトコル違反。
    let buf = [
        0x10, 0x16, // CONNECT, Remaining Length = 22
        0x00, 0x04, b'M', b'Q', b'T', b'T', // Protocol Name
        0x04, // Protocol Level
        0x42, // Connect Flags: password=1, username=0
        0x00, 0x3C, // Keep Alive
        0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1', // Client ID
        0x00, 0x01, 0xAB, // Password
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connect_with_will_flags_but_no_will_is_rejected() {
    // Will Flag=0 なのに Will QoS または Will Retain が設定されている。
    let buf = [
        0x10, 0x13, // CONNECT, Remaining Length = 19
        0x00, 0x04, b'M', b'Q', b'T', b'T', // Protocol Name
        0x04, // Protocol Level
        0x3A, // Connect Flags: will_qos=1, will_retain=1, will_flag=0
        0x00, 0x3C, // Keep Alive
        0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1', // Client ID
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connect_with_empty_will_topic_is_rejected() {
    let buf = [
        0x10, 0x18, // CONNECT, Remaining Length = 24
        0x00, 0x04, b'M', b'Q', b'T', b'T', // Protocol Name
        0x04, // Protocol Level
        0x06, // Connect Flags: will_flag=1, will_qos=0
        0x00, 0x3C, // Keep Alive
        0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1', // Client ID
        0x00, 0x00, // Will Topic = ""
        0x00, 0x01, 0xAB, // Will Payload
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connect_with_wildcard_in_will_topic_is_rejected() {
    let buf = [
        0x10, 0x1A, // CONNECT, Remaining Length = 26
        0x00, 0x04, b'M', b'Q', b'T', b'T', // Protocol Name
        0x04, // Protocol Level
        0x06, // Connect Flags: will_flag=1, will_qos=0
        0x00, 0x3C, // Keep Alive
        0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1', // Client ID
        0x00, 0x02, b'a', b'+', // Will Topic = "a+"
        0x00, 0x01, 0xAB, // Will Payload
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connack_with_reserved_header_flags_is_rejected() {
    let buf = [0x2F, 0x02, 0x00, 0x00];
    assert_eq!(decode_v311(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn connack_with_session_present_and_non_accepted_return_code_is_rejected() {
    let buf = [0x20, 0x02, 0x01, 0x02];
    assert_eq!(decode_v311(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connack_with_invalid_return_code_is_rejected() {
    let buf = [0x20, 0x02, 0x00, 0x06];
    assert_eq!(decode_v311(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn publish_with_invalid_qos3_is_rejected() {
    // PUBLISH の flags 下位 4 ビットに QoS 3 (0x06) を設定する。
    let buf = [0x36, 0x07, 0x00, 0x01, b't', 0x00, 0x01, 0x00, 0x01];
    assert_eq!(decode_v311(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn publish_with_dup_on_qos0_is_rejected() {
    // QoS 0 の PUBLISH に DUP ビットを立てる。
    let buf = [0x38, 0x05, 0x00, 0x01, b't', 0x00, 0x00];
    assert_eq!(decode_v311(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn publish_with_empty_topic_is_rejected() {
    let buf = [0x30, 0x05, 0x00, 0x00, 0x00, 0x00, b'x'];
    assert_eq!(decode_v311(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn publish_with_wildcard_in_topic_is_rejected() {
    let buf = [0x30, 0x06, 0x00, 0x03, b'a', b'+', b'b', 0x00];
    assert_eq!(decode_v311(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn publish_qos1_with_zero_packet_id_is_rejected() {
    let buf = [0x32, 0x08, 0x00, 0x01, b't', 0x00, 0x00, 0x00, 0x00, b'x'];
    assert_eq!(decode_v311(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn puback_with_invalid_flags_is_rejected() {
    let buf = [0x4F, 0x02, 0x00, 0x01];
    assert_eq!(decode_v311(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn puback_with_zero_packet_id_is_rejected() {
    let buf = [0x40, 0x02, 0x00, 0x00];
    assert_eq!(decode_v311(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn pubrec_with_invalid_flags_is_rejected() {
    let buf = [0x5F, 0x02, 0x00, 0x01];
    assert_eq!(decode_v311(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn pubrec_with_zero_packet_id_is_rejected() {
    let buf = [0x50, 0x02, 0x00, 0x00];
    assert_eq!(decode_v311(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn pubrel_with_invalid_flags_is_rejected() {
    // PUBREL の正しいフラグは 0x02。
    let buf = [0x60, 0x02, 0x00, 0x01];
    assert_eq!(decode_v311(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn pubrel_with_zero_packet_id_is_rejected() {
    let buf = [0x62, 0x02, 0x00, 0x00];
    assert_eq!(decode_v311(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn pubcomp_with_invalid_flags_is_rejected() {
    let buf = [0x7F, 0x02, 0x00, 0x01];
    assert_eq!(decode_v311(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn pubcomp_with_zero_packet_id_is_rejected() {
    let buf = [0x70, 0x02, 0x00, 0x00];
    assert_eq!(decode_v311(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn subscribe_with_invalid_flags_is_rejected() {
    // SUBSCRIBE の正しいフラグは 0x02。
    let buf = [0x80, 0x09, 0x00, 0x01, 0x00, 0x03, b'a', b'/', b'b', 0x00];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn subscribe_with_zero_packet_id_is_rejected() {
    let buf = [0x82, 0x08, 0x00, 0x00, 0x00, 0x03, b'a', b'/', b'b', 0x00];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn subscribe_with_empty_topic_filters_is_rejected() {
    let buf = [0x82, 0x02, 0x00, 0x01];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn subscribe_with_invalid_topic_filter_is_rejected() {
    let buf = [0x82, 0x07, 0x00, 0x01, 0x00, 0x02, b'a', b'+', 0x00];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn subscribe_with_reserved_qos_bits_is_rejected() {
    // Requested QoS バイトの予約ビットが立っている。
    let buf = [0x82, 0x08, 0x00, 0x01, 0x00, 0x03, b'a', b'/', b'b', 0xF0];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn suback_with_invalid_flags_is_rejected() {
    let buf = [0x9F, 0x03, 0x00, 0x01, 0x00];
    assert_eq!(decode_v311(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn suback_with_zero_packet_id_is_rejected() {
    let buf = [0x90, 0x03, 0x00, 0x00, 0x00];
    assert_eq!(decode_v311(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn suback_with_empty_return_codes_is_rejected() {
    let buf = [0x90, 0x02, 0x00, 0x01];
    assert_eq!(decode_v311(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn suback_with_invalid_return_code_is_rejected() {
    let buf = [0x90, 0x03, 0x00, 0x01, 0x03];
    assert_eq!(decode_v311(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn unsubscribe_with_invalid_flags_is_rejected() {
    // UNSUBSCRIBE の正しいフラグは 0x02。
    let buf = [0xA0, 0x08, 0x00, 0x01, 0x00, 0x03, b'a', b'/', b'b'];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn unsubscribe_with_zero_packet_id_is_rejected() {
    let buf = [0xA2, 0x07, 0x00, 0x00, 0x00, 0x03, b'a', b'/', b'b'];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn unsubscribe_with_empty_topic_filters_is_rejected() {
    let buf = [0xA2, 0x02, 0x00, 0x01];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn unsubscribe_with_invalid_topic_filter_is_rejected() {
    let buf = [0xA2, 0x06, 0x00, 0x01, 0x00, 0x02, b'a', b'+'];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn unsuback_with_invalid_flags_is_rejected() {
    let buf = [0xBF, 0x02, 0x00, 0x01];
    assert_eq!(decode_v311(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn unsuback_with_zero_packet_id_is_rejected() {
    let buf = [0xB0, 0x02, 0x00, 0x00];
    assert_eq!(decode_v311(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn pingreq_with_invalid_flags_is_rejected() {
    let buf = [0xCF, 0x00];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn pingreq_with_non_zero_remaining_length_is_rejected() {
    let buf = [0xC0, 0x01, 0x00];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn pingresp_with_invalid_flags_is_rejected() {
    let buf = [0xDF, 0x00];
    assert_eq!(decode_v311(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn pingresp_with_non_zero_remaining_length_is_rejected() {
    let buf = [0xD0, 0x01, 0x00];
    assert_eq!(decode_v311(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn disconnect_with_invalid_flags_is_rejected() {
    // v3.1.1 の DISCONNECT は Client → Server 専用種別のため低水準 codec で検証する。
    // Decoder 経由では種別 0xE0 の方向拒否がフラグ検証に優先する。
    let buf = [0xEF, 0x00];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn disconnect_with_non_zero_remaining_length_is_rejected() {
    let buf = [0xE0, 0x01, 0x00];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn truncated_remaining_length_is_rejected() {
    // 残り長さの継続ビットが無限に続くような不正な可変長バイト整数。
    // 方向に中立なエラー（RemainingLength フェーズで完結する）のため Decoder 経由で検証する。
    let buf = [0x10, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
    assert_eq!(decode_v311(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn truncated_packet_is_rejected() {
    // 宣言された残り長さに対してデータが不足している。
    // InsufficientData は完全フレーム前提の直接 decode でのみ返る
    // （Decoder 経由では `Ok(None)` になる）ため、低水準 codec で検証する。
    let buf = [0x30, 0x10, 0x00, 0x05, b'h'];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::InsufficientData));
}

#[test]
fn connect_with_empty_client_id_and_clean_session_false_is_rejected() {
    // MQTT v3.1.1 §3.1.3.1 [MQTT-3.1.3-7][MQTT-3.1.3-8]: 長さ 0 の ClientId は CleanSession=1 と組み合わせなければならない。CleanSession=0 との組み合わせはプロトコル違反として拒否する。
    let buf = [
        0x10, // CONNECT
        0x0C, // Remaining Length = 12
        0x00, 0x04, b'M', b'Q', b'T', b'T', // プロトコル名
        0x04, // プロトコルレベル 4
        0x00, // Connect Flags (CleanSession=0)
        0x00, 0x3C, // Keep Alive = 60
        0x00, 0x00, // ClientId 長 = 0
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn complete_frame_with_inner_length_mismatch_is_rejected_for_incoming_types() {
    // Remaining Length でフレームは完結しているが、内部の長さフィールドが
    // 境界を超えて指している入力。追加入力では解決しないため、
    // InsufficientData ではなく MalformedPacket になる。
    // 双方向種別（PUBLISH）は Decoder 経由で検証する。
    // PUBLISH のトピック名長がフレーム残量を超える。
    let buf = [0x30, 0x07, 0xFF, 0xFF, 0x61, 0x61, 0x61, 0x61, 0x61];
    assert_eq!(decode_v311(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn complete_frame_with_inner_length_mismatch_is_rejected_for_outgoing_only_types() {
    // Client → Server 専用種別（CONNECT / SUBSCRIBE / UNSUBSCRIBE）は
    // Decoder 経由では方向拒否で先に弾かれるため、低水準 codec で検証する。
    let cases: &[(&str, &[u8])] = &[
        (
            "CONNECT の client_id 長がフレーム残量を超える",
            &[
                0x10, 0x0C, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C, 0xFF, 0xFF,
            ],
        ),
        (
            "CONNECT の password の Binary Data 長がフレーム残量を超える",
            &[
                0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0xC2, 0x00, 0x3C, 0x00, 0x00,
                0x00, 0x00, 0xFF, 0xFF,
            ],
        ),
        (
            "CONNECT の Will トピック長がフレーム残量を超える",
            &[
                0x10, 0x0E, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x06, 0x00, 0x3C, 0x00, 0x00,
                0xFF, 0xFF,
            ],
        ),
        (
            "SUBSCRIBE の Topic Filter 長がフレーム残量を超える",
            &[0x82, 0x05, 0x00, 0x01, 0xFF, 0xFF, 0x00],
        ),
        (
            "UNSUBSCRIBE の Topic Filter 長がフレーム残量を超える",
            &[0xA2, 0x04, 0x00, 0x01, 0xFF, 0xFF],
        ),
    ];
    for (name, buf) in cases {
        assert_eq!(
            Packet::decode(buf),
            Err(DecodeError::MalformedPacket),
            "{name}"
        );
    }
}
