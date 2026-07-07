//! MQTT v5.0 各パケットの decode エラーパスを網羅する統合テスト。
//!
//! 経路の振り分け:
//! - Server → Client 種別・双方向種別・方向に中立なエラー系は、実利用経路である
//!   `Decoder::decode` 経由で検証する。
//! - Client → Server 専用種別（CONNECT / SUBSCRIBE / UNSUBSCRIBE / PINGREQ）のエラー系は
//!   `Decoder` 経由では方向拒否で先に弾かれるため、低水準 `codec::Packet` 経由で検証する。

use shiguredo_mqtt::codec::limits::Limits;
use shiguredo_mqtt::decoder::{Decoder, VersionedIncomingPacket};
use shiguredo_mqtt::error::DecodeError;
use shiguredo_mqtt::v5::packet::codec::Packet;
use shiguredo_mqtt::v5::property::Properties;

/// 完全な 1 フレームを v5 デコーダーに供給してデコードする。
fn decode_v5(buf: &[u8]) -> Result<Option<VersionedIncomingPacket>, DecodeError> {
    let mut decoder = Decoder::new_v5(Limits::new());
    decoder.feed(buf).expect("バイト列の供給に成功すること");
    decoder.decode()
}

#[test]
fn invalid_packet_type_is_rejected() {
    // 0x00 は Reserved 種別。方向に中立なエラーのため Decoder 経由で検証する。
    let buf = [0x00, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::InvalidPacketType));
}

#[test]
fn client_to_server_packets_are_rejected_by_decoder() {
    // Client → Server 専用種別（CONNECT / SUBSCRIBE / UNSUBSCRIBE / PINGREQ）は
    // クライアントの受信方向には存在してはならない（MQTT v5.0 §2.1.2 Table 2-1）。
    // Decoder は先頭バイト上位 4 ビットを packet_type として UnexpectedPacket を返す。
    // 種別判定は内容検証より先に行われるため、Remaining Length 0 の最小フレームで検証する。
    let cases: &[(&str, &[u8], u8)] = &[
        ("CONNECT", &[0x10, 0x00], 0x10),
        ("SUBSCRIBE", &[0x82, 0x00], 0x80),
        ("UNSUBSCRIBE", &[0xA2, 0x00], 0xA0),
        ("PINGREQ", &[0xC0, 0x00], 0xC0),
    ];
    for (name, buf, packet_type) in cases {
        assert_eq!(
            decode_v5(buf),
            Err(DecodeError::UnexpectedPacket {
                packet_type: *packet_type
            }),
            "{name}"
        );
    }
}

#[test]
fn client_to_server_packet_with_invalid_flags_is_rejected_as_unexpected_packet() {
    // 逆方向種別かつ不正フラグの入力（フラグ 0x01 の CONNECT 先頭バイト 0x11）は
    // 種別判定が先に行われるため、InvalidPacketFlags ではなく UnexpectedPacket が優先される。
    // CONNECT は Client → Server 専用種別であり、クライアントの受信方向には存在してはならない
    // （MQTT v5.0 §2.1.2 Table 2-1）。
    let buf = [0x11, 0x00];
    assert_eq!(
        decode_v5(&buf),
        Err(DecodeError::UnexpectedPacket { packet_type: 0x10 })
    );
}

#[test]
fn connect_with_reserved_header_flags_is_rejected() {
    let buf = [
        0x1F, // CONNECT | 全フラグ
        0x14, // Remaining Length = 20
        0x00, 0x04, b'M', b'Q', b'T', b'T', // Protocol Name
        0x05, // Protocol Level
        0x02, // Connect Flags
        0x00, 0x3C, // Keep Alive
        0x00, // Properties Length = 0
        0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1', // Client ID
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn connect_with_bad_protocol_name_is_rejected() {
    let buf = [
        0x10, 0x14, // CONNECT, Remaining Length = 20
        0x00, 0x04, b'M', b'Q', b'T', b'X', // Protocol Name = "MQTX"
        0x05, 0x02, 0x00, 0x3C, 0x00, 0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1',
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connect_with_bad_protocol_level_is_rejected() {
    let buf = [
        0x10, 0x14, // CONNECT, Remaining Length = 20
        0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, // Protocol Level = 4 (v5 では不正)
        0x02, 0x00, 0x3C, 0x00, 0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1',
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connect_with_reserved_connect_flag_is_rejected() {
    let buf = [
        0x10, 0x14, // CONNECT, Remaining Length = 20
        0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x03, // reserved bit 0
        0x00, 0x3C, 0x00, 0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1',
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connect_with_empty_protocol_name_is_rejected() {
    // MQTT v5.0 §3.1.2.1 [MQTT-3.1.2-1]: プロトコル名が空文字列の CONNECT は不正。
    let buf = [
        0x10, 0x10, // CONNECT, Remaining Length = 16
        0x00, 0x00, // Protocol Name length = 0
        0x05, 0x02, // Protocol Level, Connect Flags
        0x00, 0x3C, 0x00, 0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1',
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connect_with_will_flags_but_no_will_is_rejected() {
    let buf = [
        0x10, 0x14, // CONNECT, Remaining Length = 20
        0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05,
        0x3A, // will_qos=1, will_retain=1, will_flag=0
        0x00, 0x3C, 0x00, 0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1',
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connect_with_empty_will_topic_is_rejected() {
    let buf = [
        0x10, 0x1A, // CONNECT, Remaining Length = 26
        0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x06, // will_flag=1
        0x00, 0x3C, 0x00, 0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1',
        0x00, // Will Properties Length
        0x00, 0x00, // Will Topic = ""
        0x00, 0x01, 0xAB,
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connect_with_wildcard_in_will_topic_is_rejected() {
    let buf = [
        0x10, 0x1C, // CONNECT, Remaining Length = 28
        0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x06, 0x00, 0x3C, 0x00, 0x00, 0x07, b'c', b'l',
        b'i', b'e', b'n', b't', b'1', 0x00, 0x00, 0x02, b'a', b'+', 0x00, 0x01, 0xAB,
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connect_with_authentication_data_only_is_rejected() {
    // Authentication Method なしで Authentication Data のみ。
    let buf = [
        0x10, 0x17, // CONNECT, Remaining Length = 23
        0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x03, // Properties Length
        0x16, 0x01, 0xAB, // Authentication Data
        0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1',
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connect_with_invalid_session_expiry_interval_property_is_rejected() {
    // Session Expiry Interval (0x11) の値が 4 バイト未満。
    let buf = [
        0x10, 0x16, // CONNECT, Remaining Length = 22
        0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x02, // Properties Length
        0x11, 0x00, // 値が 1 バイトしかない
        0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1',
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connect_with_duplicate_property_identifier_is_rejected() {
    // Session Expiry Interval (0x11) が重複。
    let mut properties = Properties::new();
    properties.push(shiguredo_mqtt::v5::property::Property::SessionExpiryInterval(1));
    properties.push(shiguredo_mqtt::v5::property::Property::SessionExpiryInterval(2));
    let mut prop_buf = vec![0u8; 32];
    let prop_len = properties
        .encode(&mut prop_buf)
        .expect("プロパティをエンコードできること");

    let mut buf = vec![
        0x10, 0x00, // 後で残り長さを設定
        0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C,
    ];
    buf.extend_from_slice(&prop_buf[..prop_len]);
    buf.extend_from_slice(&[0x00, 0x07, b'c', b'l', b'i', b'e', b'n', b't', b'1']);
    let remaining_len = buf.len() - 2;
    buf[1] = remaining_len as u8;

    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connack_with_reserved_header_flags_is_rejected() {
    let buf = [0x2F, 0x03, 0x00, 0x00, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn connack_with_session_present_and_non_success_reason_is_rejected() {
    let buf = [0x20, 0x03, 0x01, 0x87, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connack_with_invalid_reason_code_is_rejected() {
    let buf = [0x20, 0x03, 0x00, 0x01, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn connack_with_non_connack_reason_code_is_rejected() {
    // 0x9E / 0xA1 / 0xA2 は SUBACK / DISCONNECT 用の Reason Code であり、
    // MQTT v5.0 §3.2.2.2 の Connect Reason Code 表には存在しない。
    for reason_code in [0x9E, 0xA1, 0xA2] {
        let buf = [0x20, 0x03, 0x00, reason_code, 0x00];
        assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
    }
}

#[test]
fn connack_with_invalid_property_is_rejected() {
    // CONNACK に Subscription Identifier (0x0B) は許可されていない。
    let buf = [0x20, 0x05, 0x00, 0x00, 0x02, 0x0B, 0x01];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn publish_with_invalid_qos3_is_rejected() {
    let buf = [
        0x36, 0x0C, 0x00, 0x05, b'h', b'e', b'l', b'l', b'o', 0x00, 0x01, 0x00, 0x01, 0x02,
    ];
    assert_eq!(decode_v5(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn publish_with_dup_on_qos0_is_rejected() {
    let buf = [0x38, 0x07, 0x00, 0x01, b't', 0x00, 0x00, 0x00, b'x'];
    assert_eq!(decode_v5(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn publish_with_empty_topic_and_no_topic_alias_is_rejected() {
    let buf = [
        0x30, 0x04, 0x00, 0x00, // Topic = ""
        0x00, // Properties Length
        b'x',
    ];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn publish_with_wildcard_in_topic_is_rejected() {
    let buf = [0x30, 0x07, 0x00, 0x03, b'a', b'+', b'b', 0x00, b'x'];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn publish_qos1_with_zero_packet_id_is_rejected() {
    let buf = [0x32, 0x07, 0x00, 0x01, b't', 0x00, 0x00, 0x00, b'x'];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn publish_qos0_with_packet_id_in_remaining_length_is_rejected() {
    // MQTT v5.0 §3.3.2.2:
    // QoS 0 の PUBLISH には Packet Identifier が存在しない。
    // トピック名の直後に packet_id 領域（0x01 0x00）を配置すると、
    // QoS 0 では packet_id を読まないため 0x01 を Properties 長と解釈し、
    // プロパティデータが不足して MalformedPacket となる。
    let buf = [0x30, 0x07, 0x00, 0x03, b'a', b'/', b'b', 0x01, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn publish_with_invalid_property_is_rejected() {
    // PUBLISH に Server Keep Alive (0x13) は許可されていない。
    let buf = [0x30, 0x08, 0x00, 0x01, b't', 0x03, 0x13, 0x00, 0x3C, b'x'];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn truncated_property_is_rejected() {
    // プロパティ長がプロパティの実サイズより短い場合、切り詰めとして MalformedPacket になる。
    // PUBLISH の Properties Length を 2 とし、3 バイト必要な Topic Alias (0x23) を含める。
    let buf = [0x30, 0x07, 0x00, 0x01, b't', 0x02, 0x23, 0x00, 0x01, b'x'];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn publish_with_wildcard_in_response_topic_is_rejected() {
    let buf = [
        0x30, 0x0C, 0x00, 0x01, b't', 0x07, 0x08, 0x00, 0x04, b'r', b'e', b'/', b'#', b'x',
    ];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn puback_with_invalid_flags_is_rejected() {
    let buf = [0x42, 0x02, 0x00, 0x01];
    assert_eq!(decode_v5(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn puback_with_zero_packet_id_is_rejected() {
    let buf = [0x40, 0x02, 0x00, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn puback_with_invalid_reason_code_is_rejected() {
    let buf = [0x40, 0x03, 0x00, 0x01, 0xFF];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn puback_with_invalid_property_is_rejected() {
    // PUBACK に Topic Alias (0x23) は許可されていない。
    let buf = [0x40, 0x07, 0x00, 0x01, 0x80, 0x03, 0x23, 0x00, 0x01];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn pubrec_with_invalid_flags_is_rejected() {
    let buf = [0x52, 0x02, 0x00, 0x01];
    assert_eq!(decode_v5(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn pubrec_with_zero_packet_id_is_rejected() {
    let buf = [0x50, 0x02, 0x00, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn pubrec_with_invalid_reason_code_is_rejected() {
    let buf = [0x50, 0x03, 0x00, 0x01, 0xFF];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn pubrel_with_invalid_flags_is_rejected() {
    // PUBREL の正しいフラグは 0x02。
    let buf = [0x60, 0x02, 0x00, 0x01];
    assert_eq!(decode_v5(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn pubrel_with_zero_packet_id_is_rejected() {
    let buf = [0x62, 0x02, 0x00, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn pubrel_with_invalid_reason_code_is_rejected() {
    let buf = [0x62, 0x03, 0x00, 0x01, 0xFF];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn pubcomp_with_invalid_flags_is_rejected() {
    let buf = [0x72, 0x02, 0x00, 0x01];
    assert_eq!(decode_v5(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn pubcomp_with_zero_packet_id_is_rejected() {
    let buf = [0x70, 0x02, 0x00, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn pubcomp_with_invalid_reason_code_is_rejected() {
    let buf = [0x70, 0x03, 0x00, 0x01, 0xFF];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn subscribe_with_invalid_flags_is_rejected() {
    let buf = [
        0x80, 0x09, 0x00, 0x01, 0x00, 0x00, 0x03, b'a', b'/', b'b', 0x01,
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn subscribe_with_zero_packet_id_is_rejected() {
    let buf = [
        0x82, 0x09, 0x00, 0x00, 0x00, 0x00, 0x03, b'a', b'/', b'b', 0x01,
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn subscribe_with_empty_subscriptions_is_rejected() {
    let buf = [0x82, 0x03, 0x00, 0x01, 0x00];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn subscribe_with_invalid_topic_filter_is_rejected() {
    let buf = [0x82, 0x08, 0x00, 0x01, 0x00, 0x00, 0x02, b'a', b'+', 0x01];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn subscribe_with_invalid_retain_handling_is_rejected() {
    // retain_handling = 3
    let buf = [
        0x82, 0x09, 0x00, 0x01, 0x00, 0x00, 0x03, b'a', b'/', b'b', 0x31,
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn subscribe_with_reserved_options_bits_is_rejected() {
    let buf = [
        0x82, 0x09, 0x00, 0x01, 0x00, 0x00, 0x03, b'a', b'/', b'b', 0xC1,
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn subscribe_with_shared_subscription_no_local_is_rejected() {
    // $share/group/a に No Local=1
    let buf = [
        0x82, 0x14, 0x00, 0x01, 0x00, 0x00, 0x0E, b'$', b's', b'h', b'a', b'r', b'e', b'/', b'g',
        b'r', b'o', b'u', b'p', b'/', b'a', 0x04,
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn subscribe_with_invalid_property_is_rejected() {
    // SUBSCRIBE に Topic Alias (0x23) は許可されていない。
    let buf = [
        0x82, 0x0C, 0x00, 0x01, 0x03, 0x23, 0x00, 0x01, 0x00, 0x03, b'a', b'/', b'b', 0x01,
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn suback_with_invalid_flags_is_rejected() {
    let buf = [0x92, 0x04, 0x00, 0x01, 0x00, 0x01];
    assert_eq!(decode_v5(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn suback_with_zero_packet_id_is_rejected() {
    let buf = [0x90, 0x04, 0x00, 0x00, 0x00, 0x01];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn suback_with_empty_reason_codes_is_rejected() {
    let buf = [0x90, 0x03, 0x00, 0x01, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn suback_with_invalid_reason_code_is_rejected() {
    let buf = [0x90, 0x04, 0x00, 0x01, 0x00, 0xFF];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn suback_with_invalid_property_is_rejected() {
    // SUBACK に Topic Alias (0x23) は許可されていない。
    let buf = [0x90, 0x07, 0x00, 0x01, 0x03, 0x23, 0x00, 0x01, 0x01];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn unsubscribe_with_invalid_flags_is_rejected() {
    let buf = [0xA0, 0x08, 0x00, 0x01, 0x00, 0x00, 0x03, b'a', b'/', b'b'];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn unsubscribe_with_zero_packet_id_is_rejected() {
    let buf = [0xA2, 0x07, 0x00, 0x00, 0x00, 0x00, 0x03, b'a', b'/', b'b'];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn unsubscribe_with_empty_topic_filters_is_rejected() {
    let buf = [0xA2, 0x03, 0x00, 0x01, 0x00];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn unsubscribe_with_invalid_topic_filter_is_rejected() {
    let buf = [0xA2, 0x07, 0x00, 0x01, 0x00, 0x00, 0x02, b'a', b'+'];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn unsubscribe_with_invalid_property_is_rejected() {
    // UNSUBSCRIBE に Topic Alias (0x23) は許可されていない。
    let buf = [
        0xA2, 0x0B, 0x00, 0x01, 0x03, 0x23, 0x00, 0x01, 0x00, 0x03, b'a', b'/', b'b',
    ];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn unsuback_with_invalid_flags_is_rejected() {
    let buf = [0xB2, 0x04, 0x00, 0x01, 0x00, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn unsuback_with_zero_packet_id_is_rejected() {
    let buf = [0xB0, 0x04, 0x00, 0x00, 0x00, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn unsuback_with_empty_reason_codes_is_rejected() {
    let buf = [0xB0, 0x03, 0x00, 0x01, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn unsuback_with_invalid_reason_code_is_rejected() {
    let buf = [0xB0, 0x04, 0x00, 0x01, 0x00, 0xFF];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn unsuback_with_invalid_property_is_rejected() {
    // UNSUBACK に Topic Alias (0x23) は許可されていない。
    let buf = [0xB0, 0x07, 0x00, 0x01, 0x03, 0x23, 0x00, 0x01, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
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
    assert_eq!(decode_v5(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn pingresp_with_non_zero_remaining_length_is_rejected() {
    let buf = [0xD0, 0x01, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn disconnect_with_invalid_flags_is_rejected() {
    let buf = [0xE1, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn disconnect_with_invalid_reason_code_is_rejected() {
    let buf = [0xE0, 0x01, 0x03];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn disconnect_with_session_expiry_interval_from_server_is_rejected() {
    // MQTT v5.0 §3.14.2.2.2 [MQTT-3.14.2-2]: Server → Client 方向の DISCONNECT に Session Expiry Interval (0x11) は禁止。
    let buf = [0xE0, 0x07, 0x00, 0x05, 0x11, 0x00, 0x00, 0x00, 0x3C];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn disconnect_with_invalid_property_is_rejected() {
    // DISCONNECT に Topic Alias (0x23) は許可されていない。
    let buf = [0xE0, 0x05, 0x80, 0x03, 0x23, 0x00, 0x01];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn auth_with_invalid_flags_is_rejected() {
    let buf = [0xF1, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::InvalidPacketFlags));
}

#[test]
fn auth_with_invalid_reason_code_is_rejected() {
    let buf = [0xF0, 0x01, 0x01];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn auth_without_authentication_method_is_rejected() {
    let buf = [0xF0, 0x02, 0x18, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn auth_intermediate_form_without_property_length_success_is_rejected() {
    // MQTT v5.0 §3.15.2.1 末尾は Reason Code と Property Length の同時省略のみ許容する。
    // 中間形は MQTT v5.0 §3.15.2.2.2 上 Protocol Error だが、本ライブラリでは
    // 既存どおり DecodeError::MalformedPacket で観測する。
    let buf = [0xF0, 0x01, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn auth_intermediate_form_without_property_length_continue_is_rejected() {
    // MQTT v5.0 §3.15.2.1 末尾は Reason Code と Property Length の同時省略のみ許容する。
    // Continue (0x18) の中間形も MQTT v5.0 §3.15.2.2.2 上 Protocol Error だが、
    // 本ライブラリでは既存どおり DecodeError::MalformedPacket で観測する。
    let buf = [0xF0, 0x01, 0x18];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn auth_explicit_form_with_empty_properties_is_rejected() {
    // MQTT v5.0 §3.15.2.2.2、および MQTT v5.0 §2.2.2.1 [MQTT-2.2.2-1]
    // （空である旨は Property Length 0 で示す）。明示形は Protocol Error だが、
    // 本ライブラリでは既存どおり DecodeError::MalformedPacket で観測する。
    let buf = [0xF0, 0x02, 0x00, 0x00];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn auth_with_invalid_property_is_rejected() {
    // AUTH に Topic Alias (0x23) は許可されていない。
    let buf = [
        0xF0, 0x0E, 0x00, 0x0C, // Properties Length
        0x15, 0x00, 0x06, b'm', b'e', b't', b'h', b'o', b'd', // Authentication Method
        0x23, 0x00, 0x01, // Topic Alias
    ];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn truncated_remaining_length_is_rejected() {
    // 方向に中立なエラー（RemainingLength フェーズで完結する）のため Decoder 経由で検証する。
    let buf = [0x10, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn truncated_packet_is_rejected() {
    // InsufficientData は完全フレーム前提の直接 decode でのみ返る
    // （Decoder 経由では `Ok(None)` になる）ため、低水準 codec で検証する。
    let buf = [0x30, 0x10, 0x00, 0x05, b'h'];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::InsufficientData));
}

#[test]
fn variable_byte_integer_overflow_is_rejected() {
    // VariableByteInteger::MAX + 1
    let buf = [0x10, 0x80, 0x80, 0x80, 0x80, 0x01];
    assert_eq!(decode_v5(&buf), Err(DecodeError::MalformedPacket));
}

#[test]
fn complete_frame_with_inner_length_mismatch_is_rejected_for_incoming_types() {
    // Remaining Length でフレームは完結しているが、内部の長さフィールドが
    // 境界を超えて指している入力。追加入力では解決しないため、各種別で
    // InsufficientData ではなく MalformedPacket になる。
    // Server → Client・双方向種別は Decoder 経由で検証する。
    let cases: &[(&str, &[u8])] = &[
        (
            "CONNACK のプロパティ長がフレーム残量を超える",
            &[0x20, 0x03, 0x00, 0x00, 0x05],
        ),
        (
            "PUBLISH のトピック名長がフレーム残量を超える",
            &[0x30, 0x07, 0xFF, 0xFF, 0x61, 0x61, 0x61, 0x61, 0x61],
        ),
        (
            "PUBACK のプロパティ長がフレーム残量を超える",
            &[0x40, 0x04, 0x00, 0x01, 0x00, 0x05],
        ),
        (
            "PUBREC のプロパティ長がフレーム残量を超える",
            &[0x50, 0x04, 0x00, 0x01, 0x00, 0x05],
        ),
        (
            "PUBREL のプロパティ長がフレーム残量を超える",
            &[0x62, 0x04, 0x00, 0x01, 0x00, 0x05],
        ),
        (
            "PUBCOMP のプロパティ長がフレーム残量を超える",
            &[0x70, 0x04, 0x00, 0x01, 0x00, 0x05],
        ),
        (
            "SUBACK のプロパティ長がフレーム残量を超える",
            &[0x90, 0x04, 0x00, 0x01, 0x05, 0x00],
        ),
        (
            "UNSUBACK のプロパティ長がフレーム残量を超える",
            &[0xB0, 0x04, 0x00, 0x01, 0x05, 0x00],
        ),
        (
            "DISCONNECT のプロパティ長 VBI がフレーム内で途切れる",
            &[0xE0, 0x02, 0x00, 0x80],
        ),
        (
            "AUTH のプロパティ長がフレーム残量を超える",
            &[0xF0, 0x02, 0x18, 0x05],
        ),
    ];
    for (name, buf) in cases {
        assert_eq!(decode_v5(buf), Err(DecodeError::MalformedPacket), "{name}");
    }
}

#[test]
fn complete_frame_with_inner_length_mismatch_is_rejected_for_outgoing_only_types() {
    // Client → Server 専用種別（CONNECT / SUBSCRIBE / UNSUBSCRIBE）は
    // Decoder 経由では方向拒否で先に弾かれるため、低水準 codec で検証する。
    let cases: &[(&str, &[u8])] = &[
        (
            "CONNECT の client_id 長がフレーム残量を超える",
            &[
                0x10, 0x0D, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x00, 0xFF,
                0xFF,
            ],
        ),
        (
            "CONNECT の password の Binary Data 長がフレーム残量を超える",
            &[
                0x10, 0x11, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0xC2, 0x00, 0x3C, 0x00, 0x00,
                0x00, 0x00, 0x00, 0xFF, 0xFF,
            ],
        ),
        (
            "CONNECT の Will トピック長がフレーム残量を超える",
            &[
                0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x06, 0x00, 0x3C, 0x00, 0x00,
                0x00, 0x00, 0xFF, 0xFF,
            ],
        ),
        (
            "SUBSCRIBE の Topic Filter 長がフレーム残量を超える",
            &[0x82, 0x05, 0x00, 0x01, 0x00, 0xFF, 0xFF],
        ),
        (
            "UNSUBSCRIBE のプロパティ長がフレーム残量を超える",
            &[0xA2, 0x03, 0x00, 0x01, 0x05],
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
