//! MQTT v3.1.1 packet codec の単体テスト。

use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::codec::variable_byte_integer::VariableByteInteger;
use shiguredo_mqtt::error::{DecodeError, EncodeError};
use shiguredo_mqtt::v311::connect::Connect;
use shiguredo_mqtt::v311::disconnect::Disconnect;
use shiguredo_mqtt::v311::packet::codec::Packet;
use shiguredo_mqtt::v311::subscribe::{Subscribe, Subscription};

#[test]
fn encode_to_vec_matches_encode() {
    let packet = Packet::Disconnect(Disconnect);
    let vec = packet
        .encode_to_vec()
        .expect("Vec へのエンコードに成功すること");
    let mut buf = [0u8; 16];
    let len = packet.encode(&mut buf).expect("エンコードに成功すること");
    assert_eq!(vec, &buf[..len]);
}

#[test]
fn invalid_packet_type_is_rejected() {
    let buf = [0xF0, 0x00];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::InvalidPacketType));
}

#[test]
fn reserved_header_flags_on_connect_are_rejected() {
    let packet = Packet::Connect(Connect {
        client_id: "client-1".to_string(),
        clean_session: true,
        keep_alive: 60,
        will: None,
        username: None,
        password: None,
    });
    let mut buf = [0u8; 256];
    let len = packet.encode(&mut buf).expect("エンコードに成功すること");
    buf[0] |= 0x0F;
    assert_eq!(
        Packet::decode(&buf[..len]),
        Err(DecodeError::InvalidPacketFlags)
    );
}

#[test]
fn wildcard_in_publish_topic_is_rejected() {
    let buf = [0x30, 0x06, 0x00, 0x03, 0x61, 0x2B, 0x62, 0x00];
    assert_eq!(Packet::decode(&buf), Err(DecodeError::MalformedPacket));
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
        });
    }
    let packet = Packet::Subscribe(Subscribe {
        packet_id: 1,
        topic_filters: subscriptions,
    });
    assert_eq!(
        packet.encode_to_vec(),
        Err(EncodeError::PacketTooLarge {
            size: packet.encoded_len(),
            limit: VariableByteInteger::MAX as usize,
        })
    );
}
