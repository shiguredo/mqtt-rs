//! MQTT v3.1.1 用の簡易 E2E クライアント。

use std::time::Duration;

use shiguredo_mqtt::codec::MqttVersion;
use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::decoder::{Decoder, VersionedIncomingPacket};
use shiguredo_mqtt::error::{DecodeError, EncodeError};
use shiguredo_mqtt::state::qos_flow::Action;
use shiguredo_mqtt::state::session::Session;
use shiguredo_mqtt::v311::connack::ConnectReturnCode;
use shiguredo_mqtt::v311::connect::Connect;
use shiguredo_mqtt::v311::disconnect::Disconnect;
use shiguredo_mqtt::v311::packet::{IncomingPacket, OutgoingPacket};
use shiguredo_mqtt::v311::publish::Publish;
use tokio::net::TcpStream;

use shiguredo_mqtt::v311::puback::PubAck;
use shiguredo_mqtt::v311::pubcomp::PubComp;
use shiguredo_mqtt::v311::pubrec::PubRec;
use shiguredo_mqtt::v311::pubrel::PubRel;
use shiguredo_mqtt::v311::suback::SubscribeReturnCode;
use shiguredo_mqtt::v311::subscribe::{Subscribe, Subscription};
use shiguredo_mqtt::v311::unsubscribe::Unsubscribe;

/// クライアント動作中に発生しうるエラー。
#[derive(Debug)]
pub enum ClientError {
    /// I/O エラー。
    Io(std::io::Error),
    /// パケットのエンコードに失敗した。
    Encode(EncodeError),
    /// パケットのデコードに失敗した。
    Decode(DecodeError),
    /// 接続が拒否された。
    ConnectRefused(ConnectReturnCode),
    /// セッションの作成または QoS フロー処理に失敗した。
    Session(String),
    /// 予期しないパケット種別を受信した。
    UnexpectedPacket,
    /// 予期しないパケット識別子を受信した。
    UnexpectedPacketIdentifier,
    /// タイムアウトした。
    Timeout,
    /// 接続が切断された。
    Disconnected,
}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<EncodeError> for ClientError {
    fn from(e: EncodeError) -> Self {
        Self::Encode(e)
    }
}

impl From<DecodeError> for ClientError {
    fn from(e: DecodeError) -> Self {
        Self::Decode(e)
    }
}

impl From<crate::client::TransportError> for ClientError {
    fn from(e: crate::client::TransportError) -> Self {
        match e {
            crate::client::TransportError::Io(e) => Self::Io(e),
            crate::client::TransportError::Decode(e) => Self::Decode(e),
            crate::client::TransportError::Timeout => Self::Timeout,
            crate::client::TransportError::Disconnected => Self::Disconnected,
        }
    }
}

/// MQTT v3.1.1 クライアント。
///
/// 受信方向の QoS フロー状態は Sans-I/O 状態機械 `Session` に委譲する。
pub struct MqttClient {
    stream: TcpStream,
    next_packet_id: u16,
    session: Session,
    decoder: Decoder,
}

impl MqttClient {
    /// 指定したホスト・ポートに TCP で接続する。
    pub async fn connect_tcp(host: &str, port: u16) -> std::io::Result<Self> {
        let stream = TcpStream::connect((host, port)).await?;
        // MQTT の制御パケットは小さくバースト的に送るため、
        // Nagle アルゴリズムを無効化してハンドシェイクの遅延を避ける。
        stream.set_nodelay(true)?;
        // 実際のセッションは connect_v311_with_options で作り直す。
        let session = Session::new_v311(String::new(), true)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;
        Ok(Self {
            stream,
            next_packet_id: 1,
            session,
            decoder: Decoder::new(MqttVersion::V311),
        })
    }

    /// Session への参照を返す。
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// 次に使用するパケット識別子を取得する。
    fn next_packet_id(&mut self) -> u16 {
        let id = self.next_packet_id;
        self.next_packet_id = self.next_packet_id.wrapping_add(1);
        if self.next_packet_id == 0 {
            self.next_packet_id = 1;
        }
        id
    }

    /// MQTT v3.1.1 接続を確立する。
    pub async fn connect_v311(&mut self, client_id: &str) -> Result<(), ClientError> {
        self.connect_v311_with_options(client_id, true, None).await
    }

    /// MQTT v3.1.1 接続を確立する。クリーンセッションの有無を指定できる。
    pub async fn connect_v311_with_clean_session(
        &mut self,
        client_id: &str,
        clean_session: bool,
    ) -> Result<(), ClientError> {
        self.connect_v311_with_options(client_id, clean_session, None)
            .await
    }

    /// MQTT v3.1.1 接続を確立する。Will メッセージを指定できる。
    pub async fn connect_v311_with_will(
        &mut self,
        client_id: &str,
        will_topic: &str,
        will_payload: &[u8],
        will_qos: QoS,
    ) -> Result<(), ClientError> {
        let will = shiguredo_mqtt::v311::connect::Will {
            topic: will_topic.to_string(),
            payload: will_payload.to_vec(),
            qos: will_qos,
            retain: false,
        };
        self.connect_v311_with_options(client_id, true, Some(will))
            .await
    }

    async fn connect_v311_with_options(
        &mut self,
        client_id: &str,
        clean_session: bool,
        will: Option<shiguredo_mqtt::v311::connect::Will>,
    ) -> Result<(), ClientError> {
        // 接続のたびにセッションを作り直し、受信方向の QoS フロー状態を管理する。
        self.session = Session::new_v311(client_id.to_string(), clean_session)
            .map_err(|e| ClientError::Session(e.to_string()))?;
        let connect = OutgoingPacket::Connect(Connect {
            client_id: client_id.to_string(),
            clean_session,
            keep_alive: 60,
            will,
            username: None,
            password: None,
        });
        self.session.connect_sent();
        self.send_packet(&connect).await?;
        let packet = self.recv_packet(Duration::from_secs(5)).await?;
        match packet {
            IncomingPacket::ConnAck(connack) => {
                if connack.return_code != ConnectReturnCode::Accepted {
                    return Err(ClientError::ConnectRefused(connack.return_code));
                }
                self.session.connected(connack.session_present);
                Ok(())
            }
            _ => Err(ClientError::UnexpectedPacket),
        }
    }

    /// 指定したトピックを購読する。
    pub async fn subscribe(&mut self, topic: &str, qos: QoS) -> Result<(), ClientError> {
        self.subscribe_many(&[(topic, qos)]).await
    }

    /// 複数のトピックをまとめて購読する。
    pub async fn subscribe_many(&mut self, topics: &[(&str, QoS)]) -> Result<(), ClientError> {
        let packet_id = self.next_packet_id();
        let subscribe = OutgoingPacket::Subscribe(Subscribe {
            packet_id,
            topic_filters: topics
                .iter()
                .map(|(topic, qos)| Subscription {
                    topic_filter: topic.to_string(),
                    qos: *qos,
                })
                .collect(),
        });
        self.send_packet(&subscribe).await?;
        let packet = self.recv_packet(Duration::from_secs(5)).await?;
        match packet {
            IncomingPacket::SubAck(suback) => {
                if suback.packet_id != packet_id {
                    return Err(ClientError::UnexpectedPacketIdentifier);
                }
                if suback.return_codes.len() != topics.len()
                    || suback.return_codes.contains(&SubscribeReturnCode::Failure)
                {
                    return Err(ClientError::UnexpectedPacket);
                }
                Ok(())
            }
            _ => Err(ClientError::UnexpectedPacket),
        }
    }

    /// 指定したトピックの購読を解除する。
    pub async fn unsubscribe(&mut self, topic: &str) -> Result<(), ClientError> {
        let packet_id = self.next_packet_id();
        let unsubscribe = OutgoingPacket::Unsubscribe(Unsubscribe {
            packet_id,
            topic_filters: vec![topic.to_string()],
        });
        self.send_packet(&unsubscribe).await?;
        let packet = self.recv_packet(Duration::from_secs(5)).await?;
        match packet {
            IncomingPacket::UnsubAck(unsuback) if unsuback.packet_id == packet_id => Ok(()),
            _ => Err(ClientError::UnexpectedPacket),
        }
    }

    /// 指定したトピックに QoS 0 でメッセージを公開する。
    pub async fn publish(&mut self, topic: &str, payload: &[u8]) -> Result<(), ClientError> {
        self.publish_with_qos_and_retain(topic, payload, QoS::AtMostOnce, false)
            .await
    }

    /// 指定したトピックに指定した QoS でメッセージを公開する。
    pub async fn publish_with_qos(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
    ) -> Result<(), ClientError> {
        self.publish_with_qos_and_retain(topic, payload, qos, false)
            .await
    }

    /// 指定したトピックに retain フラグ付きでメッセージを公開する。
    pub async fn publish_with_retain(
        &mut self,
        topic: &str,
        payload: &[u8],
    ) -> Result<(), ClientError> {
        self.publish_with_qos_and_retain(topic, payload, QoS::AtMostOnce, true)
            .await
    }

    async fn publish_with_qos_and_retain(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retain: bool,
    ) -> Result<(), ClientError> {
        let packet_id = self.next_packet_id();
        let publish = OutgoingPacket::Publish(Publish {
            dup: false,
            qos,
            retain,
            topic: topic.to_string(),
            packet_id: if qos == QoS::AtMostOnce {
                None
            } else {
                Some(packet_id)
            },
            payload: payload.to_vec(),
        });
        self.send_packet(&publish).await?;

        match qos {
            QoS::AtMostOnce => Ok(()),
            QoS::AtLeastOnce => {
                let packet = self.recv_packet(Duration::from_secs(5)).await?;
                match packet {
                    IncomingPacket::PubAck(puback) if puback.packet_id == packet_id => Ok(()),
                    _ => Err(ClientError::UnexpectedPacket),
                }
            }
            QoS::ExactlyOnce => {
                let packet = self.recv_packet(Duration::from_secs(5)).await?;
                match packet {
                    IncomingPacket::PubRec(pubrec) if pubrec.packet_id == packet_id => {
                        let pubrel = OutgoingPacket::PubRel(PubRel { packet_id });
                        self.send_packet(&pubrel).await?;
                        let packet = self.recv_packet(Duration::from_secs(5)).await?;
                        match packet {
                            IncomingPacket::PubComp(pubcomp) if pubcomp.packet_id == packet_id => {
                                Ok(())
                            }
                            _ => Err(ClientError::UnexpectedPacket),
                        }
                    }
                    _ => Err(ClientError::UnexpectedPacket),
                }
            }
        }
    }

    /// TCP 接続を強制的に切断する。Will メッセージの配信を検証する際に使用する。
    pub fn force_disconnect(self) {
        drop(self.stream);
    }

    /// 指定した時間内に PUBLISH パケットを受信する。
    ///
    /// 受信方向の QoS フロー状態は `Session` に記録する。
    /// MQTT v3.1.1 §4.3.3:
    /// PUBREL 受信前に再受信した同一 Packet Identifier の QoS 2 PUBLISH は
    /// 同一メッセージの再送であり、onward recipient へ重複配送してはならない。
    /// 重複の場合は PUBREC の再送と PUBREL / PUBCOMP の交換だけを行い、
    /// 次の PUBLISH を待ち続ける。
    pub async fn recv_publish(&mut self, dur: Duration) -> Result<Publish, ClientError> {
        loop {
            let publish = match self.recv_packet(dur).await? {
                IncomingPacket::Publish(p) => p,
                _ => return Err(ClientError::UnexpectedPacket),
            };

            match publish.qos {
                QoS::AtMostOnce => return Ok(publish),
                QoS::AtLeastOnce => {
                    // QoS > 0 の PUBLISH には Packet Identifier が必須 (デコーダーが保証する)。
                    let packet_id = publish.packet_id.ok_or(ClientError::UnexpectedPacket)?;
                    self.session
                        .handle_publish_qos1(packet_id)
                        .map_err(|e| ClientError::Session(e.to_string()))?;
                    let puback = OutgoingPacket::PubAck(PubAck { packet_id });
                    self.send_packet(&puback).await?;
                    self.session.puback_sent(packet_id);
                    return Ok(publish);
                }
                QoS::ExactlyOnce => {
                    // QoS > 0 の PUBLISH には Packet Identifier が必須 (デコーダーが保証する)。
                    let packet_id = publish.packet_id.ok_or(ClientError::UnexpectedPacket)?;
                    let action = self
                        .session
                        .handle_publish_qos2(packet_id)
                        .map_err(|e| ClientError::Session(e.to_string()))?;
                    let is_duplicate = matches!(
                        action,
                        Action::SendPubrec {
                            is_duplicate: true,
                            ..
                        }
                    );
                    let pubrec = OutgoingPacket::PubRec(PubRec { packet_id });
                    self.send_packet(&pubrec).await?;
                    // v3.1.1 に Reason Code はないため、成功 (0x00) として記録する。
                    self.session.pubrec_sent(0x00);

                    let packet = self.recv_packet(dur).await?;
                    match packet {
                        IncomingPacket::PubRel(pubrel) if pubrel.packet_id == packet_id => {
                            self.session
                                .handle_pubrel(packet_id)
                                .map_err(|e| ClientError::Session(e.to_string()))?
                                .ok_or(ClientError::UnexpectedPacket)?;
                            let pubcomp = OutgoingPacket::PubComp(PubComp { packet_id });
                            self.send_packet(&pubcomp).await?;
                            self.session.pubcomp_sent();
                            // 重複メッセージは呼び出し元へ配送せず、次の PUBLISH を待つ。
                            if is_duplicate {
                                continue;
                            }
                            return Ok(publish);
                        }
                        _ => return Err(ClientError::UnexpectedPacket),
                    }
                }
            }
        }
    }

    /// MQTT 接続を切断する。
    pub async fn disconnect(&mut self) -> Result<(), ClientError> {
        let disconnect = OutgoingPacket::Disconnect(Disconnect);
        self.send_packet(&disconnect).await?;
        self.session.disconnect_sent();
        Ok(())
    }

    /// パケットをエンコードして送信する。
    async fn send_packet(&mut self, packet: &OutgoingPacket) -> Result<(), ClientError> {
        let buf = packet.encode_to_vec()?;
        crate::client::send_bytes(&mut self.stream, &buf).await?;
        Ok(())
    }

    /// 1 つの MQTT v3.1.1 パケットを受信する。
    async fn recv_packet(&mut self, dur: Duration) -> Result<IncomingPacket, ClientError> {
        match crate::client::recv_packet(&mut self.decoder, &mut self.stream, dur).await? {
            VersionedIncomingPacket::V311(packet) => Ok(packet),
            // v3.1.1 クライアントで v5 パケットがデコードされることはない。
            _ => Err(ClientError::UnexpectedPacket),
        }
    }
}
