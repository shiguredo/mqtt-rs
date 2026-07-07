//! Sans-I/O な `shiguredo_mqtt` を s2n-quic の bidirectional stream 上で動かす
//! MQTT v5.0 クライアント（MQTT over QUIC）。
//!
//! ネットワーク I/O とイベントループは本モジュールが担当し、
//! プロトコル状態は `Session` と `Decoder` に委譲する。
//! EMQX の MQTT over QUIC 実装と同様に、1 本の bidirectional stream 上で
//! MQTT 制御パケットを TCP と同様のフレーミングで送受信する。

use std::time::{Duration, Instant};

use s2n_quic::Client;
use s2n_quic::client::Connect;
use s2n_quic::provider::tls::rustls::Client as TlsClient;
use s2n_quic::stream::BidirectionalStream;
use shiguredo_mqtt::codec::MqttVersion;
use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::decoder::{Decoder, VersionedIncomingPacket};
use shiguredo_mqtt::state::qos_flow::Action;
use shiguredo_mqtt::state::session::{ConnackParams, ConnackReason, Session};
use shiguredo_mqtt::v5::connack::ConnectReasonCode;
use shiguredo_mqtt::v5::connect::{Connect as MqttConnect, Will};
use shiguredo_mqtt::v5::disconnect::{Disconnect, DisconnectReasonCode};
use shiguredo_mqtt::v5::packet::{IncomingPacket, OutgoingPacket};
use shiguredo_mqtt::v5::pingreq::PingReq;
use shiguredo_mqtt::v5::property::{Properties, Property};
use shiguredo_mqtt::v5::puback::{PubAck, PubAckReasonCode};
use shiguredo_mqtt::v5::pubcomp::{PubComp, PubCompReasonCode};
use shiguredo_mqtt::v5::publish::Publish;
use shiguredo_mqtt::v5::pubrec::{PubRec, PubRecReasonCode};
use shiguredo_mqtt::v5::pubrel::{PubRel, PubRelReasonCode};
use shiguredo_mqtt::v5::suback::SubAckReasonCode;
use shiguredo_mqtt::v5::subscribe::{RetainHandling, Subscribe, Subscription};
use shiguredo_mqtt::v5::unsuback::UnsubAckReasonCode;
use shiguredo_mqtt::v5::unsubscribe::Unsubscribe;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{self, timeout};
use tracing::{debug, info, warn};

use crate::error::Error;

/// Keep Alive 判定に使う単調増加ミリ秒クロック。
struct Clock {
    start: Instant,
}

impl Clock {
    fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// 起動からの経過ミリ秒を返す。
    fn now(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}

/// MQTT over QUIC の v5.0 クライアント。
///
/// `endpoint` と `connection` は stream の生存期間中に drop されないよう保持する。
pub struct MqttClient {
    /// s2n-quic Client endpoint。drop すると接続が切れるため保持する。
    _endpoint: Client,
    /// keep_alive 設定済みの QUIC 接続。
    _connection: s2n_quic::connection::Connection,
    stream: BidirectionalStream,
    session: Session,
    decoder: Decoder,
    clock: Clock,
}

impl MqttClient {
    /// 指定ホストへ MQTT over QUIC 接続を確立する。
    ///
    /// `ca_pem` は trust anchor となる CA 証明書 (PEM 文字列)。
    /// `server_name` は TLS SNI / 証明書検証に使う名前。
    /// ALPN は EMQX の quicer listener が受け付ける `"mqtt"` を固定する。
    pub async fn connect_quic(
        host: &str,
        port: u16,
        ca_pem: &str,
        server_name: &str,
    ) -> Result<Self, Error> {
        // rcgen / ブローカー側 CA を trust anchor として登録し、
        // ALPN は EMQX の quicer listener 実装が受け付ける "mqtt" のみ
        // (v5.8 系。既定の "h3" とは非互換なので必ず上書きする)。
        let tls = TlsClient::builder()
            .with_certificate(ca_pem)
            .map_err(|e| Error::Quic(format!("TLS certificate: {e}")))?
            .with_application_protocols([b"mqtt".as_slice()].into_iter())
            .map_err(|e| Error::Quic(format!("ALPN protocols: {e}")))?
            .build()
            .map_err(|e| Error::Quic(format!("TLS client build: {e}")))?;

        // ワイルドカードで OS 割り当ての UDP ポートを bind し、
        // 送信用の s2n-quic Client を起動する。
        let endpoint = Client::builder()
            .with_tls(tls)
            .map_err(|e| Error::Quic(format!("Client TLS provider: {e}")))?
            .with_io("0.0.0.0:0")
            .map_err(|e| Error::Quic(format!("Client UDP bind: {e}")))?
            .start()
            .map_err(|e| Error::Quic(format!("Client start: {e}")))?;

        // Client の IO は 0.0.0.0:0 で IPv4 のみを bind するため、
        // IPv6 のアドレスに接続しようとすると失敗する。
        // macOS では localhost の解決が ::1 を優先する場合があるため、
        // IPv4 のアドレスだけを抽出する。
        let addr = tokio::net::lookup_host(format!("{host}:{port}"))
            .await?
            .find(|a| a.is_ipv4())
            .ok_or_else(|| Error::Quic(format!("no IPv4 address resolved for {host}:{port}")))?;

        info!(%addr, %server_name, "connecting QUIC");
        let connect = Connect::new(addr).with_server_name(server_name);
        let mut connection = endpoint
            .connect(connect)
            .await
            .map_err(|e| Error::Quic(format!("QUIC handshake: {e}")))?;
        // idle timeout でアイドル切断されないよう keep_alive を有効化する。
        connection
            .keep_alive(true)
            .map_err(|e| Error::Quic(format!("keep_alive: {e}")))?;

        // EMQX の MQTT over QUIC 実装は、1 本の bidirectional stream 上で
        // MQTT の制御パケットを TCP と同様のフレーミングで送受信する。
        let stream = connection
            .open_bidirectional_stream()
            .await
            .map_err(|e| Error::Quic(format!("open bidirectional stream: {e}")))?;

        Ok(Self {
            _endpoint: endpoint,
            _connection: connection,
            stream,
            session: Session::new_v5(String::new(), true)?,
            decoder: Decoder::new(MqttVersion::V5),
            clock: Clock::new(),
        })
    }

    /// MQTT CONNECT / CONNACK でセッションを確立する。
    pub async fn connect(
        &mut self,
        client_id: &str,
        keep_alive_secs: u16,
        clean_start: bool,
    ) -> Result<(), Error> {
        self.connect_with_options(
            client_id,
            keep_alive_secs,
            clean_start,
            None,
            Properties::new(),
        )
        .await
    }

    /// MQTT 接続を確立する。クリーンスタートの有無を指定できる。
    pub async fn connect_with_clean_start(
        &mut self,
        client_id: &str,
        clean_start: bool,
    ) -> Result<(), Error> {
        self.connect_with_options(client_id, 60, clean_start, None, Properties::new())
            .await
    }

    /// MQTT 接続を確立する。セッション有効期限を指定し、`clean_start=false` で接続する。
    pub async fn connect_with_session_expiry(
        &mut self,
        client_id: &str,
        session_expiry_interval: u32,
    ) -> Result<(), Error> {
        let mut properties = Properties::new();
        properties.push(Property::SessionExpiryInterval(session_expiry_interval));
        self.connect_with_options(client_id, 60, false, None, properties)
            .await
    }

    /// MQTT 接続を確立する。Will メッセージを指定できる。
    pub async fn connect_with_will(
        &mut self,
        client_id: &str,
        will_topic: &str,
        will_payload: &[u8],
        will_qos: QoS,
    ) -> Result<(), Error> {
        self.connect_with_will_and_properties(
            client_id,
            will_topic,
            will_payload,
            will_qos,
            Properties::new(),
        )
        .await
    }

    /// MQTT 接続を確立する。Will メッセージと Will プロパティを指定できる。
    pub async fn connect_with_will_and_properties(
        &mut self,
        client_id: &str,
        will_topic: &str,
        will_payload: &[u8],
        will_qos: QoS,
        will_properties: Properties,
    ) -> Result<(), Error> {
        let will = Will {
            topic: will_topic.to_string(),
            payload: will_payload.to_vec(),
            qos: will_qos,
            retain: false,
            properties: will_properties,
        };
        self.connect_with_options(client_id, 60, true, Some(will), Properties::new())
            .await
    }

    /// MQTT CONNECT / CONNACK でセッションを確立する（内部共通実装）。
    async fn connect_with_options(
        &mut self,
        client_id: &str,
        keep_alive_secs: u16,
        clean_start: bool,
        will: Option<Will>,
        properties: Properties,
    ) -> Result<(), Error> {
        self.session = Session::new_v5(client_id.to_string(), clean_start)?;
        self.session
            .configure_for_connect(keep_alive_secs, 0, 0, 65535, 0)?;

        let connect = OutgoingPacket::Connect(MqttConnect {
            client_id: client_id.to_string(),
            clean_start,
            keep_alive: keep_alive_secs,
            properties,
            will,
            username: None,
            password: None,
        });
        self.session.connect_sent();
        self.send_packet(&connect).await?;

        let packet = self.recv_packet(Duration::from_secs(10)).await?;
        let IncomingPacket::ConnAck(connack) = packet else {
            return Err(Error::UnexpectedPacket);
        };
        if connack.reason_code != ConnectReasonCode::Success {
            return Err(Error::ConnectRefused(connack.reason_code));
        }

        let params = connack_params_from(&connack);
        self.session.apply_connack(params)?;
        // デコーダーを作り直すとバッファ内の後続バイトが消えるため、制限だけ差し替える。
        self.decoder.set_limits(*self.session.limits());
        info!(
            client_id = self.session.client_id(),
            session_present = connack.session_present,
            "MQTT connected"
        );
        Ok(())
    }

    /// DISCONNECT を送信して接続を終了する。
    pub async fn disconnect(&mut self) -> Result<(), Error> {
        if !self.session.is_active() {
            return Ok(());
        }
        let disconnect = OutgoingPacket::Disconnect(Disconnect {
            reason_code: DisconnectReasonCode::NormalDisconnection,
            properties: Properties::new(),
        });
        self.session.disconnect_sent();
        self.send_packet(&disconnect).await?;
        self.session.disconnected();
        info!("MQTT disconnected");
        Ok(())
    }

    /// QUIC 接続を強制的に切断する。Will メッセージの配信を検証する際に使用する。
    ///
    /// MQTT DISCONNECT は送らず、endpoint / connection / stream ごと破棄する。
    pub fn force_disconnect(self) {
        drop(self);
    }

    /// トピックを購読する。
    pub async fn subscribe(&mut self, topic: &str, qos: QoS) -> Result<(), Error> {
        let packet_id = self
            .session
            .allocate_packet_id()
            .ok_or(Error::PacketIdExhausted)?;

        let subscription = Subscription {
            topic_filter: topic.to_string(),
            qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::SendRetained,
        };
        let entries = vec![subscription.clone().into()];
        self.session.validate_outgoing_subscribe(&entries)?;
        self.session.subscribe_sent(packet_id, entries);

        let subscribe = OutgoingPacket::Subscribe(Subscribe {
            packet_id,
            subscriptions: vec![subscription],
            properties: Properties::new(),
        });
        if let Err(e) = self.send_packet(&subscribe).await {
            self.session.abort_subscribe(packet_id);
            return Err(e);
        }

        let packet = match self.recv_while_connected(Duration::from_secs(10)).await {
            Ok(p) => p,
            Err(e) => {
                self.session.abort_subscribe(packet_id);
                return Err(e);
            }
        };
        match packet {
            IncomingPacket::SubAck(suback) if suback.packet_id == packet_id => {
                if suback.reason_codes.len() != 1
                    || !matches!(
                        suback.reason_codes[0],
                        SubAckReasonCode::GrantedQoS0
                            | SubAckReasonCode::GrantedQoS1
                            | SubAckReasonCode::GrantedQoS2
                    )
                {
                    self.session.abort_subscribe(packet_id);
                    return Err(Error::UnexpectedPacket);
                }
                let reason_codes = [suback.reason_codes[0].as_u8()];
                self.session.handle_suback(packet_id, &reason_codes)?;
                info!(topic, ?qos, "subscribed");
                Ok(())
            }
            _ => {
                self.session.abort_subscribe(packet_id);
                Err(Error::UnexpectedPacket)
            }
        }
    }

    /// 指定したトピックの購読を解除する。
    pub async fn unsubscribe(&mut self, topic: &str) -> Result<(), Error> {
        let packet_id = self
            .session
            .allocate_packet_id()
            .ok_or(Error::PacketIdExhausted)?;

        let topic_filters = vec![topic.to_string()];
        self.session
            .unsubscribe_sent(packet_id, topic_filters.clone());

        let unsubscribe = OutgoingPacket::Unsubscribe(Unsubscribe {
            packet_id,
            topic_filters,
            properties: Properties::new(),
        });
        if let Err(e) = self.send_packet(&unsubscribe).await {
            self.session.abort_unsubscribe(packet_id);
            return Err(e);
        }

        let packet = match self.recv_while_connected(Duration::from_secs(10)).await {
            Ok(p) => p,
            Err(e) => {
                self.session.abort_unsubscribe(packet_id);
                return Err(e);
            }
        };
        match packet {
            IncomingPacket::UnsubAck(unsuback) if unsuback.packet_id == packet_id => {
                if unsuback.reason_codes.len() != 1
                    || unsuback.reason_codes[0] != UnsubAckReasonCode::Success
                {
                    self.session.abort_unsubscribe(packet_id);
                    return Err(Error::UnexpectedPacket);
                }
                let reason_codes = [unsuback.reason_codes[0].as_u8()];
                self.session.handle_unsuback(packet_id, &reason_codes)?;
                info!(topic, "unsubscribed");
                Ok(())
            }
            _ => {
                self.session.abort_unsubscribe(packet_id);
                Err(Error::UnexpectedPacket)
            }
        }
    }

    /// トピックへメッセージを公開する。
    pub async fn publish(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retain: bool,
    ) -> Result<(), Error> {
        let packet_id = if qos == QoS::AtMostOnce {
            None
        } else {
            let id = self
                .session
                .allocate_packet_id()
                .ok_or(Error::PacketIdExhausted)?;
            if let Err(e) = self.session.publish_sent(qos, id) {
                self.session.release_packet_id(id);
                return Err(e.into());
            }
            Some(id)
        };

        let publish = OutgoingPacket::Publish(Publish {
            dup: false,
            qos,
            retain,
            topic: topic.to_string(),
            packet_id,
            properties: Properties::new(),
            payload: payload.to_vec(),
        });
        let encoded_len =
            publish.encoded_len() + 1 + encoded_remaining_length_size(publish.encoded_len());
        if let Err(e) = self
            .session
            .validate_outgoing_publish(qos, retain, encoded_len)
        {
            if let Some(id) = packet_id {
                self.session.abort_publish(id);
            }
            return Err(e.into());
        }

        if let Err(e) = self.send_packet(&publish).await {
            if let Some(id) = packet_id {
                self.session.abort_publish(id);
            }
            return Err(e);
        }
        info!(
            topic,
            ?qos,
            retain,
            payload_len = payload.len(),
            "published"
        );

        match qos {
            QoS::AtMostOnce => Ok(()),
            QoS::AtLeastOnce => {
                let pkt_id = packet_id.expect("QoS 1 requires packet identifier");
                self.await_puback(pkt_id).await
            }
            QoS::ExactlyOnce => {
                let pkt_id = packet_id.expect("QoS 2 requires packet identifier");
                self.await_qos2_completion(pkt_id).await
            }
        }
    }

    /// 指定した時間内に PUBLISH パケットを受信する。
    ///
    /// MQTT v5.0 §4.3.3 [MQTT-4.3.3-10]:
    /// PUBREL 受信前に再受信した同一 Packet Identifier の QoS 2 PUBLISH は
    /// 同一メッセージの再送であり、呼び出し元へ再配送してはならない。
    /// 重複の場合は PUBREC の再送と PUBREL / PUBCOMP の交換だけを行い、
    /// 次の PUBLISH を待ち続ける。
    pub async fn recv_publish(&mut self, dur: Duration) -> Result<Publish, Error> {
        let deadline = Instant::now() + dur;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Timeout);
            }
            let packet = self.recv_while_connected(remaining).await?;
            match packet {
                IncomingPacket::Publish(publish) => match publish.qos {
                    QoS::AtMostOnce => return Ok(publish),
                    QoS::AtLeastOnce => {
                        let packet_id = publish.packet_id.ok_or(Error::UnexpectedPacket)?;
                        self.session.handle_publish_qos1(packet_id)?;
                        let puback = OutgoingPacket::PubAck(PubAck {
                            packet_id,
                            reason_code: PubAckReasonCode::Success,
                            properties: Properties::new(),
                        });
                        self.send_packet(&puback).await?;
                        self.session.puback_sent(packet_id);
                        return Ok(publish);
                    }
                    QoS::ExactlyOnce => {
                        let packet_id = publish.packet_id.ok_or(Error::UnexpectedPacket)?;
                        let action = self.session.handle_publish_qos2(packet_id)?;
                        let is_duplicate = matches!(
                            action,
                            Action::SendPubrec {
                                is_duplicate: true,
                                ..
                            }
                        );
                        let pubrec = OutgoingPacket::PubRec(PubRec {
                            packet_id,
                            reason_code: PubRecReasonCode::Success,
                            properties: Properties::new(),
                        });
                        self.send_packet(&pubrec).await?;
                        self.session.pubrec_sent(PubRecReasonCode::Success.as_u8());

                        let remaining = deadline.saturating_duration_since(Instant::now());
                        if remaining.is_zero() {
                            return Err(Error::Timeout);
                        }
                        let packet = self.recv_while_connected(remaining).await?;
                        match packet {
                            IncomingPacket::PubRel(pubrel) if pubrel.packet_id == packet_id => {
                                self.session
                                    .handle_pubrel(packet_id)?
                                    .ok_or(Error::UnexpectedPacket)?;
                                let pubcomp = OutgoingPacket::PubComp(PubComp {
                                    packet_id,
                                    reason_code: PubCompReasonCode::Success,
                                    properties: Properties::new(),
                                });
                                self.send_packet(&pubcomp).await?;
                                self.session.pubcomp_sent();
                                // 重複メッセージは呼び出し元へ配送せず、次の PUBLISH を待つ。
                                if is_duplicate {
                                    continue;
                                }
                                return Ok(publish);
                            }
                            _ => return Err(Error::UnexpectedPacket),
                        }
                    }
                },
                IncomingPacket::PingResp(_) => {
                    debug!("PINGRESP received while waiting for PUBLISH");
                    self.session.pingresp_received();
                }
                other => {
                    // 待ち受け中に他パケットが来た場合は既存ハンドラへ委譲する。
                    // 接続終了ならエラー、それ以外は次の PUBLISH を待つ。
                    if self.handle_incoming(other).await? {
                        return Err(Error::Disconnected);
                    }
                }
            }
        }
    }

    /// 購読メッセージを受信し続ける。Ctrl+C で終了する。
    pub async fn run_subscribe_loop(&mut self) -> Result<(), Error> {
        info!("waiting for PUBLISH (Ctrl+C to quit)");
        let mut read_buf = [0u8; 4096];
        loop {
            self.check_keep_alive().await?;

            let tick = self.keep_alive_tick();
            tokio::select! {
                ctrl_c = tokio::signal::ctrl_c() => {
                    ctrl_c?;
                    info!("Ctrl+C received");
                    break;
                }
                result = self.stream.read(&mut read_buf) => {
                    let n = result?;
                    if n == 0 {
                        return Err(Error::Disconnected);
                    }
                    self.decoder.feed(&read_buf[..n])?;
                    while let Some(versioned) = self.decoder.decode()? {
                        let VersionedIncomingPacket::V5(packet) = versioned else {
                            return Err(Error::UnexpectedPacket);
                        };
                        if self.handle_incoming(packet).await? {
                            return Ok(());
                        }
                    }
                }
                _ = time::sleep(tick) => {
                    // Keep Alive 判定はループ先頭で行う。
                }
            }
        }
        Ok(())
    }

    async fn await_puback(&mut self, packet_id: u16) -> Result<(), Error> {
        let packet = match self.recv_while_connected(Duration::from_secs(10)).await {
            Ok(p) => p,
            Err(e) => {
                self.session.abort_publish(packet_id);
                return Err(e);
            }
        };
        match packet {
            IncomingPacket::PubAck(puback) if puback.packet_id == packet_id => {
                if self.session.handle_puback(packet_id)?.is_none() {
                    self.session.abort_publish(packet_id);
                    return Err(Error::UnexpectedPacket);
                }
                Ok(())
            }
            other => {
                // PUBACK 待ち中に別パケットが来た場合は先に処理してから再待機する。
                if self.handle_incoming(other).await? {
                    self.session.abort_publish(packet_id);
                    return Err(Error::Disconnected);
                }
                Box::pin(self.await_puback(packet_id)).await
            }
        }
    }

    async fn await_qos2_completion(&mut self, packet_id: u16) -> Result<(), Error> {
        let packet = match self.recv_while_connected(Duration::from_secs(10)).await {
            Ok(p) => p,
            Err(e) => {
                self.session.abort_publish(packet_id);
                return Err(e);
            }
        };
        match packet {
            IncomingPacket::PubRec(pubrec) if pubrec.packet_id == packet_id => {
                let action = self
                    .session
                    .handle_pubrec(packet_id, pubrec.reason_code.as_u8())?;
                if !matches!(action, Some(Action::SendPubrel { .. })) {
                    if action.is_none() {
                        self.session.abort_publish(packet_id);
                    }
                    return Err(Error::PublishRejected(pubrec.reason_code));
                }
                let pubrel = OutgoingPacket::PubRel(PubRel {
                    packet_id,
                    reason_code: PubRelReasonCode::Success,
                    properties: Properties::new(),
                });
                if let Err(e) = self.send_packet(&pubrel).await {
                    self.session.abort_publish(packet_id);
                    return Err(e);
                }
                self.await_pubcomp(packet_id).await
            }
            other => {
                if self.handle_incoming(other).await? {
                    self.session.abort_publish(packet_id);
                    return Err(Error::Disconnected);
                }
                Box::pin(self.await_qos2_completion(packet_id)).await
            }
        }
    }

    async fn await_pubcomp(&mut self, packet_id: u16) -> Result<(), Error> {
        let packet = match self.recv_while_connected(Duration::from_secs(10)).await {
            Ok(p) => p,
            Err(e) => {
                self.session.abort_publish(packet_id);
                return Err(e);
            }
        };
        match packet {
            IncomingPacket::PubComp(pubcomp) if pubcomp.packet_id == packet_id => {
                if self.session.handle_pubcomp(packet_id)?.is_none() {
                    self.session.abort_publish(packet_id);
                    return Err(Error::UnexpectedPacket);
                }
                Ok(())
            }
            other => {
                if self.handle_incoming(other).await? {
                    self.session.abort_publish(packet_id);
                    return Err(Error::Disconnected);
                }
                Box::pin(self.await_pubcomp(packet_id)).await
            }
        }
    }

    /// 受信パケットを処理する。接続終了なら `true` を返す。
    async fn handle_incoming(&mut self, packet: IncomingPacket) -> Result<bool, Error> {
        match packet {
            IncomingPacket::Publish(publish) => {
                self.handle_incoming_publish(publish).await?;
                Ok(false)
            }
            IncomingPacket::PingResp(_) => {
                debug!("PINGRESP received");
                self.session.pingresp_received();
                Ok(false)
            }
            IncomingPacket::PubRel(pubrel) => {
                match self.session.handle_pubrel(pubrel.packet_id)? {
                    Some(Action::SendPubcomp { packet_id }) => {
                        let pubcomp = OutgoingPacket::PubComp(PubComp {
                            packet_id,
                            reason_code: PubCompReasonCode::Success,
                            properties: Properties::new(),
                        });
                        self.send_packet(&pubcomp).await?;
                        // 正常完了時のみ受信クォータを解放する。
                        self.session.pubcomp_sent();
                    }
                    Some(Action::ResendPubcomp { packet_id }) => {
                        // 受信フローは既に完了済みのため pubcomp_sent は呼ばない。
                        let pubcomp = OutgoingPacket::PubComp(PubComp {
                            packet_id,
                            reason_code: PubCompReasonCode::Success,
                            properties: Properties::new(),
                        });
                        self.send_packet(&pubcomp).await?;
                    }
                    Some(other) => {
                        warn!(?other, "unexpected action for PUBREL");
                        return Err(Error::UnexpectedPacket);
                    }
                    None => {
                        warn!(
                            packet_id = pubrel.packet_id,
                            "PUBREL without active receive flow"
                        );
                    }
                }
                Ok(false)
            }
            IncomingPacket::Disconnect(disconnect) => {
                info!(reason = ?disconnect.reason_code, "DISCONNECT received from server");
                self.session.disconnect_received();
                Ok(true)
            }
            IncomingPacket::Auth(_) => {
                warn!("AUTH is not supported by this example");
                Err(Error::UnexpectedPacket)
            }
            // 送信フロー待ち以外で届いた ACK 類はプロトコル上想定外として扱う。
            IncomingPacket::ConnAck(_)
            | IncomingPacket::PubAck(_)
            | IncomingPacket::PubRec(_)
            | IncomingPacket::PubComp(_)
            | IncomingPacket::SubAck(_)
            | IncomingPacket::UnsubAck(_) => Err(Error::UnexpectedPacket),
        }
    }

    async fn handle_incoming_publish(&mut self, publish: Publish) -> Result<(), Error> {
        let topic = publish.topic.clone();
        let qos = publish.qos;
        let payload = publish.payload.clone();
        match qos {
            QoS::AtMostOnce => {}
            QoS::AtLeastOnce => {
                let packet_id = publish.packet_id.ok_or(Error::UnexpectedPacket)?;
                self.session.handle_publish_qos1(packet_id)?;
                let puback = OutgoingPacket::PubAck(PubAck {
                    packet_id,
                    reason_code: PubAckReasonCode::Success,
                    properties: Properties::new(),
                });
                self.send_packet(&puback).await?;
                self.session.puback_sent(packet_id);
            }
            QoS::ExactlyOnce => {
                let packet_id = publish.packet_id.ok_or(Error::UnexpectedPacket)?;
                let action = self.session.handle_publish_qos2(packet_id)?;
                let pubrec = OutgoingPacket::PubRec(PubRec {
                    packet_id,
                    reason_code: PubRecReasonCode::Success,
                    properties: Properties::new(),
                });
                self.send_packet(&pubrec).await?;
                self.session.pubrec_sent(PubRecReasonCode::Success.as_u8());
                // MQTT v5.0 §4.3.3 [MQTT-4.3.3-10]:
                // PUBREL 受信前に再受信した同一 Packet Identifier の PUBLISH は
                // 同一メッセージの再送であり、アプリケーションへ再配送してはならない。
                // PUBREC の再送だけを行い、ここで処理を終える。
                if let Action::SendPubrec {
                    is_duplicate: true, ..
                } = action
                {
                    debug!(packet_id, "duplicate qos 2 publish, delivery suppressed");
                    return Ok(());
                }
            }
        }
        // ペイロードはバイナリの可能性があるため、表示は損失を許容した UTF-8 変換にする。
        let text = String::from_utf8_lossy(&payload);
        info!(%topic, ?qos, payload = %text, "message received");
        Ok(())
    }

    async fn check_keep_alive(&mut self) -> Result<(), Error> {
        let now = self.clock.now();
        if self.session.keep_alive().has_timed_out(now) {
            return Err(Error::KeepAliveTimeout);
        }
        if self.session.keep_alive().should_send_pingreq(now) {
            debug!("sending PINGREQ");
            let pingreq = OutgoingPacket::PingReq(PingReq);
            self.send_packet(&pingreq).await?;
            // pingreq_sent は last_activity も更新するため activity は不要。
            self.session.pingreq_sent(now);
        }
        Ok(())
    }

    fn keep_alive_tick(&self) -> Duration {
        let secs = self.session.keep_alive().keep_alive_secs();
        if secs == 0 {
            Duration::from_secs(1)
        } else {
            Duration::from_millis(u64::from(secs) * 1000 / 4).max(Duration::from_millis(200))
        }
    }

    /// 接続確立後の受信。Keep Alive を維持しながら 1 パケット返す。
    async fn recv_while_connected(&mut self, overall: Duration) -> Result<IncomingPacket, Error> {
        let deadline = Instant::now() + overall;
        loop {
            self.check_keep_alive().await?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Timeout);
            }
            let wait = remaining.min(self.keep_alive_tick());
            match timeout(wait, self.try_recv_ready()).await {
                Ok(Ok(Some(packet))) => return Ok(packet),
                Ok(Ok(None)) => continue,
                Ok(Err(e)) => return Err(e),
                Err(_) => continue,
            }
        }
    }

    /// デコーダーに完成パケットがあれば返し、なければ短く読み込む。
    async fn try_recv_ready(&mut self) -> Result<Option<IncomingPacket>, Error> {
        if let Some(versioned) = self.decoder.decode()? {
            let VersionedIncomingPacket::V5(packet) = versioned else {
                return Err(Error::UnexpectedPacket);
            };
            return Ok(Some(packet));
        }
        let mut buf = [0u8; 4096];
        let n = self.stream.read(&mut buf).await?;
        if n == 0 {
            return Err(Error::Disconnected);
        }
        self.decoder.feed(&buf[..n])?;
        if let Some(versioned) = self.decoder.decode()? {
            let VersionedIncomingPacket::V5(packet) = versioned else {
                return Err(Error::UnexpectedPacket);
            };
            return Ok(Some(packet));
        }
        Ok(None)
    }

    async fn recv_packet(&mut self, dur: Duration) -> Result<IncomingPacket, Error> {
        let deadline = Instant::now() + dur;
        loop {
            if let Some(versioned) = self.decoder.decode()? {
                let VersionedIncomingPacket::V5(packet) = versioned else {
                    return Err(Error::UnexpectedPacket);
                };
                return Ok(packet);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Timeout);
            }
            let mut buf = [0u8; 4096];
            let n = timeout(remaining, self.stream.read(&mut buf))
                .await
                .map_err(|_| Error::Timeout)??;
            if n == 0 {
                return Err(Error::Disconnected);
            }
            self.decoder.feed(&buf[..n])?;
        }
    }

    async fn send_packet(&mut self, packet: &OutgoingPacket) -> Result<(), Error> {
        let buf = packet.encode_to_vec()?;
        self.stream.write_all(&buf).await?;
        // BidirectionalStream::flush は s2n-quic 固有の Result を返すため、
        // AsyncWriteExt::flush 経由ではなく io::Error へ明示変換する。
        self.stream.flush().await.map_err(std::io::Error::from)?;
        // PINGREQ 以外の Control Packet 送信完了を Keep Alive に反映する。
        if !matches!(packet, OutgoingPacket::PingReq(_)) {
            self.session.activity(self.clock.now());
        }
        Ok(())
    }
}

/// CONNACK から Session へ渡すパラメータを組み立てる。
fn connack_params_from(connack: &shiguredo_mqtt::v5::connack::ConnAck) -> ConnackParams {
    let mut session_expiry_interval = None;
    let mut receive_maximum = None;
    let mut topic_alias_maximum = None;
    let mut server_keep_alive = None;
    let mut maximum_packet_size = None;
    let mut maximum_qos = None;
    let mut retain_available = None;
    let mut wildcard_subscription_available = None;
    let mut subscription_identifiers_available = None;
    let mut shared_subscription_available = None;
    let mut assigned_client_identifier = None;
    let mut authentication_method = None;

    for prop in connack.properties.iter() {
        match prop {
            Property::SessionExpiryInterval(v) => session_expiry_interval = Some(*v),
            Property::ReceiveMaximum(v) => receive_maximum = Some(*v),
            Property::TopicAliasMaximum(v) => topic_alias_maximum = Some(*v),
            Property::ServerKeepAlive(v) => server_keep_alive = Some(*v),
            Property::MaximumPacketSize(v) => maximum_packet_size = Some(*v),
            Property::MaximumQoS(v) => maximum_qos = QoS::from_u8(*v),
            Property::RetainAvailable(v) => retain_available = Some(*v == 1),
            Property::WildcardSubscriptionAvailable(v) => {
                wildcard_subscription_available = Some(*v == 1)
            }
            Property::SubscriptionIdentifierAvailable(v) => {
                subscription_identifiers_available = Some(*v == 1)
            }
            Property::SharedSubscriptionAvailable(v) => {
                shared_subscription_available = Some(*v == 1)
            }
            Property::AssignedClientIdentifier(v) => assigned_client_identifier = Some(v.clone()),
            Property::AuthenticationMethod(v) => authentication_method = Some(v.clone()),
            _ => {}
        }
    }

    ConnackParams {
        session_present: connack.session_present,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval,
        receive_maximum,
        topic_alias_maximum,
        server_keep_alive,
        maximum_packet_size,
        maximum_qos,
        retain_available,
        wildcard_subscription_available,
        subscription_identifiers_available,
        shared_subscription_available,
        assigned_client_identifier,
        authentication_method,
    }
}

/// Remaining Length の Variable Byte Integer が占めるバイト数を返す。
fn encoded_remaining_length_size(remaining_length: usize) -> usize {
    match remaining_length {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        _ => 4,
    }
}
