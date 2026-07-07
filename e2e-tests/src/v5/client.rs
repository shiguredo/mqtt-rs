//! MQTT v5.0 用の簡易 E2E クライアント。
//!
//! Sans-I/O 状態機械である `Session` を活用し、
//! パケット識別子の割り当てや QoS フロー状態の管理を行う。

use std::time::Duration;

use shiguredo_mqtt::codec::MqttVersion;
use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::decoder::{Decoder, VersionedIncomingPacket};
use shiguredo_mqtt::error::{DecodeError, EncodeError};
use shiguredo_mqtt::state::auth::AuthAction;
use shiguredo_mqtt::state::qos_flow::{Action, FlowError};
use shiguredo_mqtt::state::session::{ConnackError, ConnackParams, ConnackReason, Session};
use shiguredo_mqtt::v5::auth::{Auth, AuthReasonCode};
use shiguredo_mqtt::v5::connack::{ConnAck, ConnectReasonCode};
use shiguredo_mqtt::v5::connect::Connect;
use shiguredo_mqtt::v5::disconnect::Disconnect;
use shiguredo_mqtt::v5::disconnect::DisconnectReasonCode;
use shiguredo_mqtt::v5::packet::{IncomingPacket, OutgoingPacket};
use shiguredo_mqtt::v5::property::Properties;
use shiguredo_mqtt::v5::property::Property;
use shiguredo_mqtt::v5::puback::PubAck;
use shiguredo_mqtt::v5::puback::PubAckReasonCode;
use shiguredo_mqtt::v5::pubcomp::PubComp;
use shiguredo_mqtt::v5::pubcomp::PubCompReasonCode;
use shiguredo_mqtt::v5::publish::Publish;
use shiguredo_mqtt::v5::pubrec::PubRec;
use shiguredo_mqtt::v5::pubrec::PubRecReasonCode;
use shiguredo_mqtt::v5::pubrel::PubRel;
use shiguredo_mqtt::v5::pubrel::PubRelReasonCode;
use shiguredo_mqtt::v5::suback::SubAckReasonCode;
use shiguredo_mqtt::v5::subscribe::RetainHandling;
use shiguredo_mqtt::v5::subscribe::Subscribe;
use shiguredo_mqtt::v5::subscribe::Subscription;
use shiguredo_mqtt::v5::unsuback::UnsubAckReasonCode;
use shiguredo_mqtt::v5::unsubscribe::Unsubscribe;
use tokio::net::TcpStream;

use crate::scram::{SCRAM_SHA_256_METHOD, ScramSha256Client};

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
    ConnectRefused(ConnectReasonCode),
    /// セッションの作成に失敗した。
    Session(String),
    /// 予期しないパケット種別を受信した。
    UnexpectedPacket,
    /// 予期しないパケット識別子を受信した。
    UnexpectedPacketIdentifier,
    /// QoS 2 の PUBLISH がエラー Reason Code の PUBREC で拒否された。
    PublishRejected(PubRecReasonCode),
    /// Session の apply_connack が CONNACK を拒否した。
    ///
    /// Session Present プロトコル違反、Receive Maximum の 0、Assigned Client Identifier の
    /// 欠落、Authentication Method の欠落・不一致・想定外の混入など、apply_connack が
    /// 返す全てのエラーをそのままラップして伝える。
    ConnackFailure(ConnackError),
    /// タイムアウトした。
    Timeout,
    /// 接続が切断された。
    Disconnected,
    /// 自身の Receive Maximum を超える PUBLISH を受信した、または、
    /// サーバー通知の Receive Maximum に対する送信クォータを超える
    /// PUBLISH (QoS > 0) 送信が試みられた。
    ReceiveMaximumExceeded,
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

impl From<FlowError> for ClientError {
    fn from(_: FlowError) -> Self {
        Self::UnexpectedPacket
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

/// MQTT v5.0 クライアント。
///
/// Sans-I/O 状態機械 `Session` を内部で使用し、
/// パケット識別子管理や QoS フロー制御を委譲する。
pub struct MqttClient {
    stream: TcpStream,
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
        let session = Session::new_v5(String::new(), true)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;
        Ok(Self {
            stream,
            session,
            decoder: Decoder::new(MqttVersion::V5),
        })
    }

    /// Session への参照を返す。
    pub fn session(&self) -> &Session {
        &self.session
    }

    // ==================================================================
    // 接続管理
    // ==================================================================

    /// MQTT v5.0 接続を確立する。
    pub async fn connect_v5(&mut self, client_id: &str) -> Result<(), ClientError> {
        self.connect_v5_with_options(client_id, true, None, Properties::new())
            .await
    }

    /// MQTT v5.0 接続を確立する。クリーンスタートの有無を指定できる。
    pub async fn connect_v5_with_clean_start(
        &mut self,
        client_id: &str,
        clean_start: bool,
    ) -> Result<(), ClientError> {
        self.connect_v5_with_options(client_id, clean_start, None, Properties::new())
            .await
    }

    /// MQTT v5.0 接続を確立する。セッション有効期限を指定し、clean_start=false で接続する。
    pub async fn connect_v5_with_session_expiry(
        &mut self,
        client_id: &str,
        session_expiry_interval: u32,
    ) -> Result<(), ClientError> {
        let mut properties = Properties::new();
        properties.push(Property::SessionExpiryInterval(session_expiry_interval));
        self.connect_v5_with_options(client_id, false, None, properties)
            .await
    }

    /// MQTT v5.0 接続を確立する。Will メッセージを指定できる。
    pub async fn connect_v5_with_will(
        &mut self,
        client_id: &str,
        will_topic: &str,
        will_payload: &[u8],
        will_qos: QoS,
    ) -> Result<(), ClientError> {
        self.connect_v5_with_will_and_properties(
            client_id,
            will_topic,
            will_payload,
            will_qos,
            Properties::new(),
        )
        .await
    }

    /// MQTT v5.0 接続を確立する。Will メッセージと Will プロパティを指定できる。
    pub async fn connect_v5_with_will_and_properties(
        &mut self,
        client_id: &str,
        will_topic: &str,
        will_payload: &[u8],
        will_qos: QoS,
        will_properties: Properties,
    ) -> Result<(), ClientError> {
        let will = shiguredo_mqtt::v5::connect::Will {
            topic: will_topic.to_string(),
            payload: will_payload.to_vec(),
            qos: will_qos,
            retain: false,
            properties: will_properties,
        };
        self.connect_v5_with_options(client_id, true, Some(will), Properties::new())
            .await
    }

    /// MQTT v5.0 接続を確立する。CONNECT プロパティを指定できる。
    pub async fn connect_v5_with_properties(
        &mut self,
        client_id: &str,
        clean_start: bool,
        properties: Properties,
    ) -> Result<(), ClientError> {
        self.connect_v5_with_options(client_id, clean_start, None, properties)
            .await
    }

    /// MQTT v5.0 Enhanced Authentication (SCRAM-SHA-256) で接続する。
    ///
    /// CONNECT の username / password フィールドは使わず、SCRAM ユーザー名は
    /// client-first の `n=` に載せる (MQTT v5.0 §4.12)。
    ///
    /// 流れ:
    /// 1. CONNECT (Method + client-first) → `connect_sent_with_auth`
    /// 2. AUTH 0x18 (server-first) → 受信 Method で `auth_received` → client-final 送信
    /// 3. 成功 CONNACK → ServerSignature 検証 → `apply_connack`
    /// 4. 失敗 CONNACK → `ConnectRefused` (AUTH 往復のあと)
    ///
    /// `AuthAction::Failed` / `MethodMismatch` は [`ClientError::UnexpectedPacket`] に落とす。
    pub async fn connect_v5_with_scram(
        &mut self,
        client_id: &str,
        username_for_scram: &str,
        password: &str,
    ) -> Result<(), ClientError> {
        let (mut scram, client_first) =
            ScramSha256Client::client_first(username_for_scram, password)
                .map_err(|_| ClientError::UnexpectedPacket)?;

        self.session = Session::new_v5(client_id.to_string(), true)
            .map_err(|e| ClientError::Session(e.to_string()))?;
        self.session
            .configure_for_connect(60, 0, 0, 65535, 0)
            .map_err(|e| ClientError::Session(e.to_string()))?;

        let mut properties = Properties::new();
        properties.push(Property::AuthenticationMethod(
            SCRAM_SHA_256_METHOD.to_string(),
        ));
        properties.push(Property::AuthenticationData(client_first));

        let connect = OutgoingPacket::Connect(Connect {
            client_id: client_id.to_string(),
            clean_start: true,
            keep_alive: 60,
            properties,
            will: None,
            username: None,
            password: None,
        });
        self.session
            .connect_sent_with_auth(SCRAM_SHA_256_METHOD.to_string())
            .expect("v5 セッションでは常に成功する");
        self.send_packet(&connect).await?;

        // server-first (AUTH 0x18) を待つ。誤パスワードでもここまでは成功する。
        let packet = self.recv_packet(Duration::from_secs(5)).await?;
        let IncomingPacket::Auth(server_first_auth) = packet else {
            return Err(ClientError::UnexpectedPacket);
        };
        if server_first_auth.reason_code != AuthReasonCode::ContinueAuthentication {
            return Err(ClientError::UnexpectedPacket);
        }

        // 受信 Method をそのまま渡す (定数禁止。MQTT v5.0 §4.12 [MQTT-4.12.0-5])。
        let received_method = server_first_auth.authentication_method();
        match self
            .session
            .auth_received(server_first_auth.reason_code.as_u8(), received_method)
        {
            AuthAction::Continue => {}
            AuthAction::Failed | AuthAction::MethodMismatch | AuthAction::Authenticated => {
                return Err(ClientError::UnexpectedPacket);
            }
        }

        let server_first_data = authentication_data_from_properties(&server_first_auth.properties)
            .ok_or(ClientError::UnexpectedPacket)?;
        let client_final = scram
            .client_final(server_first_data)
            .map_err(|_| ClientError::UnexpectedPacket)?;

        let mut auth_properties = Properties::new();
        auth_properties.push(Property::AuthenticationMethod(
            SCRAM_SHA_256_METHOD.to_string(),
        ));
        auth_properties.push(Property::AuthenticationData(client_final));
        let client_final_auth = OutgoingPacket::Auth(Auth {
            reason_code: AuthReasonCode::ContinueAuthentication,
            properties: auth_properties,
        });
        self.send_packet(&client_final_auth).await?;

        // 成功時は CONNACK Success、失敗時 (誤パスワード) は NotAuthorized 等。
        let packet = self.recv_packet(Duration::from_secs(5)).await?;
        let IncomingPacket::ConnAck(connack) = packet else {
            return Err(ClientError::UnexpectedPacket);
        };
        if connack.reason_code != ConnectReasonCode::Success {
            return Err(ClientError::ConnectRefused(connack.reason_code));
        }

        // 成功 CONNACK の Authentication Data (server-final) で ServerSignature を検証する。
        // Data 欠落・形式不正・署名不一致はいずれも失敗とする。
        let server_final = authentication_data_from_properties(&connack.properties)
            .ok_or(ClientError::UnexpectedPacket)?;
        scram
            .verify_server_final(server_final)
            .map_err(|_| ClientError::UnexpectedPacket)?;

        self.apply_success_connack(&connack)
    }

    async fn connect_v5_with_options(
        &mut self,
        client_id: &str,
        clean_start: bool,
        will: Option<shiguredo_mqtt::v5::connect::Will>,
        properties: Properties,
    ) -> Result<(), ClientError> {
        // Session を新規作成し、CONNECT 用に設定する。
        self.session = Session::new_v5(client_id.to_string(), clean_start)
            .map_err(|e| ClientError::Session(e.to_string()))?;

        let mut receive_maximum: u16 = 65535;
        let mut maximum_packet_size: u32 = 0;
        let mut connect_authentication_method: Option<String> = None;
        for prop in properties.iter() {
            match prop {
                Property::ReceiveMaximum(v) => receive_maximum = *v,
                Property::MaximumPacketSize(v) => maximum_packet_size = *v,
                Property::AuthenticationMethod(v) => {
                    connect_authentication_method = Some(v.clone())
                }
                _ => {}
            }
        }
        self.session
            .configure_for_connect(60, 0, 0, receive_maximum, maximum_packet_size)
            .map_err(|e| ClientError::Session(e.to_string()))?;

        let connect = OutgoingPacket::Connect(Connect {
            client_id: client_id.to_string(),
            clean_start,
            keep_alive: 60,
            properties,
            will,
            username: None,
            password: None,
        });
        // CONNECT に Authentication Method プロパティを含めた場合は Session 側にも
        // 初回認証の開始を伝えて InitialAuthenticating 状態に遷移させる。
        // これを怠ると、サーバーが MQTT v5.0 §4.12 [MQTT-4.12.0-5] に従い成功 CONNACK に
        // 同じ Method を含めて返した際に、Session::apply_connack が
        // ConnackError::UnexpectedAuthenticationMethod で拒否してしまう。
        if let Some(method) = connect_authentication_method {
            self.session
                .connect_sent_with_auth(method)
                .expect("v5 セッションでは常に成功する");
        } else {
            self.session.connect_sent();
        }
        self.send_packet(&connect).await?;

        let packet = self.recv_packet(Duration::from_secs(5)).await?;
        match packet {
            IncomingPacket::ConnAck(connack) => {
                if connack.reason_code != ConnectReasonCode::Success {
                    return Err(ClientError::ConnectRefused(connack.reason_code));
                }
                self.apply_success_connack(&connack)
            }
            _ => Err(ClientError::UnexpectedPacket),
        }
    }

    /// 成功 CONNACK のパラメータを Session に適用し、デコーダー制限を更新する。
    fn apply_success_connack(&mut self, connack: &ConnAck) -> Result<(), ClientError> {
        let session_present = connack.session_present;
        let mut session_expiry: Option<u32> = None;
        let mut receive_max: Option<u16> = None;
        let mut topic_alias_max: Option<u16> = None;
        let mut server_keep_alive: Option<u16> = None;
        let mut maximum_packet_size: Option<u32> = None;
        let mut maximum_qos: Option<QoS> = None;
        let mut retain_available: Option<bool> = None;
        let mut wildcard_subscription_available: Option<bool> = None;
        let mut subscription_identifiers_available: Option<bool> = None;
        let mut shared_subscription_available: Option<bool> = None;
        let mut assigned_client_identifier: Option<String> = None;
        let mut authentication_method: Option<String> = None;

        for prop in connack.properties.iter() {
            match prop {
                Property::SessionExpiryInterval(v) => session_expiry = Some(*v),
                Property::ReceiveMaximum(v) => receive_max = Some(*v),
                Property::TopicAliasMaximum(v) => topic_alias_max = Some(*v),
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
                Property::AssignedClientIdentifier(v) => {
                    assigned_client_identifier = Some(v.clone())
                }
                Property::AuthenticationMethod(v) => authentication_method = Some(v.clone()),
                _ => {}
            }
        }
        self.session
            .apply_connack(ConnackParams {
                session_present,
                reason_code: ConnackReason::V5(ConnectReasonCode::Success),
                session_expiry_interval: session_expiry,
                receive_maximum: receive_max,
                topic_alias_maximum: topic_alias_max,
                server_keep_alive,
                maximum_packet_size,
                maximum_qos,
                retain_available,
                wildcard_subscription_available,
                subscription_identifiers_available,
                shared_subscription_available,
                assigned_client_identifier,
                authentication_method,
            })
            .map_err(ClientError::ConnackFailure)?;
        // デコーダーを作り直すと内部バッファに受信済みの後続パケット
        // （永続セッション再開時の offline メッセージ等）が失われるため、
        // 制限値のみを差し替える。
        self.decoder.set_limits(*self.session.limits());
        Ok(())
    }

    /// MQTT 接続を切断する。
    pub async fn disconnect(&mut self) -> Result<(), ClientError> {
        let disconnect = OutgoingPacket::Disconnect(Disconnect {
            reason_code: DisconnectReasonCode::NormalDisconnection,
            properties: Properties::new(),
        });
        self.session.disconnect_sent();
        self.send_packet(&disconnect).await?;
        self.session.disconnected();
        Ok(())
    }

    /// TCP 接続を強制的に切断する。Will メッセージの配信を検証する際に使用する。
    pub fn force_disconnect(self) {
        drop(self.stream);
    }

    // ==================================================================
    // サブスクリプション管理
    // ==================================================================

    /// 指定したトピックを購読する。
    pub async fn subscribe(&mut self, topic: &str, qos: QoS) -> Result<(), ClientError> {
        self.subscribe_many(&[(topic, qos)]).await
    }

    /// 指定したトピックをサブスクリプションオプション付きで購読する。
    pub async fn subscribe_with_options(
        &mut self,
        topic: &str,
        qos: QoS,
        no_local: bool,
        retain_as_published: bool,
        retain_handling: RetainHandling,
    ) -> Result<(), ClientError> {
        self.subscribe_many_with_options(&[(
            topic,
            qos,
            no_local,
            retain_as_published,
            retain_handling,
        )])
        .await
    }

    /// 複数のトピックをまとめて購読する。
    pub async fn subscribe_many(&mut self, topics: &[(&str, QoS)]) -> Result<(), ClientError> {
        let subscriptions: Vec<Subscription> = topics
            .iter()
            .map(|(topic, qos)| Subscription {
                topic_filter: topic.to_string(),
                qos: *qos,
                no_local: false,
                retain_as_published: false,
                retain_handling: RetainHandling::SendRetained,
            })
            .collect();
        self.subscribe_many_with_options_inner(&subscriptions).await
    }

    /// 複数のトピックをサブスクリプションオプション付きでまとめて購読する。
    pub async fn subscribe_many_with_options(
        &mut self,
        topics: &[(&str, QoS, bool, bool, RetainHandling)],
    ) -> Result<(), ClientError> {
        let subscriptions: Vec<Subscription> = topics
            .iter()
            .map(
                |(topic, qos, no_local, retain_as_published, retain_handling)| Subscription {
                    topic_filter: topic.to_string(),
                    qos: *qos,
                    no_local: *no_local,
                    retain_as_published: *retain_as_published,
                    retain_handling: *retain_handling,
                },
            )
            .collect();
        self.subscribe_many_with_options_inner(&subscriptions).await
    }

    async fn subscribe_many_with_options_inner(
        &mut self,
        subscriptions: &[Subscription],
    ) -> Result<(), ClientError> {
        let packet_id =
            self.session
                .allocate_packet_id()
                .ok_or(ClientError::Io(std::io::Error::other(
                    "packet identifier exhausted",
                )))?;

        // wire 送信の前に Session に pending 登録する。
        let entries = subscriptions.iter().cloned().map(Into::into).collect();
        self.session.subscribe_sent(packet_id, entries);

        let subscribe = OutgoingPacket::Subscribe(Subscribe {
            packet_id,
            subscriptions: subscriptions.to_vec(),
            properties: Properties::new(),
        });
        if let Err(e) = self.send_packet(&subscribe).await {
            self.session.abort_subscribe(packet_id);
            return Err(e);
        }

        let packet = match self.recv_packet(Duration::from_secs(5)).await {
            Ok(p) => p,
            Err(e) => {
                self.session.abort_subscribe(packet_id);
                return Err(e);
            }
        };
        match packet {
            IncomingPacket::SubAck(suback) => {
                if suback.packet_id != packet_id {
                    self.session.abort_subscribe(packet_id);
                    return Err(ClientError::UnexpectedPacketIdentifier);
                }
                if suback.reason_codes.len() != subscriptions.len()
                    || !suback.reason_codes.iter().all(|code| {
                        matches!(
                            code,
                            SubAckReasonCode::GrantedQoS0
                                | SubAckReasonCode::GrantedQoS1
                                | SubAckReasonCode::GrantedQoS2
                        )
                    })
                {
                    // SUBACK に失敗理由コード (0x80 以上、仕様上正当) が含まれる、
                    // または個数不整合 (仕様違反) の場合。いずれも本 e2e クライアントは
                    // 購読を確定させずに破棄したいため、handle_suback ではなく
                    // abort_subscribe を使う (pending は明示破棄、packet_id は解放)。
                    self.session.abort_subscribe(packet_id);
                    return Err(ClientError::UnexpectedPacket);
                }
                let reason_codes: Vec<u8> = suback.reason_codes.iter().map(|c| c.as_u8()).collect();
                self.session
                    .handle_suback(packet_id, &reason_codes)
                    .map_err(|_| ClientError::UnexpectedPacket)?;
                Ok(())
            }
            _ => {
                self.session.abort_subscribe(packet_id);
                Err(ClientError::UnexpectedPacket)
            }
        }
    }

    /// 指定したトピックの購読を解除する。
    pub async fn unsubscribe(&mut self, topic: &str) -> Result<(), ClientError> {
        let packet_id =
            self.session
                .allocate_packet_id()
                .ok_or(ClientError::Io(std::io::Error::other(
                    "packet identifier exhausted",
                )))?;

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

        let packet = match self.recv_packet(Duration::from_secs(5)).await {
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
                    // UNSUBACK に Success (0x00) 以外の理由コード (0x11 /
                    // 0x80 以上、いずれも仕様上正当) が含まれる、または個数不整合
                    // (仕様違反) の場合。いずれも本 e2e クライアントは購読解除を
                    // 確定させずに破棄したいため、handle_unsuback ではなく
                    // abort_unsubscribe を使う (pending は明示破棄、packet_id は解放)。
                    self.session.abort_unsubscribe(packet_id);
                    return Err(ClientError::UnexpectedPacket);
                }
                let reason_codes: Vec<u8> =
                    unsuback.reason_codes.iter().map(|c| c.as_u8()).collect();
                self.session
                    .handle_unsuback(packet_id, &reason_codes)
                    .map_err(|_| ClientError::UnexpectedPacket)?;
                Ok(())
            }
            _ => {
                self.session.abort_unsubscribe(packet_id);
                Err(ClientError::UnexpectedPacket)
            }
        }
    }

    // ==================================================================
    // パブリッシュ
    // ==================================================================

    /// 指定したトピックに QoS 0 でメッセージを公開する。
    pub async fn publish(&mut self, topic: &str, payload: &[u8]) -> Result<(), ClientError> {
        self.publish_with_qos_and_retain(topic, payload, QoS::AtMostOnce, false, Properties::new())
            .await
    }

    /// 指定したトピックに指定した QoS でメッセージを公開する。
    pub async fn publish_with_qos(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
    ) -> Result<(), ClientError> {
        self.publish_with_qos_and_retain(topic, payload, qos, false, Properties::new())
            .await
    }

    /// 指定したトピックに retain フラグ付きでメッセージを公開する。
    pub async fn publish_with_retain(
        &mut self,
        topic: &str,
        payload: &[u8],
    ) -> Result<(), ClientError> {
        self.publish_with_qos_and_retain(topic, payload, QoS::AtMostOnce, true, Properties::new())
            .await
    }

    /// 指定したトピックにプロパティを付与してメッセージを公開する。
    pub async fn publish_with_properties(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retain: bool,
        properties: Properties,
    ) -> Result<(), ClientError> {
        self.publish_with_qos_and_retain(topic, payload, qos, retain, properties)
            .await
    }

    async fn publish_with_qos_and_retain(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retain: bool,
        properties: Properties,
    ) -> Result<(), ClientError> {
        let packet_id =
            if qos == QoS::AtMostOnce {
                None
            } else {
                let id = self.session.allocate_packet_id().ok_or(ClientError::Io(
                    std::io::Error::other("packet identifier exhausted"),
                ))?;
                // Session に送信クォータと QoS フロー状態をまとめて記録する。
                // publish_sent が Err のときは wire 未送信のため release_packet_id で
                // パケット識別子だけを解放する (abort_publish は呼ばない)。
                if self.session.publish_sent(qos, id).is_err() {
                    self.session.release_packet_id(id);
                    return Err(ClientError::ReceiveMaximumExceeded);
                }
                Some(id)
            };

        let publish = OutgoingPacket::Publish(Publish {
            dup: false,
            qos,
            retain,
            topic: topic.to_string(),
            packet_id,
            properties,
            payload: payload.to_vec(),
        });
        if let Err(e) = self.send_packet(&publish).await {
            if let Some(id) = packet_id {
                self.session.abort_publish(id);
            }
            return Err(e);
        }

        match qos {
            QoS::AtMostOnce => Ok(()),
            QoS::AtLeastOnce => {
                let pkt_id = packet_id.expect("QoS 1 にはパケット識別子が必要です");
                let packet = match self.recv_packet(Duration::from_secs(5)).await {
                    Ok(p) => p,
                    Err(e) => {
                        self.session.abort_publish(pkt_id);
                        return Err(e);
                    }
                };
                match packet {
                    IncomingPacket::PubAck(puback) if puback.packet_id == pkt_id => {
                        // handle_puback が Ok(None) を返すのはサーバー側の状態破損
                        // (未送信 PUBLISH に対する PUBACK など) 時のみだが、その場合も
                        // 新規に消費した送信クォータとパケット識別子を巻き戻すため
                        // 防御的に abort_publish を呼ぶ。
                        if self.session.handle_puback(pkt_id)?.is_none() {
                            self.session.abort_publish(pkt_id);
                            return Err(ClientError::UnexpectedPacket);
                        }
                        Ok(())
                    }
                    _ => {
                        self.session.abort_publish(pkt_id);
                        Err(ClientError::UnexpectedPacket)
                    }
                }
            }
            QoS::ExactlyOnce => {
                let pkt_id = packet_id.expect("QoS 2 にはパケット識別子が必要です");
                let packet = match self.recv_packet(Duration::from_secs(5)).await {
                    Ok(p) => p,
                    Err(e) => {
                        self.session.abort_publish(pkt_id);
                        return Err(e);
                    }
                };
                match packet {
                    IncomingPacket::PubRec(pubrec) if pubrec.packet_id == pkt_id => {
                        let action = self
                            .session
                            .handle_pubrec(pkt_id, pubrec.reason_code.as_u8())?;
                        // Reason Code 0x80 以上の PUBREC はフロー中断であり、
                        // PUBREL を送信してはならない。
                        // (MQTT v5.0 §4.3.3 [MQTT-4.3.3-4] / MQTT v5.0 §4.4 [MQTT-4.4.0-2])
                        if !matches!(action, Some(Action::SendPubrel { .. })) {
                            // Aborted の場合は handle_pubrec 内で release 済み。
                            // None の場合のみ abort_publish で明示解放する。
                            if action.is_none() {
                                self.session.abort_publish(pkt_id);
                            }
                            return Err(ClientError::PublishRejected(pubrec.reason_code));
                        }
                        let pubrel = OutgoingPacket::PubRel(PubRel {
                            packet_id: pkt_id,
                            reason_code: PubRelReasonCode::Success,
                            properties: Properties::new(),
                        });
                        if let Err(e) = self.send_packet(&pubrel).await {
                            self.session.abort_publish(pkt_id);
                            return Err(e);
                        }
                        let packet = match self.recv_packet(Duration::from_secs(5)).await {
                            Ok(p) => p,
                            Err(e) => {
                                self.session.abort_publish(pkt_id);
                                return Err(e);
                            }
                        };
                        match packet {
                            IncomingPacket::PubComp(pubcomp) if pubcomp.packet_id == pkt_id => {
                                // handle_pubcomp が Ok(None) を返すのはサーバー側の
                                // 状態破損時のみだが、防御的に abort_publish を呼び
                                // PUBREL 送信までに消費した送信クォータと
                                // パケット識別子を巻き戻す。
                                if self.session.handle_pubcomp(pkt_id)?.is_none() {
                                    self.session.abort_publish(pkt_id);
                                    return Err(ClientError::UnexpectedPacket);
                                }
                                Ok(())
                            }
                            _ => {
                                self.session.abort_publish(pkt_id);
                                Err(ClientError::UnexpectedPacket)
                            }
                        }
                    }
                    _ => {
                        self.session.abort_publish(pkt_id);
                        Err(ClientError::UnexpectedPacket)
                    }
                }
            }
        }
    }

    // ==================================================================
    // 受信
    // ==================================================================

    /// 指定した時間内に PUBLISH パケットを受信する。
    ///
    /// MQTT v5.0 §4.3.3 [MQTT-4.3.3-10]:
    /// PUBREL 受信前に再受信した同一 Packet Identifier の QoS 2 PUBLISH は
    /// 同一メッセージの再送であり、呼び出し元へ再配送してはならない。
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
                        .map_err(|_| ClientError::ReceiveMaximumExceeded)?;
                    self.send_puback(packet_id).await?;
                    return Ok(publish);
                }
                QoS::ExactlyOnce => {
                    // QoS > 0 の PUBLISH には Packet Identifier が必須 (デコーダーが保証する)。
                    let packet_id = publish.packet_id.ok_or(ClientError::UnexpectedPacket)?;
                    let action = self
                        .session
                        .handle_publish_qos2(packet_id)
                        .map_err(|_| ClientError::ReceiveMaximumExceeded)?;
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

                    let packet = self.recv_packet(dur).await?;
                    match packet {
                        IncomingPacket::PubRel(pubrel) if pubrel.packet_id == packet_id => {
                            self.session
                                .handle_pubrel(packet_id)?
                                .ok_or(ClientError::UnexpectedPacket)?;
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
                        _ => return Err(ClientError::UnexpectedPacket),
                    }
                }
            }
        }
    }

    /// 指定した時間内に QoS 1 の PUBLISH を受信するが、PUBACK は送らない。
    ///
    /// Receive Maximum の観測に使う。呼び出し側が `send_puback` で確認応答する。
    pub async fn recv_publish_without_ack(
        &mut self,
        dur: Duration,
    ) -> Result<Publish, ClientError> {
        let publish = match self.recv_packet(dur).await? {
            IncomingPacket::Publish(p) => p,
            _ => return Err(ClientError::UnexpectedPacket),
        };

        match publish.qos {
            QoS::AtLeastOnce => {
                // QoS > 0 の PUBLISH には Packet Identifier が必須 (デコーダーが保証する)。
                let packet_id = publish.packet_id.ok_or(ClientError::UnexpectedPacket)?;
                self.session
                    .handle_publish_qos1(packet_id)
                    .map_err(|_| ClientError::ReceiveMaximumExceeded)?;
                Ok(publish)
            }
            _ => Err(ClientError::UnexpectedPacket),
        }
    }

    /// 受信済みの QoS 1 PUBLISH に対して PUBACK を送信する。
    pub async fn send_puback(&mut self, packet_id: u16) -> Result<(), ClientError> {
        let puback = OutgoingPacket::PubAck(PubAck {
            packet_id,
            reason_code: PubAckReasonCode::Success,
            properties: Properties::new(),
        });
        self.send_packet(&puback).await?;
        self.session.puback_sent(packet_id);
        Ok(())
    }

    /// パケットをエンコードして送信する。
    async fn send_packet(&mut self, packet: &OutgoingPacket) -> Result<(), ClientError> {
        let buf = packet.encode_to_vec()?;
        crate::client::send_bytes(&mut self.stream, &buf).await?;
        Ok(())
    }

    /// 1 つの MQTT v5.0 パケットを受信する。
    async fn recv_packet(&mut self, dur: Duration) -> Result<IncomingPacket, ClientError> {
        match crate::client::recv_packet(&mut self.decoder, &mut self.stream, dur).await? {
            VersionedIncomingPacket::V5(packet) => Ok(packet),
            // v5 クライアントで v3.1.1 パケットがデコードされることはない。
            _ => Err(ClientError::UnexpectedPacket),
        }
    }
}

/// Properties から Authentication Data プロパティのバイト列を取り出す。
fn authentication_data_from_properties(properties: &Properties) -> Option<&[u8]> {
    properties.iter().find_map(|prop| match prop {
        Property::AuthenticationData(data) => Some(data.as_slice()),
        _ => None,
    })
}
