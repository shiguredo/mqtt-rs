use shiguredo_mqtt::codec::MqttVersion;
use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::state::auth::{AuthError, AuthState};
use shiguredo_mqtt::state::flow_control::FlowControlError;
use shiguredo_mqtt::state::qos_flow::{Action, FlowError};
use shiguredo_mqtt::state::session::{
    ConnackError, ConnackParams, ConnackReason, ConnectionState, ServerCapabilities,
    ServerCapabilityError, Session, SessionError,
};
use shiguredo_mqtt::state::subscribe::SubscriptionEntry;
use shiguredo_mqtt::v5::connack::ConnectReasonCode;
use shiguredo_mqtt::v311::connack::ConnectReturnCode;

/// `make_base` から v5 専用フィールドを 1 つだけ `Some` にした
/// `(フィールド名, ConnackParams)` のリストを返す。
fn v5_only_field_cases<F>(mut make_base: F) -> Vec<(&'static str, ConnackParams)>
where
    F: FnMut() -> ConnackParams,
{
    vec![
        ("session_expiry_interval", {
            let mut params = make_base();
            params.session_expiry_interval = Some(1);
            params
        }),
        ("receive_maximum", {
            let mut params = make_base();
            params.receive_maximum = Some(1);
            params
        }),
        ("topic_alias_maximum", {
            let mut params = make_base();
            params.topic_alias_maximum = Some(1);
            params
        }),
        ("server_keep_alive", {
            let mut params = make_base();
            params.server_keep_alive = Some(1);
            params
        }),
        ("maximum_packet_size", {
            let mut params = make_base();
            params.maximum_packet_size = Some(1);
            params
        }),
        ("maximum_qos", {
            let mut params = make_base();
            params.maximum_qos = Some(QoS::AtMostOnce);
            params
        }),
        ("retain_available", {
            let mut params = make_base();
            params.retain_available = Some(false);
            params
        }),
        ("wildcard_subscription_available", {
            let mut params = make_base();
            params.wildcard_subscription_available = Some(false);
            params
        }),
        ("subscription_identifiers_available", {
            let mut params = make_base();
            params.subscription_identifiers_available = Some(false);
            params
        }),
        ("shared_subscription_available", {
            let mut params = make_base();
            params.shared_subscription_available = Some(false);
            params
        }),
        ("assigned_client_identifier", {
            let mut params = make_base();
            params.assigned_client_identifier = Some("id".into());
            params
        }),
        ("authentication_method", {
            let mut params = make_base();
            params.authentication_method = Some("METHOD".into());
            params
        }),
    ]
}

#[test]
fn new_v5_session() {
    let session = Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    assert_eq!(session.protocol_version(), MqttVersion::V5);
    assert!(session.is_v5());
    assert_eq!(session.client_id(), "client-1");
    assert!(session.clean_start());
    assert_eq!(session.connection_state(), ConnectionState::Disconnected);
}

#[test]
fn new_v311_session() {
    let session =
        Session::new_v311("client-1".into(), true).expect("セッションの作成に成功すること");
    assert_eq!(session.protocol_version(), MqttVersion::V311);
    assert!(session.is_v311());
}

#[test]
fn empty_client_id_with_clean_session_false_is_rejected_for_v311() {
    // MQTT v3.1.1 §3.1.3.1 [MQTT-3.1.3-7]:
    // client_id が空の場合、Clean Session は true でなければならない。
    assert!(matches!(
        Session::new_v311("".into(), false),
        Err(SessionError::InvalidClientId)
    ));
}

#[test]
fn empty_client_id_with_clean_session_true_is_allowed_for_v311() {
    // client_id が空でも Clean Session が true なら v3.1.1 では許可される。
    let session = Session::new_v311("".into(), true).expect("セッションの作成に成功すること");
    assert_eq!(session.client_id(), "");
    assert!(session.clean_start());
}

#[test]
fn empty_client_id_is_allowed_for_v5() {
    // v5.0 では空 Client ID はサーバーが一意の ClientID を割り当てる。
    let session = Session::new_v5("".into(), false).expect("セッションの作成に成功すること");
    assert_eq!(session.client_id(), "");
}

// ==================================================================
// configure_for_connect / apply_connack
// ==================================================================
#[test]
fn configure_for_connect_rejects_zero_receive_maximum() {
    // MQTT v5.0 §3.1.2.11.3:
    // Receive Maximum に 0 を指定することは Protocol Error である。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    assert_eq!(
        session.configure_for_connect(60, 0, 0, 0, 0),
        Err(FlowControlError::InvalidReceiveMaximum)
    );
}

#[test]
fn apply_connack_none_params_keep_existing() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .configure_for_connect(60, 300, 4, 10, 0)
        .expect("CONNECT パラメータを設定できること");
    session.connect_sent();

    session
        .apply_connack(ConnackParams {
            session_present: false,
            reason_code: ConnackReason::V5(ConnectReasonCode::Success),
            session_expiry_interval: None,
            receive_maximum: None,
            topic_alias_maximum: None,
            server_keep_alive: None,
            maximum_packet_size: None,
            maximum_qos: None,
            retain_available: None,
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            assigned_client_identifier: None,
            authentication_method: None,
        })
        .expect("CONNACK を受理すること");

    assert_eq!(session.connection_state(), ConnectionState::Connected);
    // 未指定のパラメータは変更されない。
    assert_eq!(session.keep_alive().keep_alive_secs(), 60);
    assert_eq!(session.session_expiry_interval(), 300);
}

#[test]
fn apply_connack_params_survive_session_reset() {
    // clean_start=true（セッション状態がリセットされる経路）でも、
    // CONNACK で通知された Receive Maximum / Topic Alias Maximum は
    // リセット後に適用されるため消えない。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();

    session
        .apply_connack(ConnackParams {
            session_present: false,
            reason_code: ConnackReason::V5(ConnectReasonCode::Success),
            session_expiry_interval: None,
            receive_maximum: Some(10),
            topic_alias_maximum: Some(8),
            server_keep_alive: None,
            maximum_packet_size: None,
            maximum_qos: None,
            retain_available: None,
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            assigned_client_identifier: None,
            authentication_method: None,
        })
        .expect("CONNACK を受理すること");

    assert_eq!(session.connection_state(), ConnectionState::Connected);
    assert_eq!(session.flow_control().receive_maximum(), 10);
    assert_eq!(session.topic_alias_manager().peer_maximum(), 8);
}

#[test]
fn server_capabilities_defaults_and_apply() {
    // 未通知時は仕様の既定値（MQTT v5.0 §3.2.2.3）を保持する。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    let caps = session.server_capabilities();
    assert_eq!(caps.maximum_packet_size, None);
    assert_eq!(caps.maximum_qos, QoS::ExactlyOnce);
    assert!(caps.retain_available);
    assert!(caps.wildcard_subscription_available);
    assert!(caps.subscription_identifiers_available);
    assert!(caps.shared_subscription_available);

    session.connect_sent();
    session
        .apply_connack(ConnackParams {
            session_present: false,
            reason_code: ConnackReason::V5(ConnectReasonCode::Success),
            session_expiry_interval: None,
            receive_maximum: None,
            topic_alias_maximum: None,
            server_keep_alive: None,
            maximum_packet_size: Some(1024),
            maximum_qos: Some(QoS::AtLeastOnce),
            retain_available: Some(false),
            wildcard_subscription_available: Some(false),
            subscription_identifiers_available: Some(false),
            shared_subscription_available: Some(false),
            assigned_client_identifier: None,
            authentication_method: None,
        })
        .expect("CONNACK を受理すること");

    let caps = session.server_capabilities();
    assert_eq!(caps.maximum_packet_size, Some(1024));
    assert_eq!(caps.maximum_qos, QoS::AtLeastOnce);
    assert!(!caps.retain_available);
    assert!(!caps.wildcard_subscription_available);
    assert!(!caps.subscription_identifiers_available);
    assert!(!caps.shared_subscription_available);

    // サーバー能力値は接続スコープの状態であり、再接続開始時に既定値へ戻る。
    session.disconnected();
    session.connect_sent();
    let caps = session.server_capabilities();
    assert_eq!(caps.maximum_packet_size, None);
    assert_eq!(caps.maximum_qos, QoS::ExactlyOnce);
    assert!(caps.retain_available);
}

#[test]
fn validate_publish_rejects_qos_exceeding_maximum() {
    let mut caps = ServerCapabilities::new();
    caps.maximum_qos = QoS::AtMostOnce;

    assert!(caps.validate_publish(QoS::AtMostOnce, false, 100).is_ok());
    assert_eq!(
        caps.validate_publish(QoS::AtLeastOnce, false, 100),
        Err(ServerCapabilityError::MaximumQoSExceeded)
    );
}

#[test]
fn validate_publish_rejects_retain_when_unavailable() {
    let mut caps = ServerCapabilities::new();
    caps.retain_available = false;

    assert!(caps.validate_publish(QoS::AtMostOnce, false, 100).is_ok());
    assert_eq!(
        caps.validate_publish(QoS::AtMostOnce, true, 100),
        Err(ServerCapabilityError::RetainNotAvailable)
    );
}

#[test]
fn validate_publish_rejects_oversized_packet() {
    let mut caps = ServerCapabilities::new();
    caps.maximum_packet_size = Some(1024);

    assert!(caps.validate_publish(QoS::AtMostOnce, false, 1024).is_ok());
    assert_eq!(
        caps.validate_publish(QoS::AtMostOnce, false, 1025),
        Err(ServerCapabilityError::MaximumPacketSizeExceeded {
            size: 1025,
            limit: 1024,
        })
    );
}

#[test]
fn validate_subscribe_accepts_requested_qos_exceeding_maximum_qos() {
    // MQTT v5.0 §3.2.2.3.4 [MQTT-3.2.2-10]:
    // QoS 1 や QoS 2 の PUBLISH をサポートしないサーバーであっても、
    // Requested QoS 0 / 1 / 2 を含む SUBSCRIBE パケットを受理しなければ
    // ならない。したがってクライアント側で Requested QoS を
    // Maximum QoS で制限してはならない。
    let mut caps = ServerCapabilities::new();
    caps.maximum_qos = QoS::AtMostOnce;

    assert!(
        caps.validate_subscribe(&[
            SubscriptionEntry::new("a/b", QoS::AtMostOnce),
            SubscriptionEntry::new("c/d", QoS::AtLeastOnce),
            SubscriptionEntry::new("e/f", QoS::ExactlyOnce),
        ])
        .is_ok()
    );
}

#[test]
fn validate_publish_still_rejects_qos_exceeding_maximum_qos() {
    // MQTT v5.0 §3.2.2.3.4 [MQTT-3.2.2-11]:
    // Maximum QoS を超える QoS の PUBLISH は送信してはならない。
    // Requested QoS の検証を削除しても、送信 PUBLISH の検証は維持される。
    let mut caps = ServerCapabilities::new();
    caps.maximum_qos = QoS::AtMostOnce;

    assert!(caps.validate_publish(QoS::AtMostOnce, false, 10).is_ok());
    assert_eq!(
        caps.validate_publish(QoS::AtLeastOnce, false, 10),
        Err(ServerCapabilityError::MaximumQoSExceeded)
    );
}

#[test]
fn validate_subscribe_rejects_wildcard_when_unavailable() {
    let mut caps = ServerCapabilities::new();
    caps.wildcard_subscription_available = false;

    let plain = SubscriptionEntry::new("a/b", QoS::AtMostOnce);
    let wildcard = SubscriptionEntry::new("a/+/b", QoS::AtMostOnce);

    assert!(caps.validate_subscribe(&[plain]).is_ok());
    assert_eq!(
        caps.validate_subscribe(&[wildcard]),
        Err(ServerCapabilityError::WildcardSubscriptionNotAvailable)
    );
}

#[test]
fn validate_subscribe_rejects_subscription_identifier_when_unavailable() {
    let mut caps = ServerCapabilities::new();
    caps.subscription_identifiers_available = false;

    let mut with_id = SubscriptionEntry::new("a/b", QoS::AtMostOnce);
    with_id.subscription_identifier = Some(1);

    assert!(
        caps.validate_subscribe(&[SubscriptionEntry::new("a/b", QoS::AtMostOnce)])
            .is_ok()
    );
    assert_eq!(
        caps.validate_subscribe(&[with_id]),
        Err(ServerCapabilityError::SubscriptionIdentifierNotAvailable)
    );
}

#[test]
fn validate_subscribe_rejects_shared_subscription_when_unavailable() {
    let mut caps = ServerCapabilities::new();
    caps.shared_subscription_available = false;

    let plain = SubscriptionEntry::new("a/b", QoS::AtMostOnce);
    let shared = SubscriptionEntry::new("$share/group/a/b", QoS::AtMostOnce);

    assert!(caps.validate_subscribe(&[plain]).is_ok());
    assert_eq!(
        caps.validate_subscribe(&[shared]),
        Err(ServerCapabilityError::SharedSubscriptionNotAvailable)
    );
}

#[test]
fn session_validate_outgoing_publish_delegates_to_capabilities() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();
    session
        .apply_connack(ConnackParams {
            session_present: false,
            reason_code: ConnackReason::V5(ConnectReasonCode::Success),
            session_expiry_interval: None,
            receive_maximum: None,
            topic_alias_maximum: None,
            server_keep_alive: None,
            maximum_packet_size: Some(100),
            maximum_qos: Some(QoS::AtMostOnce),
            retain_available: Some(false),
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            assigned_client_identifier: None,
            authentication_method: None,
        })
        .expect("CONNACK を受理すること");

    assert!(
        session
            .validate_outgoing_publish(QoS::AtMostOnce, false, 100)
            .is_ok()
    );
    assert_eq!(
        session.validate_outgoing_publish(QoS::AtLeastOnce, false, 100),
        Err(ServerCapabilityError::MaximumQoSExceeded)
    );
    assert_eq!(
        session.validate_outgoing_publish(QoS::AtMostOnce, true, 100),
        Err(ServerCapabilityError::RetainNotAvailable)
    );
    assert_eq!(
        session.validate_outgoing_publish(QoS::AtMostOnce, false, 101),
        Err(ServerCapabilityError::MaximumPacketSizeExceeded {
            size: 101,
            limit: 100,
        })
    );
}

#[test]
fn session_validate_outgoing_subscribe_delegates_to_capabilities() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();
    session
        .apply_connack(ConnackParams {
            session_present: false,
            reason_code: ConnackReason::V5(ConnectReasonCode::Success),
            session_expiry_interval: None,
            receive_maximum: None,
            topic_alias_maximum: None,
            server_keep_alive: None,
            maximum_packet_size: None,
            maximum_qos: None,
            retain_available: None,
            wildcard_subscription_available: Some(false),
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            assigned_client_identifier: None,
            authentication_method: None,
        })
        .expect("CONNACK を受理すること");

    let plain = SubscriptionEntry::new("a/b", QoS::AtMostOnce);
    let wildcard = SubscriptionEntry::new("a/+/b", QoS::AtMostOnce);

    assert!(session.validate_outgoing_subscribe(&[plain]).is_ok());
    assert_eq!(
        session.validate_outgoing_subscribe(&[wildcard]),
        Err(ServerCapabilityError::WildcardSubscriptionNotAvailable)
    );
}

#[test]
fn session_present_false_resets_flow_control() {
    // session_present=false で再接続した場合、前回の Receive Maximum が引き継がれない。
    let mut session =
        Session::new_v5("client-1".into(), false).expect("セッションの作成に成功すること");
    session.connect_sent();
    // 初回接続で Receive Maximum を 10 に設定する。
    session
        .apply_connack(ConnackParams {
            session_present: false,
            reason_code: ConnackReason::V5(ConnectReasonCode::Success),
            session_expiry_interval: None,
            receive_maximum: Some(10),
            topic_alias_maximum: None,
            server_keep_alive: None,
            maximum_packet_size: None,
            maximum_qos: None,
            retain_available: None,
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            assigned_client_identifier: None,
            authentication_method: None,
        })
        .expect("CONNACK を受理すること");
    assert_eq!(session.flow_control().receive_maximum(), 10);

    session.disconnect_sent();
    session.disconnected();

    // 再接続を開始するとフロー制御は接続スコープの状態として再初期化され、
    // CONNACK で Receive Maximum が通知されなければ既定値 (65535) となる。
    session.connect_sent();
    session
        .apply_connack(ConnackParams {
            session_present: false,
            reason_code: ConnackReason::V5(ConnectReasonCode::Success),
            session_expiry_interval: None,
            receive_maximum: None,
            topic_alias_maximum: None,
            server_keep_alive: None,
            maximum_packet_size: None,
            maximum_qos: None,
            retain_available: None,
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            assigned_client_identifier: None,
            authentication_method: None,
        })
        .expect("CONNACK を受理すること");

    assert_eq!(session.flow_control().receive_maximum(), 65535);
}

#[test]
fn apply_connack_rejects_refused_reason_code_v5() {
    // Reason Code 0x80 以上の CONNACK は拒否応答としてエラーを返す。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();

    let result = session.apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::UnspecifiedError),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: None,
    });

    assert_eq!(result, Err(ConnackError::ConnectionRefused));
    // 検証エラー時は接続状態を遷移させない（connected() に未到達）。
    assert_eq!(
        session.connection_state(),
        ConnectionState::Connecting,
        "検証エラー時は接続状態を遷移させない"
    );
}

#[test]
fn apply_connack_rejects_refused_return_code_v311() {
    // v3.1.1 では Return Code が 0 以外の CONNACK は拒否応答としてエラーを返す。
    let mut session =
        Session::new_v311("client-1".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();

    let result = session.apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V311(ConnectReturnCode::IdentifierRejected),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: None,
    });

    assert_eq!(result, Err(ConnackError::ConnectionRefused));
    // 検証エラー時は接続状態を遷移させない（connected() に未到達）。
    assert_eq!(
        session.connection_state(),
        ConnectionState::Connecting,
        "検証エラー時は接続状態を遷移させない"
    );
}

#[test]
fn apply_connack_rejects_session_present_with_clean_start_true() {
    // MQTT v5.0 §3.2.2.1.1 [MQTT-3.2.2-2] / [MQTT-3.2.2-4]:
    // Clean Start=1 なのに Session Present=1 の CONNACK を受信した場合、
    // Network Connection を閉じなければならない。
    // MQTT v3.1.1 §3.2.2.2 [MQTT-3.2.2-1] でも Clean Session=1 時の
    // Session Present=0 がサーバーに要求されるため、同様に拒否する。
    for protocol_version in [MqttVersion::V5, MqttVersion::V311] {
        let mut session = Session::new(protocol_version, "client-1".into(), true)
            .expect("セッションの作成に成功すること");
        session.connect_sent();
        let reason_code = match protocol_version {
            MqttVersion::V5 => ConnackReason::V5(ConnectReasonCode::Success),
            MqttVersion::V311 => ConnackReason::V311(ConnectReturnCode::Accepted),
        };

        let result = session.apply_connack(ConnackParams {
            session_present: true,
            reason_code,
            session_expiry_interval: None,
            receive_maximum: None,
            topic_alias_maximum: None,
            server_keep_alive: None,
            maximum_packet_size: None,
            maximum_qos: None,
            retain_available: None,
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            assigned_client_identifier: None,
            authentication_method: None,
        });

        assert_eq!(
            result,
            Err(ConnackError::SessionPresentProtocolError),
            "{protocol_version:?} セッションでも clean_start=true かつ session_present=true は拒否すること"
        );
        assert_eq!(
            session.connection_state(),
            ConnectionState::Connecting,
            "{protocol_version:?} セッションで拒否されたとき接続状態が変更されないこと"
        );
    }
}

#[test]
fn apply_connack_rejects_zero_receive_maximum() {
    // MQTT v5.0 §3.2.2.3.3:
    // Receive Maximum に 0 を指定することは Protocol Error である。
    // 検証フェーズで拒否され、Session の状態は呼び出し前のまま保たれる。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();

    let result = session.apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: Some(0),
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: None,
    });

    // 戻り値が InvalidReceiveMaximum であること。
    assert_eq!(result, Err(ConnackError::InvalidReceiveMaximum));
    // 検証コードが「未到達」の適用フェーズ側代表: flow_control().receive_maximum()
    // が FlowControl::new() の既定値（65535）のままであること。
    assert_eq!(
        session.flow_control().receive_maximum(),
        65535,
        "適用フェーズに到達していないので既定値のまま"
    );
    // 検証コードが「発火前」の共通不変: 接続状態が Connecting のまま
    // （connected() に未到達）であること。
    assert_eq!(
        session.connection_state(),
        ConnectionState::Connecting,
        "検証エラー時は接続状態を遷移させない"
    );
}

#[test]
fn apply_connack_rejects_zero_maximum_packet_size() {
    // MQTT v5.0 §3.2.2.3.6:
    // Maximum Packet Size に 0 を指定することは Protocol Error である。
    // 検証フェーズで拒否され、Session の状態は呼び出し前のまま保たれる。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();

    let result = session.apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: Some(0),
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: None,
    });

    // 戻り値が InvalidMaximumPacketSize であること。
    assert_eq!(result, Err(ConnackError::InvalidMaximumPacketSize));
    // 検証コードが「未到達」の適用フェーズ側代表: maximum_packet_size が
    // ServerCapabilities::new() の既定値（None）のままであること。
    assert_eq!(
        session.server_capabilities().maximum_packet_size,
        None,
        "適用フェーズに到達していないので既定値のまま"
    );
    // 検証コードが「発火前」の共通不変: 接続状態が Connecting のまま
    // （connected() に未到達）であること。
    assert_eq!(
        session.connection_state(),
        ConnectionState::Connecting,
        "検証エラー時は接続状態を遷移させない"
    );
}

#[test]
fn apply_connack_rejects_maximum_qos_exactly_once() {
    // MQTT v5.0 §3.2.2.3.4:
    // Maximum QoS プロパティの値は 0 または 1 のみであり、それ以外は Protocol Error である。
    // QoS 2 (ExactlyOnce) を含む CONNACK は検証フェーズで拒否され、
    // Session の状態は呼び出し前のまま保たれる。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();

    let result = session.apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: Some(QoS::ExactlyOnce),
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: None,
    });

    // 戻り値が InvalidMaximumQoS であること。
    // maximum_qos の既定値は QoS::ExactlyOnce であり拒否対象値と一致するため、
    // 代表 accessor による適用フェーズ非到達の確認には使えない。
    // 代わりに connection_state() で「connected() に未到達」を確認する。
    assert_eq!(result, Err(ConnackError::InvalidMaximumQoS));
    // 検証コードが「発火前」の共通不変: 接続状態が Connecting のまま
    // （connected() に未到達）であること。適用フェーズは最先頭で connected() を
    // 呼び ConnectionState を Connected に遷移させるので、Connecting のままなら
    // 適用フェーズには 1 行も到達していないと保証できる。
    assert_eq!(
        session.connection_state(),
        ConnectionState::Connecting,
        "検証エラー時は接続状態を遷移させない"
    );
}

#[test]
fn apply_connack_accepts_session_present_with_empty_local_state() {
    // clean_start=false かつローカル状態が空でも session_present=true は正常。
    // 正常切断後はクライアント Session State（未完了 QoS メッセージ）が空でも、
    // サーバー側セッションの再開は永続セッションの正当な利用である。
    let mut session =
        Session::new_v5("client-1".into(), false).expect("セッションの作成に成功すること");
    session.connect_sent();

    let result = session.apply_connack(ConnackParams {
        session_present: true,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: None,
    });

    assert_eq!(result, Ok(()));
    assert!(session.is_connected());
}

#[test]
fn apply_connack_assigns_client_identifier_for_empty_client_id() {
    // MQTT v5.0 §3.2.2.3.7:
    // 空の Client Identifier で接続した場合、CONNACK に Assigned Client Identifier が含まれる。
    let mut session = Session::new_v5("".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();
    assert!(session.client_id().is_empty());

    session
        .apply_connack(ConnackParams {
            session_present: false,
            reason_code: ConnackReason::V5(ConnectReasonCode::Success),
            session_expiry_interval: None,
            receive_maximum: None,
            topic_alias_maximum: None,
            server_keep_alive: None,
            maximum_packet_size: None,
            maximum_qos: None,
            retain_available: None,
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            assigned_client_identifier: Some("server-assigned-id".into()),
            authentication_method: None,
        })
        .expect("CONNACK を受理すること");

    assert_eq!(session.client_id(), "server-assigned-id");
}

#[test]
fn apply_connack_rejects_missing_assigned_client_identifier_for_empty_client_id() {
    // MQTT v5.0 §3.2.2.3.7 [MQTT-3.2.2-16]:
    // クライアントが長さゼロの Client Identifier で接続した場合、サーバーは
    // Assigned Client Identifier を含む CONNACK で応答しなければならない。
    // 欠落した成功 CONNACK は拒否し、接続状態を Connected に遷移させない。
    let mut session = Session::new_v5("".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();

    let result = session.apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: None,
    });

    assert_eq!(result, Err(ConnackError::AssignedClientIdentifierMissing));
    // 検証エラー時は接続状態を遷移させない（connected() に未到達）。
    assert_eq!(
        session.connection_state(),
        ConnectionState::Connecting,
        "検証エラー時は接続状態を遷移させない"
    );
}

#[test]
fn apply_connack_rejects_empty_assigned_client_identifier_for_empty_client_id() {
    // MQTT v5.0 §3.2.2.3.7 [MQTT-3.2.2-16]:
    // クライアントが長さゼロの Client Identifier で接続した場合、サーバーは
    // Assigned Client Identifier を含む CONNACK で応答しなければならず、
    // その値はサーバー内で他のどのセッションにも現在使われていない新しい
    // Client Identifier でなければならない。空文字列はこの要求を満たさないため、
    // 欠落と同様に拒否し、接続状態を Connected に遷移させない。
    let mut session = Session::new_v5("".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();

    let result = session.apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: Some("".into()),
        authentication_method: None,
    });

    // 戻り値が AssignedClientIdentifierMissing であること。
    // 代表 accessor 候補である client_id は事前値（空文字列）と適用フェーズで
    // 代入される値（Some("") の中身）が一致するため使えない。代わりに
    // connection_state() で「connected() に未到達」を確認する。
    assert_eq!(result, Err(ConnackError::AssignedClientIdentifierMissing));
    // 検証コードが「発火前」の共通不変: 接続状態が Connecting のまま
    // （connected() に未到達）であること。適用フェーズは最先頭で connected() を
    // 呼び ConnectionState を Connected に遷移させるので、Connecting のままなら
    // 適用フェーズには 1 行も到達していないと保証できる。
    assert_eq!(
        session.connection_state(),
        ConnectionState::Connecting,
        "検証エラー時は接続状態を遷移させない"
    );
}

#[test]
fn apply_connack_allows_missing_assigned_client_identifier_for_non_empty_client_id() {
    // Client Identifier が空でないセッションでは Assigned Client Identifier は
    // 任意である（MQTT v5.0 §3.2.2.3.7）。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();

    let result = session.apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: None,
    });

    assert_eq!(result, Ok(()));
    assert!(session.is_connected());
}

#[test]
fn apply_connack_keeps_existing_client_id_when_assigned_is_unexpected() {
    // サーバーが予期せず Assigned Client Identifier を返した場合でも、
    // 既存の Client Identifier が空でなければ上書きしない。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();

    session
        .apply_connack(ConnackParams {
            session_present: false,
            reason_code: ConnackReason::V5(ConnectReasonCode::Success),
            session_expiry_interval: None,
            receive_maximum: None,
            topic_alias_maximum: None,
            server_keep_alive: None,
            maximum_packet_size: None,
            maximum_qos: None,
            retain_available: None,
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            assigned_client_identifier: Some("server-assigned-id".into()),
            authentication_method: None,
        })
        .expect("CONNACK を受理すること");

    assert_eq!(session.client_id(), "client-1");
}

// ==================================================================
// disconnect_received
// ==================================================================
#[test]
fn disconnect_received_transitions_to_disconnected() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();
    session.connected(false);
    assert_eq!(session.connection_state(), ConnectionState::Connected);

    session.disconnect_received();
    assert_eq!(session.connection_state(), ConnectionState::Disconnected);
}

// ==================================================================
// pingreq_sent / pingresp_received
// ==================================================================
#[test]
fn activity_resets_send_timer() {
    // Session::activity が KeepAlive::activity に委譲し、送信タイマーを更新することを確認する。
    // is_awaiting_pingresp == false は、誤って pingreq_sent に委譲した場合を検出するため必須。
    // 境界値は activity(30_000) 後の last_activity = 30_000 と Keep Alive 60 秒（threshold 60_000 ms）。
    // configure_for_connect は set_keep_alive のみで last_activity を触らない。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .configure_for_connect(60, 0, 0, 10, 0)
        .expect("configure_for_connect が成功すること");

    session.activity(30_000);
    assert!(!session.keep_alive().is_awaiting_pingresp());
    assert!(!session.keep_alive().should_send_pingreq(89_999));
    assert!(session.keep_alive().should_send_pingreq(90_000));
}

#[test]
fn reconnect_clears_pingresp_awaiting_state() {
    // Connected 状態で PINGREQ を送った後に切断し、再度 CONNECT を送ると、
    // reset_connection_state 経由で PINGRESP 待ちがクリアされる。
    // 前接続の待ちが新接続に持ち越されると誤タイムアウトになるため、
    // 実運用の再接続サイクルでこの不変条件が守られることを検証する。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .configure_for_connect(60, 0, 0, 10, 0)
        .expect("configure_for_connect が成功すること");
    session.connect_sent();
    session.connected(false);

    // Connected 状態で PINGREQ を送信し、PINGRESP 待ちを立てる。
    session.pingreq_sent(0);
    assert!(session.keep_alive().is_awaiting_pingresp());

    // 切断遷移を経由する。
    session.disconnected();
    assert!(session.keep_alive().is_awaiting_pingresp());

    // 再接続で PINGRESP 待ちがクリアされる。
    session
        .configure_for_connect(60, 0, 0, 10, 0)
        .expect("configure_for_connect が成功すること");
    session.connect_sent();
    assert!(!session.keep_alive().is_awaiting_pingresp());
}

#[test]
fn apply_connack_server_keep_alive_does_not_clear_awaiting() {
    // apply_connack 経由の Server Keep Alive 適用は set_keep_alive を呼ぶだけで、
    // pingreq_sent_at には触れない不変条件を直接に確かめる。
    // MQTT の正常系では CONNACK 受信時に PINGRESP 待ちは立たないが、
    // 本テストは「apply_connack が pingreq_sent_at に触れない」ことだけを検証する
    // ため、意図的に Connecting 状態で pingreq_sent を呼ぶ構成にしている。
    // MQTT v5.0 §3.1.2.10 [MQTT-3.1.2-21] / MQTT v5.0 §3.2.2.3.14 [MQTT-3.2.2-21]。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .configure_for_connect(60, 0, 0, 10, 0)
        .expect("configure_for_connect が成功すること");
    session.connect_sent();

    session.pingreq_sent(0);
    assert!(session.keep_alive().is_awaiting_pingresp());

    session
        .apply_connack(ConnackParams {
            session_present: false,
            reason_code: ConnackReason::V5(ConnectReasonCode::Success),
            session_expiry_interval: None,
            receive_maximum: None,
            topic_alias_maximum: None,
            server_keep_alive: Some(1),
            maximum_packet_size: None,
            maximum_qos: None,
            retain_available: None,
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            assigned_client_identifier: None,
            authentication_method: None,
        })
        .expect("CONNACK を受理すること");

    // PINGRESP 待ちは維持されており、新しい keep_alive_secs = 1 に応じて
    // 1_000 ms 以上経過で has_timed_out が true を返す。
    assert!(session.keep_alive().is_awaiting_pingresp());
    assert!(!session.keep_alive().has_timed_out(999));
    assert!(session.keep_alive().has_timed_out(1_000));
}

// ==================================================================
// auth_received / 拡張認証委譲 API
// ==================================================================
#[test]
fn reauthenticate_sent_requires_authenticated() {
    // MQTT v5.0 §4.12.1 [MQTT-4.12.1-1]:
    // 再認証は初回認証の完了 (Authenticated) 後にのみ開始できる。
    // Idle 状態からの再認証開始は状態を変えずに拒否する。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    assert_eq!(
        session.reauthenticate_sent(),
        Err(AuthError::NotAuthenticated)
    );
    assert!(!session.is_reauthenticating());

    // InitialAuthenticating からの再認証開始も拒否される。
    session
        .connect_sent_with_auth("X".into())
        .expect("v5 セッションで初回認証を開始できること");
    assert_eq!(
        session.reauthenticate_sent(),
        Err(AuthError::NotAuthenticated)
    );
    assert!(session.is_initial_authenticating());
}

#[test]
fn apply_connack_completes_initial_authentication() {
    // 初回認証の完了は成功 CONNACK (Method 一致) で行われる。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .connect_sent_with_auth("SCRAM-SHA-256".into())
        .expect("v5 セッションで初回認証を開始できること");
    assert!(session.is_initial_authenticating());

    session
        .apply_connack(ConnackParams {
            session_present: false,
            reason_code: ConnackReason::V5(ConnectReasonCode::Success),
            session_expiry_interval: None,
            receive_maximum: None,
            topic_alias_maximum: None,
            server_keep_alive: None,
            maximum_packet_size: None,
            maximum_qos: None,
            retain_available: None,
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            assigned_client_identifier: None,
            authentication_method: Some("SCRAM-SHA-256".into()),
        })
        .expect("成功 CONNACK で初回認証が完了すること");
    assert!(session.is_connected());
    assert!(session.auth_state().is_authenticated());
}

#[test]
fn apply_connack_rejects_missing_authentication_method_on_initial_auth() {
    // MQTT v5.0 §4.12 [MQTT-4.12.0-5]:
    // 初回認証中の成功 CONNACK には CONNECT と同じ Method が必須。
    // 欠落の場合は AuthenticationMethodMissing で拒否し、状態を変えない。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .connect_sent_with_auth("SCRAM-SHA-256".into())
        .expect("v5 セッションで初回認証を開始できること");

    let result = session.apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: None,
    });

    assert_eq!(result, Err(ConnackError::AuthenticationMethodMissing));
    // 検証エラー時は接続状態を遷移させない（connected() に未到達）。
    assert_eq!(
        session.connection_state(),
        ConnectionState::Connecting,
        "検証エラー時は接続状態を遷移させない"
    );
    assert!(session.is_initial_authenticating());
}

#[test]
fn apply_connack_rejects_mismatched_authentication_method_on_initial_auth() {
    // MQTT v5.0 §4.12 [MQTT-4.12.0-5]:
    // 初回認証中の成功 CONNACK の Method が CONNECT と一致しないと拒否。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .connect_sent_with_auth("SCRAM-SHA-256".into())
        .expect("v5 セッションで初回認証を開始できること");

    let result = session.apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: Some("OTHER".into()),
    });

    assert_eq!(result, Err(ConnackError::AuthenticationMethodMismatch));
    // 検証エラー時は接続状態を遷移させない（connected() に未到達）。
    assert_eq!(
        session.connection_state(),
        ConnectionState::Connecting,
        "検証エラー時は接続状態を遷移させない"
    );
    assert!(session.is_initial_authenticating());
}

#[test]
fn apply_connack_rejects_unexpected_authentication_method_when_not_authenticating() {
    // MQTT v5.0 §4.12 [MQTT-4.12.0-6]:
    // 認証を開始していないセッションへの CONNACK に Method が含まれるのは違反。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();

    let result = session.apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: Some("SCRAM-SHA-256".into()),
    });

    assert_eq!(result, Err(ConnackError::UnexpectedAuthenticationMethod));
    // 検証エラー時は接続状態を遷移させない（connected() に未到達）。
    assert_eq!(
        session.connection_state(),
        ConnectionState::Connecting,
        "検証エラー時は接続状態を遷移させない"
    );
}

#[test]
fn apply_connack_error_keeps_flow_control_unchanged() {
    // apply_connack がエラーを返す場合、flow_control を含む全状態は変更されない。
    // 具体的には、以下の順序で検証が並んでいるため、Receive Maximum=0 の検証で
    // 拒否する前に flow_control.set_receive_maximum() が呼ばれてはならない。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .configure_for_connect(60, 0, 0, 5, 0)
        .expect("CONNECT パラメータを設定できること");
    // configure_for_connect で own_receive_maximum=5 になり、
    // ピア方向の receive_maximum は初期値 65535 のまま。
    let before = session.flow_control().receive_maximum();
    assert_eq!(before, 65535);
    session.connect_sent();

    let result = session.apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: Some(0),
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: None,
    });
    assert_eq!(result, Err(ConnackError::InvalidReceiveMaximum));
    // ピア方向の receive_maximum が Some(0) に触られていないこと。
    assert_eq!(session.flow_control().receive_maximum(), before);
    assert!(!session.is_connected());
}

#[test]
fn apply_connack_v311_rejects_v5_only_params_before_connection_refused() {
    // MQTT v3.1.1 セッションで非成功 Return Code と v5 専用フィールドが
    // 両方含まれる CONNACK を受けた場合、構造違反 (V5OnlyParameters) が
    // セマンティクス違反 (ConnectionRefused) より先に検出される。
    let make_base = || ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V311(ConnectReturnCode::UnacceptableProtocolVersion),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: None,
    };
    for (name, params) in v5_only_field_cases(make_base) {
        let mut session =
            Session::new_v311("client-1".into(), true).expect("セッションの作成に成功すること");
        session.connect_sent();

        let result = session.apply_connack(params);
        assert_eq!(
            result,
            Err(ConnackError::V5OnlyParameters),
            "フィールド {name} が Some なら ConnectionRefused より先に V5OnlyParameters を返すこと"
        );
        assert_eq!(
            session.connection_state(),
            ConnectionState::Connecting,
            "フィールド {name} で拒否されたとき接続状態が変更されないこと"
        );
    }
}

#[test]
fn apply_connack_v311_rejects_version_mismatch_before_v5_only_params() {
    // MQTT v3.1.1 セッションに v5 の Reason Code と v5 専用フィールドが
    // 両方含まれる CONNACK を受けた場合、バージョン不一致 (VersionMismatch) が
    // 構造違反 (V5OnlyParameters) より先に検出される。
    // MQTT v3.1.1 の CONNACK 可変ヘッダは Session Present (MQTT v3.1.1 §3.2.2.2)
    // と Connect Return Code (MQTT v3.1.1 §3.2.2.3) のみであり、
    // v5 の Connect Reason Code は存在しない。
    let make_base = || ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: None,
    };
    for (name, params) in v5_only_field_cases(make_base) {
        let mut session =
            Session::new_v311("client-1".into(), true).expect("セッションの作成に成功すること");
        session.connect_sent();

        let result = session.apply_connack(params);
        assert_eq!(
            result,
            Err(ConnackError::VersionMismatch),
            "フィールド {name} が Some なら V5OnlyParameters より先に VersionMismatch を返すこと"
        );
        assert_eq!(
            session.connection_state(),
            ConnectionState::Connecting,
            "フィールド {name} で拒否されたとき接続状態が変更されないこと"
        );
    }
}

#[test]
fn apply_connack_error_on_method_missing_keeps_auth_state_unchanged() {
    // Method 検証失敗時に auth_state が変更されないことを確認する。
    // 検証は「最後の fallible 操作」として設計されているため、失敗しても
    // InitialAuthenticating と auth_method は保たれる。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .connect_sent_with_auth("SCRAM-SHA-256".into())
        .expect("v5 セッションで初回認証を開始できること");
    let before_method = session.auth_state().auth_method().map(String::from);

    let _ = session.apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: None,
    });

    assert!(session.is_initial_authenticating());
    assert_eq!(
        session.auth_state().auth_method().map(String::from),
        before_method
    );
}

#[test]
fn apply_connack_error_on_method_mismatch_keeps_auth_state_unchanged() {
    // Method 不一致でも auth_state と auth_method が保たれる。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .connect_sent_with_auth("SCRAM-SHA-256".into())
        .expect("v5 セッションで初回認証を開始できること");
    let before_method = session.auth_state().auth_method().map(String::from);

    let _ = session.apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: Some("OTHER".into()),
    });

    assert!(session.is_initial_authenticating());
    assert_eq!(
        session.auth_state().auth_method().map(String::from),
        before_method
    );
}

#[test]
fn apply_connack_error_on_unexpected_method_keeps_state_unchanged() {
    // 拡張認証を開始していないセッションで Method 付き CONNACK を受けた場合、
    // 状態は Idle のまま変化しない。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();

    let result = session.apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: Some("SCRAM-SHA-256".into()),
    });

    assert_eq!(result, Err(ConnackError::UnexpectedAuthenticationMethod));
    assert_eq!(session.auth_state().state(), AuthState::Idle);
    assert_eq!(session.auth_state().auth_method(), None);
    assert!(!session.is_connected());
}

#[test]
fn apply_connack_method_missing_does_not_apply_receive_maximum() {
    // 検証と適用の分離: 初回認証中に Method 欠落の成功 CONNACK を受けたとき、
    // ConnackParams で指定した Receive Maximum が flow_control に適用されないこと。
    // 誰かが検証フェーズと適用フェーズを混ぜてしまう退行を検出する。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .configure_for_connect(60, 0, 0, 5, 0)
        .expect("CONNECT パラメータを設定できること");
    session
        .connect_sent_with_auth("SCRAM-SHA-256".into())
        .expect("v5 セッションで初回認証を開始できること");
    // ピア方向の Receive Maximum の事前値。configure_for_connect() は
    // own_receive_maximum を設定するだけで、ピア方向は既定値 65535 のまま。
    let before_receive_maximum = session.flow_control().receive_maximum();

    let result = session.apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        // 事前値 (65535) と明確に異なる値を指定して、適用されたら検出できるようにする。
        receive_maximum: Some(7),
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: None,
    });

    assert_eq!(result, Err(ConnackError::AuthenticationMethodMissing));
    // Receive Maximum は適用されないため事前値のまま。
    assert_eq!(
        session.flow_control().receive_maximum(),
        before_receive_maximum
    );
    // 接続状態も遷移しない。
    assert!(!session.is_connected());
    // auth_state は初回認証中のまま。
    assert!(session.is_initial_authenticating());
}

// ==================================================================
// pending_retransmissions
// ==================================================================
#[test]
fn pending_retransmissions_preserve_order_across_reconnect() {
    // 再接続（セッション継続）後も再送一覧が元の送信順を維持すること。
    // MQTT v5.0 §4.4 [MQTT-4.4.0-1] の再送場面に対応する。
    let mut session =
        Session::new_v5("client-1".into(), false).expect("セッションの作成に成功すること");
    session.connect_sent();
    session.connected(true);
    session.qos_flow_mut().publish_sent_qos1(1);
    session.qos_flow_mut().publish_sent_qos1(2);
    // 識別子 1 が完了して解放され、新しい送信で再利用される（送信順は 2 → 1 になる）。
    let _ = session.qos_flow_mut().puback_received(1);
    session.qos_flow_mut().publish_sent_qos1(1);

    session.disconnected();
    session.connect_sent();
    session.connected(true);

    let retrans = session.pending_retransmissions();
    let ids: Vec<u16> = retrans.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![2, 1]);
}

#[test]
fn pending_retransmissions_excludes_pubrel_waiting() {
    // QoS 2 受信側（PUBREL 待ち）は再送対象外。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");

    session.qos_flow_mut().publish_received_qos2(30); // PUBREL 待ち
    session.qos_flow_mut().publish_sent_qos1(10); // PUBACK 待ち

    let retrans = session.pending_retransmissions();
    assert_eq!(retrans.len(), 1); // QoS 1 のみ
    assert_eq!(retrans[0].0, 10);
}

#[test]
fn pending_retransmissions_includes_qos2_pubcomp_waiting() {
    // QoS 2 送信側（PUBCOMP 待ち）は PUBREL 再送対象。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");

    session.qos_flow_mut().publish_sent_qos2(40);
    let _ = session.qos_flow_mut().pubrec_received(40, 0x00);

    let retrans = session.pending_retransmissions();
    assert_eq!(retrans.len(), 1);
    assert_eq!(retrans[0].0, 40);
}

#[test]
fn resend_publish_consumes_send_quota() {
    // 永続セッション再開時、再送は新しい接続の send quota を消費する。
    let mut session =
        Session::new_v5("client-1".into(), false).expect("セッションの作成に成功すること");
    session.connect_sent();
    session.connected(true);

    session
        .flow_control_mut()
        .set_receive_maximum(2)
        .expect("Receive Maximum を設定できること");
    session.qos_flow_mut().publish_sent_qos1(1);
    session
        .flow_control_mut()
        .publish_sent()
        .expect("quota が残っている間は送信できること");
    session.qos_flow_mut().publish_sent_qos1(2);
    session
        .flow_control_mut()
        .publish_sent()
        .expect("quota が残っている間は送信できること");

    // 再接続（セッション継続）して flow_control がリセットされる。
    session.disconnected();
    session.connect_sent();
    session.connected(true);
    session
        .flow_control_mut()
        .set_receive_maximum(2)
        .expect("Receive Maximum を設定できること");

    assert_eq!(session.pending_retransmissions().len(), 2);
    assert_eq!(session.flow_control().outstanding_count(), 0);

    let action = session.resend_publish(1).expect("再送を記録できること");
    assert_eq!(action, Some(Action::ResendPublish { packet_id: 1 }));
    assert_eq!(session.flow_control().outstanding_count(), 1);

    let action = session.resend_publish(2).expect("再送を記録できること");
    assert_eq!(action, Some(Action::ResendPublish { packet_id: 2 }));
    assert_eq!(session.flow_control().outstanding_count(), 2);
}

#[test]
fn resend_publish_rejects_exceeding_receive_maximum() {
    // 再接続後の Receive Maximum が小さくなった場合、超過分の再送は拒否される。
    let mut session =
        Session::new_v5("client-1".into(), false).expect("セッションの作成に成功すること");
    session.connect_sent();
    session.connected(true);

    // 初回接続では Receive Maximum=2 で 2 つの PUBLISH を未確認にする。
    session
        .flow_control_mut()
        .set_receive_maximum(2)
        .expect("Receive Maximum を設定できること");
    session.qos_flow_mut().publish_sent_qos1(1);
    session
        .flow_control_mut()
        .publish_sent()
        .expect("quota が残っている間は送信できること");
    session.qos_flow_mut().publish_sent_qos1(2);
    session
        .flow_control_mut()
        .publish_sent()
        .expect("quota が残っている間は送信できること");

    session.disconnected();
    session.connect_sent();
    session.connected(true);
    // 再接続後は Receive Maximum=1 に縮小される。
    session
        .flow_control_mut()
        .set_receive_maximum(1)
        .expect("Receive Maximum を設定できること");

    assert!(session.resend_publish(1).is_ok());
    assert_eq!(
        session.resend_publish(2),
        Err(FlowControlError::ReceiveMaximumExceeded)
    );
}

#[test]
fn resend_publish_returns_none_for_non_pending() {
    // 再送対象でないパケット識別子に対しては None を返す。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    assert_eq!(session.resend_publish(1), Ok(None));
}

#[test]
fn resend_publish_same_connection_returns_none_for_v5() {
    // MQTT v5.0 §4.4 [MQTT-4.4.0-1]: 再送が許されるのは再接続直後のみ。
    // 同一接続内で send quota を消費済みのフローに対する resend_publish は
    // None を返し、send quota を二重に消費しない。
    let mut session =
        Session::new_v5("client-1".into(), false).expect("セッションの作成に成功すること");
    session.connect_sent();
    session.connected(true);

    session
        .publish_sent(QoS::AtLeastOnce, 1)
        .expect("PUBLISH の送信を記録できること");
    assert_eq!(session.flow_control().outstanding_count(), 1);

    assert_eq!(session.resend_publish(1), Ok(None));
    assert_eq!(session.flow_control().outstanding_count(), 1);

    // PUBACK で quota が完全に回復し、リークしない。
    session.handle_puback(1).expect("PUBACK を処理できること");
    assert_eq!(session.flow_control().outstanding_count(), 0);
}

#[test]
fn resend_publish_same_connection_does_not_consume_quota_for_v311() {
    // MQTT v3.1.1 §4.4 は同一接続内の再送を禁止していない（同節の
    // Non normative comment はデータ消失が起こる旧来のネットワークでの再送に
    // 言及している）ため、再送アクションは返すが、send quota は消費済みなので
    // 二重に消費しない。
    let mut session =
        Session::new_v311("client-1".into(), true).expect("セッションの作成に成功すること");
    session.connect_sent();
    session.connected(false);

    session
        .publish_sent(QoS::AtLeastOnce, 1)
        .expect("PUBLISH の送信を記録できること");
    assert_eq!(session.flow_control().outstanding_count(), 1);

    let action = session.resend_publish(1).expect("再送を記録できること");
    assert_eq!(action, Some(Action::ResendPublish { packet_id: 1 }));
    assert_eq!(session.flow_control().outstanding_count(), 1);

    // PUBACK で quota が完全に回復し、リークしない。
    session.handle_puback(1).expect("PUBACK を処理できること");
    assert_eq!(session.flow_control().outstanding_count(), 0);
}

#[test]
fn resend_publish_after_reconnect_charges_quota_once() {
    // 再接続後の初回再送は新しい接続の send quota を消費し、同一接続内の
    // 2 回目の再送は MQTT v5.0 §4.4 [MQTT-4.4.0-1] により対象外となる。
    let mut session =
        Session::new_v5("client-1".into(), false).expect("セッションの作成に成功すること");
    session.connect_sent();
    session.connected(true);

    session
        .publish_sent(QoS::AtLeastOnce, 1)
        .expect("PUBLISH の送信を記録できること");
    assert_eq!(session.flow_control().outstanding_count(), 1);

    // 再接続（セッション継続）で flow_control と quota 消費状態がリセットされる。
    session.disconnected();
    session.connect_sent();
    session.connected(true);
    assert_eq!(session.flow_control().outstanding_count(), 0);

    let action = session.resend_publish(1).expect("再送を記録できること");
    assert_eq!(action, Some(Action::ResendPublish { packet_id: 1 }));
    assert_eq!(session.flow_control().outstanding_count(), 1);

    // 同一接続内の 2 回目の再送は None を返し、quota を消費しない。
    assert_eq!(session.resend_publish(1), Ok(None));
    assert_eq!(session.flow_control().outstanding_count(), 1);

    // PUBACK で quota が完全に回復し、リークしない。
    session.handle_puback(1).expect("PUBACK を処理できること");
    assert_eq!(session.flow_control().outstanding_count(), 0);
}

#[test]
fn handle_publish_qos1_returns_send_puback() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");

    let action = session
        .handle_publish_qos1(10)
        .expect("PUBLISH 受信が許容されること");

    assert_eq!(action, Action::SendPuback { packet_id: 10 });
    assert_eq!(session.flow_control().incoming_count(), 1);
    assert!(session.qos_flow().has_incoming_flow(10));
}

#[test]
fn handle_publish_qos1_duplicate_does_not_consume_receive_quota() {
    // MQTT v5.0 §3.3.4 [MQTT-3.3.4-9] / MQTT v5.0 §4.3.2:
    // PUBACK 送信前の同一 Packet Identifier の PUBLISH 再着は同一メッセージの
    // 再送であり、新たな未確認 PUBLISH ではない。
    // 再着のたびに受信枠を消費すると、フロー完了時の puback_sent() は
    // 1 回しか解放しないため受信枠が恒久的に減ってしまう。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .configure_for_connect(60, 0, 0, 1, 0)
        .expect("CONNECT パラメータを設定できること");

    let action = session
        .handle_publish_qos1(1)
        .expect("PUBLISH 受信が許容されること");
    assert_eq!(action, Action::SendPuback { packet_id: 1 });
    assert_eq!(session.flow_control().incoming_count(), 1);

    // Receive Maximum = 1 で受信枠が埋まっていても、同一 Packet Identifier の
    // 再着は重複として受理され、受信枠を追加で消費しない。
    let action = session
        .handle_publish_qos1(1)
        .expect("重複 PUBLISH が受信枠を消費せずに受理されること");
    assert_eq!(action, Action::SendPuback { packet_id: 1 });
    assert_eq!(session.flow_control().incoming_count(), 1);

    // PUBACK 送信で受信枠が完全に解放され、次のメッセージを受信できる。
    session.puback_sent(1);
    assert_eq!(session.flow_control().incoming_count(), 0);
    assert!(!session.qos_flow().has_incoming_flow(1));
    assert!(session.handle_publish_qos1(2).is_ok());
}

#[test]
fn handle_publish_qos2_returns_send_pubrec() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");

    let action = session
        .handle_publish_qos2(20)
        .expect("PUBLISH 受信が許容されること");

    assert_eq!(
        action,
        Action::SendPubrec {
            packet_id: 20,
            is_duplicate: false
        }
    );
    assert!(session.qos_flow().is_active(20));
    assert_eq!(session.flow_control().incoming_count(), 1);
}

#[test]
fn handle_publish_qos2_duplicate_does_not_consume_receive_quota() {
    // MQTT v5.0 §4.3.3 [MQTT-4.3.3-10] / MQTT v3.1.1 §4.3.3:
    // PUBREL 受信前の同一 Packet Identifier の PUBLISH 再着は同一メッセージの
    // 再送であり、新たな未確認 PUBLISH ではない（MQTT v5.0 §3.3.4 [MQTT-3.3.4-9]）。
    // 再着のたびに受信枠を消費すると、フロー完了時の pubcomp_sent() は
    // 1 回しか解放しないため受信枠が恒久的に減ってしまう。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .configure_for_connect(60, 0, 0, 1, 0)
        .expect("CONNECT パラメータを設定できること");

    let action = session
        .handle_publish_qos2(1)
        .expect("PUBLISH 受信が許容されること");
    assert_eq!(
        action,
        Action::SendPubrec {
            packet_id: 1,
            is_duplicate: false
        }
    );
    assert_eq!(session.flow_control().incoming_count(), 1);

    // Receive Maximum = 1 で受信枠が埋まっていても、同一 Packet Identifier の
    // 再着は重複として受理され、受信枠を追加で消費しない。
    let action = session
        .handle_publish_qos2(1)
        .expect("重複 PUBLISH が受信枠を消費せずに受理されること");
    assert_eq!(
        action,
        Action::SendPubrec {
            packet_id: 1,
            is_duplicate: true
        }
    );
    assert_eq!(session.flow_control().incoming_count(), 1);

    // フローが完了すると受信枠が完全に解放され、次のメッセージを受信できる。
    session
        .handle_pubrel(1)
        .expect("PUBREL を処理できること")
        .expect("PUBCOMP 送信アクションが返ること");
    session.pubcomp_sent();
    assert_eq!(session.flow_control().incoming_count(), 0);
    assert!(session.handle_publish_qos2(2).is_ok());
}

#[test]
fn handle_publish_qos1_rejects_exceeding_receive_maximum() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .configure_for_connect(60, 0, 0, 1, 0)
        .expect("CONNECT パラメータを設定できること");

    assert!(session.handle_publish_qos1(1).is_ok());
    assert_eq!(
        session.handle_publish_qos1(2),
        Err(FlowControlError::ReceiveMaximumExceeded)
    );
}

#[test]
fn puback_sent_reduces_incoming_count() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .configure_for_connect(60, 0, 0, 2, 0)
        .expect("CONNECT パラメータを設定できること");

    session
        .handle_publish_qos1(1)
        .expect("PUBLISH 受信が許容されること");
    session
        .handle_publish_qos1(2)
        .expect("PUBLISH 受信が許容されること");
    assert!(!session.flow_control().can_receive());
    session.puback_sent(1);
    assert!(session.flow_control().can_receive());
}

#[test]
fn pubrec_sent_error_reduces_incoming_count() {
    // Reason Code 0x80 以上のエラー PUBREC で QoS 2 フローは終了するため、
    // その時点で受信方向の未確認カウントを解放する。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .configure_for_connect(60, 0, 0, 2, 0)
        .expect("CONNECT パラメータを設定できること");

    session
        .handle_publish_qos2(1)
        .expect("PUBLISH 受信が許容されること");
    assert_eq!(session.flow_control().incoming_count(), 1);
    // 0x80 = Unspecified error
    session.pubrec_sent(0x80);
    assert_eq!(session.flow_control().incoming_count(), 0);
}

#[test]
fn handle_pubrel_returns_send_pubcomp() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");

    session
        .handle_publish_qos2(30)
        .expect("PUBLISH 受信が許容されること");
    let action = session.handle_pubrel(30);

    assert_eq!(action, Ok(Some(Action::SendPubcomp { packet_id: 30 })));
    assert!(!session.qos_flow().is_active(30));
}

#[test]
fn handle_puback_unknown_packet_id_returns_none() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");

    assert_eq!(session.handle_puback(1), Ok(None));
}

#[test]
fn handle_puback_state_mismatch_returns_error() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.qos_flow_mut().publish_sent_qos2(1);

    assert_eq!(session.handle_puback(1), Err(FlowError::StateMismatch));
}

#[test]
fn handle_pubrec_unknown_packet_id_returns_none() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");

    assert_eq!(session.handle_pubrec(1, 0x00), Ok(None));
}

#[test]
fn handle_pubrec_state_mismatch_returns_error() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.qos_flow_mut().publish_sent_qos1(1);

    assert_eq!(
        session.handle_pubrec(1, 0x00),
        Err(FlowError::StateMismatch)
    );
}

#[test]
fn handle_pubcomp_unknown_packet_id_returns_none() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");

    assert_eq!(session.handle_pubcomp(1), Ok(None));
}

#[test]
fn handle_pubcomp_state_mismatch_returns_error() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.qos_flow_mut().publish_sent_qos1(1);

    assert_eq!(session.handle_pubcomp(1), Err(FlowError::StateMismatch));
}

#[test]
fn handle_pubrel_unknown_packet_id_returns_resend_pubcomp() {
    // MQTT v5.0 §4.3.3 [MQTT-4.3.3-11] / MQTT v3.1.1 §4.3.3:
    // 該当フローがない PUBREL にも PUBCOMP で応答しなければならない。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");

    assert_eq!(
        session.handle_pubrel(1),
        Ok(Some(Action::ResendPubcomp { packet_id: 1 }))
    );
}

#[test]
fn handle_pubrel_resend_after_pubcomp_lost() {
    // PUBCOMP 消失 → PUBREL 再送のシナリオ。
    // 再送 PUBREL には ResendPubcomp が返り、受信枠は二重に解放されない。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .configure_for_connect(60, 0, 0, 2, 0)
        .expect("CONNECT パラメータを設定できること");

    // 別の受信フローを 1 つ抱えた状態にしておく。
    session
        .handle_publish_qos2(1)
        .expect("PUBLISH 受信が許容されること");
    session.pubrec_sent(0x00);

    // 対象フローを PUBCOMP 送信まで完了させる。
    session
        .handle_publish_qos2(2)
        .expect("PUBLISH 受信が許容されること");
    session.pubrec_sent(0x00);
    assert_eq!(
        session.handle_pubrel(2),
        Ok(Some(Action::SendPubcomp { packet_id: 2 }))
    );
    session.pubcomp_sent();
    assert_eq!(session.flow_control().incoming_count(), 1);

    // PUBCOMP が消失し、サーバーが PUBREL を再送してきたと想定する。
    // ResendPubcomp では pubcomp_sent() を呼ばないため、
    // 進行中の別フローの受信枠が誤って解放されない。
    assert_eq!(
        session.handle_pubrel(2),
        Ok(Some(Action::ResendPubcomp { packet_id: 2 }))
    );
    assert_eq!(session.flow_control().incoming_count(), 1);
}

#[test]
fn handle_pubrel_state_mismatch_returns_error() {
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.qos_flow_mut().publish_sent_qos1(1);

    assert_eq!(session.handle_pubrel(1), Err(FlowError::StateMismatch));
}

// ======================================================================
// 送信パケットの統合ハンドラ
// ======================================================================
#[test]
fn publish_sent_qos1_updates_all_sub_state_machines() {
    // publish_sent(QoS 1) が flow_control / qos_flow / packet_id_manager の
    // 状態をまとめて 1 増やすことを検証する。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .flow_control_mut()
        .set_receive_maximum(2)
        .expect("Receive Maximum を設定できること");
    let id = session
        .allocate_packet_id()
        .expect("パケット識別子を割り当てられること");

    session
        .publish_sent(QoS::AtLeastOnce, id)
        .expect("送信クォータが残っている間は成功すること");

    assert_eq!(session.flow_control().outstanding_count(), 1);
    assert!(session.qos_flow().is_active(id));
    assert_eq!(session.packet_id_manager().in_use_count(), 1);
}

#[test]
fn publish_sent_qos2_updates_all_sub_state_machines() {
    // publish_sent(QoS 2) も QoS 1 と同じく 3 つのサブ状態機械を進めることを検証する。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .flow_control_mut()
        .set_receive_maximum(2)
        .expect("Receive Maximum を設定できること");
    let id = session
        .allocate_packet_id()
        .expect("パケット識別子を割り当てられること");

    session
        .publish_sent(QoS::ExactlyOnce, id)
        .expect("送信クォータが残っている間は成功すること");

    assert_eq!(session.flow_control().outstanding_count(), 1);
    assert!(session.qos_flow().is_active(id));
    assert_eq!(session.packet_id_manager().in_use_count(), 1);
}

#[test]
fn abort_publish_without_publish_sent_is_defensive() {
    // publish_sent を通っていない packet_id で abort_publish を誤呼び出しした
    // 場合、is_active ガードにより flow_control.publish_acked が呼ばれず、
    // 他フローの送信クォータを侵食しないことを検証する。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session
        .flow_control_mut()
        .set_receive_maximum(2)
        .expect("Receive Maximum を設定できること");

    // 別の QoS 1 フローが 1 つ進行中。
    let victim = session
        .allocate_packet_id()
        .expect("パケット識別子を割り当てられること");
    session
        .publish_sent(QoS::AtLeastOnce, victim)
        .expect("publish_sent は成功すること");
    assert_eq!(session.flow_control().outstanding_count(), 1);

    // publish_sent を通っていない別の packet_id で abort_publish を呼ぶ。
    session.abort_publish(9999);

    // 他フローの送信クォータは侵食されない。
    assert_eq!(session.flow_control().outstanding_count(), 1);
    assert!(session.qos_flow().is_active(victim));
}

#[test]
fn unsubscribe_sent_then_abort_unsubscribe_drops_pending() {
    // unsubscribe_sent → abort_unsubscribe で pending から削除される。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    // 事前に active を作っておく。abort_unsubscribe では active は削除されない。
    let sub_id = session
        .allocate_packet_id()
        .expect("パケット識別子を割り当てられること");
    session.subscribe_sent(
        sub_id,
        vec![SubscriptionEntry::new("a/b", QoS::AtLeastOnce)],
    );
    session
        .handle_suback(sub_id, &[0x01])
        .expect("SUBACK を処理できること");
    assert_eq!(session.subscription_manager().active_count(), 1);

    let unsub_id = session
        .allocate_packet_id()
        .expect("パケット識別子を割り当てられること");
    session.unsubscribe_sent(unsub_id, vec!["a/b".into()]);
    session.abort_unsubscribe(unsub_id);

    assert_eq!(session.packet_id_manager().in_use_count(), 0);
    assert!(
        session
            .subscription_manager()
            .pending_unsubscribe_ids()
            .is_empty()
    );
    // active は abort では削除されない。
    assert_eq!(session.subscription_manager().active_count(), 1);
}

#[test]
fn allocate_packet_id_returns_none_on_exhaustion() {
    // PacketIdManager が枯渇状態のとき None を返す。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    // 65535 個の連続 allocate で枯渇させる。プール実装のスタイル (単純 u16
    // カウンタ) を仮定せず、実際に in_use_count が 65535 になるまで呼ぶ。
    for _ in 0..65535 {
        assert!(session.allocate_packet_id().is_some());
    }
    assert!(session.allocate_packet_id().is_none());
}

#[test]
fn publish_sent_qos0_is_rejected() {
    // QoS 0 には確認応答フローが存在せず send quota の対象でもない
    // (MQTT v5.0 §4.9 [MQTT-4.9.0-2] の対象は QoS > 0 の PUBLISH のみ) ため、
    // QoS 0 での publish_sent は Err で明示的に拒否され、状態は変更されない。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    assert_eq!(
        session.publish_sent(QoS::AtMostOnce, 1),
        Err(FlowControlError::InvalidQoS)
    );
    assert_eq!(session.flow_control().outstanding_count(), 0);
    assert!(!session.qos_flow().is_active(1));
}

#[test]
fn publish_sent_zero_packet_id_is_rejected() {
    // packet_id == 0 は MQTT v5.0 §2.2.1 [MQTT-2.2.1-3] / MQTT v3.1.1 §2.3.1
    // [MQTT-2.3.1-1] 違反のため、Err で明示的に拒否され、状態は変更されない
    // (silent no-op にすると呼び出し側が成功とみなして不正な wire 送信をし得る)。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    assert_eq!(
        session.publish_sent(QoS::AtLeastOnce, 0),
        Err(FlowControlError::InvalidPacketId)
    );
    assert_eq!(session.flow_control().outstanding_count(), 0);
    assert!(!session.qos_flow().is_active(0));
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "subscribe_sent must be called with a non-zero packet identifier")]
fn subscribe_sent_zero_packet_id_panics_in_debug_build() {
    // packet_id == 0 で subscribe_sent を呼ぶと debug ビルドで panic する。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.subscribe_sent(0, vec![SubscriptionEntry::new("a/b", QoS::AtLeastOnce)]);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "subscribe_sent must be called with a non-empty subscriptions list")]
fn subscribe_sent_empty_subscriptions_panics_in_debug_build() {
    // subscriptions が空だと MQTT v5.0 §3.8.3 [MQTT-3.8.3-2] /
    // MQTT v3.1.1 §3.8.3 [MQTT-3.8.3-3] 違反のため、debug ビルドで panic する。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.subscribe_sent(1, vec![]);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "unsubscribe_sent must be called with a non-zero packet identifier")]
fn unsubscribe_sent_zero_packet_id_panics_in_debug_build() {
    // packet_id == 0 で unsubscribe_sent を呼ぶと debug ビルドで panic する。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.unsubscribe_sent(0, vec!["a/b".into()]);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "unsubscribe_sent must be called with a non-empty topic_filters list")]
fn unsubscribe_sent_empty_topic_filters_panics_in_debug_build() {
    // topic_filters が空だと MQTT v5.0 §3.10.3 [MQTT-3.10.3-2] /
    // MQTT v3.1.1 §3.10.3 [MQTT-3.10.3-2] 違反のため、debug ビルドで panic する。
    let mut session =
        Session::new_v5("client-1".into(), true).expect("セッションの作成に成功すること");
    session.unsubscribe_sent(1, vec![]);
}
