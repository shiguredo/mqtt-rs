use shiguredo_mqtt::state::flow_control::{FlowControl, FlowControlError};

// 正常系の quota 消費・回復・reset は pbt/tests/prop_flow_control.rs で検証する。
// ここにはエラーパスと固定デフォルト契約だけを残す。

/// デフォルト状態の不変条件。固定値の契約確認であり PBT 向きではない。
#[test]
fn default_state_contracts() {
    let fc = FlowControl::new();
    assert!(fc.can_send());
    assert_eq!(fc.available(), 65535);
    assert_eq!(fc.own_receive_maximum(), 65535);
    assert_eq!(fc.incoming_available(), 65535);
    assert!(fc.can_receive());
    assert!(!fc.is_initialized());
}

/// MQTT v5.0 §3.2.2.3.3: Receive Maximum に 0 は Protocol Error。
#[test]
fn set_receive_maximum_rejects_zero() {
    let mut fc = FlowControl::new();
    assert_eq!(
        fc.set_receive_maximum(0),
        Err(FlowControlError::InvalidReceiveMaximum)
    );
    assert_eq!(fc.receive_maximum(), 65535);
}

/// MQTT v5.0 §3.1.2.11.3: Receive Maximum に 0 は Protocol Error。
#[test]
fn set_own_receive_maximum_rejects_zero() {
    let mut fc = FlowControl::new();
    assert_eq!(
        fc.set_own_receive_maximum(0),
        Err(FlowControlError::InvalidReceiveMaximum)
    );
    assert_eq!(fc.own_receive_maximum(), 65535);
}

/// set_receive_maximum 成功時に initialized フラグが立つこと。
#[test]
fn initialized_flag_is_set() {
    let mut fc = FlowControl::new();
    assert!(!fc.is_initialized());
    fc.set_receive_maximum(50)
        .expect("Receive Maximum を設定できること");
    assert!(fc.is_initialized());
}
