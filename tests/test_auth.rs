use shiguredo_mqtt::state::auth::{AuthState, AuthStateMachine};

#[test]
fn new_state_is_idle() {
    let auth = AuthStateMachine::new();
    assert_eq!(auth.state(), AuthState::Idle);
    assert!(!auth.is_authenticating());
    assert!(!auth.is_initial_authenticating());
    assert!(!auth.is_reauthenticating());
    assert!(!auth.is_authenticated());
}
