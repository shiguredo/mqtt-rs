use shiguredo_mqtt::state::keep_alive::KeepAlive;

// 正常系の間隔判定・タイムアウト・reset は pbt/tests/prop_keep_alive.rs で検証する。
// ここには setter の固定値契約だけを残す。

/// set_keep_alive が値を書き換えること。
#[test]
fn set_keep_alive_updates_value() {
    let mut ka = KeepAlive::new(60);
    assert_eq!(ka.keep_alive_secs(), 60);
    ka.set_keep_alive(120);
    assert_eq!(ka.keep_alive_secs(), 120);
}
