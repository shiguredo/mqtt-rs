use shiguredo_mqtt::state::packet_id::PacketIdManager;

/// 二重解放は再利用候補を 1 回分しか増やさない。
#[test]
fn double_release_is_noop() {
    let mut manager = PacketIdManager::new();
    let id = manager.allocate().expect("識別子を割り当てられること");
    manager.release(id);
    manager.release(id);
    assert_eq!(manager.allocate(), Some(id));
}
