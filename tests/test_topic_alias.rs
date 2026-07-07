use shiguredo_mqtt::state::topic_alias::TopicAliasManager;

// 正常系の登録・解決・再利用・reset は pbt/tests/prop_topic_alias.rs で検証する。
// ここにはエラーパスと初期状態契約だけを残す。

/// 初期状態ではエイリアス最大値が 0 である。
#[test]
fn new_manager_has_no_maximum() {
    let manager = TopicAliasManager::new();
    assert_eq!(manager.own_maximum(), 0);
    assert_eq!(manager.peer_maximum(), 0);
    assert_eq!(manager.alias_count(), 0);
}

/// 未登録エイリアスへの空トピック解決は失敗する。
#[test]
fn empty_topic_without_registered_alias_fails() {
    let mut manager = TopicAliasManager::new();
    manager.set_own_maximum(16);
    assert!(manager.resolve_on_receive("", 1).is_none());
}

/// エイリアス未使用（0）かつトピック名が空の PUBLISH は不正。
#[test]
fn empty_topic_with_zero_alias_fails() {
    let mut manager = TopicAliasManager::new();
    manager.set_own_maximum(16);
    assert!(manager.resolve_on_receive("", 0).is_none());
}
