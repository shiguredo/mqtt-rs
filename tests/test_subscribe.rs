use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::state::subscribe::{SubscriptionEntry, SubscriptionManager};
use shiguredo_mqtt::v5::subscribe::{RetainHandling, Subscription};

// SUBSCRIBE/SUBACK の成功・失敗・shortage・reset は
// pbt/tests/prop_subscription.rs で検証する。
// ここには UNSUBACK の Reason Code 分岐と Subscription Identifier、
// packet_id=0 の無視など PBT 化しにくい契約を残す。

/// UNSUBACK の Reason Code 不足時はアクティブ購読を維持する。
#[test]
fn unsuback_reason_code_shortage_keeps_active() {
    let mut manager = SubscriptionManager::new();
    let sub_id = 10;
    let unsub_id = 20;

    manager.subscribe_sent(
        sub_id,
        vec![
            SubscriptionEntry::new("a/b", QoS::AtLeastOnce),
            SubscriptionEntry::new("c/d", QoS::ExactlyOnce),
        ],
    );
    manager
        .suback_received(sub_id, &[0x00, 0x02])
        .expect("SUBACK 受信で確認されること");
    assert_eq!(manager.active_count(), 2);

    manager.unsubscribe_sent(unsub_id, vec!["a/b".into(), "c/d".into()]);

    // v5.0 で reason code 数が購読解除要求数より少ない場合、
    // アクティブ一覧を維持したまま失敗として扱う。
    assert!(manager.unsuback_received(unsub_id, &[0x00]).is_none());
    assert_eq!(manager.active_count(), 2);
    assert!(manager.is_subscribed("a/b"));
    assert!(manager.is_subscribed("c/d"));
}

/// MQTT v3.1.1 の空 Reason Code UNSUBACK は全フィルタを解除する。
#[test]
fn unsuback_v311_empty_reason_codes_removes_active() {
    let mut manager = SubscriptionManager::new();
    let sub_id = 10;
    let unsub_id = 20;

    manager.subscribe_sent(sub_id, vec![SubscriptionEntry::new("a/b", QoS::AtMostOnce)]);
    manager
        .suback_received(sub_id, &[0x00])
        .expect("SUBACK 受信で確認されること");
    assert_eq!(manager.active_count(), 1);

    // MQTT v3.1.1 の UNSUBACK には per-filter の Reason Code がない。
    manager.unsubscribe_sent(unsub_id, vec!["a/b".into()]);
    let unsubscribed = manager
        .unsuback_received(unsub_id, &[])
        .expect("UNSUBACK 受信で解除されること");

    assert_eq!(unsubscribed.len(), 1);
    assert_eq!(manager.active_count(), 0);
    assert!(!manager.is_subscribed("a/b"));
}

/// MQTT v5.0 §3.11.3: 0x11 (No subscription existed) はローカル購読も削除する。
#[test]
fn unsuback_no_subscription_existed_removes_active() {
    let mut manager = SubscriptionManager::new();
    let sub_id = 10;
    let unsub_id = 20;

    let mut entry = SubscriptionEntry::new("a/b", QoS::AtMostOnce);
    entry.subscription_identifier = Some(42);
    manager.subscribe_sent(sub_id, vec![entry]);
    manager
        .suback_received(sub_id, &[0x00])
        .expect("SUBACK 受信で確認されること");
    assert_eq!(manager.active_count(), 1);
    assert_eq!(manager.find_by_subscription_identifier(42).len(), 1);

    manager.unsubscribe_sent(unsub_id, vec!["a/b".into()]);
    let unsubscribed = manager
        .unsuback_received(unsub_id, &[0x11])
        .expect("UNSUBACK 受信で解除されること");

    assert_eq!(unsubscribed, vec![String::from("a/b")]);
    assert_eq!(manager.active_count(), 0);
    assert!(!manager.is_subscribed("a/b"));
    assert!(manager.find_by_subscription_identifier(42).is_empty());
}

/// MQTT v5.0 §3.11.3: 0x80 以上は失敗のためローカル購読を維持する。
#[test]
fn unsuback_error_reason_codes_keep_active() {
    let mut manager = SubscriptionManager::new();
    let sub_id = 10;
    let unsub_id = 20;

    manager.subscribe_sent(
        sub_id,
        vec![
            SubscriptionEntry::new("a/b", QoS::AtMostOnce),
            SubscriptionEntry::new("c/d", QoS::AtMostOnce),
            SubscriptionEntry::new("e/f", QoS::AtMostOnce),
        ],
    );
    manager
        .suback_received(sub_id, &[0x00, 0x00, 0x00])
        .expect("SUBACK 受信で確認されること");
    assert_eq!(manager.active_count(), 3);

    manager.unsubscribe_sent(unsub_id, vec!["a/b".into(), "c/d".into(), "e/f".into()]);
    let unsubscribed = manager
        .unsuback_received(unsub_id, &[0x80, 0x87, 0x11])
        .expect("UNSUBACK を処理できること");

    assert_eq!(unsubscribed, vec![String::from("e/f")]);
    assert_eq!(manager.active_count(), 2);
    assert!(manager.is_subscribed("a/b"));
    assert!(manager.is_subscribed("c/d"));
    assert!(!manager.is_subscribed("e/f"));
}

/// packet_id = 0 の SUBSCRIBE は無視される。
#[test]
fn zero_packet_id_is_ignored() {
    let mut manager = SubscriptionManager::new();
    manager.subscribe_sent(0, vec![SubscriptionEntry::new("x", QoS::AtMostOnce)]);
    assert_eq!(manager.pending_subscribe_ids().len(), 0);
}

/// SubscriptionEntry → v5 Subscription 変換でオプションが保持される。
#[test]
fn subscription_entry_preserves_v5_options_and_identifier() {
    let mut entry = SubscriptionEntry::new("a/b", QoS::AtLeastOnce);
    entry.no_local = true;
    entry.retain_as_published = true;
    entry.retain_handling = RetainHandling::DoNotSendRetained;
    entry.subscription_identifier = Some(42);

    let subscription: Subscription = entry.into();
    assert_eq!(subscription.topic_filter, "a/b");
    assert_eq!(subscription.qos, QoS::AtLeastOnce);
    assert!(subscription.no_local);
    assert!(subscription.retain_as_published);
    assert_eq!(
        subscription.retain_handling,
        RetainHandling::DoNotSendRetained
    );
}

/// Subscription Identifier の逆引きが一致エントリを返す。
#[test]
fn subscription_identifier_lookup_returns_matching_entries() {
    let mut manager = SubscriptionManager::new();
    let mut entry1 = SubscriptionEntry::new("a/b", QoS::AtLeastOnce);
    entry1.subscription_identifier = Some(42);
    let mut entry2 = SubscriptionEntry::new("c/d", QoS::ExactlyOnce);
    entry2.subscription_identifier = Some(42);
    let mut entry3 = SubscriptionEntry::new("e/f", QoS::AtMostOnce);
    entry3.subscription_identifier = Some(7);

    manager.subscribe_sent(1, vec![entry1, entry2, entry3]);
    manager
        .suback_received(1, &[0x01, 0x02, 0x00])
        .expect("SUBACK 受信で確認されること");

    let found = manager.find_by_subscription_identifier(42);
    assert_eq!(found.len(), 2);
    assert!(found.iter().any(|e| e.topic_filter == "a/b"));
    assert!(found.iter().any(|e| e.topic_filter == "c/d"));

    let found = manager.find_by_subscription_identifier(7);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].topic_filter, "e/f");

    assert!(manager.find_by_subscription_identifier(99).is_empty());
}

/// UNSUBSCRIBE 後に Subscription Identifier の逆引きが消える。
#[test]
fn subscription_identifier_lookup_removed_on_unsubscribe() {
    let mut manager = SubscriptionManager::new();
    let mut entry = SubscriptionEntry::new("a/b", QoS::AtLeastOnce);
    entry.subscription_identifier = Some(42);

    manager.subscribe_sent(1, vec![entry]);
    manager
        .suback_received(1, &[0x01])
        .expect("SUBACK 受信で確認されること");
    assert_eq!(manager.find_by_subscription_identifier(42).len(), 1);

    manager.unsubscribe_sent(2, vec!["a/b".into()]);
    manager
        .unsuback_received(2, &[0x00])
        .expect("UNSUBACK 受信で解除されること");
    assert!(manager.find_by_subscription_identifier(42).is_empty());
}

/// 同一フィルタの再購読で Subscription Identifier が更新される。
#[test]
fn subscription_identifier_updated_on_resubscribe_with_different_id() {
    let mut manager = SubscriptionManager::new();

    let mut entry1 = SubscriptionEntry::new("a/b", QoS::AtLeastOnce);
    entry1.subscription_identifier = Some(42);
    manager.subscribe_sent(1, vec![entry1]);
    manager
        .suback_received(1, &[0x01])
        .expect("SUBACK 受信で確認されること");
    assert_eq!(manager.find_by_subscription_identifier(42).len(), 1);

    let mut entry2 = SubscriptionEntry::new("a/b", QoS::AtLeastOnce);
    entry2.subscription_identifier = Some(99);
    manager.subscribe_sent(2, vec![entry2]);
    manager
        .suback_received(2, &[0x01])
        .expect("SUBACK 受信で確認されること");

    assert!(manager.find_by_subscription_identifier(42).is_empty());

    let found = manager.find_by_subscription_identifier(99);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].topic_filter, "a/b");
    assert_eq!(found[0].subscription_identifier, Some(99));
}
