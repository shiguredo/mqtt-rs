//! TopicAliasManager のプロパティベーステスト。

use proptest::prelude::*;
use shiguredo_mqtt::state::topic_alias::TopicAliasManager;

/// 受信用の登録操作。
#[derive(Debug, Clone)]
struct ReceiveOp {
    topic: String,
    alias: u16,
}

fn receive_op_strategy(max: u16) -> impl Strategy<Value = ReceiveOp> {
    ("[a-zA-Z0-9/]{1,16}", 1u16..=max).prop_map(|(topic, alias)| ReceiveOp { topic, alias })
}

proptest! {
    /// 受信時のトピックエイリアス解決: 登録 → 空トピックでの解決が一貫する。
    ///
    /// MQTT v5.0 §3.3.2.3.4:
    /// Topic Alias を含む PUBLISH は、以降の空トピック PUBLISH で同じエイリアスを使える。
    #[test]
    fn alias_resolve_consistency(
        max in 1u16..=32u16,
        alias in 1u16..=32u16,
        topic_name in "[a-zA-Z0-9/]{1,32}",
    ) {
        prop_assume!(alias <= max);
        let mut manager = TopicAliasManager::new();
        manager.set_own_maximum(max);

        let resolved = manager
            .resolve_on_receive(&topic_name, alias)
            .expect("トピック名を解決できること");
        prop_assert_eq!(resolved, topic_name.clone());

        let from_alias = manager
            .resolve_on_receive("", alias)
            .expect("エイリアスから解決できること");
        prop_assert_eq!(from_alias, topic_name);
    }

    /// 同じエイリアスへの再登録は上書きされ、以降の解決は最新トピックを返す。
    #[test]
    fn receive_alias_overwrite_keeps_latest_mapping(
        max in 1u16..=16u16,
        alias in 1u16..=16u16,
        first in "[a-zA-Z0-9]{1,16}",
        second in "[a-zA-Z0-9]{1,16}",
    ) {
        prop_assume!(alias <= max);
        prop_assume!(first != second);
        let mut manager = TopicAliasManager::new();
        manager.set_own_maximum(max);

        manager
            .resolve_on_receive(&first, alias)
            .expect("初回登録に成功すること");
        manager
            .resolve_on_receive(&second, alias)
            .expect("上書き登録に成功すること");

        let resolved = manager
            .resolve_on_receive("", alias)
            .expect("上書き後のエイリアス解決に成功すること");
        prop_assert_eq!(resolved, second);
    }

    /// 受信側のランダムな登録列に対して、エイリアス → トピックのモデルと一致する。
    #[test]
    fn receive_mapping_matches_model(
        (max, ops) in (1u16..=8u16).prop_flat_map(|max| {
            proptest::collection::vec(receive_op_strategy(max), 1..20)
                .prop_map(move |ops| (max, ops))
        }),
    ) {
        let mut manager = TopicAliasManager::new();
        manager.set_own_maximum(max);
        let mut model = std::collections::BTreeMap::<u16, String>::new();

        for op in ops {
            let resolved = manager
                .resolve_on_receive(&op.topic, op.alias)
                .expect("own_maximum 内の登録は成功すること");
            prop_assert_eq!(resolved, op.topic.clone());
            model.insert(op.alias, op.topic);
        }

        for (alias, topic) in &model {
            let resolved = manager
                .resolve_on_receive("", *alias)
                .expect("モデル上のエイリアスは解決できること");
            prop_assert_eq!(&resolved, topic);
        }
        prop_assert_eq!(manager.alias_count(), model.len());
    }

    /// エイリアスが own_maximum を超えると解決に失敗する。
    #[test]
    fn alias_exceeding_maximum_returns_none(
        max in 1u16..=100u16,
        alias in 1u16..=65535u16,
        topic in prop_oneof![Just(String::new()), Just("t".to_string())],
    ) {
        prop_assume!(alias > max);
        let mut manager = TopicAliasManager::new();
        manager.set_own_maximum(max);
        prop_assert!(manager.resolve_on_receive(&topic, alias).is_none());
    }

    /// 送信用エイリアス: 登録したトピックは find で同じエイリアスを返し、
    /// 再登録でも同じ値を再利用する（既存マッピングを破壊しない）。
    #[test]
    fn send_alias_register_find_and_reuse(
        max in 1u16..=16u16,
        topics in proptest::collection::vec("[a-zA-Z0-9]{1,12}", 1..8),
    ) {
        let mut manager = TopicAliasManager::new();
        manager.set_peer_maximum(max);

        let mut model = std::collections::BTreeMap::<String, u16>::new();
        for topic in &topics {
            if let Some(existing) = model.get(topic) {
                let alias = manager
                    .register_for_send(topic)
                    .expect("既存トピックは再登録できること");
                prop_assert_eq!(alias, *existing);
                prop_assert_eq!(manager.find_alias_for_topic(topic), Some(*existing));
                continue;
            }

            if model.len() >= max as usize {
                // 枠が埋まっているときは既存マッピングを壊さず None。
                prop_assert_eq!(manager.register_for_send(topic), None);
                prop_assert_eq!(manager.find_alias_for_topic(topic), None);
                // 既登録のマッピングは保持される。
                for (known_topic, known_alias) in &model {
                    prop_assert_eq!(
                        manager.find_alias_for_topic(known_topic),
                        Some(*known_alias)
                    );
                }
                continue;
            }

            let alias = manager
                .register_for_send(topic)
                .expect("空きがある間は割り当てられること");
            prop_assert!((1..=max).contains(&alias));
            prop_assert!(!model.values().any(|a| *a == alias));
            model.insert(topic.clone(), alias);
            prop_assert_eq!(manager.find_alias_for_topic(topic), Some(alias));
        }
    }

    /// peer_maximum が 0 の場合は送信用エイリアスが割り当てられない。
    #[test]
    fn no_send_alias_when_peer_max_zero(
        topic_name in "[a-zA-Z0-9]{1,32}",
    ) {
        let mut manager = TopicAliasManager::new();
        prop_assert_eq!(manager.peer_maximum(), 0);
        prop_assert_eq!(manager.register_for_send(&topic_name), None);
        prop_assert_eq!(manager.find_alias_for_topic(&topic_name), None);
    }

    /// 受信側マッピングは送信用エイリアスとして再利用されない。
    #[test]
    fn received_alias_is_not_reused_for_send(
        max in 1u16..=16u16,
        alias in 1u16..=16u16,
        topic in "[a-zA-Z0-9]{1,16}",
    ) {
        prop_assume!(alias <= max);
        let mut manager = TopicAliasManager::new();
        manager.set_own_maximum(max);
        manager.set_peer_maximum(max);
        manager
            .resolve_on_receive(&topic, alias)
            .expect("受信マッピングを登録できること");
        prop_assert_eq!(manager.find_alias_for_topic(&topic), None);
    }

    /// reset_mappings は最大値を維持したままマッピングだけ消す。
    /// reset は最大値も含めてすべて消す。
    #[test]
    fn reset_variants_clear_expected_state(
        own_max in 1u16..=16u16,
        peer_max in 1u16..=16u16,
        alias in 1u16..=16u16,
        topic in "[a-zA-Z0-9/]{1,16}",
        full_reset in any::<bool>(),
    ) {
        prop_assume!(alias <= own_max);
        let mut manager = TopicAliasManager::new();
        manager.set_own_maximum(own_max);
        manager.set_peer_maximum(peer_max);
        manager
            .resolve_on_receive(&topic, alias)
            .expect("トピック名を解決できること");
        let send_alias = manager.register_for_send(&topic);
        prop_assert!(manager.alias_count() > 0);

        if full_reset {
            manager.reset();
            prop_assert_eq!(manager.alias_count(), 0);
            prop_assert_eq!(manager.own_maximum(), 0);
            prop_assert_eq!(manager.peer_maximum(), 0);
            prop_assert!(manager.resolve_on_receive("", alias).is_none());
            prop_assert_eq!(manager.find_alias_for_topic(&topic), None);
        } else {
            manager.reset_mappings();
            prop_assert_eq!(manager.alias_count(), 0);
            prop_assert_eq!(manager.own_maximum(), own_max);
            prop_assert_eq!(manager.peer_maximum(), peer_max);
            prop_assert!(manager.resolve_on_receive("", alias).is_none());
            if send_alias.is_some() {
                prop_assert_eq!(manager.find_alias_for_topic(&topic), None);
            }
        }
    }
}
