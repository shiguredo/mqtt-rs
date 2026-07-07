//! SubscriptionManager のプロパティベーステスト。

mod helpers;

use proptest::prelude::*;
use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::state::subscribe::{SubscriptionEntry, SubscriptionManager};

use helpers::qos_strategy;

/// 要求 QoS に対する成功 SUBACK Reason Code（Granted QoS）。
fn granted_reason_code(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0x00,
        QoS::AtLeastOnce => 0x01,
        QoS::ExactlyOnce => 0x02,
    }
}

fn topic_filter_strategy() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9]{1,8}/[a-zA-Z0-9]{1,8}"
}

proptest! {
    /// SUBSCRIBE → SUBACK → UNSUBSCRIBE → UNSUBACK のラウンドトリップ。
    ///
    /// Granted QoS は要求 QoS と一致させ、成功時だけ active に入ることを検証する。
    #[test]
    fn subscribe_unsubscribe_roundtrip(
        packet_id in 1u16..=65535u16,
        unsub_id in 1u16..=65535u16,
        topic_filter in topic_filter_strategy(),
        qos in qos_strategy(),
    ) {
        prop_assume!(packet_id != unsub_id);
        let mut manager = SubscriptionManager::new();

        manager.subscribe_sent(
            packet_id,
            vec![SubscriptionEntry::new(topic_filter.clone(), qos)],
        );
        prop_assert_eq!(manager.active_count(), 0);
        prop_assert!(manager.pending_subscribe_ids().contains(&packet_id));

        let granted = granted_reason_code(qos);
        let confirmed = manager
            .suback_received(packet_id, &[granted])
            .expect("SUBACK で確認されること");
        prop_assert_eq!(confirmed.len(), 1);
        prop_assert_eq!(confirmed[0].granted_qos, Some(qos));
        prop_assert_eq!(manager.active_count(), 1);
        prop_assert!(manager.is_subscribed(&topic_filter));
        prop_assert!(!manager.pending_subscribe_ids().contains(&packet_id));

        manager.unsubscribe_sent(unsub_id, vec![topic_filter.clone()]);
        prop_assert_eq!(manager.active_count(), 1);

        let unsubscribed = manager
            .unsuback_received(unsub_id, &[0x00])
            .expect("UNSUBACK で解除されること");
        prop_assert_eq!(unsubscribed.len(), 1);
        prop_assert_eq!(manager.active_count(), 0);
        prop_assert!(!manager.is_subscribed(&topic_filter));
    }

    /// 複数トピックの SUBACK で成功 (<= 0x02) だけがアクティブになり、
    /// 失敗 (>= 0x80) はアクティブに入らない。
    #[test]
    fn multi_topic_suback_activates_only_successes(
        packet_id in 1u16..=65535u16,
        entries in proptest::collection::vec(
            (topic_filter_strategy(), qos_strategy(), any::<bool>()),
            1..6,
        ),
    ) {
        // トピックフィルタの重複は is_subscribed の検証を曖昧にするため除外する。
        let mut seen = std::collections::BTreeSet::new();
        for (topic, _, _) in &entries {
            prop_assume!(seen.insert(topic.clone()));
        }

        let mut manager = SubscriptionManager::new();
        let subs: Vec<SubscriptionEntry> = entries
            .iter()
            .map(|(topic, qos, _)| SubscriptionEntry::new(topic.clone(), *qos))
            .collect();
        manager.subscribe_sent(packet_id, subs);

        let reason_codes: Vec<u8> = entries
            .iter()
            .map(|(_, qos, success)| {
                if *success {
                    granted_reason_code(*qos)
                } else {
                    0x80
                }
            })
            .collect();
        let confirmed = manager
            .suback_received(packet_id, &reason_codes)
            .expect("SUBACK で確認されること");

        let expected_successes: Vec<&(String, QoS, bool)> =
            entries.iter().filter(|(_, _, success)| *success).collect();
        prop_assert_eq!(confirmed.len(), expected_successes.len());
        prop_assert_eq!(manager.active_count(), expected_successes.len());

        for (topic, qos, success) in &entries {
            if *success {
                prop_assert!(manager.is_subscribed(topic));
                prop_assert!(
                    confirmed
                        .iter()
                        .any(|c| c.topic_filter == *topic && c.granted_qos == Some(*qos))
                );
            } else {
                prop_assert!(!manager.is_subscribed(topic));
            }
        }
    }

    /// SUBACK の Reason Code 数がサブスクリプション数より少ない場合、
    /// どのエントリもアクティブ化しない。
    ///
    /// pending は突合時に先に取り外されるため、不足時も pending からは消える。
    #[test]
    fn suback_reason_code_shortage_does_not_activate(
        packet_id in 1u16..=65535u16,
        entries in proptest::collection::vec(
            (topic_filter_strategy(), qos_strategy()),
            2..6,
        ),
        short_by in 1usize..5usize,
    ) {
        prop_assume!(short_by < entries.len());
        let mut manager = SubscriptionManager::new();
        let subs: Vec<SubscriptionEntry> = entries
            .iter()
            .map(|(topic, qos)| SubscriptionEntry::new(topic.clone(), *qos))
            .collect();
        manager.subscribe_sent(packet_id, subs);

        let codes: Vec<u8> = entries
            .iter()
            .take(entries.len() - short_by)
            .map(|(_, qos)| granted_reason_code(*qos))
            .collect();
        prop_assert!(manager.suback_received(packet_id, &codes).is_none());
        prop_assert_eq!(manager.active_count(), 0);
        prop_assert!(!manager.pending_subscribe_ids().contains(&packet_id));
        for (topic, _) in &entries {
            prop_assert!(!manager.is_subscribed(topic));
        }
    }

    /// リセット後はすべての状態がクリアされる。
    #[test]
    fn subscription_reset_clears_all(
        packet_ids in proptest::collection::vec(1u16..=65535u16, 0..20),
    ) {
        let mut unique = std::collections::BTreeSet::new();
        for id in &packet_ids {
            prop_assume!(unique.insert(*id));
        }

        let mut manager = SubscriptionManager::new();
        for &id in &packet_ids {
            manager.subscribe_sent(
                id,
                vec![SubscriptionEntry::new(format!("topic/{id}"), QoS::AtMostOnce)],
            );
        }
        manager.reset();
        prop_assert_eq!(manager.active_count(), 0);
        prop_assert!(manager.pending_subscribe_ids().is_empty());
        prop_assert!(manager.pending_unsubscribe_ids().is_empty());
    }
}
