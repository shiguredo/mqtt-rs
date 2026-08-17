//! SubscriptionManager のプロパティベーステスト。

mod helpers;

use noprop::TestCaseContext;
use shiguredo_mqtt::codec::qos::QoS;
use shiguredo_mqtt::state::subscribe::{SubscriptionEntry, SubscriptionManager};

/// 要求 QoS に対する成功 SUBACK Reason Code（Granted QoS）。
fn granted_reason_code(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0x00,
        QoS::AtLeastOnce => 0x01,
        QoS::ExactlyOnce => 0x02,
    }
}

/// 英数字のみで構成される文字列を valid-by-construction で生成する。
fn sample_alnum(ctx: &mut TestCaseContext, min: usize, max: usize) -> String {
    let len = noprop::sample_usize_in(ctx, min..=max);
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        let idx = noprop::sample_usize_in(ctx, 0..62);
        let c = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[idx] as char;
        s.push(c);
    }
    s
}

/// トピックフィルターサンプラ。`[a-zA-Z0-9]{1,8}/[a-zA-Z0-9]{1,8}` 相当。
fn sample_topic_filter(ctx: &mut TestCaseContext) -> String {
    format!("{}/{}", sample_alnum(ctx, 1, 8), sample_alnum(ctx, 1, 8))
}

/// SUBSCRIBE → SUBACK → UNSUBSCRIBE → UNSUBACK のラウンドトリップ。
///
/// Granted QoS は要求 QoS と一致させ、成功時だけ active に入ることを検証する。
#[test]
fn subscribe_unsubscribe_roundtrip() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet_id = noprop::sample_usize_in(ctx, 1..=65535) as u16;
        // packet_id と異なる UNSUBSCRIBE 用識別子を引く。衝突確率は 1/65535 程度であり、
        // max_attempts=4 で枯渇する確率は実質ゼロである。
        let unsub_id = noprop::sample_with_rejection(ctx, 4, |ctx| {
            let id = noprop::sample_usize_in(ctx, 1..=65535) as u16;
            (id != packet_id).then_some(id)
        });
        let topic_filter = sample_topic_filter(ctx);
        let qos = helpers::sample_qos(ctx);

        let mut manager = SubscriptionManager::new();

        manager.subscribe_sent(
            packet_id,
            vec![SubscriptionEntry::new(topic_filter.clone(), qos)],
        );
        assert_eq!(manager.active_count(), 0);
        assert!(manager.pending_subscribe_ids().contains(&packet_id));

        let granted = granted_reason_code(qos);
        let confirmed = manager
            .suback_received(packet_id, &[granted])
            .expect("SUBACK で確認されること");
        assert_eq!(confirmed.len(), 1);
        assert_eq!(confirmed[0].granted_qos, Some(qos));
        assert_eq!(manager.active_count(), 1);
        assert!(manager.is_subscribed(&topic_filter));
        assert!(!manager.pending_subscribe_ids().contains(&packet_id));

        manager.unsubscribe_sent(unsub_id, vec![topic_filter.clone()]);
        assert_eq!(manager.active_count(), 1);

        let unsubscribed = manager
            .unsuback_received(unsub_id, &[0x00])
            .expect("UNSUBACK で解除されること");
        assert_eq!(unsubscribed.len(), 1);
        assert_eq!(manager.active_count(), 0);
        assert!(!manager.is_subscribed(&topic_filter));
        Ok(())
    })?;
    Ok(())
}

/// 複数トピックの SUBACK で成功 (<= 0x02) だけがアクティブになり、
/// 失敗 (>= 0x80) はアクティブに入らない。
#[test]
fn multi_topic_suback_activates_only_successes() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet_id = noprop::sample_usize_in(ctx, 1..=65535) as u16;
        let entries_len = noprop::sample_usize_in(ctx, 1..=5);
        let mut entries: Vec<(String, QoS, bool)> = Vec::with_capacity(entries_len);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..entries_len {
            let topic = sample_topic_filter(ctx);
            // トピックフィルタの重複は is_subscribed の検証を曖昧にするため除外する。
            if !seen.insert(topic.clone()) {
                ctx.reject_case();
            }
            let qos = helpers::sample_qos(ctx);
            let success = noprop::sample_bool(ctx);
            entries.push((topic, qos, success));
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
        assert_eq!(confirmed.len(), expected_successes.len());
        assert_eq!(manager.active_count(), expected_successes.len());

        for (topic, qos, success) in &entries {
            if *success {
                assert!(manager.is_subscribed(topic));
                assert!(
                    confirmed
                        .iter()
                        .any(|c| c.topic_filter == *topic && c.granted_qos == Some(*qos))
                );
            } else {
                assert!(!manager.is_subscribed(topic));
            }
        }
        Ok(())
    })?;
    Ok(())
}

/// SUBACK の Reason Code 数がサブスクリプション数より少ない場合、
/// どのエントリもアクティブ化しない。
///
/// pending は突合時に先に取り外されるため、不足時も pending からは消える。
#[test]
fn suback_reason_code_shortage_does_not_activate() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let packet_id = noprop::sample_usize_in(ctx, 1..=65535) as u16;
        let entries_len = noprop::sample_usize_in(ctx, 2..=5);
        // Reason Code 数を entries_len 未満に valid-by-construction で制約する（棄却なし）。
        let short_by = noprop::sample_usize_in(ctx, 1..=entries_len - 1);
        let mut entries: Vec<(String, QoS)> = Vec::with_capacity(entries_len);
        for _ in 0..entries_len {
            entries.push((sample_topic_filter(ctx), helpers::sample_qos(ctx)));
        }

        let mut manager = SubscriptionManager::new();
        let subs: Vec<SubscriptionEntry> = entries
            .iter()
            .map(|(topic, qos)| SubscriptionEntry::new(topic.clone(), *qos))
            .collect();
        manager.subscribe_sent(packet_id, subs);

        let codes: Vec<u8> = entries
            .iter()
            .take(entries_len - short_by)
            .map(|(_, qos)| granted_reason_code(*qos))
            .collect();
        assert!(manager.suback_received(packet_id, &codes).is_none());
        assert_eq!(manager.active_count(), 0);
        assert!(!manager.pending_subscribe_ids().contains(&packet_id));
        for (topic, _) in &entries {
            assert!(!manager.is_subscribed(topic));
        }
        Ok(())
    })?;
    Ok(())
}

/// リセット後はすべての状態がクリアされる。
#[test]
fn subscription_reset_clears_all() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let ids_len = noprop::sample_usize_in(ctx, 0..=20);
        let mut packet_ids = Vec::with_capacity(ids_len);
        let mut unique = std::collections::BTreeSet::new();
        for _ in 0..ids_len {
            let id = noprop::sample_usize_in(ctx, 1..=65535) as u16;
            // pending 管理は識別子の一意性を前提とするため、重複時はケースごと棄却する。
            if !unique.insert(id) {
                ctx.reject_case();
            }
            packet_ids.push(id);
        }

        let mut manager = SubscriptionManager::new();
        for &id in &packet_ids {
            manager.subscribe_sent(
                id,
                vec![SubscriptionEntry::new(
                    format!("topic/{id}"),
                    QoS::AtMostOnce,
                )],
            );
        }
        manager.reset();
        assert_eq!(manager.active_count(), 0);
        assert!(manager.pending_subscribe_ids().is_empty());
        assert!(manager.pending_unsubscribe_ids().is_empty());
        Ok(())
    })?;
    Ok(())
}
