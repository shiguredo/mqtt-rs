//! TopicAliasManager のプロパティベーステスト。

use noprop::TestCaseContext;
use shiguredo_mqtt::state::topic_alias::TopicAliasManager;

/// 英数字のみで構成される文字列を valid-by-construction で生成する。
///
/// 元の proptest 戦略（`[a-zA-Z0-9/]` 等）の文字集合を英数字に絞り、
/// 棄却なしで生成する。エイリアス解決のテストは文字内容に依存しないため、
/// 分布を厳密に保つ必要はない。
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

/// 受信用の登録操作。
#[derive(Debug, Clone)]
struct ReceiveOp {
    topic: String,
    alias: u16,
}

fn sample_receive_op(ctx: &mut TestCaseContext, max: u16) -> ReceiveOp {
    let topic = sample_alnum(ctx, 1, 16);
    let alias = noprop::sample_usize_in(ctx, 1..=max as usize) as u16;
    ReceiveOp { topic, alias }
}

/// 受信時のトピックエイリアス解決: 登録 → 空トピックでの解決が一貫する。
///
/// MQTT v5.0 §3.3.2.3.4:
/// Topic Alias を含む PUBLISH は、以降の空トピック PUBLISH で同じエイリアスを使える。
#[test]
fn alias_resolve_consistency() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let max = noprop::sample_usize_in(ctx, 1..=32) as u16;
        // own_maximum 内のエイリアスを valid-by-construction で生成する（棄却なし）。
        let alias = noprop::sample_usize_in(ctx, 1..=max as usize) as u16;
        let topic_name = sample_alnum(ctx, 1, 32);

        let mut manager = TopicAliasManager::new();
        manager.set_own_maximum(max);

        let resolved = manager
            .resolve_on_receive(&topic_name, alias)
            .expect("トピック名を解決できること");
        assert_eq!(resolved, topic_name.clone());

        let from_alias = manager
            .resolve_on_receive("", alias)
            .expect("エイリアスから解決できること");
        assert_eq!(from_alias, topic_name);
        Ok(())
    })?;
    Ok(())
}

/// 同じエイリアスへの再登録は上書きされ、以降の解決は最新トピックを返す。
#[test]
fn receive_alias_overwrite_keeps_latest_mapping() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let max = noprop::sample_usize_in(ctx, 1..=16) as u16;
        let alias = noprop::sample_usize_in(ctx, 1..=max as usize) as u16;
        let first = sample_alnum(ctx, 1, 16);
        // 2 つ目のトピックを first と異なる値で引く。長さ 16 の英数字列どうしの
        // 衝突確率は 1/62^16 程度であり、max_attempts=4 で枯渇する確率は実質ゼロである。
        let second = noprop::sample_with_rejection(ctx, 4, |ctx| {
            let s = sample_alnum(ctx, 1, 16);
            (s != first).then_some(s)
        });

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
        assert_eq!(resolved, second);
        Ok(())
    })?;
    Ok(())
}

/// 受信側のランダムな登録列に対して、エイリアス → トピックのモデルと一致する。
#[test]
fn receive_mapping_matches_model() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let max = noprop::sample_usize_in(ctx, 1..=8) as u16;
        let ops_len = noprop::sample_usize_in(ctx, 1..=20);

        let mut manager = TopicAliasManager::new();
        manager.set_own_maximum(max);
        let mut model = std::collections::BTreeMap::<u16, String>::new();

        for _ in 0..ops_len {
            let op = sample_receive_op(ctx, max);
            let resolved = manager
                .resolve_on_receive(&op.topic, op.alias)
                .expect("own_maximum 内の登録は成功すること");
            assert_eq!(resolved, op.topic.clone());
            model.insert(op.alias, op.topic);
        }

        for (alias, topic) in &model {
            let resolved = manager
                .resolve_on_receive("", *alias)
                .expect("モデル上のエイリアスは解決できること");
            assert_eq!(&resolved, topic);
        }
        assert_eq!(manager.alias_count(), model.len());
        Ok(())
    })?;
    Ok(())
}

/// エイリアスが own_maximum を超えると解決に失敗する。
#[test]
fn alias_exceeding_maximum_returns_none() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let max = noprop::sample_usize_in(ctx, 1..=100) as u16;
        // own_maximum を超えるエイリアスを valid-by-construction で生成する（棄却なし）。
        let alias = noprop::sample_usize_in(ctx, (max as usize) + 1..=65535) as u16;
        let topic = noprop::sample_choice(ctx, &["".to_string(), "t".to_string()]);

        let mut manager = TopicAliasManager::new();
        manager.set_own_maximum(max);
        assert!(manager.resolve_on_receive(&topic, alias).is_none());
        Ok(())
    })?;
    Ok(())
}

/// 送信用エイリアス: 登録したトピックは find で同じエイリアスを返し、
/// 再登録でも同じ値を再利用する（既存マッピングを破壊しない）。
#[test]
fn send_alias_register_find_and_reuse() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let max = noprop::sample_usize_in(ctx, 1..=16) as u16;
        let topics_len = noprop::sample_usize_in(ctx, 1..=8);
        let topics: Vec<String> = (0..topics_len).map(|_| sample_alnum(ctx, 1, 12)).collect();

        let mut manager = TopicAliasManager::new();
        manager.set_peer_maximum(max);

        let mut model = std::collections::BTreeMap::<String, u16>::new();
        for topic in &topics {
            if let Some(existing) = model.get(topic) {
                let alias = manager
                    .register_for_send(topic)
                    .expect("既存トピックは再登録できること");
                assert_eq!(alias, *existing);
                assert_eq!(manager.find_alias_for_topic(topic), Some(*existing));
                continue;
            }

            if model.len() >= max as usize {
                // 枠が埋まっているときは既存マッピングを壊さず None。
                assert_eq!(manager.register_for_send(topic), None);
                assert_eq!(manager.find_alias_for_topic(topic), None);
                // 既登録のマッピングは保持される。
                for (known_topic, known_alias) in &model {
                    assert_eq!(
                        manager.find_alias_for_topic(known_topic),
                        Some(*known_alias)
                    );
                }
                continue;
            }

            let alias = manager
                .register_for_send(topic)
                .expect("空きがある間は割り当てられること");
            assert!((1..=max).contains(&alias));
            assert!(!model.values().any(|a| *a == alias));
            model.insert(topic.clone(), alias);
            assert_eq!(manager.find_alias_for_topic(topic), Some(alias));
        }
        Ok(())
    })?;
    Ok(())
}

/// peer_maximum が 0 の場合は送信用エイリアスが割り当てられない。
#[test]
fn no_send_alias_when_peer_max_zero() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let topic_name = sample_alnum(ctx, 1, 32);
        let mut manager = TopicAliasManager::new();
        assert_eq!(manager.peer_maximum(), 0);
        assert_eq!(manager.register_for_send(&topic_name), None);
        assert_eq!(manager.find_alias_for_topic(&topic_name), None);
        Ok(())
    })?;
    Ok(())
}

/// 受信側マッピングは送信用エイリアスとして再利用されない。
#[test]
fn received_alias_is_not_reused_for_send() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let max = noprop::sample_usize_in(ctx, 1..=16) as u16;
        let alias = noprop::sample_usize_in(ctx, 1..=max as usize) as u16;
        let topic = sample_alnum(ctx, 1, 16);

        let mut manager = TopicAliasManager::new();
        manager.set_own_maximum(max);
        manager.set_peer_maximum(max);
        manager
            .resolve_on_receive(&topic, alias)
            .expect("受信マッピングを登録できること");
        assert_eq!(manager.find_alias_for_topic(&topic), None);
        Ok(())
    })?;
    Ok(())
}

/// reset_mappings は最大値を維持したままマッピングだけ消す。
/// reset は最大値も含めてすべて消す。
#[test]
fn reset_variants_clear_expected_state() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let own_max = noprop::sample_usize_in(ctx, 1..=16) as u16;
        let peer_max = noprop::sample_usize_in(ctx, 1..=16) as u16;
        // own_maximum 内のエイリアスを valid-by-construction で生成する（棄却なし）。
        let alias = noprop::sample_usize_in(ctx, 1..=own_max as usize) as u16;
        let topic = sample_alnum(ctx, 1, 16);
        let full_reset = noprop::sample_bool(ctx);

        let mut manager = TopicAliasManager::new();
        manager.set_own_maximum(own_max);
        manager.set_peer_maximum(peer_max);
        manager
            .resolve_on_receive(&topic, alias)
            .expect("トピック名を解決できること");
        let send_alias = manager.register_for_send(&topic);
        assert!(manager.alias_count() > 0);

        if full_reset {
            manager.reset();
            assert_eq!(manager.alias_count(), 0);
            assert_eq!(manager.own_maximum(), 0);
            assert_eq!(manager.peer_maximum(), 0);
            assert!(manager.resolve_on_receive("", alias).is_none());
            assert_eq!(manager.find_alias_for_topic(&topic), None);
        } else {
            manager.reset_mappings();
            assert_eq!(manager.alias_count(), 0);
            assert_eq!(manager.own_maximum(), own_max);
            assert_eq!(manager.peer_maximum(), peer_max);
            assert!(manager.resolve_on_receive("", alias).is_none());
            if send_alias.is_some() {
                assert_eq!(manager.find_alias_for_topic(&topic), None);
            }
        }
        Ok(())
    })?;
    Ok(())
}
