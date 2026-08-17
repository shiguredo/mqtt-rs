//! PacketIdManager のプロパティベーステスト。

use shiguredo_mqtt::state::packet_id::PacketIdManager;

/// 割り当てた識別子はすべて一意であり、解放後に再利用される。
/// 各操作後に in_use_count がモデル集合の大きさと一致することも検証する。
#[test]
fn packet_id_uniqueness() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let ops_len = noprop::sample_usize_in(ctx, 0..=200);
        let mut manager = PacketIdManager::new();
        let mut allocated = std::collections::BTreeSet::new();

        for _ in 0..ops_len {
            let op = noprop::sample_bool(ctx);
            let seed_value = noprop::sample_u16(ctx);
            if !op {
                if let Some(id) = manager.allocate() {
                    assert!(
                        allocated.insert(id),
                        "重複したパケット識別子が割り当てられた"
                    );
                }
            } else if !allocated.is_empty() {
                let ids: Vec<u16> = allocated.iter().copied().collect();
                let idx = (seed_value as usize) % ids.len();
                let id = ids[idx];
                allocated.remove(&id);
                manager.release(id);
            }
            assert_eq!(manager.in_use_count(), allocated.len());
            assert_eq!(manager.is_available(), allocated.len() < 65535);
        }
        Ok(())
    })?;

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}

/// 割り当てと解放を繰り返しても不変条件が保たれる。
#[test]
fn packet_id_allocate_release_roundtrip() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let alloc_count = noprop::sample_usize_in(ctx, 0..=100);
        let release_count = noprop::sample_usize_in(ctx, 0..=100);
        let mut manager = PacketIdManager::new();
        let mut allocated = Vec::new();

        for _ in 0..alloc_count {
            if let Some(id) = manager.allocate() {
                allocated.push(id);
            }
        }
        if alloc_count > 0 {
            assert!(manager.in_use_count() > 0);
            assert!(manager.is_available());
        }

        for i in 0..release_count {
            if allocated.is_empty() {
                break;
            }
            let id = allocated.remove(0);
            manager.release(id);
            assert_eq!(manager.in_use_count(), alloc_count - (i + 1));
        }
        Ok(())
    })?;

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}

/// 解放したパケット識別子は再度割り当てられ、
/// 解放していない識別子は再割り当てされない。
#[test]
fn packet_id_released_ids_are_reusable() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let mask_len = noprop::sample_usize_in(ctx, 1..=100);
        let mut manager = PacketIdManager::new();

        let ids: Vec<u16> = (0..mask_len)
            .map(|_| {
                manager
                    .allocate()
                    .expect("パケット識別子を割り当てられること")
            })
            .collect();
        let mask: Vec<bool> = (0..mask_len).map(|_| noprop::sample_bool(ctx)).collect();
        let released: std::collections::BTreeSet<u16> = ids
            .iter()
            .zip(&mask)
            .filter(|(_, release)| **release)
            .map(|(id, _)| *id)
            .collect();
        let kept: std::collections::BTreeSet<u16> = ids
            .iter()
            .copied()
            .filter(|id| !released.contains(id))
            .collect();
        for &id in &released {
            manager.release(id);
        }

        let mut reallocated = std::collections::BTreeSet::new();
        for _ in 0..released.len() {
            let id = manager
                .allocate()
                .expect("解放済みのパケット識別子が再割り当てされること");
            assert!(
                !kept.contains(&id),
                "使用中のパケット識別子が再割り当てされた"
            );
            assert!(
                reallocated.insert(id),
                "重複したパケット識別子が割り当てられた"
            );
        }
        assert_eq!(reallocated, released);
        Ok(())
    })?;

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}

/// リセット後は使用中が空になり、1 から再割り当てできる。
#[test]
fn packet_id_reset_clears_and_restarts() -> noprop::TestResult {
    let seed = noprop::seed_from_env_or_time("MQTT_PBT_SEED")?;
    let mut runner = noprop::Runner::new(seed);

    runner.run(256, |ctx| {
        let mask_len = noprop::sample_usize_in(ctx, 1..=50);
        let mut manager = PacketIdManager::new();
        let ids: Vec<u16> = (0..mask_len)
            .map(|_| {
                manager
                    .allocate()
                    .expect("パケット識別子を割り当てられること")
            })
            .collect();
        for id in &ids {
            if noprop::sample_bool(ctx) {
                manager.release(*id);
            }
        }
        manager.reset();
        assert_eq!(manager.in_use_count(), 0);
        assert!(manager.is_available());
        assert_eq!(manager.allocate(), Some(1));
        Ok(())
    })?;

    // ジェネレータは valid-by-construction であり、ケース棄却が発生しないことの検証。
    assert_eq!(
        runner.stats().rejected_cases,
        0,
        "ジェネレータが valid-by-construction であること\n{runner}"
    );
    Ok(())
}
