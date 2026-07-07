//! PacketIdManager のプロパティベーステスト。

use proptest::prelude::*;
use shiguredo_mqtt::state::packet_id::PacketIdManager;

proptest! {
    /// 割り当てた識別子はすべて一意であり、解放後に再利用される。
    /// 各操作後に in_use_count がモデル集合の大きさと一致することも検証する。
    #[test]
    fn packet_id_uniqueness(ops in proptest::collection::vec(
        (0u8..=1u8, any::<u16>()), 0..200
    )) {
        let mut manager = PacketIdManager::new();
        let mut allocated = std::collections::BTreeSet::new();

        for (op, seed) in &ops {
            if *op == 0 {
                if let Some(id) = manager.allocate() {
                    prop_assert!(allocated.insert(id), "重複したパケット識別子が割り当てられた");
                }
            } else if !allocated.is_empty() {
                let ids: Vec<u16> = allocated.iter().copied().collect();
                let idx = (*seed as usize) % ids.len();
                let id = ids[idx];
                allocated.remove(&id);
                manager.release(id);
            }
            prop_assert_eq!(manager.in_use_count(), allocated.len());
            prop_assert_eq!(manager.is_available(), allocated.len() < 65535);
        }
    }

    /// 割り当てと解放を繰り返しても不変条件が保たれる。
    #[test]
    fn packet_id_allocate_release_roundtrip(
        (alloc_count, release_count) in (0usize..100usize, 0usize..100usize)
    ) {
        let mut manager = PacketIdManager::new();
        let mut allocated = Vec::new();

        for _ in 0..alloc_count {
            if let Some(id) = manager.allocate() {
                allocated.push(id);
            }
        }
        if alloc_count > 0 {
            prop_assert!(manager.in_use_count() > 0);
            prop_assert!(manager.is_available());
        }

        for i in 0..release_count {
            if allocated.is_empty() {
                break;
            }
            let id = allocated.remove(0);
            manager.release(id);
            prop_assert_eq!(manager.in_use_count(), alloc_count - (i + 1));
        }
    }

    /// 解放したパケット識別子は再度割り当てられ、
    /// 解放していない識別子は再割り当てされない。
    #[test]
    fn packet_id_released_ids_are_reusable(
        mask in proptest::collection::vec(any::<bool>(), 1..100)
    ) {
        let mut manager = PacketIdManager::new();

        let ids: Vec<u16> = mask
            .iter()
            .map(|_| manager.allocate().expect("パケット識別子を割り当てられること"))
            .collect();
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
            prop_assert!(!kept.contains(&id), "使用中のパケット識別子が再割り当てされた");
            prop_assert!(reallocated.insert(id), "重複したパケット識別子が割り当てられた");
        }
        prop_assert_eq!(reallocated, released);
    }

    /// リセット後は使用中が空になり、1 から再割り当てできる。
    #[test]
    fn packet_id_reset_clears_and_restarts(
        mask in proptest::collection::vec(any::<bool>(), 1..50)
    ) {
        let mut manager = PacketIdManager::new();
        let ids: Vec<u16> = mask
            .iter()
            .map(|_| manager.allocate().expect("パケット識別子を割り当てられること"))
            .collect();
        for (id, release) in ids.iter().zip(&mask) {
            if *release {
                manager.release(*id);
            }
        }
        manager.reset();
        prop_assert_eq!(manager.in_use_count(), 0);
        prop_assert!(manager.is_available());
        prop_assert_eq!(manager.allocate(), Some(1));
    }
}
