//! パケット識別子のプール管理。
//!
//! MQTT v5.0 §2.2.1 を参照。
//!
//! パケット識別子は 1 から 65535 の範囲で、QoS 1 または 2 の PUBLISH、
//! SUBSCRIBE、UNSUBSCRIBE で使用される。この状態機械は Sans-I/O であり、
//! 利用者が割り当てと解放を明示的に呼び出す。

use alloc::collections::BTreeSet;

/// パケット識別子のプール。
///
/// 利用可能な識別子を管理し、重複のない割り当てを行う。
/// 解放された識別子は再利用可能になる。
#[derive(Debug, Clone)]
pub struct PacketIdManager {
    /// 次に割り当てるパケット識別子（単調増加、再利用リストが空の場合に使用）。
    /// u32 で管理し、65535 を超えたら枯渇と判定する。
    next_id: u32,
    /// 解放されて再利用可能になったパケット識別子の集合。
    free_list: BTreeSet<u16>,
    /// 現在使用中のパケット識別子の集合。
    in_use: BTreeSet<u16>,
}

impl PacketIdManager {
    /// 空のパケット識別子プールを新規作成する。
    pub fn new() -> Self {
        Self {
            next_id: 1,
            free_list: BTreeSet::new(),
            in_use: BTreeSet::new(),
        }
    }

    /// 新しいパケット識別子を割り当てる。
    ///
    /// 解放済みの識別子があればそれを再利用し、なければ単調増加で割り当てる。
    /// 全ての識別子 (1..=65535) が使用中の場合は `None` を返す。
    pub fn allocate(&mut self) -> Option<u16> {
        // 再利用可能な識別子を優先する。
        if let Some(id) = self.free_list.first().copied() {
            self.free_list.remove(&id);
            self.in_use.insert(id);
            return Some(id);
        }

        // 新しい識別子を割り当てる。
        while self.next_id <= 65535 {
            let id = self.next_id as u16;
            self.next_id += 1;
            if !self.in_use.contains(&id) {
                self.in_use.insert(id);
                return Some(id);
            }
        }

        None
    }

    /// パケット識別子を解放する。
    ///
    /// 解放された識別子は以降の `allocate()` 呼び出しで再利用される。
    /// 既に解放済みの識別子を指定した場合は何もしない。
    pub fn release(&mut self, id: u16) {
        if id == 0 {
            return;
        }
        if self.in_use.remove(&id) {
            self.free_list.insert(id);
        }
    }

    /// 使用中のパケット識別子の数を返す。
    pub fn in_use_count(&self) -> usize {
        self.in_use.len()
    }

    /// 利用可能なパケット識別子が存在するかどうかを返す。
    pub fn is_available(&self) -> bool {
        !self.free_list.is_empty() || self.next_id <= 65535
    }

    /// 全ての状態をリセットし、初期化されたプールに戻す。
    pub fn reset(&mut self) {
        self.next_id = 1;
        self.free_list.clear();
        self.in_use.clear();
    }
}

impl Default for PacketIdManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 一意性・解放後再利用は pbt/tests/prop_packet_id.rs で検証する。
    // ここにはエラーパス・枯渇・内部状態操作が必要な境界だけを残す。

    /// packet_id = 0 の解放は何もしない。
    #[test]
    fn release_zero_is_noop() {
        let mut manager = PacketIdManager::new();
        let id = manager.allocate().expect("識別子を割り当てられること");
        manager.release(0);
        assert!(manager.in_use.contains(&id));
    }

    /// 上限付近で枯渇すること。内部 next_id を操作する境界テスト。
    #[test]
    fn allocate_up_to_max() {
        let mut manager = PacketIdManager::new();
        manager.next_id = 65535;
        assert_eq!(manager.allocate(), Some(65535));
        assert_eq!(manager.allocate(), None);
    }

    /// free_list が空かつ next_id が枯渇すると is_available が false になる。
    #[test]
    fn is_available_detects_exhaustion() {
        let mut manager = PacketIdManager::new();
        assert!(manager.is_available());
        manager.next_id = 65536;
        manager.free_list.clear();
        assert!(!manager.is_available());
    }
}
