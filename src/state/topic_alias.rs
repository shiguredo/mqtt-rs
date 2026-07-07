//! トピックエイリアスの双方向マッピング管理。
//!
//! MQTT v5.0 §3.3.2.3.4 を参照。
//!
//! トピックエイリアスは Topic Alias Maximum (0x22) で合意された範囲内で
//! トピック名の代わりに整数値を使用する仕組みである。
//! この状態機械は Sans-I/O であり、エイリアスとトピック名のマッピングを管理する。

use alloc::string::String;
use alloc::vec::Vec;

/// トピックエイリアスの双方向マッピングを管理する状態機械。
///
/// 受信時: エイリアス → トピック名の解決
/// 送信時: トピック名 → エイリアスの登録・検索
///
/// MQTT v5.0 §3.3.2.3.4:
/// Topic Alias が 0 の場合はマッピングに使用しない。
/// 接続が確立されるたびにマッピングはクリアされる。
#[derive(Debug, Clone)]
pub struct TopicAliasManager {
    /// エイリアス → トピック名のマッピング（受信用）。
    /// インデックスがエイリアス値、要素がマッピングされたトピック名。
    alias_to_topic: Vec<Option<String>>,
    /// エイリアス → トピック名のマッピング（送信用）。
    /// インデックスがエイリアス値、要素がマッピングされたトピック名。
    topic_to_alias: Vec<Option<String>>,
    /// 自分が送信に使用できる最大エイリアス数。
    /// CONNECT の Topic Alias Maximum で相手に通知した値。
    own_maximum: u16,
    /// 相手から通知された最大エイリアス数。
    /// CONNACK の Topic Alias Maximum で通知された値。
    peer_maximum: u16,
}

impl TopicAliasManager {
    /// 空のトピックエイリアスマネージャーを新規作成する。
    ///
    /// 初期状態ではエイリアス最大値は 0 であり、エイリアスは使用できない。
    pub fn new() -> Self {
        Self {
            alias_to_topic: Vec::new(),
            topic_to_alias: Vec::new(),
            own_maximum: 0,
            peer_maximum: 0,
        }
    }

    /// 自身が送信に使用する最大トピックエイリアス数を設定する。
    ///
    /// 接続確立時に CONNECT パケットの Topic Alias Maximum プロパティの値で呼び出す。
    /// この値は自分が相手から受け入れ可能なエイリアス最大数を示す。
    ///
    /// 設定後、マッピングテーブルが適切なサイズにリサイズされる。
    pub fn set_own_maximum(&mut self, max: u16) {
        self.own_maximum = max;
        self.ensure_receive_capacity(max as usize);
    }

    /// 相手から通知された最大トピックエイリアス数を設定する。
    ///
    /// 接続確立時に CONNACK パケットの Topic Alias Maximum プロパティの値で呼び出す。
    /// この値は相手が受け入れ可能なエイリアス最大数を示す。
    pub fn set_peer_maximum(&mut self, max: u16) {
        self.peer_maximum = max;
        self.ensure_send_capacity(max as usize);
    }

    /// 相手が通知した最大トピックエイリアス数を返す。
    pub fn peer_maximum(&self) -> u16 {
        self.peer_maximum
    }

    /// 自身が通知した最大トピックエイリアス数を返す。
    pub fn own_maximum(&self) -> u16 {
        self.own_maximum
    }

    /// 受信した PUBLISH パケットのトピックエイリアスを解決する。
    ///
    /// トピック名が空でない場合は、そのトピック名をエイリアスにマッピングして返す。
    /// トピック名が空の場合は、エイリアス値からトピック名を解決して返す。
    ///
    /// 戻り値は解決されたトピック名。解決できない場合は `None` を返す。
    pub fn resolve_on_receive(&mut self, topic_name: &str, topic_alias: u16) -> Option<String> {
        if topic_alias == 0 {
            // エイリアス未使用。トピック名をそのまま使用する。
            // ただしトピック名が空の場合は不正な PUBLISH として None を返す。
            if topic_name.is_empty() {
                return None;
            }
            return Some(topic_name.into());
        }

        if topic_alias > self.own_maximum {
            // MQTT v5.0 §3.3.2.3.4:
            // Topic Alias が Topic Alias Maximum を超えている場合はプロトコルエラー。
            return None;
        }

        if topic_name.is_empty() {
            // トピック名が空の場合はエイリアスから解決する。
            self.alias_to_topic
                .get(topic_alias as usize)
                .and_then(|entry| entry.clone())
        } else {
            // トピック名が存在する場合は新しいマッピングを登録する。
            self.ensure_receive_capacity(topic_alias as usize);
            self.alias_to_topic[topic_alias as usize] = Some(topic_name.into());
            Some(topic_name.into())
        }
    }

    /// 送信用のトピックエイリアスを検索する。
    ///
    /// トピック名に対応するエイリアスが既に登録されていればその値を返す。
    /// 登録されていなければ `None` を返す。
    pub fn find_alias_for_topic(&self, topic_name: &str) -> Option<u16> {
        if self.peer_maximum == 0 {
            return None;
        }
        // 送信用マッピングからトピック名を検索する。
        for (i, entry) in self.topic_to_alias.iter().enumerate() {
            if let Some(name) = entry
                && name == topic_name
            {
                let alias = i as u16;
                if alias > 0 && alias <= self.peer_maximum {
                    return Some(alias);
                }
            }
        }
        None
    }

    /// 送信用に新しいトピックエイリアスを割り当てて登録する。
    ///
    /// 既にエイリアスが存在する場合はその値を返す。
    /// 存在しない場合は利用可能なエイリアスを探して新規登録する。
    /// 利用可能なエイリアスがない場合は既存マッピングを破壊せず `None` を返す。
    pub fn register_for_send(&mut self, topic_name: &str) -> Option<u16> {
        if self.peer_maximum == 0 {
            return None;
        }

        // 既存のエイリアスを検索する。
        if let Some(alias) = self.find_alias_for_topic(topic_name) {
            return Some(alias);
        }

        let max = self.peer_maximum as usize;
        self.ensure_send_capacity(max);

        // 空きエイリアスを探す（1 から開始）。
        for alias in 1..=max {
            if self.topic_to_alias[alias].is_none() {
                self.topic_to_alias[alias] = Some(topic_name.into());
                return Some(alias as u16);
            }
        }

        // 空きがない場合は既存マッピングを破壊しない。
        None
    }

    /// 送受信両方向のマッピングのみをクリアする（最大値の設定は維持する）。
    ///
    /// MQTT v5.0 §3.3.2.3.4 [MQTT-3.3.2-7]:
    /// 受信者はトピックエイリアスのマッピングを Network Connection を
    /// またいで持ち越してはならない。
    /// 新しい接続を開始するたびに呼び出す。
    pub fn reset_mappings(&mut self) {
        self.alias_to_topic.clear();
        self.topic_to_alias.clear();
    }

    /// 全てのマッピングと最大値の設定をクリアする。
    ///
    /// セッション全体を破棄するときに呼び出す。
    pub fn reset(&mut self) {
        self.alias_to_topic.clear();
        self.topic_to_alias.clear();
        self.own_maximum = 0;
        self.peer_maximum = 0;
    }

    /// 登録されているエイリアスの数を返す。
    pub fn alias_count(&self) -> usize {
        let receive_count = self
            .alias_to_topic
            .iter()
            .filter(|entry| entry.is_some())
            .count();
        let send_count = self
            .topic_to_alias
            .iter()
            .filter(|entry| entry.is_some())
            .count();
        receive_count + send_count
    }

    fn ensure_receive_capacity(&mut self, index: usize) {
        if self.alias_to_topic.len() <= index {
            self.alias_to_topic.resize(index + 1, None);
        }
    }

    fn ensure_send_capacity(&mut self, index: usize) {
        if self.topic_to_alias.len() <= index {
            self.topic_to_alias.resize(index + 1, None);
        }
    }
}

impl Default for TopicAliasManager {
    fn default() -> Self {
        Self::new()
    }
}
