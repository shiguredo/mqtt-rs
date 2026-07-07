//! サブスクリプション状態管理。
//!
//! MQTT v5.0 §3.8、MQTT v3.1.1 §3.8 を参照。
//!
//! アクティブなトピックフィルタサブスクリプションの一覧を管理し、
//! SUBSCRIBE / UNSUBSCRIBE の送信状態を追跡する。
//! この状態機械は Sans-I/O であり、サブスクリプションの登録・解除と
//! 確認応答の突合を行う。

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;

use crate::codec::qos::QoS;
use crate::v5::subscribe::RetainHandling;

/// 1 つのサブスクリプションのエントリ。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionEntry {
    /// トピックフィルタ。
    pub topic_filter: String,
    /// 要求した最大 QoS。
    pub requested_qos: QoS,
    /// 実際に許可された QoS（SUBACK 受信後に設定）。
    pub granted_qos: Option<QoS>,
    /// No Local サブスクリプションかどうか。
    ///
    /// MQTT v5.0 §3.8.3.1。v3.1.1 では既定値 false。
    pub no_local: bool,
    /// 保持メッセージを RETAIN フラグ付きで送信するかどうか。
    ///
    /// MQTT v5.0 §3.8.3.1。v3.1.1 では既定値 false。
    pub retain_as_published: bool,
    /// 保持メッセージの扱いオプション。
    ///
    /// MQTT v5.0 §3.8.3.1 の Subscription Options で定義されたビット位置と、
    /// MQTT v5.0 §3.3.1.3 [MQTT-3.3.1-9] が定める値 0（保持メッセージを送信する）の
    /// 挙動に対応する。MQTT v3.1.1 §3.8.3 [MQTT-3-8.3-4]（規範番号のハイフン位置は原典どおり）は
    /// SUBSCRIBE の Requested QoS byte の上位 6 ビットを reserved と定めており
    /// v3.1.1 に Retain Handling は存在しないが、MQTT v3.1.1 §3.3.1.3 [MQTT-3.3.1-6] は
    /// 新規サブスクリプション確立時に保持メッセージを送信することを MUST として定めているため、
    /// v5.0 の既定値 `SendRetained` と挙動が一致する。v3.1.1 経路では
    /// [`RetainHandling::SendRetained`] のまま保持され、SUBSCRIBE 送信では参照されない。
    pub retain_handling: RetainHandling,
    /// サブスクリプション識別子。
    ///
    /// MQTT v5.0 §3.8.2.1.2。SUBSCRIBE に付与され、対応する PUBLISH で
    /// 返される。v3.1.1 では使用されない。
    pub subscription_identifier: Option<u32>,
}

impl SubscriptionEntry {
    /// 指定されたトピックフィルタと要求 QoS で新しいサブスクリプションエントリを作成する。
    ///
    /// v5 購読オプションと Subscription Identifier は既定値（無効）で初期化される。
    /// これらを設定する場合は、作成後に各フィールドを直接変更すること。
    pub fn new(topic_filter: impl Into<String>, requested_qos: QoS) -> Self {
        Self {
            topic_filter: topic_filter.into(),
            requested_qos,
            ..Default::default()
        }
    }
}

impl Default for SubscriptionEntry {
    fn default() -> Self {
        Self {
            topic_filter: String::new(),
            requested_qos: QoS::AtMostOnce,
            granted_qos: None,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::default(),
            subscription_identifier: None,
        }
    }
}

/// サブスクリプション状態管理。
///
/// SUBSCRIBE 送信時に対応する SUBACK を待つ状態を追跡し、
/// 確認されたサブスクリプションをアクティブ一覧に追加する。
#[derive(Debug, Clone)]
pub struct SubscriptionManager {
    /// 確認済みのアクティブなサブスクリプション。
    /// トピックフィルタ → サブスクリプション情報。
    active: BTreeMap<String, SubscriptionEntry>,
    /// SUBACK を待っている未確認の SUBSCRIBE。
    /// パケット識別子 → サブスクリプション一覧。
    pending_subscribes: BTreeMap<u16, Vec<SubscriptionEntry>>,
    /// UNSUBACK を待っている未確認の UNSUBSCRIBE。
    /// パケット識別子 → トピックフィルタ一覧。
    pending_unsubscribes: BTreeMap<u16, Vec<String>>,
    /// サブスクリプション識別子 → トピックフィルタ一覧。
    ///
    /// MQTT v5.0 §3.8.2.1.2: 同じ SUBSCRIBE パケット内で
    /// Subscription Identifier が重複してはならないが、異なる SUBSCRIBE
    /// パケット間では同じ識別子を再利用できる。そのため 1 つの識別子が
    /// 複数のトピックフィルタに対応し得る。
    subscription_ids: BTreeMap<u32, Vec<String>>,
}

impl SubscriptionManager {
    /// 空のサブスクリプション管理を新規作成する。
    pub fn new() -> Self {
        Self {
            active: BTreeMap::new(),
            pending_subscribes: BTreeMap::new(),
            pending_unsubscribes: BTreeMap::new(),
            subscription_ids: BTreeMap::new(),
        }
    }

    /// 確認済みのアクティブなサブスクリプション数を返す。
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    /// 指定されたトピックフィルタがアクティブかどうかを返す。
    pub fn is_subscribed(&self, topic_filter: &str) -> bool {
        self.active.contains_key(topic_filter)
    }

    /// アクティブなサブスクリプションの一覧を返す。
    ///
    /// MQTT v5.0 §3.2.2.1.1 [MQTT-3.2.2-5]:
    /// Session Present=0 の CONNACK を受信した場合、クライアントは以前の
    /// セッション状態を破棄する。破棄後に購読を再確立するために、
    /// このメソッドで取得した購読一覧を基に SUBSCRIBE を再送信できる。
    pub fn active_subscriptions(&self) -> Vec<&SubscriptionEntry> {
        self.active.values().collect()
    }

    /// 指定したサブスクリプション識別子に紐づくアクティブなサブスクリプション一覧を返す。
    ///
    /// MQTT v5.0 §3.8.2.1.2: SUBSCRIBE に付与された Subscription Identifier は、
    /// 対応する受信 PUBLISH に含まれて返される。クライアントはこのメソッドで
    /// 識別子から購読エントリを逆引きし、PUBLISH とハンドラーを対応付けることができる。
    pub fn find_by_subscription_identifier(&self, id: u32) -> Vec<&SubscriptionEntry> {
        self.subscription_ids
            .get(&id)
            .map(|filters| {
                filters
                    .iter()
                    .filter_map(|filter| self.active.get(filter))
                    .collect()
            })
            .unwrap_or_default()
    }

    // ======================================================================
    // サブスクリプション
    // ======================================================================

    /// SUBSCRIBE パケットを送信したときに呼び出す。
    ///
    /// 送信したサブスクリプションは SUBACK が返るまで pending 状態になる。
    pub fn subscribe_sent(&mut self, packet_id: u16, subscriptions: Vec<SubscriptionEntry>) {
        if packet_id == 0 || subscriptions.is_empty() {
            return;
        }
        self.pending_subscribes.insert(packet_id, subscriptions);
    }

    /// SUBACK 待ちの SUBSCRIBE を破棄する。
    ///
    /// SUBSCRIBE の wire 送信失敗や SUBACK 受信のタイムアウトなど、
    /// SUBACK 待ちを能動的に打ち切るときに呼ぶ。
    ///
    /// 戻り値は破棄された pending エントリ。該当エントリが無い場合は `None`。
    pub fn drop_pending_subscribe(&mut self, packet_id: u16) -> Option<Vec<SubscriptionEntry>> {
        self.pending_subscribes.remove(&packet_id)
    }

    /// SUBACK パケットを受信したときに呼び出す。
    ///
    /// SUBACK の理由コードに基づいて、成功したサブスクリプションをアクティブ一覧に追加する。
    /// 失敗したサブスクリプションは破棄される。
    ///
    /// 同一トピックフィルタで再購読した場合は、旧エントリを上書きする前に
    /// 旧 Subscription Identifier の逆引きを削除する。
    ///
    /// 理由コード数が要求したサブスクリプション数と一致しない場合は、
    /// どのエントリも成功扱いにせず `None` を返す。
    ///
    /// 戻り値: 確認されたサブスクリプションエントリの一覧。
    pub fn suback_received(
        &mut self,
        packet_id: u16,
        reason_codes: &[u8],
    ) -> Option<Vec<SubscriptionEntry>> {
        let pending = self.pending_subscribes.remove(&packet_id)?;

        // MQTT v3.1.1 / v5.0 ともに、SUBACK の reason code 数は
        // 要求したサブスクリプション数と一致しなければならない。
        if reason_codes.len() != pending.len() {
            return None;
        }

        let mut confirmed = Vec::new();
        for (i, mut entry) in pending.into_iter().enumerate() {
            let code = reason_codes[i];
            // 0x00-0x02 は成功、それ以外は失敗。
            if code <= 0x02 {
                entry.granted_qos = QoS::from_u8(code);

                // 同一トピックフィルタの既存エントリがある場合は、
                // 旧 Subscription Identifier の逆引きを先に削除する。
                if let Some(old) = self.active.get(&entry.topic_filter)
                    && let Some(old_id) = old.subscription_identifier
                {
                    self.remove_subscription_identifier(&entry.topic_filter, old_id);
                }

                if let Some(id) = entry.subscription_identifier {
                    self.subscription_ids
                        .entry(id)
                        .or_default()
                        .push(entry.topic_filter.clone());
                }
                self.active
                    .insert(entry.topic_filter.clone(), entry.clone());
                confirmed.push(entry);
            }
        }

        Some(confirmed)
    }

    /// 指定された Subscription Identifier からトピックフィルタを削除する。
    ///
    /// 最後のフィルタが削除された識別子はマップから除去する。
    fn remove_subscription_identifier(&mut self, topic_filter: &str, id: u32) {
        if let Some(filters) = self.subscription_ids.get_mut(&id) {
            filters.retain(|f| f != topic_filter);
            if filters.is_empty() {
                self.subscription_ids.remove(&id);
            }
        }
    }

    // ======================================================================
    // 購読解除
    // ======================================================================

    /// UNSUBSCRIBE パケットを送信したときに呼び出す。
    ///
    /// 送信したトピックフィルタは UNSUBACK が返るまで pending 状態になる。
    pub fn unsubscribe_sent(&mut self, packet_id: u16, topic_filters: Vec<String>) {
        if packet_id == 0 || topic_filters.is_empty() {
            return;
        }
        self.pending_unsubscribes.insert(packet_id, topic_filters);
    }

    /// UNSUBACK 待ちの UNSUBSCRIBE を破棄する。
    ///
    /// UNSUBSCRIBE の wire 送信失敗や UNSUBACK 受信のタイムアウトなど、
    /// UNSUBACK 待ちを能動的に打ち切るときに呼ぶ。
    ///
    /// 戻り値は破棄された pending トピックフィルタ一覧。該当エントリが無い場合は `None`。
    pub fn drop_pending_unsubscribe(&mut self, packet_id: u16) -> Option<Vec<String>> {
        self.pending_unsubscribes.remove(&packet_id)
    }

    /// UNSUBACK パケットを受信したときに呼び出す。
    ///
    /// MQTT v5.0 では UNSUBACK に per-filter の Reason Code が含まれる。
    /// MQTT v5.0 §3.11.3 の 0x00 (Success) と 0x11 (No subscription existed) は
    /// どちらも「サーバー側にその購読が存在しない」ことが確定する結果のため、
    /// アクティブ一覧と Subscription Identifier の逆引きから削除する。
    /// 0x80 以上のエラー Reason Code は購読解除が失敗しており購読が残っている
    /// 可能性があるため、アクティブ一覧を維持する。
    ///
    /// MQTT v3.1.1 の UNSUBACK には per-filter の Reason Code が存在しないため、
    /// `reason_codes` が空の場合はすべての購読解除が成功したものとして扱う。
    /// v5.0 で理由コード数が要求した購読解除数と一致しない場合は、
    /// どのエントリもアクティブ一覧から削除せず `None` を返す。
    ///
    /// 戻り値: ローカルのアクティブ一覧から削除されたトピックフィルタの一覧。
    /// 0x11 のフィルタも、ローカルに購読が存在した場合はこの一覧に含まれる。
    pub fn unsuback_received(
        &mut self,
        packet_id: u16,
        reason_codes: &[u8],
    ) -> Option<Vec<String>> {
        let pending = self.pending_unsubscribes.remove(&packet_id)?;

        // MQTT v3.1.1 の UNSUBACK には per-filter の Reason Code がない。
        // v5.0 の場合は理由コード数と要求数の一致を検証する。
        if !reason_codes.is_empty() && reason_codes.len() != pending.len() {
            return None;
        }

        let mut unsubscribed = Vec::new();
        for (i, filter) in pending.into_iter().enumerate() {
            // MQTT v3.1.1 は空配列を受け取るため、常に成功とみなす。
            // v5.0 は 0x00 (Success) と 0x11 (No subscription existed) で
            // ローカル購読を削除する。どちらもサーバー側に購読が存在しない
            // ことが確定するためである（MQTT v5.0 §3.11.3）。
            let removed_on_server =
                reason_codes.is_empty() || matches!(reason_codes[i], 0x00 | 0x11);
            if !removed_on_server {
                continue;
            }
            if let Some(removed) = self.active.remove(&filter) {
                if let Some(id) = removed.subscription_identifier {
                    self.remove_subscription_identifier(&filter, id);
                }
                unsubscribed.push(filter);
            }
        }

        Some(unsubscribed)
    }

    // ======================================================================
    // その他
    // ======================================================================

    /// SUBACK 待ちのパケット識別子一覧を返す。
    pub fn pending_subscribe_ids(&self) -> Vec<u16> {
        self.pending_subscribes.keys().copied().collect()
    }

    /// UNSUBACK 待ちのパケット識別子一覧を返す。
    pub fn pending_unsubscribe_ids(&self) -> Vec<u16> {
        self.pending_unsubscribes.keys().copied().collect()
    }

    /// すべての状態をリセットする。
    pub fn reset(&mut self) {
        self.active.clear();
        self.pending_subscribes.clear();
        self.pending_unsubscribes.clear();
        self.subscription_ids.clear();
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl From<crate::v5::subscribe::Subscription> for SubscriptionEntry {
    /// v5 の SUBSCRIBE パケット項目からサブスクリプションエントリを組み立てる。
    ///
    /// `subscription_identifier` は v5 の `Subscription` に含まれず SUBSCRIBE パケットの
    /// Properties に置かれるため、常に `None` になる。個別に設定したい場合は変換後に
    /// フィールドを更新すること。
    fn from(subscription: crate::v5::subscribe::Subscription) -> Self {
        Self {
            topic_filter: subscription.topic_filter,
            requested_qos: subscription.qos,
            granted_qos: None,
            no_local: subscription.no_local,
            retain_as_published: subscription.retain_as_published,
            retain_handling: subscription.retain_handling,
            subscription_identifier: None,
        }
    }
}
