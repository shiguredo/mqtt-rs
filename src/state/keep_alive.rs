//! Keep Alive 補助。
//!
//! MQTT v5.0 §3.1.2.10、MQTT v3.1.1 §3.1.2.10 を参照。
//!
//! Keep Alive はクライアントがサーバーに対して定期的に PINGREQ を送信することで
//! 接続が生きていることを示す仕組みである。
//! この状態機械は Sans-I/O であり、利用者がタイムスタンプを注入して
//! PINGREQ の送信が必要かどうか、および PINGRESP の応答期限が過ぎたかどうかを判断する。
//!
//! タイムアウト判定はクライアント規範に基づく。
//! MQTT v5.0 §3.1.2.10 のクライアント SHOULD 段落
//! （「If a Client does not receive a PINGRESP packet within a reasonable amount
//! of time after it has sent a PINGREQ, it SHOULD close the Network Connection
//! to the Server」）および
//! MQTT v3.1.1 §3.1.2.10 のクライアント SHOULD 段落
//! （「If a Client does not receive a PINGRESP Packet within a reasonable amount
//! of time after it has sent a PINGREQ, it SHOULD close the Network Connection
//! to the Server」）はいずれも規範番号を持たないが、両バージョンで同旨の
//! 規範として、PINGREQ を送信してから合理的な時間内に PINGRESP を受信できない
//! 場合はクライアントが接続を閉じるべきだと定めている。
//! 本実装では「合理的な時間」を Keep Alive 間隔と同値（もっとも余裕を持たせた選択）に固定する。

/// Keep Alive 補助状態機械。
///
/// 利用者は最終送信時刻を [`activity`](Self::activity) で記録し、
/// Keep Alive 間隔を超過していないかを [`should_send_pingreq`](Self::should_send_pingreq) で確認する。
/// PINGREQ を送信したら [`pingreq_sent`](Self::pingreq_sent) を呼び、
/// PINGRESP を受信したら [`pingresp_received`](Self::pingresp_received) を呼ぶ。
/// PINGRESP の応答期限が過ぎたかどうかは [`has_timed_out`](Self::has_timed_out) で判定する。
///
/// 単位は利用者が選択した時刻表現（ミリ秒やシステム時刻など）を使用する。
#[derive(Debug, Clone)]
pub struct KeepAlive {
    /// CONNECT で指定された Keep Alive 間隔（秒）。
    /// 0 の場合は Keep Alive 機構が無効であることを示す。
    keep_alive_secs: u16,
    /// 最後にパケットを送信した時刻（利用者が注入する値）。
    last_activity: u64,
    /// 直近の PINGREQ 送信時刻。`None` のとき PINGRESP 待ちではない。
    pingreq_sent_at: Option<Timestamp>,
}

/// 利用者が [`KeepAlive::should_send_pingreq`] および [`KeepAlive::has_timed_out`] に
/// 注入する時刻の単位。ミリ秒単位の単調増加カウンターを想定している。
pub type Timestamp = u64;

impl KeepAlive {
    /// 指定された Keep Alive 間隔で新規作成する。
    ///
    /// `keep_alive_secs` は CONNECT パケットで指定された値（秒）。
    /// 0 が指定された場合は Keep Alive 機構が無効となる。
    pub fn new(keep_alive_secs: u16) -> Self {
        Self {
            keep_alive_secs,
            last_activity: 0,
            pingreq_sent_at: None,
        }
    }

    /// Keep Alive 間隔（秒）を返す。
    pub fn keep_alive_secs(&self) -> u16 {
        self.keep_alive_secs
    }

    /// Keep Alive 間隔を更新する。
    ///
    /// CONNECT または Server Keep Alive プロパティ
    /// （MQTT v5.0 §3.1.2.10 [MQTT-3.1.2-21] / MQTT v5.0 §3.2.2.3.14 [MQTT-3.2.2-21]）
    /// で Keep Alive 値が更新されたときに呼び出す。
    /// MQTT v3.1.1 に Server Keep Alive は存在しない。
    ///
    /// PINGRESP 待ち状態はクリアしない。
    /// PINGRESP 待ち中に本 API を呼ぶと、新しい `secs` に基づいて
    /// [`has_timed_out`](Self::has_timed_out) が即時タイムアウト判定を返すことがある。
    /// CONNACK 受信以外で本 API を呼ぶ場合は、事前に [`is_awaiting_pingresp`](Self::is_awaiting_pingresp)
    /// を確認すること。
    pub fn set_keep_alive(&mut self, secs: u16) {
        self.keep_alive_secs = secs;
    }

    /// パケットを送信したことを記録する。
    ///
    /// `now` は現在時刻（利用者が選択した単位、通常はミリ秒）。
    /// この呼び出しにより Keep Alive の送信タイマーがリセットされる。
    ///
    /// PINGREQ 送信時は [`pingreq_sent`](Self::pingreq_sent) を呼ぶこと。
    /// [`pingreq_sent`](Self::pingreq_sent) は内部で `last_activity` も更新するため、
    /// PINGREQ 送信時に本 API を追加で呼ぶ必要はない。
    pub fn activity(&mut self, now: Timestamp) {
        self.last_activity = now;
    }

    /// PINGREQ を送信したことを記録する。
    ///
    /// `now` は現在時刻。この呼び出しにより PINGRESP 待ち状態が立ち、
    /// 送信タイマー（`last_activity`）も同時に更新される。
    ///
    /// 利用者は PINGREQ を送信するたびに本 API を呼ぶこと。
    /// 呼ばない場合、[`has_timed_out`](Self::has_timed_out) は
    /// PINGRESP 待ちを検出できず、Keep Alive のタイムアウト機能そのものが働かない。
    ///
    /// 連続で呼ばれた場合は最新の送信時刻で上書きする。
    /// 二重送信の抑止は利用者の責務であり、状態は
    /// [`is_awaiting_pingresp`](Self::is_awaiting_pingresp) で確認できる。
    pub fn pingreq_sent(&mut self, now: Timestamp) {
        self.pingreq_sent_at = Some(now);
        self.last_activity = now;
    }

    /// PINGRESP を受信したことを記録する。
    ///
    /// PINGRESP 待ち状態を解除する。PINGRESP 未待ち状態でも安全に呼び出せる（冪等）。
    pub fn pingresp_received(&mut self) {
        self.pingreq_sent_at = None;
    }

    /// PINGRESP 待ち中かどうかを返す。
    ///
    /// [`pingreq_sent`](Self::pingreq_sent) を呼んだ後、
    /// [`pingresp_received`](Self::pingresp_received) を呼ぶまでの間は `true` を返す。
    /// [`should_send_pingreq`](Self::should_send_pingreq) の結果を無視して
    /// 二重送信を抑止したい場合の判断材料として使う。
    pub fn is_awaiting_pingresp(&self) -> bool {
        self.pingreq_sent_at.is_some()
    }

    /// PINGREQ を送信すべきかどうかを返す。
    ///
    /// `now` は現在時刻。最後のアクティビティから Keep Alive 間隔全体が
    /// 経過している場合に `true` を返す。
    /// MQTT v5.0 §3.1.2.10 [MQTT-3.1.2-20] および
    /// MQTT v3.1.1 §3.1.2.10 [MQTT-3.1.2-23]:
    /// Keep Alive 期間内に制御パケットを何も送信していない場合、
    /// PINGREQ を送信しなければならない。
    ///
    /// Keep Alive が 0 の場合は常に `false` を返す。
    ///
    /// PINGRESP 待ち中でも `true` を返しうる。
    /// 利用者は [`is_awaiting_pingresp`](Self::is_awaiting_pingresp) または
    /// [`has_timed_out`](Self::has_timed_out) を先に判定すること。
    pub fn should_send_pingreq(&self, now: Timestamp) -> bool {
        if self.keep_alive_secs == 0 {
            return false;
        }

        let keep_alive_ms = (self.keep_alive_secs as u64) * 1000;
        let threshold_ms = keep_alive_ms;

        now.saturating_sub(self.last_activity) >= threshold_ms
    }

    /// PINGRESP の応答期限を過ぎたかどうかを返す。
    ///
    /// 次の両バージョンのクライアント SHOULD 段落（両バージョンとも規範番号なし）に基づき、
    /// PINGREQ 送信後に PINGRESP を受信できないまま応答期限を過ぎたかどうかを返す。
    /// 応答期限は Keep Alive 間隔と同値（`(keep_alive_secs as u64) * 1000` ミリ秒）に固定する。
    ///
    /// - MQTT v5.0 §3.1.2.10:
    ///   「If a Client does not receive a PINGRESP packet within a reasonable amount
    ///   of time after it has sent a PINGREQ, it SHOULD close the Network Connection
    ///   to the Server」（規範番号なし）。
    /// - MQTT v3.1.1 §3.1.2.10:
    ///   「If a Client does not receive a PINGRESP Packet within a reasonable amount
    ///   of time after it has sent a PINGREQ, it SHOULD close the Network Connection
    ///   to the Server」（規範番号なし）。
    ///
    /// 次のいずれかの状態では常に `false` を返す。
    ///
    /// - Keep Alive = 0（機構無効）:
    ///   - MQTT v5.0 §3.1.2.10:
    ///     「A Keep Alive value of 0 has the effect of turning off the Keep Alive
    ///     mechanism」（規範番号なし）。
    ///   - MQTT v3.1.1 §3.1.2.10:
    ///     「A Keep Alive value of zero (0) has the effect of turning off the keep
    ///     alive mechanism」（規範番号なし）。
    /// - PINGRESP 待ちでない（[`pingreq_sent`](Self::pingreq_sent) を一度も呼んでいない、
    ///   または既に [`pingresp_received`](Self::pingresp_received) を呼んで待ちを解除した）。
    pub fn has_timed_out(&self, now: Timestamp) -> bool {
        if self.keep_alive_secs == 0 {
            return false;
        }

        let Some(sent_at) = self.pingreq_sent_at else {
            return false;
        };

        let deadline_ms = (self.keep_alive_secs as u64) * 1000;
        now.saturating_sub(sent_at) >= deadline_ms
    }

    /// すべての状態をリセットする。
    ///
    /// [`Session`](crate::state::session::Session) 全体をリセットするときに用いる。
    /// 新しい接続を開始する際の PINGRESP 待ちだけを解除したい場合は
    /// [`pingresp_received`](Self::pingresp_received) を用いる。
    pub fn reset(&mut self) {
        self.keep_alive_secs = 0;
        self.last_activity = 0;
        self.pingreq_sent_at = None;
    }
}

impl Default for KeepAlive {
    fn default() -> Self {
        Self::new(0)
    }
}
