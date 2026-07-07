//! フロー制御 — Receive Maximum に基づく PUBLISH 送信制限。
//!
//! MQTT v5.0 §4.9 を参照。
//!
//! Receive Maximum は CONNECT / CONNACK で交換され、
//! 受信者が同時に処理可能な未確認 PUBLISH (QoS 1/2) の最大数を示す。
//! この状態機械は Sans-I/O であり、利用者が送信前に制限を確認する。
//!
//! MQTT v3.1.1 ではこの機能は存在しないが、
//! 状態機械自体はプロトコルバージョンに関係なく使用できる。

use core::fmt;

/// フロー制御で発生するエラー。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowControlError {
    /// Receive Maximum を超える未確認 QoS 1/2 PUBLISH を送信または受信しようとした。
    ///
    /// 受信方向でこのエラーが発生した場合、利用者は MQTT v5.0 §4.9、
    /// MQTT v5.0 §4.13.1 に従い、Reason Code 0x93 (Receive Maximum exceeded) の
    /// DISCONNECT パケットを送信して接続を切断すべきである。
    ReceiveMaximumExceeded,
    /// Receive Maximum が 0 だった。
    ///
    /// MQTT v5.0 §3.1.2.11.3 および MQTT v5.0 §3.2.2.3.3:
    /// Receive Maximum に 0 を指定することは Protocol Error である。
    InvalidReceiveMaximum,
    /// QoS 0 の PUBLISH を送信フローとして記録しようとした。
    ///
    /// MQTT v5.0 §4.9 [MQTT-4.9.0-2] の send quota の対象は QoS > 0 の
    /// PUBLISH のみであり、QoS 0 には確認応答フローも存在しないため、
    /// `Session::publish_sent` を QoS 0 で呼び出すことは契約違反である。
    InvalidQoS,
    /// パケット識別子が 0 だった。
    ///
    /// MQTT v5.0 §2.2.1 [MQTT-2.2.1-3] / MQTT v3.1.1 §2.3.1 [MQTT-2.3.1-1]:
    /// PUBLISH (QoS > 0) には非ゼロのパケット識別子を割り当てなければならない。
    InvalidPacketId,
}

impl fmt::Display for FlowControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReceiveMaximumExceeded => {
                write!(
                    f,
                    "attempted to send or receive a PUBLISH exceeding Receive Maximum"
                )
            }
            Self::InvalidReceiveMaximum => {
                write!(f, "Receive Maximum must not be 0")
            }
            Self::InvalidQoS => {
                write!(f, "publish flow must not be recorded for QoS 0")
            }
            Self::InvalidPacketId => {
                write!(f, "packet identifier must not be 0")
            }
        }
    }
}

impl core::error::Error for FlowControlError {}

/// フロー制御状態機械。
///
/// 相手から通知された Receive Maximum に基づき、
/// 現在送信可能な PUBLISH の残り枠数を管理するとともに、
/// 自身が受け入れ可能な Receive Maximum に対する未確認受信 PUBLISH 数も管理する。
///
/// MQTT v5.0 §3.1.2.11.3 および MQTT v5.0 §3.2.2.3.3:
/// Receive Maximum が指定されない場合のデフォルト値は 65535。
#[derive(Debug, Clone)]
pub struct FlowControl {
    /// 相手から通知された Receive Maximum。
    /// 0 は「受信できない」を意味するが、仕様上 1..=65535 が有効範囲。
    /// 未設定の場合はデフォルト値 65535 を使用する。
    receive_maximum: u16,
    /// 現在未確認の PUBLISH (QoS 1/2) の数（送信方向）。
    outstanding_count: u16,
    /// この接続で Receive Maximum が設定されたかどうか。
    initialized: bool,
    /// 自身が受け入れ可能な Receive Maximum。
    /// 未設定の場合はデフォルト値 65535 を使用する。
    own_receive_maximum: u16,
    /// 現在未確認の PUBLISH (QoS 1/2) の数（受信方向）。
    incoming_count: u16,
}

impl FlowControl {
    /// デフォルト値でフロー制御を新規作成する。
    ///
    /// デフォルトの Receive Maximum は 65535（MQTT v5.0 §3.1.2.11.3 のデフォルト）である。
    /// CONNECT または CONNACK で Receive Maximum プロパティを受け取ったら
    /// `set_receive_maximum()` で更新すること。
    pub fn new() -> Self {
        Self {
            receive_maximum: 65535,
            outstanding_count: 0,
            initialized: false,
            own_receive_maximum: 65535,
            incoming_count: 0,
        }
    }

    /// 相手から通知された Receive Maximum を設定する。
    ///
    /// CONNACK の Receive Maximum プロパティ（0x21）を受け取ったときに呼び出す。
    /// MQTT v5.0 §3.2.2.3.3:
    /// Receive Maximum は 0 以外でなければならない。
    pub fn set_receive_maximum(&mut self, max: u16) -> Result<(), FlowControlError> {
        if max == 0 {
            return Err(FlowControlError::InvalidReceiveMaximum);
        }
        self.receive_maximum = max;
        self.initialized = true;
        Ok(())
    }

    /// 現在の Receive Maximum 値を返す。
    pub fn receive_maximum(&self) -> u16 {
        self.receive_maximum
    }

    /// 自身が受け入れ可能な Receive Maximum を設定する。
    ///
    /// CONNECT の Receive Maximum プロパティ（0x21）を送信するときに呼び出す。
    /// MQTT v5.0 §3.1.2.11.3:
    /// Receive Maximum は 0 以外でなければならない。
    pub fn set_own_receive_maximum(&mut self, max: u16) -> Result<(), FlowControlError> {
        if max == 0 {
            return Err(FlowControlError::InvalidReceiveMaximum);
        }
        self.own_receive_maximum = max;
        Ok(())
    }

    /// 現在の自身の Receive Maximum 値を返す。
    pub fn own_receive_maximum(&self) -> u16 {
        self.own_receive_maximum
    }

    /// 新たに PUBLISH (QoS 1/2) を送信できるかどうかを返す。
    pub fn can_send(&self) -> bool {
        self.outstanding_count < self.receive_maximum
    }

    /// 残り送信可能な PUBLISH 数を返す。
    pub fn available(&self) -> u16 {
        self.receive_maximum.saturating_sub(self.outstanding_count)
    }

    /// QoS 1 または QoS 2 の PUBLISH を送信するときに呼び出す。
    ///
    /// 送信前に `can_send()` で確認することが推奨される。
    /// Receive Maximum を超過している場合は `Err(FlowControlError::ReceiveMaximumExceeded)`
    /// を返し、`outstanding_count` は増加しない。
    pub fn publish_sent(&mut self) -> Result<(), FlowControlError> {
        if !self.can_send() {
            return Err(FlowControlError::ReceiveMaximumExceeded);
        }
        self.outstanding_count = self.outstanding_count.saturating_add(1);
        Ok(())
    }

    /// PUBLISH が確認されたときに呼び出す。
    ///
    /// MQTT v5.0 §4.9: send quota は PUBACK または PUBCOMP を
    /// 受信したとき（Reason Code がエラーの場合を含む）、および
    /// Reason Code 0x80 以上の PUBREC を受信したときに 1 増える。
    /// これらのいずれの場合もこのメソッドを呼び出すこと。
    ///
    /// これにより未確認カウントが 1 減少する。
    pub fn publish_acked(&mut self) {
        self.outstanding_count = self.outstanding_count.saturating_sub(1);
    }

    /// 現在の未確認 PUBLISH 数を返す。
    pub fn outstanding_count(&self) -> u16 {
        self.outstanding_count
    }

    /// 新たに QoS 1 または QoS 2 の PUBLISH を受信するときに呼び出す。
    ///
    /// 受信前に `can_receive()` で確認することが推奨される。
    /// 自身の Receive Maximum を超過している場合は
    /// `Err(FlowControlError::ReceiveMaximumExceeded)` を返し、
    /// `incoming_count` は増加しない。
    ///
    /// エラーが発生した場合、利用者は MQTT v5.0 §4.9 に従い、
    /// Reason Code 0x93 (Receive Maximum exceeded) の DISCONNECT パケットを
    /// 送信して接続を切断しなければならない。
    pub fn publish_received(&mut self) -> Result<(), FlowControlError> {
        if !self.can_receive() {
            return Err(FlowControlError::ReceiveMaximumExceeded);
        }
        self.incoming_count = self.incoming_count.saturating_add(1);
        Ok(())
    }

    /// 受信した PUBLISH に対する確認応答フローが完了したときに呼び出す。
    ///
    /// MQTT v5.0 §3.3.4 [MQTT-3.3.4-9]: サーバーは、PUBACK・PUBCOMP・
    /// Reason Code 0x80 以上の PUBREC のいずれかを受信するまで、
    /// その PUBLISH を未確認として数える。したがって呼び出しタイミングは
    /// 次のとおりである。
    ///
    /// - QoS 1: PUBACK を送信したとき。
    /// - QoS 2: PUBCOMP を送信したとき（成功 PUBREC の送信では呼び出さない）。
    /// - QoS 2: Reason Code 0x80 以上のエラー PUBREC を送信したとき。
    ///
    /// これにより受信方向の未確認カウントが 1 減少する。
    pub fn publish_ack_sent(&mut self) {
        self.incoming_count = self.incoming_count.saturating_sub(1);
    }

    /// 現在の受信方向の未確認 PUBLISH 数を返す。
    pub fn incoming_count(&self) -> u16 {
        self.incoming_count
    }

    /// 新たに QoS 1/2 の PUBLISH を受信できるかどうかを返す。
    pub fn can_receive(&self) -> bool {
        self.incoming_count < self.own_receive_maximum
    }

    /// 受信方向の残り受信可能な PUBLISH 数を返す。
    pub fn incoming_available(&self) -> u16 {
        self.own_receive_maximum.saturating_sub(self.incoming_count)
    }

    /// Receive Maximum が明示的に設定されたかどうかを返す。
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// 状態をリセットする。
    ///
    /// 新しい接続が確立されたときに呼び出す。
    /// `keep_own_receive_maximum` が true の場合、自身の Receive Maximum は
    /// `configure_for_connect` で設定された値を維持する。false の場合は
    /// デフォルト値 65535 に戻す。
    pub fn reset(&mut self, keep_own_receive_maximum: bool) {
        let own_receive_maximum = if keep_own_receive_maximum {
            self.own_receive_maximum
        } else {
            65535
        };
        self.receive_maximum = 65535;
        self.outstanding_count = 0;
        self.initialized = false;
        self.own_receive_maximum = own_receive_maximum;
        self.incoming_count = 0;
    }
}

impl Default for FlowControl {
    fn default() -> Self {
        Self::new()
    }
}
