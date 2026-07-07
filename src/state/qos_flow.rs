//! QoS 1 / QoS 2 のプロトコルフロー状態機械。
//!
//! MQTT v5.0 §4.3.2 (QoS 1: At least once delivery) および
//! MQTT v5.0 §4.3.3 (QoS 2: Exactly once delivery) を参照。
//!
//! この状態機械はパケット識別子ごとのハンドシェイク状態を管理し、
//! 受信パケットに応じた応答アクションを生成する。
//! Sans-I/O 設計により、I/O は利用者が行う。

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

/// QoS フロー内のパケット識別子ごとの状態。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowState {
    /// QoS 1: PUBLISH を送信し、PUBACK を待っている。
    AwaitingPuback,
    /// QoS 2: PUBLISH を送信し、PUBREC を待っている。
    AwaitingPubrec,
    /// QoS 2: PUBREL を送信し、PUBCOMP を待っている。
    AwaitingPubcomp,
    /// QoS 2: PUBLISH を受信し、PUBREL を待っている。
    AwaitingPubrel,
    /// QoS 1: PUBLISH を受信し、自側の PUBACK 送信完了を待っている。
    ///
    /// MQTT v5.0 §3.3.4 [MQTT-3.3.4-9]:
    /// PUBACK を送るまでその PUBLISH は未確認である。同一 Packet Identifier の
    /// 再送を別メッセージとして数えないために受信方向で追跡する。
    IncomingAwaitingPuback,
}

/// QoS フロー状態機械が生成するアクション。
///
/// 利用者はこのアクションに従って適切な応答パケットを送信する。
/// MQTT v5.0 であれば ReasonCode や Properties を適切に設定すること。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// PUBACK パケットを送信する（QoS 1 の受信応答）。
    SendPuback {
        /// 応答対象のパケット識別子。
        packet_id: u16,
    },
    /// PUBREC パケットを送信する（QoS 2 の受信応答、第一段階）。
    SendPubrec {
        /// 応答対象のパケット識別子。
        packet_id: u16,
        /// 同じ Packet Identifier の PUBLISH を PUBREL 受信前に再受信した場合は true。
        /// MQTT v5.0 §4.3.3 [MQTT-4.3.3-10]:
        /// 重複メッセージを onward recipient に配送してはならない。
        is_duplicate: bool,
    },
    /// PUBREL パケットを送信する（QoS 2 の PUBREC への応答）。
    SendPubrel {
        /// 応答対象のパケット識別子。
        packet_id: u16,
    },
    /// PUBCOMP パケットを送信する（QoS 2 の PUBREL への応答）。
    SendPubcomp {
        /// 応答対象のパケット識別子。
        packet_id: u16,
    },
    /// 該当する受信フローが存在しない PUBREL に対して PUBCOMP パケットを再送する。
    ///
    /// MQTT v5.0 §4.3.3 [MQTT-4.3.3-11] / MQTT v3.1.1 §4.3.3:
    /// 受信者は PUBREL に対して同じ Packet Identifier の PUBCOMP で
    /// 応答しなければならない。過去に送信した PUBCOMP がネットワーク上で
    /// 消失した場合、送信者は PUBREL を再送するため、フロー完了後の
    /// PUBREL にも PUBCOMP を返す必要がある。
    ///
    /// `SendPubcomp` と異なり受信フローは既に完了しているため、
    /// 利用者は受信枠の解放（`Session::pubcomp_sent()` の呼び出し）を
    /// 行ってはならない。
    ResendPubcomp {
        /// 応答対象のパケット識別子。
        packet_id: u16,
    },
    /// PUBLISH パケットを再送する必要がある。
    /// DUP フラグを立てて再送すること。
    ResendPublish {
        /// 再送対象のパケット識別子。
        packet_id: u16,
    },
    /// PUBREL パケットを再送する必要がある。
    ResendPubrel {
        /// 再送対象のパケット識別子。
        packet_id: u16,
    },
    /// QoS フローが完了した。
    Complete {
        /// 完了したパケット識別子。
        packet_id: u16,
    },
    /// QoS 2 フローが Reason Code 0x80 以上の PUBREC により中断された。
    ///
    /// MQTT v5.0 §4.4 [MQTT-4.4.0-2]:
    /// 対象の PUBLISH は確認済みとして扱い、再送してはならない。
    /// 利用者はパケット識別子の解放と、送信クォータの回復
    /// （MQTT v5.0 §4.9）を行うこと。
    Aborted {
        /// 中断されたパケット識別子。
        packet_id: u16,
    },
}

/// QoS フロー処理時のエラー。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowError {
    /// 該当するフローが存在しない。
    Unknown,
    /// 受信パケットとフロー状態が不一致。
    ///
    /// MQTT v5.0 §4.13.1:
    /// 状態不一致パケットは Protocol Error である。
    StateMismatch,
}

impl core::fmt::Display for FlowError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Unknown => write!(f, "no matching QoS flow"),
            Self::StateMismatch => {
                write!(f, "received packet does not match QoS flow state")
            }
        }
    }
}

impl core::error::Error for FlowError {}

/// QoS 1 / QoS 2 フロー状態機械。
///
/// 各パケット識別子のハンドシェイク状態を追跡し、
/// 受信パケットに応じたアクションを返す。
///
/// MQTT v5.0 §2.2.1:
/// クライアントとサーバーはそれぞれ独立にパケット識別子を割り当てるため、
/// 送信方向と受信方向で同じパケット識別子が同時に使用され得る。
/// このため送信方向と受信方向のフローは独立した空間で管理する。
#[derive(Debug, Clone)]
pub struct QosFlowManager {
    /// 送信方向（自分が送信した PUBLISH）のパケット識別子 → 現在のフロー
    outgoing: BTreeMap<u16, OutgoingFlow>,
    /// 受信方向（相手から受信した PUBLISH）のパケット識別子 → 現在のフロー状態
    incoming: BTreeMap<u16, FlowState>,
    /// 送信方向フローの順序付けに使う単調増加カウンタ。
    next_seq: u64,
}

/// 送信方向の 1 つのフロー。順序付け用のシーケンス番号と状態を保持する。
///
/// シーケンス番号は PUBLISH 送信時に採番し、PUBREC の初回受信時に打ち直す。
/// 順序の契約は [`QosFlowManager::pending_packet_ids`] を参照。
#[derive(Debug, Clone, Copy)]
struct OutgoingFlow {
    /// 再送一覧の順序を決めるシーケンス番号。
    seq: u64,
    /// 現在のフロー状態。
    state: FlowState,
    /// 現在の Network Connection で send quota を消費済みかどうか。
    ///
    /// MQTT v5.0 §4.9: send quota は Network Connection をまたいで保存されず、
    /// 新しい接続ごとに再初期化される。このため PUBLISH 送信時に true とし、
    /// 再接続時に [`QosFlowManager::clear_quota_charges`] で false に戻す。
    /// 再送時の二重消費の防止に使用する。
    quota_charged: bool,
}

impl QosFlowManager {
    /// 空の QoS フロー状態機械を新規作成する。
    pub fn new() -> Self {
        Self {
            outgoing: BTreeMap::new(),
            incoming: BTreeMap::new(),
            next_seq: 0,
        }
    }

    /// 次のシーケンス番号を採番する。
    fn allocate_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    /// アクティブなフローの数を返す（送信方向と受信方向の合計）。
    pub fn active_flow_count(&self) -> usize {
        self.outgoing.len() + self.incoming.len()
    }

    /// 指定されたパケット識別子が送信・受信いずれかの方向でフロー中かどうかを返す。
    pub fn is_active(&self, packet_id: u16) -> bool {
        self.outgoing.contains_key(&packet_id) || self.incoming.contains_key(&packet_id)
    }

    /// 指定されたパケット識別子が受信方向でフロー中かどうかを返す。
    ///
    /// 次のいずれかに該当する。
    /// - QoS 1: PUBACK 送信完了前（[`FlowState::IncomingAwaitingPuback`]）
    /// - QoS 2: PUBREL 受信前（[`FlowState::AwaitingPubrel`]）
    ///
    /// MQTT v5.0 §3.3.4 [MQTT-3.3.4-9] / MQTT v5.0 §4.3.3 [MQTT-4.3.3-10] /
    /// MQTT v3.1.1 §4.3.3:
    /// 未確認の同一 Packet Identifier の PUBLISH 再受信は同一メッセージの再送であり、
    /// 新たな未確認 PUBLISH としては数えない。この判定に使用する。
    pub fn has_incoming_flow(&self, packet_id: u16) -> bool {
        self.incoming.contains_key(&packet_id)
    }

    /// 指定されたパケット識別子の送信フローが、現在の Network Connection で
    /// send quota を消費済みかどうかを返す。
    ///
    /// フローが存在しない場合は false を返す。
    pub fn is_quota_charged(&self, packet_id: u16) -> bool {
        self.outgoing
            .get(&packet_id)
            .is_some_and(|flow| flow.quota_charged)
    }

    /// 指定されたパケット識別子の送信フローを send quota 消費済みとして記録する。
    ///
    /// 再送で send quota を消費したときに呼び出す。フローが存在しない場合は何もしない。
    pub fn mark_quota_charged(&mut self, packet_id: u16) {
        if let Some(flow) = self.outgoing.get_mut(&packet_id) {
            flow.quota_charged = true;
        }
    }

    /// すべての送信フローの send quota 消費状態をクリアする。
    ///
    /// MQTT v5.0 §4.9: send quota は Network Connection をまたいで保存されず、
    /// 新しい接続ごとに再初期化される。再接続時（フロー制御のリセット時）に
    /// 呼び出すことで、セッション状態として持ち越された未確認フローの再送が
    /// 新しい接続の send quota を正しく消費できるようにする。
    pub fn clear_quota_charges(&mut self) {
        for flow in self.outgoing.values_mut() {
            flow.quota_charged = false;
        }
    }

    // ======================================================================
    // 送信側 API
    // ======================================================================

    /// QoS 1 の PUBLISH を送信したときに呼び出す。
    ///
    /// このパケット識別子は PUBACK を待つ状態に遷移する。
    /// すでにフロー中のパケット識別子を指定した場合は何もしない。
    pub fn publish_sent_qos1(&mut self, packet_id: u16) {
        if packet_id == 0 || self.outgoing.contains_key(&packet_id) {
            return;
        }
        let seq = self.allocate_seq();
        let flow = OutgoingFlow {
            seq,
            state: FlowState::AwaitingPuback,
            quota_charged: true,
        };
        self.outgoing.insert(packet_id, flow);
    }

    /// QoS 2 の PUBLISH を送信したときに呼び出す。
    ///
    /// このパケット識別子は PUBREC を待つ状態に遷移する。
    /// すでにフロー中のパケット識別子を指定した場合は何もしない。
    pub fn publish_sent_qos2(&mut self, packet_id: u16) {
        if packet_id == 0 || self.outgoing.contains_key(&packet_id) {
            return;
        }
        let seq = self.allocate_seq();
        let flow = OutgoingFlow {
            seq,
            state: FlowState::AwaitingPubrec,
            quota_charged: true,
        };
        self.outgoing.insert(packet_id, flow);
    }

    // ======================================================================
    // 受信側 API
    // ======================================================================

    /// PUBACK を受信したときに呼び出す。
    ///
    /// 戻り値:
    /// - `Ok(Some(Action::Complete))`: QoS 1 フローが完了した。
    /// - `Ok(None)`: 該当するフローがない。
    /// - `Err(FlowError::StateMismatch)`: 状態が不一致（重複 PUBACK または不正なパケット）。
    pub fn puback_received(&mut self, packet_id: u16) -> Result<Option<Action>, FlowError> {
        if let Some(flow) = self.outgoing.remove(&packet_id) {
            match flow.state {
                FlowState::AwaitingPuback => Ok(Some(Action::Complete { packet_id })),
                _ => {
                    // 状態が一致しない場合はフローをそのままにする（シーケンス番号も維持する）。
                    self.outgoing.insert(packet_id, flow);
                    Err(FlowError::StateMismatch)
                }
            }
        } else if self.incoming.contains_key(&packet_id) {
            // 受信方向のフローに対する PUBACK は状態不一致である。
            Err(FlowError::StateMismatch)
        } else {
            Ok(None)
        }
    }

    /// PUBREC を受信したときに呼び出す。
    ///
    /// `reason_code` には受信した PUBREC の Reason Code を渡す。
    /// Reason Code を持たない MQTT v3.1.1 では 0x00 を渡すこと。
    ///
    /// 戻り値:
    /// - `Ok(Some(Action::SendPubrel))`: PUBREL を送信する必要がある。
    /// - `Ok(Some(Action::Aborted))`: フローが中断された。PUBREL を送信してはならない。
    /// - `Ok(None)`: 該当するフローがない。
    /// - `Err(FlowError::StateMismatch)`: 状態が不一致。
    ///
    /// MQTT v5.0 §4.3.3 [MQTT-4.3.3-4]:
    /// 送信者は Reason Code が 0x80 未満の PUBREC を受信したときに
    /// PUBREL パケットを送信しなければならない。
    pub fn pubrec_received(
        &mut self,
        packet_id: u16,
        reason_code: u8,
    ) -> Result<Option<Action>, FlowError> {
        // MQTT v5.0 §4.4 [MQTT-4.4.0-2]:
        // Reason Code 0x80 以上の PUBREC を受信した PUBLISH は確認済みとして扱い、
        // 再送してはならない。PUBREL も送信せずフローを終了する。
        if reason_code >= 0x80 {
            if let Some(flow) = self.outgoing.get(&packet_id) {
                return match flow.state {
                    FlowState::AwaitingPubrec => {
                        self.outgoing.remove(&packet_id);
                        Ok(Some(Action::Aborted { packet_id }))
                    }
                    // PUBREL 送信後（PUBCOMP 待ち）にエラー PUBREC を受信するのは
                    // 状態不一致であり、フローを維持して切断判断を上位層に委ねる。
                    _ => Err(FlowError::StateMismatch),
                };
            }
            if self.incoming.contains_key(&packet_id) {
                return Err(FlowError::StateMismatch);
            }
            return Ok(None);
        }

        if let Some(flow) = self.outgoing.remove(&packet_id) {
            match flow.state {
                FlowState::AwaitingPubrec => {
                    // MQTT v5.0 §4.6 [MQTT-4.6.0-4]:
                    // PUBREL は対応する PUBREC の受信順で送信しなければならないため、
                    // 初回の PUBREC 受信時にシーケンス番号を打ち直す
                    // （重複 PUBREC がある場合は初回の受信順と解釈する）。
                    let seq = self.allocate_seq();
                    let flow = OutgoingFlow {
                        seq,
                        state: FlowState::AwaitingPubcomp,
                        // PUBREC の受信では send quota は回復しないため、消費状態を引き継ぐ。
                        quota_charged: flow.quota_charged,
                    };
                    self.outgoing.insert(packet_id, flow);
                    Ok(Some(Action::SendPubrel { packet_id }))
                }
                // MQTT v5.0 §4.3.3 [MQTT-4.3.3-4]:
                // PUBREC の再送や重複受信に対しても PUBREL を返す。
                // 順序の規範は初回 PUBREC の受信順のため、シーケンス番号は打ち直さない。
                FlowState::AwaitingPubcomp => {
                    self.outgoing.insert(packet_id, flow);
                    Ok(Some(Action::SendPubrel { packet_id }))
                }
                _ => {
                    // 状態が一致しない場合はフローをそのままにする（シーケンス番号も維持する）。
                    // MQTT v5.0 §4.13.1: 状態不一致パケットは Protocol Error である。
                    // 切断判断は上位層の責務とする。
                    self.outgoing.insert(packet_id, flow);
                    Err(FlowError::StateMismatch)
                }
            }
        } else if self.incoming.contains_key(&packet_id) {
            // 受信方向のフローに対する PUBREC は状態不一致である。
            Err(FlowError::StateMismatch)
        } else {
            Ok(None)
        }
    }

    /// PUBREL を受信したときに呼び出す。
    ///
    /// MQTT v5.0 §4.3.3 [MQTT-4.3.3-11] / MQTT v3.1.1 §4.3.3:
    /// 受信者は PUBREL に対して同じ Packet Identifier の PUBCOMP で
    /// 応答しなければならない。該当する受信フローが存在しない場合
    /// （送信済み PUBCOMP の消失により相手が PUBREL を再送したケース）も
    /// PUBCOMP の再送が必要なため、`Action::ResendPubcomp` を返す。
    ///
    /// 戻り値:
    /// - `Ok(Some(Action::SendPubcomp))`: PUBCOMP を送信する必要がある。
    /// - `Ok(Some(Action::ResendPubcomp))`: 該当フローはないが PUBCOMP を再送する必要がある。
    /// - `Err(FlowError::StateMismatch)`: 状態が不一致。
    pub fn pubrel_received(&mut self, packet_id: u16) -> Result<Option<Action>, FlowError> {
        if let Some(state) = self.incoming.remove(&packet_id) {
            match state {
                FlowState::AwaitingPubrel => Ok(Some(Action::SendPubcomp { packet_id })),
                other => {
                    // 状態が一致しない場合はフローをそのままにする。
                    // MQTT v5.0 §4.13.1: 状態不一致パケットは Protocol Error である。
                    // 切断判断は上位層の責務とする。
                    self.incoming.insert(packet_id, other);
                    Err(FlowError::StateMismatch)
                }
            }
        } else if self.outgoing.contains_key(&packet_id) {
            // 送信方向のフローに対する PUBREL は状態不一致である。
            Err(FlowError::StateMismatch)
        } else {
            Ok(Some(Action::ResendPubcomp { packet_id }))
        }
    }

    /// PUBCOMP を受信したときに呼び出す。
    ///
    /// 戻り値:
    /// - `Ok(Some(Action::Complete))`: QoS 2 フローが完了した。
    /// - `Ok(None)`: 該当するフローがない。
    /// - `Err(FlowError::StateMismatch)`: 状態が不一致。
    pub fn pubcomp_received(&mut self, packet_id: u16) -> Result<Option<Action>, FlowError> {
        if let Some(flow) = self.outgoing.remove(&packet_id) {
            match flow.state {
                FlowState::AwaitingPubcomp => Ok(Some(Action::Complete { packet_id })),
                _ => {
                    // 状態が一致しない場合はフローをそのままにする（シーケンス番号も維持する）。
                    // MQTT v5.0 §4.13.1: 状態不一致パケットは Protocol Error である。
                    // 切断判断は上位層の責務とする。
                    self.outgoing.insert(packet_id, flow);
                    Err(FlowError::StateMismatch)
                }
            }
        } else if self.incoming.contains_key(&packet_id) {
            // 受信方向のフローに対する PUBCOMP は状態不一致である。
            Err(FlowError::StateMismatch)
        } else {
            Ok(None)
        }
    }

    /// QoS 1 の PUBLISH を受信したときに呼び出す。
    ///
    /// 戻り値: `Action::SendPuback` — PUBACK を送信する必要がある。
    /// 同時に PUBACK 送信完了待ち状態へ遷移する。
    /// 同一パケット識別子に対する重複受信の場合も PUBACK を再送する。
    ///
    /// MQTT v5.0 §3.3.4 [MQTT-3.3.4-9]:
    /// PUBACK 送信前の同じ Packet Identifier の PUBLISH 再着は同一メッセージの再送であり、
    /// 新たな未確認 PUBLISH ではない。利用者は [`Self::puback_sent`] でフローを完了させること。
    pub fn publish_received_qos1(&mut self, packet_id: u16) -> Action {
        // PUBACK 送信前の同じ Packet Identifier の PUBLISH は重複とみなす。
        // insert は既存エントリを上書きするだけなので、再着でも状態は維持される。
        self.incoming
            .insert(packet_id, FlowState::IncomingAwaitingPuback);
        Action::SendPuback { packet_id }
    }

    /// QoS 1 の受信 PUBLISH に対する PUBACK を送信したときに呼び出す。
    ///
    /// [`Self::publish_received_qos1`] で開始した受信フローを完了し、
    /// 同一 Packet Identifier の追跡を解除する。
    /// 該当する受信フローが無い場合は何もしない。
    pub fn puback_sent(&mut self, packet_id: u16) {
        if let Some(state) = self.incoming.remove(&packet_id)
            && state != FlowState::IncomingAwaitingPuback
        {
            // 予期しない状態なら戻す。切断判断は上位層の責務とする。
            self.incoming.insert(packet_id, state);
        }
    }

    /// QoS 2 の PUBLISH を受信したときに呼び出す。
    ///
    /// 戻り値: `Action::SendPubrec` — PUBREC を送信する必要がある。
    /// 同時に PUBREL を待つ状態に遷移する。
    /// 同一パケット識別子に対する重複受信の場合も PUBREC を再送する。
    /// MQTT v5.0 §4.3.3 [MQTT-4.3.3-10]:
    /// 同じ Packet ID の PUBLISH を PUBREL 受信前に重複受信した場合も PUBREC を再送するが、
    /// 重複メッセージを onward recipient に配送してはならない。
    pub fn publish_received_qos2(&mut self, packet_id: u16) -> Action {
        // PUBREL 受信前の同じ Packet Identifier の PUBLISH は重複とみなす。
        let is_duplicate = self.incoming.contains_key(&packet_id);
        self.incoming.insert(packet_id, FlowState::AwaitingPubrel);
        Action::SendPubrec {
            packet_id,
            is_duplicate,
        }
    }

    /// PUBLISH の再送が必要かどうかを確認する。
    ///
    /// 指定されたパケット識別子がフロー中で、パケットが未確認の場合に再送アクションを返す。
    /// QoS 1 で PUBACK を待っている場合は `ResendPublish`、
    /// QoS 2 で PUBCOMP を待っている場合は `ResendPubrel` を返す。
    pub fn needs_retransmission(&self, packet_id: u16) -> Option<Action> {
        match self.outgoing.get(&packet_id)?.state {
            FlowState::AwaitingPuback => Some(Action::ResendPublish { packet_id }),
            FlowState::AwaitingPubrec => Some(Action::ResendPublish { packet_id }),
            FlowState::AwaitingPubcomp => Some(Action::ResendPubrel { packet_id }),
            FlowState::AwaitingPubrel | FlowState::IncomingAwaitingPuback => None,
        }
    }

    /// 送信方向の未完了パケット識別子をすべて取得する。
    ///
    /// QoS 1/2 の再送対象となる全てのパケット識別子を返す。
    /// 受信方向のフロー（PUBREL 待ち）は再送対象ではないため含まれない。
    ///
    /// 順序の契約: PUBLISH 再送対象は元の PUBLISH の送信順
    /// （MQTT v5.0 §4.6 [MQTT-4.6.0-1]）、PUBREL 再送対象は対応する
    /// PUBREC の受信順（MQTT v5.0 §4.6 [MQTT-4.6.0-4]。重複 PUBREC がある場合は初回の
    /// 受信順と解釈する）で並ぶ。MQTT v3.1.1 §4.6 にも同一番号の規範がある。
    /// 両グループの相互の並びは仕様では規定されておらず、
    /// イベント発生順とする（本実装の設計判断）。
    pub fn pending_packet_ids(&self) -> Vec<u16> {
        let mut flows: Vec<(u64, u16)> = self
            .outgoing
            .iter()
            .map(|(&packet_id, flow)| (flow.seq, packet_id))
            .collect();
        flows.sort_unstable();
        flows.into_iter().map(|(_, packet_id)| packet_id).collect()
    }

    /// パケット識別子のフローを送信・受信の両方向から強制的にクリアする。
    ///
    /// 接続が切断された場合などに使用する。
    pub fn release(&mut self, packet_id: u16) {
        self.outgoing.remove(&packet_id);
        self.incoming.remove(&packet_id);
    }

    /// すべてのフロー状態をリセットする。
    ///
    /// セッション状態が破棄されるときに呼び出す。
    pub fn reset(&mut self) {
        self.outgoing.clear();
        self.incoming.clear();
        self.next_seq = 0;
    }
}

impl Default for QosFlowManager {
    fn default() -> Self {
        Self::new()
    }
}
