//! MQTT QoS レベル。
//!
//! MQTT v5.0 §4.3 (Quality of Service levels and protocol flows) を参照。

/// MQTT QoS レベル。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum QoS {
    /// 最大 1 回配送。
    AtMostOnce = 0,
    /// 少なくとも 1 回配送。
    AtLeastOnce = 1,
    /// ちょうど 1 回配送。
    ExactlyOnce = 2,
}

impl QoS {
    /// 2 ビットの値から `QoS` を作成する。
    ///
    /// 予約値である 3 の場合は `None` を返す。
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(QoS::AtMostOnce),
            1 => Some(QoS::AtLeastOnce),
            2 => Some(QoS::ExactlyOnce),
            _ => None,
        }
    }

    /// この QoS レベルの数値を返す。
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}
