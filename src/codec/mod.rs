//! MQTT パケットのエンコード／デコードで共有される基本型。
//!
//! これらの型は MQTT v3.1.1 と v5.0 の両方の実装で使用される。

/// MQTT プロトコルバージョン。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqttVersion {
    /// MQTT v5.0。
    V5,
    /// MQTT v3.1.1。
    V311,
}

pub mod binary_data;
pub mod limits;
pub mod qos;
pub mod utf8_string;
pub mod variable_byte_integer;
