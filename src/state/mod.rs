//! MQTT Sans-I/O 状態機械。
//!
//! このモジュールは MQTT プロトコルの状態管理を Sans-I/O 方式で提供する。
//! 各状態機械は独立して使用可能であり、必要に応じて組み合わせることができる。
//!
//! MQTT v5.0 固有の機能（トピックエイリアス、認証、フロー制御）は
//! 対応する状態機械で管理され、MQTT v3.1.1 では単に使用されない。

pub mod auth;
pub mod flow_control;
pub mod keep_alive;
pub mod packet_id;
pub mod qos_flow;
pub mod session;
pub mod subscribe;
pub mod topic_alias;
