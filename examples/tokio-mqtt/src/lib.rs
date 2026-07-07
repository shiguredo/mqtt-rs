//! Tokio ベースの MQTT クライアント example。
//!
//! CLI (`tokio-mqtt` バイナリ) と統合テストの双方から利用するため、
//! クライアント実装はライブラリとして公開する。

pub mod client;
pub mod error;
