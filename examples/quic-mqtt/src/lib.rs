//! s2n-quic ベースの MQTT over QUIC クライアント example。
//!
//! CLI (`quic-mqtt` バイナリ) と統合テストの双方から利用するため、
//! クライアント実装はライブラリとして公開する。

pub mod client;
pub mod error;
