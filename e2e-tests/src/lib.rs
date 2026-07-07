//! MQTT v3.1.1 と v5.0 の E2E テスト。
//!
//! 実際の MQTT ブローカー（Mosquitto / EMQX）に対して、自前のエンコーダー／デコーダーで
//! パケットを送受信し、publish / subscribe が正しく動作することを検証する。
//!
//! Mosquitto は平文 TCP に加え、mqtts (TCP + TLS / tokio-rustls) の smoke test も行う。
//! EMQX は平文 TCP と MQTT over QUIC の smoke test を行う。
//!
//! Docker が必要。実行は `RUST_TEST_THREADS=1 cargo test -p e2e-tests`。
//! 既定の workspace test からは `--exclude e2e-tests` で除外する。

pub mod client;
pub mod scram;

pub mod v311;
pub mod v5;
