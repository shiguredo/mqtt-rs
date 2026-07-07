# mqtt-rs

[![crates.io](https://img.shields.io/crates/v/shiguredo_mqtt.svg)](https://crates.io/crates/shiguredo_mqtt)
[![docs.rs](https://docs.rs/shiguredo_mqtt/badge.svg)](https://docs.rs/shiguredo_mqtt)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![GitHub Actions](https://github.com/shiguredo/mqtt-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/shiguredo/mqtt-rs/actions/workflows/ci.yml)
[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?logo=discord&logoColor=white)](https://discord.gg/shiguredo)

## About Shiguredo's open source software

We will not respond to PRs or issues that have not been discussed on Discord. Also, Discord is only available in Japanese.

Please read <https://github.com/shiguredo/oss> before use.

## 時雨堂のオープンソースソフトウェアについて

利用前に <https://github.com/shiguredo/oss> をお読みください。

## 概要

Rust で実装された依存 0 かつ Sans I/O な MQTT クライアント専用ライブラリです。
MQTT v3.1.1 と MQTT v5.0 の両方に対応しています。

ネットワーク I/O はライブラリ内で行わず、MQTT 制御パケットのエンコード / デコード、ストリーミングデコーダー、接続状態の管理を提供します。TCP / TLS / QUIC 接続やイベントループは利用者が任意の実装を組み込めます。

本ライブラリは MQTT クライアントの実装を対象としており、サーバー・ブローカー・リスナー機能は提供しません。

## 特徴

- Sans I/O
  - <https://sans-io.readthedocs.io/index.html>
- `no_std` 対応（`alloc` は使用）
  - <https://docs.rust-embedded.org/book/intro/no-std.html>
- 依存ライブラリ 0
- MQTT v3.1.1 / v5.0 両対応
- クライアント目線の方向別パケット型
  - 送信パケットは `OutgoingPacket` によるエンコード API
  - 受信パケットは Decoder 経由の `IncomingPacket` で型レベルで分離
- ストリーミングパケットデコーダー
  - 断片入力、複数パケット連続受信に対応
  - Client → Server 専用種別のバイト列は `DecodeError::UnexpectedPacket` として拒否
- Sans I/O な接続状態管理（`state::session::Session`）
  - 接続ライフサイクル、CONNACK 適用、パケット識別子、QoS 1 / 2 フロー
  - Keep Alive、サブスクリプション管理
  - 送信失敗時の打ち消し（`abort_publish` / `abort_subscribe` / `abort_unsubscribe`）
  - セッション再開時の再送一覧（`pending_retransmissions` / `resend_publish` / `resend_pubrel`）
  - MQTT v5.0 固有: Receive Maximum によるフロー制御、トピックエイリアス、拡張認証（AUTH）
  - MQTT v5.0 固有: CONNACK 由来のサーバー能力値（`ServerCapabilities`）と送信前検証（`validate_outgoing_publish` / `validate_outgoing_subscribe`）
- パケットサイズ制限（既定値は 256 MiB、信頼できない相手には `Limits::with_max_packet_size` で明示的に上限を設定すること）

## 使い方

### パケットのエンコード

```rust
use shiguredo_mqtt::v5;
use shiguredo_mqtt::v5::property::Properties;

// クライアントから見て送信するパケットは OutgoingPacket で表す
let connect = v5::OutgoingPacket::Connect(v5::connect::Connect {
    client_id: "client-1".to_string(),
    clean_start: true,
    keep_alive: 60,
    properties: Properties::new(),
    will: None,
    username: None,
    password: None,
});

let mut buf = vec![0u8; 256];
let len = connect.encode(&mut buf).expect("failed to encode CONNECT packet");
// buf[..len] を送信...
```

### ストリーミングデコーダー

```rust
use shiguredo_mqtt::codec::limits::Limits;
use shiguredo_mqtt::codec::MqttVersion;
use shiguredo_mqtt::decoder::Decoder;

let mut decoder = Decoder::new(MqttVersion::V5).with_limits(Limits::new());

// 受信したバイト列を投入
decoder.feed(&received_bytes).expect("failed to feed bytes");

// 完成したパケットがあれば取得
if let Some(packet) = decoder.decode().expect("failed to decode packet") {
    // packet は VersionedIncomingPacket::V5(...) または VersionedIncomingPacket::V311(...)
    // クライアントから見て受信するパケットは IncomingPacket で表す
}
```

### セッション状態の管理

```rust
use shiguredo_mqtt::state::session::{ConnackParams, ConnackReason, Session};
use shiguredo_mqtt::v5::connack::ConnectReasonCode;

let mut session = Session::new_v5("client-1".to_string(), true)
    .expect("failed to create session");

// CONNECT 送信前に Keep Alive / Receive Maximum などを一括設定する
session
    .configure_for_connect(60, 0, 0, 65535, 0)
    .expect("invalid receive maximum");

// CONNECT を送信したら状態を更新する
session.connect_sent();

// CONNACK を受信したらパラメータを適用する
session
    .apply_connack(ConnackParams {
        session_present: false,
        reason_code: ConnackReason::V5(ConnectReasonCode::Success),
        session_expiry_interval: None,
        receive_maximum: None,
        topic_alias_maximum: None,
        server_keep_alive: None,
        maximum_packet_size: None,
        maximum_qos: None,
        retain_available: None,
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        assigned_client_identifier: None,
        authentication_method: None,
    })
    .expect("failed to apply CONNACK");

assert!(session.is_connected());
```

QoS 1 / 2 の PUBLISH、SUBSCRIBE / UNSUBSCRIBE、Keep Alive、AUTH なども `Session` の統合ハンドラ（`publish_sent` / `handle_puback` / `subscribe_sent` / `handle_suback` / `pingreq_sent` など）経由で状態を更新します。詳細は [docs.rs](https://docs.rs/shiguredo_mqtt) を参照してください。

## サンプル

### Tokio MQTT クライアント

`examples/tokio-mqtt` は `shiguredo_mqtt` を tokio の TCP / TLS (mqtts) 上で動かす MQTT v5.0 クライアントです。

```bash
# メッセージを公開する
cargo run -p tokio-mqtt -- publish --topic demo/hello --message 'hi'

# トピックを購読する（Ctrl+C で終了）
cargo run -p tokio-mqtt -- subscribe --topic demo/hello

# ホスト・ポート・QoS・RETAIN を指定する
cargo run -p tokio-mqtt -- publish \
  --host 127.0.0.1 --port 1883 \
  --topic demo/hello --message 'hi' --qos 1 --retain

# mqtts (TCP + TLS)
cargo run -p tokio-mqtt -- publish \
  --host 127.0.0.1 --port 8883 \
  --ca-file /path/to/ca.pem --server-name localhost \
  --topic demo/hello --message 'hi'
```

主なオプション: `--host`（既定 `127.0.0.1`）、`--port`（既定 `1883`）、`--client-id`、`--keep-alive`、`--qos`、`--retain`（publish のみ）、`--ca-file` / `--server-name`（mqtts）、`--verbose`。

### MQTT over QUIC クライアント

`examples/quic-mqtt` は `shiguredo_mqtt` を tokio / s2n-quic の QUIC 上で動かす MQTT v5.0 クライアントです。
既定ポートは EMQX の MQTT over QUIC と同じ `14567/udp` です。TLS 検証用の CA 証明書 (`--ca-file`) と SNI (`--server-name`) が必須です。

```bash
# メッセージを公開する
cargo run -p quic-mqtt -- publish \
  --ca-file /path/to/ca.pem --server-name localhost \
  --topic demo/hello --message 'hi'

# トピックを購読する（Ctrl+C で終了）
cargo run -p quic-mqtt -- subscribe \
  --ca-file /path/to/ca.pem --server-name localhost \
  --topic demo/hello

# ホスト・ポート・QoS・RETAIN を指定する
cargo run -p quic-mqtt -- publish \
  --host 127.0.0.1 --port 14567 \
  --ca-file /path/to/ca.pem --server-name localhost \
  --topic demo/hello --message 'hi' --qos 1 --retain
```

## 規格書

このライブラリが準拠している仕様書です。

- MQTT Version 5.0
  - <https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html>
- MQTT Version 3.1.1
  - <https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html>

## ライセンス

Apache License 2.0

```text
Copyright 2026-2026, Shiguredo Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
