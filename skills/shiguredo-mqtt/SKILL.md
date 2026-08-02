---
name: shiguredo-mqtt
description: 時雨堂の依存 0・no_std・Sans I/O MQTT クライアントライブラリ shiguredo_mqtt の機能・API リファレンス。MQTT v3.1.1 / v5.0 のパケットエンコード、ストリーミングデコード、Session 状態管理、QoS 1 / 2、Keep Alive、再送、購読、トピックエイリアス、Receive Maximum、拡張認証、サーバー能力値に関する実装・レビュー・質問時に使用する。
---

# shiguredo_mqtt

時雨堂の MQTT クライアント専用ライブラリを扱う際は、以下の設計境界と API 契約に従う。

## 最初に確認すること

- 実装前にリポジトリ直下の `CODEBASE.md` を読む。
- 公開 API の最新状態は `src/` と rustdoc を正とし、このスキルとの食い違いがあればソースを優先する。
- MQTT 仕様の根拠が必要なら `refs/mqtt-v5.0.html` と `refs/mqtt-v3.1.1-os.html` を確認する。
- 仕様を引用するときは `CODEBASE.md` の形式に従い、必ず `MQTT v5.0 §...` または `MQTT v3.1.1 §...` と書く。規範文には可能な限り規範番号も付ける。
- Rust コードの変更時は `shiguredo-rust` スキルも使う。

## 設計境界

- MQTT クライアントだけを実装する。サーバー、ブローカー、リスナー、複数クライアントの受け入れ、サーバー側のトピックルーティングを追加しない。
- ネットワーク I/O、TLS、QUIC、タイマー、イベントループをライブラリ内へ持ち込まない。呼び出し側で I/O を行い、結果だけを状態機械へ通知する。
- MQTT v3.1.1 と MQTT v5.0 を混同しない。共通事項でも仕様根拠はバージョンごとに確認する。
- Client → Server と Server → Client を型で分離する。送信には各バージョンの `OutgoingPacket`、受信には `Decoder` が返す `IncomingPacket` を使う。
- パケット codec と接続状態を分離する。パケットを encode / decode しただけでは `Session` の状態は更新されない。

## 特徴とバージョン

- crate 名: `shiguredo_mqtt`
- バージョン: 2026.1.0-canary.0
- Rust Edition: 2024
- 最小 Rust バージョン: 1.93
- ライセンス: Apache-2.0
- 外部依存: なし
- `no_std`: 対応。ただし `alloc` は使用する。
- 対応プロトコル: MQTT v3.1.1、MQTT v5.0
- I/O モデル: Sans I/O

## モジュール構成

| モジュール | 役割 |
|---|---|
| `codec` | `MqttVersion`、`QoS`、`Limits`、MQTT 基本データ型 |
| `decoder` | バージョン共通のストリーミング `Decoder` と `VersionedIncomingPacket` |
| `error` | `DecodeError`、`EncodeError`、`EncodeInvalidField` |
| `v311` | MQTT v3.1.1 のパケット型と方向別 packet enum |
| `v5` | MQTT v5.0 のパケット型、プロパティ、方向別 packet enum |
| `state` | I/O 非依存のセッション、QoS、購読、Keep Alive、認証、フロー制御、トピックエイリアス管理 |

## 基本型

| 型 | 内容 |
|---|---|
| `codec::MqttVersion` | `V311` / `V5` |
| `codec::qos::QoS` | `AtMostOnce` / `AtLeastOnce` / `ExactlyOnce` |
| `codec::limits::Limits` | `max_packet_size`。既定値は 256 MiB |
| `decoder::Decoder` | 断片入力と複数パケット連続入力に対応するデコーダー |
| `decoder::VersionedIncomingPacket` | `V311(v311::IncomingPacket)` / `V5(v5::IncomingPacket)` |

`Limits::new()` の 256 MiB は保守的な DoS 上限ではない。信頼できない相手やメモリ制約のある環境では `with_max_packet_size()` で小さく設定する。

## 方向別パケット型

### MQTT v5.0

| 方向 | 型 | バリアント |
|---|---|---|
| Client → Server | `v5::OutgoingPacket` | `Connect`、`Publish`、`PubAck`、`PubRec`、`PubRel`、`PubComp`、`Subscribe`、`Unsubscribe`、`PingReq`、`Disconnect`、`Auth` |
| Server → Client | `v5::IncomingPacket` | `ConnAck`、`Publish`、`PubAck`、`PubRec`、`PubRel`、`PubComp`、`SubAck`、`UnsubAck`、`PingResp`、`Disconnect`、`Auth` |

### MQTT v3.1.1

| 方向 | 型 | バリアント |
|---|---|---|
| Client → Server | `v311::OutgoingPacket` | `Connect`、`Publish`、`PubAck`、`PubRec`、`PubRel`、`PubComp`、`Subscribe`、`Unsubscribe`、`PingReq`、`Disconnect` |
| Server → Client | `v311::IncomingPacket` | `ConnAck`、`Publish`、`PubAck`、`PubRec`、`PubRel`、`PubComp`、`SubAck`、`UnsubAck`、`PingResp` |

`Decoder` はクライアント受信方向に存在しないパケットを `DecodeError::UnexpectedPacket` として拒否する。MQTT v5.0 の `AUTH` と Server → Client の `DISCONNECT` は MQTT v3.1.1 にはない。

## パケットのエンコード

各パケット型と `OutgoingPacket` は次の API を持つ。

| API | 契約 |
|---|---|
| `encoded_len()` | MQTT Remaining Length、つまり固定ヘッダーを除く長さを返す |
| `encode(&mut [u8])` | 呼び出し側のバッファへ書き込み、総書き込みバイト数を返す |
| `OutgoingPacket::encode_to_vec()` | 必要な長さを計算して `Vec<u8>` を確保し、パケット全体を返す |

固定サイズの作業バッファが不要なら `encode_to_vec()` を優先する。`encoded_len()` をパケット全体の長さと誤解してバッファを確保しない。

```rust
use shiguredo_mqtt::v5;
use shiguredo_mqtt::v5::property::Properties;

let packet = v5::OutgoingPacket::Connect(v5::connect::Connect {
    client_id: "client-1".to_string(),
    clean_start: true,
    keep_alive: 60,
    properties: Properties::new(),
    will: None,
    username: None,
    password: None,
});

let bytes = packet.encode_to_vec()?;
// 呼び出し側で bytes を TCP / TLS / QUIC へ送信する。
# Ok::<(), shiguredo_mqtt::error::EncodeError>(())
```

エンコード時には、パケット識別子、QoS と DUP の組み合わせ、トピック名 / フィルター、プロパティの許可・重複・値域・方向、Reason Code などが検証される。失敗理由は `EncodeError::InvalidField { reason: EncodeInvalidField }` で分類する。

## ストリーミングデコード

```rust
use shiguredo_mqtt::codec::limits::Limits;
use shiguredo_mqtt::decoder::{Decoder, VersionedIncomingPacket};

let limits = Limits::new().with_max_packet_size(1024 * 1024);
let mut decoder = Decoder::new_v5(limits);

decoder.feed(&received_bytes)?;
while let Some(packet) = decoder.decode()? {
    match packet {
        VersionedIncomingPacket::V5(packet) => {
            // packet を処理し、必要な Session API を別途呼ぶ。
        }
        VersionedIncomingPacket::V311(_) => unreachable!("v5 decoder only returns v5 packets"),
    }
}
# Ok::<(), shiguredo_mqtt::error::DecodeError>(())
```

- `feed()` はバイト列を追加する。断片入力と、複数パケットを含む入力を受け付ける。
- `decode()` は 1 回に 1 パケットだけ返す。`Ok(None)` になるまで繰り返す。
- `set_limits()` は未消費バッファとデコード状態を維持したまま制限だけを変更する。CONNACK 後の制限反映に使える。
- 完全なフレーム内の不正データはエラー対象の 1 パケットを消費し、後続パケットを残す。
- Remaining Length 自体の不正は境界を信頼できないため内部バッファ全体をクリアする。
- `feed()` の累積上限には固定ヘッダー最大 5 バイト分の余裕が含まれる。エラーの `limit` は設定値より最大 5 バイト大きくなり得る。

## MQTT v5.0 プロパティ

`v5::property::Properties` に `Property` を `push()` して使う。`iter()`、`has_identifier()`、`encoded_len()`、`encode()`、`decode()` を提供する。

代表的な `Property`:

- ペイロード: `PayloadFormatIndicator`、`MessageExpiryInterval`、`ContentType`、`ResponseTopic`、`CorrelationData`
- セッション: `SessionExpiryInterval`、`AssignedClientIdentifier`、`ServerKeepAlive`
- 認証: `AuthenticationMethod`、`AuthenticationData`
- フロー / サイズ: `ReceiveMaximum`、`MaximumPacketSize`、`MaximumQoS`
- トピック: `TopicAliasMaximum`、`TopicAlias`、`SubscriptionIdentifier`
- 能力値: `RetainAvailable`、`WildcardSubscriptionAvailable`、`SubscriptionIdentifierAvailable`、`SharedSubscriptionAvailable`
- 拡張: `UserProperty`、`ReasonString`、`ServerReference`

同じプロパティでも許可されるパケット、方向、重複可否が異なる。`Property` 単体の encode 成功だけでなく、必ず対象パケット全体の encode / decode で文脈検証を通す。

## Session 状態機械

`state::session::Session` は MQTT 接続全体の状態をまとめる。I/O は行わず、利用者が wire 上の成功・受信イベントを通知する。

### 構築と接続

| API | 用途 |
|---|---|
| `Session::new_v5(client_id, clean_start)` | MQTT v5.0 セッションを作る |
| `Session::new_v311(client_id, clean_session)` | MQTT v3.1.1 セッションを作る |
| `configure_for_connect(...)` | Keep Alive、セッション期限、Receive Maximum、Topic Alias Maximum などを CONNECT 前に設定する |
| `connect_sent()` | CONNECT 送信を記録する |
| `connect_sent_with_auth(method)` | MQTT v5.0 の拡張認証付き CONNECT 送信を記録する |
| `apply_connack(ConnackParams)` | CONNACK の結果とプロパティを一括反映する |
| `disconnect_sent()` / `disconnect_received()` | MQTT DISCONNECT を記録する |
| `disconnected()` | transport 切断を記録する |
| `reset()` | セッション状態を初期化する |

`apply_connack()` では protocol version、接続状態、Session Present、Receive Maximum、Maximum Packet Size、Maximum QoS、Assigned Client Identifier、Authentication Method などを検証する。MQTT v3.1.1 セッションへ v5 専用パラメーターを渡さない。

### 状態更新の原則

```text
packet を構築する
  -> Session に送信予定を登録する
  -> wire へ送信する
  -> 成功なら activity() を記録する
  -> 失敗なら対応する abort_*() で取り消す
  -> 応答受信時は handle_*() を呼ぶ
```

すべての `*_sent` が wire 後ではない。各 API の rustdoc にある呼び出し順序を確認する。特に QoS、SUBSCRIBE、UNSUBSCRIBE は wire 前に pending 状態を登録し、送信失敗時に取り消す。

## パケット識別子と送信失敗

| 処理 | 正しい順序 | wire 失敗時 |
|---|---|---|
| QoS 1 / 2 PUBLISH | `allocate_packet_id()` → `publish_sent(qos, id)?` → 送信 | `abort_publish(id)` |
| SUBSCRIBE | `allocate_packet_id()` → `subscribe_sent(id, entries)` → 送信 | `abort_subscribe(id)` |
| UNSUBSCRIBE | `allocate_packet_id()` → `unsubscribe_sent(id, filters)` → 送信 | `abort_unsubscribe(id)` |

- `publish_sent()` が `Err` を返した場合は wire へ送らず、`release_packet_id(id)` だけを呼ぶ。`abort_publish()` は呼ばない。
- `handle_puback()`、`handle_pubcomp()`、`handle_pubrec()` の中断結果、`handle_suback()`、`handle_unsuback()` は必要なリソースを内部で解放する。完了後に二重解放しない。
- QoS 0 の PUBLISH に `publish_sent()` を使わない。QoS 0 は Packet Identifier と send quota を使わない。
- 同じ Packet Identifier で `publish_sent()` を重複して呼ばない。

## QoS 1 / 2

`Session` の統合ハンドラーを優先し、低水準の `QosFlowManager`、`FlowControl`、`PacketIdManager` をばらばらに更新しない。

| 受信イベント | Session API | 主な `Action` |
|---|---|---|
| PUBACK | `handle_puback(id)` | `Complete` |
| PUBREC | `handle_pubrec(id, reason_code)` | `SendPubrel` / `Aborted` |
| PUBREL | `handle_pubrel(id)` | `SendPubcomp` / `ResendPubcomp` |
| PUBCOMP | `handle_pubcomp(id)` | `Complete` |
| QoS 1 PUBLISH | `handle_publish_qos1(id)` | `SendPuback` |
| QoS 2 PUBLISH | `handle_publish_qos2(id)` | `SendPubrec { is_duplicate }` |

- `SendPuback` を wire 送信後に `puback_sent(id)` で記録する。
- `SendPubrec` を wire 送信後に `pubrec_sent(reason_code)` で記録する。
- `SendPubcomp` を wire 送信後に `pubcomp_sent()` で記録する。
- `ResendPubcomp` は既に完了済みのフローへの再応答なので `pubcomp_sent()` を呼ばない。
- `pending_retransmissions()` で再接続後の再送順を取得し、`resend_publish()` / `resend_pubrel()` で再送を状態へ反映する。
- PUBLISH 再送時は DUP を立てる。

## Keep Alive

`Session::keep_alive()` が返す `KeepAlive` をイベントループから確認する。

| API | タイミング |
|---|---|
| `activity(now)` | PINGREQ 以外の MQTT Control Packet の wire 送信成功後 |
| `pingreq_sent(now)` | PINGREQ の wire 送信時。activity も内部更新する |
| `pingresp_received()` | PINGRESP 受信時 |
| `should_send_pingreq(now)` | PINGREQ 送信時刻の判定 |
| `has_timed_out(now)` | PINGRESP 待ちのタイムアウト判定 |

`Timestamp` は `u64` であり、単位は呼び出し側が一貫して決める。通常はミリ秒を使う。

## MQTT v5.0 固有の状態管理

### Receive Maximum とサーバー能力値

- `FlowControl` は送受信方向の QoS 1 / 2 未確認数を追跡する。
- `ServerCapabilities` は CONNACK の Maximum Packet Size、Maximum QoS、Retain Available、各種 subscription availability を保持する。
- 送信前に `validate_outgoing_publish()` / `validate_outgoing_subscribe()` を呼ぶ。
- Maximum QoS は送信 PUBLISH の制限であり、SUBSCRIBE の Requested QoS を制限しない。

### トピックエイリアス

- `TopicAliasManager::set_own_maximum()` は受信側の上限、`set_peer_maximum()` は送信側の上限を設定する。
- 受信時は `resolve_on_receive()`、送信時は `find_alias_for_topic()` / `register_for_send()` を使う。
- Network Connection をまたいでマッピングを引き継がない。再接続時にリセットする。

### 拡張認証

- 初回認証は `connect_sent_with_auth(method)` で始める。
- AUTH 受信時は `auth_received(reason_code, authentication_method)` を呼び、`AuthAction` に従う。
- 初回認証の成功は成功 CONNACK で完了する。初回認証中の AUTH Success だけでは完了しない。
- 再認証は接続・初回認証完了後に `reauthenticate_sent()` で始める。
- MQTT v3.1.1 セッションで拡張認証 API を使わない。

## エラー処理

| エラー | 主な意味 |
|---|---|
| `DecodeError::InsufficientData` | 個別 codec では追加入力が必要。`Decoder::decode()` では `Ok(None)` に変換される |
| `DecodeError::MalformedPacket` | 完全なフレーム内の構造が不正 |
| `DecodeError::UnexpectedPacket` | クライアント受信方向に存在しないパケット |
| `DecodeError::PacketTooLarge` | 設定した受信上限を超過 |
| `EncodeError::BufferTooSmall` | 呼び出し側バッファが不足 |
| `EncodeError::PacketTooLarge` | MQTT Remaining Length または計算上の上限を超過 |
| `EncodeError::InvalidField` | パケットの意味論・方向・値域・組み合わせが不正 |

codec エラーを無視して接続を継続するかは MQTT バージョンと違反内容に依存する。仕様の切断要件を確認し、イベントループ側で transport を閉じる。

## 実装・レビュー時の確認項目

- クライアント専用という境界を越えていないか。
- v3.1.1 と v5.0 のパケット、Reason Code、プロパティを混同していないか。
- 送受信方向に対応した packet enum を使っているか。
- encode / decode と Session 状態更新の両方が実装されているか。
- wire 送信失敗時に対応する `abort_*()` または `release_packet_id()` があるか。
- QoS 完了時に Packet Identifier と send quota を二重解放していないか。
- Keep Alive の `activity()` / `pingreq_sent()` / `pingresp_received()` を正しい時点で呼んでいるか。
- 信頼できない入力に明示的な `Limits` を設定しているか。
- MQTT v5.0 の送信前に CONNACK 由来の能力値を検証しているか。
- 再接続時に QoS 再送、send quota、トピックエイリアスの扱いが整合しているか。
- 認証データ、password、Will payload をログや `Debug` 出力へ露出していないか。
- 仕様根拠のバージョン、節番号、規範番号が一次資料と一致しているか。

## テスト

- 通常の workspace テストにはコンテナランタイム必須の E2E を含めない。
- 既定確認は `cargo test --workspace --exclude e2e-tests --exclude quic-mqtt --exclude tokio-mqtt` を使う。
- E2E は `shiguredo_container`（macOS: Apple container、Linux: Docker Engine）を使い、`RUST_TEST_THREADS=1 cargo test -p e2e-tests -p quic-mqtt -p tokio-mqtt` で明示実行する。
- codec のラウンドトリップや状態機械の性質は `pbt/`、任意入力へのクラッシュ耐性は `fuzz/` を使う。
- モックやスタブを使わない。

## 参照先

- 設計境界と仕様引用規約: `CODEBASE.md`
- 利用例と概要: `README.md`
- 公開モジュール: `src/lib.rs`
- ストリーミングデコーダー: `src/decoder.rs`
- 接続状態機械: `src/state/session/mod.rs`
- MQTT v5.0 パケット: `src/v5/`
- MQTT v3.1.1 パケット: `src/v311/`
- 一次資料: `refs/mqtt-v5.0.html`、`refs/mqtt-v3.1.1-os.html`
