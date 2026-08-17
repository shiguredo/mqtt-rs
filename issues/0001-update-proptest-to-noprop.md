# PBT を proptest から noprop に切り替える

- Created: 2026-08-17
- Completed: (未完了)
- Branch: feature/update-proptest-to-noprop
- Polished: (未磨き上げ)

## 目的

- 時雨堂の PBT 方針を proptest から noprop に切り替えるため
- noprop は命令的なクロージャでプロパティを書くため、手続き的な状態機械テストやモデルベーステストと相性が良く、探索空間の設計意図をコードとして直接記述できる

## 現状

- `pbt/Cargo.toml` が `proptest = "1.11"` を依存にしている
- `pbt/tests/` 配下の 12 ファイル（`prop_*.rs`）と `pbt/tests/helpers/mod.rs` が proptest の DSL（`proptest!` マクロ、`prop_assert_*!`、Strategy 型）に依存している
- `src/state/` 配下の状態機械（Session・QosFlowManager・FlowControl・PacketIdManager・SubscriptionManager・TopicAliasManager・KeepAlive・AuthStateMachine）と codec の roundtrip を PBT で検証している
- `shiguredo-rust` スキルの「PBT は proptest を使うこと」という記述も noprop に更新が必要

## 設計方針

- noprop 0.2.0 を使う（最新安定版）
- 各 `prop_*.rs` のテスト関数は `noprop::Runner::new(seed)` とクロージャによる命令的記述に置き換える
- シードは `noprop::seed_from_env_or_time("MQTT_PBT_SEED")` から取得する
- `proptest!` マクロの引数生成を、`sample_*` 系サンプラ（`sample_usize_in`・`sample_choice`・`sample_weighted_index`・`sample_with_boundaries`・`sample_with_rejection` など）に置き換える
- `prop_assert_*!` は通常の `assert_*!` に置き換える
- 制約付きの入力（フィルタが必要だった戦略）は、可能な限り valid-by-construction な生成に置き換え、`sample_with_rejection` は受容率を根拠に `max_attempts` を決める
- 空で成功する検証（空リスト・空文字列など）にはカバレッジゲート（`Cell`）を置いて、到達を確認してから断言する
- テストのログメッセージ・コメントは日本語のまま維持する

## 完了条件

- `pbt/Cargo.toml` から proptest が除去され、`noprop = "0.2"` が入っていること
- `pbt/tests/` 配下の全ファイルが noprop で書き換えられ、proptest への参照が 1 つも残っていないこと
- `cargo test -p pbt` が全て成功すること
- `cargo fmt --check` と `cargo clippy -p pbt` が全て通ること
- `shiguredo-rust` スキルの「PBT は noprop を使うこと」への更新（スキル側の作業）を促す記録が残っていること

## 解決方法

- `pbt/Cargo.toml` の依存を `proptest = "1.11"` から `noprop = "0.2"` に変更する
- `pbt/tests/helpers/mod.rs` の Strategy 型を noprop のサンプラ関数に置き換える
- `pbt/tests/prop_*.rs` を noprop の命令的記述に書き換える
- 各テストの探索空間設計（境界値・空・最大値・分布）は noprop スキルの手順に従い、カバレッジゲートで空振り検証を防ぐ
