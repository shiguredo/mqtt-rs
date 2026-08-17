# noprop スキル準拠に PBT を整備する

- Created: 2026-08-17
- Completed: 2026-08-17
- Branch: feature/refactor-noprop-skill-compliance
- Polished: (未磨き上げ)

## 目的

- proptest から noprop への移行（`issues/closed/0001-update-proptest-to-noprop.md`）で書き換えた PBT を、noprop スキルの推奨事項に沿って仕上げるため
- 移行時に一部しか適用できなかった「空振り検証の防止」と「ジェネレータの健全性検証」を全テストに徹底する

## 現状

- `pbt/tests/prop_*.rs` の 12 ファイルは noprop で記述済みだが、noprop スキルの以下の推奨事項が未適用
  - 「`runner.stats().rejected_cases == 0` を valid-by-construction チェックとして維持する」が全テストで未実装
  - 両分岐が重要なテスト（`qos2_pubrec_reason_code_boundary`・`multi_topic_suback_activates_only_successes`・`send_alias_register_find_and_reuse` など）にカバレッジゲートがない
  - 既存ゲートの近くに p 推定値（miss 確率）の根拠コメントが不十分
- カバレッジゲートは `prop_flow_control.rs`（3 テスト）と `prop_keep_alive.rs`（1 テスト）にのみ存在する

## 設計方針

- noprop スキルの「Prevent vacuous success」「Validate the exploration strategy」の手順に従う
- `ctx.reject_case()` を意図的に使うテスト（`multi_topic_suback_activates_only_successes`・`subscription_reset_clears_all`・`reset_clears_all_flows`）には `rejected_cases == 0` を付けない（棄却が想定されるため）
- ゲートは `Cell<usize>` で invariant 評価箇所でインクリメントし、`{runner}` を含むメッセージで個別に assert する
- 各ゲートの隣に p 推定値と分岐重みの根拠をコメントで記録する

## 完了条件

- `rejected_cases == 0` の検証が、`ctx.reject_case()` を使わない全テストに追加されていること
- 両分岐が重要なテストにカバレッジゲートと p 推定値コメントが追加されていること
- `cargo test -p pbt`・`cargo fmt --all -- --check`・`cargo clippy -p pbt --all-targets -- -D warnings` が全て成功すること
- 複数の固定シードで全テストが安定して成功すること

## 解決方法

- `pbt/tests/prop_*.rs` の各テストに `assert_eq!(runner.stats().rejected_cases, 0, ...)` を追加した（`ctx.reject_case()` を使う 3 テストは意図的に除外し、除外理由をコメントで明記）
- `prop_qos_flow.rs` の `qos2_pubrec_reason_code_boundary` に 0x80 境界の両分岐ゲート（PUBREL 送信側 / フロー中断側）を追加した
- `prop_subscription.rs` の `multi_topic_suback_activates_only_successes` に success / failure の両分岐ゲートを追加した
- `prop_topic_alias.rs` の `send_alias_register_find_and_reuse` に枠埋まり分岐のゲートを追加した
- 既存・新規ゲートに p 推定値と分岐重み・miss 確率の根拠コメントを追記した
- 検証: `cargo test -p pbt`・`cargo fmt --all -- --check`・`cargo clippy -p pbt --all-targets -- -D warnings` が全て成功。固定シード 8 種で安定。既知の欠陥注入 2 件（`FlowControl::publish_sent` の Err 抑制・`reason_code` 生成の 0x80 以上限定）がそれぞれプロパティとゲートで検出されることを確認した
