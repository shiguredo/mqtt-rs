# mqtt-rs

- mqtt-rs は MQTT クライアント専用ライブラリとして実装すること
  - サーバー・ブローカー・リスナーの実装は行わない
  - クライアントとして不要な機能（例: 複数クライアントの同時受け入れ、トピックのサーバー側ルーティング）は追加しない
- E2E テストは Docker が必要。既定の workspace test（prek: `cargo test --workspace --exclude e2e-tests --exclude quic-mqtt --exclude tokio-mqtt` / CI: `cargo test --workspace --all-features --exclude e2e-tests --exclude quic-mqtt --exclude tokio-mqtt`）では実行しない
- E2E テストの明示実行は `RUST_TEST_THREADS=1 cargo test -p e2e-tests -p quic-mqtt -p tokio-mqtt`（ローカルでも CI でも同じ）
- バージョンが 2026.0.0 の間は CHANGES.md を更新しないこと
- バージョンが 2026.0.0 の間はブランチを作らず develop で 1 issue 1 コミットとして進めること
- MQTT 仕様を引用するときは次の形式に従うこと
  - 必ず「MQTT v5.0」または「MQTT v3.1.1」とバージョンから書き始めること（バージョンを省略した「§4.9」だけの引用は禁止）
  - 形式は「MQTT v5.0 §<節番号>」とすること（「仕様書」「節」の語は付けない）
  - MUST / SHOULD などの規範文を引用するときは規範番号を続けること
    - 例: MQTT v5.0 §3.2.2.3.4 [MQTT-3.2.2-10]
    - 例: MQTT v3.1.1 §4.3.3
  - 両バージョンに共通する内容でも、まとめずにそれぞれの節を個別に引用すること
