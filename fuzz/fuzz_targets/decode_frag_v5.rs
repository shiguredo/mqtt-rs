#![no_main]

use libfuzzer_sys::fuzz_target;
use shiguredo_mqtt::codec::limits::Limits;
use shiguredo_mqtt::decoder::Decoder;

// Decoder のストリーミング処理を fuzz で探索する（MQTT v5.0）。
// 対象は、任意位置での断片化、feed() と decode() の交互操作、
// および Malformed Packet / 方向拒否検出後の Decoder 内部状態遷移。
//
// 入力フォーマット（bounds check の後で参照する）:
//   data[0]        max_packet_size を 512..=2552 バイトの範囲に写像する hint。
//                  下限 512 は PacketTooLarge の過剰発火で buf が育たない退化を避けるため、
//                  上限 2552 は libFuzzer の既定入力上限 4096 バイト以内に body を収めつつ
//                  feed() の累積上限（PacketTooLarge 経路）に到達可能な範囲を確保するため。
//   data[1]        続く sizes[] の長さ n（0..=255）。
//   data[2..2+n]   sizes[]: 各要素が 1 チャンクの供給バイト数（0..=255）。
//   data[2+n..]    body: Decoder に流し込むバイト列。
//
// 挙動:
//   sizes を舐めながら「feed → decode を 1 回」を繰り返し、複数フェーズ遷移を feed 境界を
//   跨いで進めさせる。最終 feed の後の排出ループで、バッファ内に残った完成 packet を消費する。
//   feed() / decode() の Err はフェーズによって挙動が異なるが、いずれも Decoder を再呼び出し
//   可能な状態に保つため continue で許容する:
//     - feed() の Err（PacketTooLarge）: buf 無変更で return するため次回 feed から続く。
//     - Payload フェーズの Err（UnexpectedPacket 等）: 1 フレームを消費して後続を維持する。
//     - RemainingLength フェーズの Err（MalformedPacket / PacketTooLarge）: reset() で
//       バッファを全クリアする。
//
// invariants:
//   - Decoder::feed() / Decoder::decode() が panic しないこと（libFuzzer が検出）。
//   - 末尾の排出ループが Ok(None) に有限回で収束すること。無限ループ化した場合は
//     libFuzzer のタイムアウトで検出される。
fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }
    let max_size_hint = data[0];
    let limits = Limits::new().with_max_packet_size(512 + (max_size_hint as usize) * 8);
    let mut decoder = Decoder::new_v5(limits);

    let sizes_len = data[1] as usize;
    let body_start = 2 + sizes_len;
    if body_start > data.len() {
        return;
    }
    let sizes = &data[2..body_start];
    let body = &data[body_start..];

    // 不変量: processed <= body.len()。chunk_len = min(sz, body.len() - processed) により
    // body[processed..processed + chunk_len] のスライシングは常に範囲内。
    let mut processed: usize = 0;
    for &sz in sizes {
        let chunk_len = (sz as usize).min(body.len() - processed);
        let chunk = &body[processed..processed + chunk_len];
        let _ = decoder.feed(chunk);
        let _ = decoder.decode();
        processed += chunk_len;
    }

    // sizes を舐め終えた残余を feed する（残余が無ければ空 feed）。
    let _ = decoder.feed(&body[processed..]);

    // Ok(None) に達するまで packet を排出する。
    loop {
        match decoder.decode() {
            Ok(None) => break,
            Ok(Some(_)) => {}
            Err(_) => continue,
        }
    }
});
