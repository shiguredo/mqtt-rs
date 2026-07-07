//! ストリーミングデコーダーの drain コンパクション改善を計測するベンチマーク。
//!
//! 小さな PINGRESP パケットを大量に一括 feed し、連続で decode する
//! ワーストケースの処理時間を計測する。
//!
//! クライアントの受信方向に存在しない PINGREQ の代わりに PINGRESP を使うが、
//! どちらも同一の 2 バイトフレームであり Decoder 内部の処理経路は同一のため、
//! ベンチ ID 文字列を据え置く過去の計測履歴と連続する。

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use shiguredo_mqtt::codec::MqttVersion;
use shiguredo_mqtt::decoder::Decoder;
use shiguredo_mqtt::v5::pingresp::PingResp;

/// `count` 個の PINGRESP パケットを連結したバイト列を返す。
fn build_small_packets(count: usize) -> Vec<u8> {
    let packet = PingResp;
    let mut buf = [0u8; 8];
    let len = packet
        .encode(&mut buf)
        .expect("PINGRESP のエンコードに成功すること");
    let encoded = &buf[..len];
    let mut data = Vec::with_capacity(encoded.len().saturating_mul(count));
    for _ in 0..count {
        data.extend_from_slice(encoded);
    }
    data
}

/// `data` を 1 回 feed し、含まれるすべてのパケットを decode する。
fn decode_all_packets(decoder: &mut Decoder, data: &[u8]) {
    decoder.feed(data).expect("feed に成功すること");
    while let Ok(Some(_packet)) = decoder.decode() {
        black_box(());
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let data = build_small_packets(10_000);

    c.bench_function("decode 10000 pingreq after bulk feed", |b| {
        b.iter(|| {
            let mut decoder = Decoder::new(MqttVersion::V5);
            decode_all_packets(&mut decoder, black_box(&data));
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
