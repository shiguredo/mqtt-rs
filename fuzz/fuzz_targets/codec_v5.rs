#![no_main]

use libfuzzer_sys::fuzz_target;

// Decoder は Client → Server 専用種別を UnexpectedPacket で早期拒否するため、
// それら種別の内部デコード経路（CONNECT / SUBSCRIBE / UNSUBSCRIBE / PINGREQ）
// は低水準 codec を直接叩いて fuzz カバレッジを維持する。
fuzz_target!(|data: &[u8]| {
    // 連結された複数フレームの 2 フレーム目以降もデコード対象にするため、
    // consumed 分だけバッファを進めて末尾まで繰り返しデコードする。
    // エラーになった時点で打ち切る（エラー自体は正常系であり、検証目的は panic の検出）。
    let mut rest = data;
    while let Ok((_, consumed)) = shiguredo_mqtt::v5::packet::codec::Packet::decode(rest) {
        rest = &rest[consumed..];
    }
});
