#![no_main]

use libfuzzer_sys::fuzz_target;
use shiguredo_mqtt::codec::MqttVersion;
use shiguredo_mqtt::decoder::Decoder;

fuzz_target!(|data: &[u8]| {
    let mut decoder = Decoder::new(MqttVersion::V5);
    let _ = decoder.feed(data);
    // Err はフェーズによって挙動が異なるが、いずれも進捗するため continue で許容し、
    // Ok(None) のみで抜ける: Payload フェーズの Err（UnexpectedPacket 等）は
    // 1 フレームを消費して後続パケットのデコードへ進み、RemainingLength フェーズの
    // Err（MalformedPacket / PacketTooLarge）は reset() によりバッファを全クリアする。
    loop {
        match decoder.decode() {
            Ok(None) => break,
            Ok(Some(_)) => {}
            Err(_) => continue,
        }
    }
});
