//! MQTT パケットのデコード時に適用する制限。

/// パケットデコード時に適用される制限。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// 固定ヘッダーを含む最大パケットサイズ（バイト数）。
    pub max_packet_size: usize,
}

impl Limits {
    /// デフォルト値で `Limits` を新規作成する。
    ///
    /// デフォルトの最大パケットサイズは 256 MiB である。
    /// これは DoS 対策としての「控えめな」値ではなく、多くの用途で実用上十分大きい値として
    /// 設定されている。信頼できない相手やメモリに厳しい環境では、
    /// [`Self::with_max_packet_size`] で明示的に小さく調整すること。
    pub fn new() -> Self {
        Self {
            max_packet_size: 256 * 1024 * 1024, // 256 MiB
        }
    }

    /// 最大パケットサイズを設定する。
    pub fn with_max_packet_size(mut self, size: usize) -> Self {
        self.max_packet_size = size;
        self
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self::new()
    }
}
