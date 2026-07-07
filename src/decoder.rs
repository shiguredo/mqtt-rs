//! ストリーミング MQTT パケットデコーダー。
//!
//! MQTT v5.0 §2.1 (Structure of an MQTT Control Packet) に定義された
//! 固定ヘッダー、可変ヘッダー、ペイロードの構造に従ってパケットを解析する。
//!
//! [`Decoder`] は受信バイト列をバッファリングし、完全な MQTT 制御パケットを
//! 1 つずつ生成する。MQTT v3.1.1 と MQTT v5.0 の両方を [`VersionedIncomingPacket`]
//! 列挙型を通じてサポートする。
//!
//! クライアント専用ライブラリとして、Client → Server 専用種別
//! （CONNECT / SUBSCRIBE / UNSUBSCRIBE / PINGREQ、および MQTT v3.1.1 の DISCONNECT）の
//! バイト列は [`DecodeError::UnexpectedPacket`] として拒否する
//! （MQTT v5.0 §2.1.2 Table 2-1 / MQTT v3.1.1 §2.2.1 Table 2.1）。

use alloc::format;
use alloc::vec::Vec;

use crate::codec::MqttVersion;
use crate::codec::limits::Limits;
use crate::codec::variable_byte_integer::VariableByteInteger;
use crate::error::DecodeError;
use crate::v5;
use crate::v311;

/// 1 パケットの固定ヘッダーに必要な最大オーバーヘッド（バイト数）。
///
/// MQTT の固定ヘッダーは種別・フラグ 1 バイトと、最大 4 バイトの
/// Variable Byte Integer からなる。feed 時点ではパケット境界が確定して
/// いないため、このオーバーヘッド分を `max_packet_size` に加えて
/// 未消費バッファの上限とする。
const MAX_PACKET_OVERHEAD: usize = 1 + 4;

/// プロトコルバージョン付きのデコード済み受信パケット。
///
/// クライアントから見て受信する（Server → Client）パケットのみを保持する。
/// Client → Server 専用種別はバリアントとして表現できない。
#[derive(Clone, PartialEq, Eq)]
pub enum VersionedIncomingPacket {
    /// MQTT v5.0 受信パケット。
    V5(v5::packet::IncomingPacket),
    /// MQTT v3.1.1 受信パケット。
    V311(v311::packet::IncomingPacket),
}

impl core::fmt::Debug for VersionedIncomingPacket {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::V5(p) => f.debug_tuple("V5").field(p).finish(),
            Self::V311(p) => f.debug_tuple("V311").field(p).finish(),
        }
    }
}

/// MQTT 制御パケットのストリーミングデコーダー。
///
/// バイト列は [`Decoder::feed`] で供給し、完全なパケットは [`Decoder::decode`] で取得する。
/// デコーダーは断片化された入力を許容し、複数のパケットが 1 つのバッファに到着した場合でも
/// 正しくパケット境界を検出する。
pub struct Decoder {
    version: MqttVersion,
    limits: Limits,
    buf: Vec<u8>,
    /// 未処理データの先頭オフセット。
    ///
    /// `decode` 成功時にパケット長だけ進め、バッファの先頭を `drain` せずに
    /// 償却 O(1) で済むようにする。一定量を超えたら `maybe_compact` で
    /// 未処理データを前方に移動する。
    buf_offset: usize,
    phase: DecodePhase,
}

impl core::fmt::Debug for Decoder {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Decoder")
            .field("version", &self.version)
            .field("limits", &self.limits)
            .field("buf", &format!("<{} bytes>", self.buf.len()))
            .field("buf_offset", &self.buf_offset)
            .field("phase", &self.phase)
            .finish()
    }
}

#[derive(Clone, Copy)]
enum DecodePhase {
    /// 次のパケットの最初のバイトを待っている。
    FixedHeader,
    /// 可変長の残り長を読み込んでいる。
    RemainingLength,
    /// `remaining_length` バイトのペイロードを読み込んでいる。
    Payload {
        rlen_len: usize,
        remaining_length: usize,
    },
}

impl core::fmt::Debug for DecodePhase {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::FixedHeader => f.write_str("FixedHeader"),
            Self::RemainingLength => f.write_str("RemainingLength"),
            Self::Payload { .. } => f.write_str("Payload"),
        }
    }
}

impl Decoder {
    /// 指定した MQTT プロトコルバージョン用の [`Decoder`] を新規作成する。
    pub fn new(version: MqttVersion) -> Self {
        Self {
            version,
            limits: Limits::new(),
            buf: Vec::new(),
            buf_offset: 0,
            phase: DecodePhase::FixedHeader,
        }
    }

    /// デコード制限を設定する。
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// MQTT v5.0 用の [`Decoder`] を新規作成する。
    pub fn new_v5(limits: Limits) -> Self {
        Self::new(MqttVersion::V5).with_limits(limits)
    }

    /// MQTT v3.1.1 用の [`Decoder`] を新規作成する。
    pub fn new_v311(limits: Limits) -> Self {
        Self::new(MqttVersion::V311).with_limits(limits)
    }

    /// デコード制限を差し替える。
    ///
    /// 内部バッファ（未消費データ）とデコード状態は保持したまま、制限値のみを更新する。
    /// CONNACK 受信後にサーバーが提示した `Maximum Packet Size` 等を反映する際に、
    /// デコーダーを作り直すと受信済みの後続パケットが失われるため、それを避ける用途を
    /// 想定する。
    pub fn set_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    /// 受信バイト列をデコーダーに供給する。
    ///
    /// 未消費バッファ（`buf_offset` 以降）と `data` の合計が、
    /// `max_packet_size` に 1 パケット分の固定ヘッダーオーバーヘッドを加えた
    /// 値を超える場合は [`DecodeError::PacketTooLarge`] を返す。
    ///
    /// feed 時点ではパケット境界が確定していないため、固定ヘッダー（最大 5 バイト）
    /// 分の余裕を持たせる。これにより、小さな `max_packet_size` を設定した場合でも
    /// 正当な小パケットが詰まった 1 チャンクが偽陽性で拒否されにくくなる。
    /// 未消費データが溜まっている場合は `decode()` を呼び出してバッファを消費すること。
    ///
    /// 空バッファへの単一巨大入力と、未処理データが残った状態での累積超過の
    /// 両方を防ぐ。
    pub fn feed(&mut self, data: &[u8]) -> Result<(), DecodeError> {
        let pending_len = self.buf.len().saturating_sub(self.buf_offset);
        let accumulated_size =
            pending_len
                .checked_add(data.len())
                .ok_or(DecodeError::PacketTooLarge {
                    size: usize::MAX,
                    limit: self.limits.max_packet_size,
                })?;
        let allowed_size = self
            .limits
            .max_packet_size
            .saturating_add(MAX_PACKET_OVERHEAD);
        if accumulated_size > allowed_size {
            return Err(DecodeError::PacketTooLarge {
                size: accumulated_size,
                limit: allowed_size,
            });
        }
        self.buf.extend_from_slice(data);
        Ok(())
    }

    /// バッファされたバイト列から完全なパケットを 1 つデコードしようと試みる。
    ///
    /// パケットが準備できた場合は `Ok(Some(packet))` を返す。さらなるバイト列が必要な場合は
    /// `Ok(None)` を返す。不正またはサイズ超過の入力に対しては `Err(_)` を返す。
    ///
    /// ペイロードフェーズで [`DecodeError::InsufficientData`] 以外のエラーが発生した場合、
    /// デコーダーはエラーの原因となった 1 パケット分だけをバッファから除去し、
    /// 後続のデータを `feed()` せずとも次のパケットのデコードを再開できる。
    /// 方向拒否（[`DecodeError::UnexpectedPacket`]）も同様に 1 フレームを消費して
    /// 後続パケットを維持する。
    ///
    /// 残り長さのデコードで [`DecodeError::InsufficientData`] 以外のエラーが発生した場合、
    /// デコーダーの内部バッファ全体がクリアされる。
    /// [`DecodeError::InsufficientData`] の場合はバッファを維持し、追加入力を待つ。
    pub fn decode(&mut self) -> Result<Option<VersionedIncomingPacket>, DecodeError> {
        match self.try_decode() {
            Ok(packet) => Ok(Some(packet)),
            Err(DecodeError::InsufficientData) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// バッファされたバイト列から完全なパケットを 1 つデコードしようと試みる内部メソッド。
    ///
    /// `FixedHeader` フェーズでは 1 バイトを消費して `RemainingLength` フェーズに遷移する。
    /// `RemainingLength` フェーズで [`DecodeError::InsufficientData`] が発生した場合はバッファを
    /// 維持し、追加入力を待つ。真正な異常またはサイズ超過の場合はバッファをクリアする。
    fn try_decode(&mut self) -> Result<VersionedIncomingPacket, DecodeError> {
        loop {
            match self.phase {
                DecodePhase::FixedHeader => {
                    if self.buf.len() <= self.buf_offset {
                        return Err(DecodeError::InsufficientData);
                    }
                    self.phase = DecodePhase::RemainingLength;
                }
                DecodePhase::RemainingLength => {
                    if self.buf.len().saturating_sub(self.buf_offset) < 2 {
                        return Err(DecodeError::InsufficientData);
                    }
                    let (vbi, rlen_len) =
                        match VariableByteInteger::decode(&self.buf[self.buf_offset + 1..]) {
                            Ok(v) => v,
                            Err(DecodeError::InsufficientData) => {
                                return Err(DecodeError::InsufficientData);
                            }
                            Err(e) => {
                                self.reset();
                                return Err(e);
                            }
                        };
                    let remaining_length = vbi.0 as usize;
                    let total_size = 1usize
                        .checked_add(rlen_len)
                        .and_then(|x| x.checked_add(remaining_length))
                        .ok_or(DecodeError::MalformedPacket)?;
                    if total_size > self.limits.max_packet_size {
                        self.reset();
                        return Err(DecodeError::PacketTooLarge {
                            size: total_size,
                            limit: self.limits.max_packet_size,
                        });
                    }
                    self.phase = DecodePhase::Payload {
                        rlen_len,
                        remaining_length,
                    };
                }
                DecodePhase::Payload {
                    rlen_len,
                    remaining_length,
                } => {
                    let packet_len = 1usize
                        .checked_add(rlen_len)
                        .and_then(|x| x.checked_add(remaining_length))
                        .ok_or(DecodeError::MalformedPacket)?;
                    let available = self.buf.len().saturating_sub(self.buf_offset);
                    if available < packet_len {
                        return Err(DecodeError::InsufficientData);
                    }
                    let start = self.buf_offset;
                    let end = self
                        .buf_offset
                        .checked_add(packet_len)
                        .ok_or(DecodeError::MalformedPacket)?;
                    let result = Self::decode_packet(self.version, &self.buf[start..end]);
                    self.buf_offset = end;
                    self.phase = DecodePhase::FixedHeader;
                    self.maybe_compact();
                    return result;
                }
            }
        }
    }

    fn reset(&mut self) {
        self.buf.clear();
        self.buf_offset = 0;
        self.phase = DecodePhase::FixedHeader;
    }

    /// 未処理データがバッファ後半を超えたら、未処理部分を前方に移動する。
    ///
    /// これにより `decode` 成功ごとの `drain` による O(N) コピーを防ぎ、
    /// 小パケットを一括デコードする場合の総計算量を O(N) に抑える。
    fn maybe_compact(&mut self) {
        if self.buf_offset > self.buf.len() / 2 {
            self.buf.drain(..self.buf_offset);
            self.buf_offset = 0;
        }
    }

    /// バッファ上の完成したパケットをプロトコルバージョンに応じてデコードする。
    ///
    /// 各バージョンの `IncomingPacket::decode` で残り長さが確定した後に
    /// `DecodeError::InsufficientData` が報告された場合、パケットは壊れているものとして
    /// `DecodeError::MalformedPacket` に変換する。
    ///
    /// Client → Server 専用種別の方向拒否判定は `IncomingPacket::decode` 側に集約しており、
    /// `DecodeError::UnexpectedPacket` はそのまま透過する。
    /// 本メソッドの呼び出し元（Payload フェーズ）は成功／失敗にかかわらず
    /// 1 フレームを消費して後続データを維持するため、方向拒否後も
    /// 後続パケットのデコードを継続できる。
    fn decode_packet(
        version: MqttVersion,
        buf: &[u8],
    ) -> Result<VersionedIncomingPacket, DecodeError> {
        match version {
            MqttVersion::V5 => match v5::packet::IncomingPacket::decode(buf) {
                Ok((p, _)) => Ok(VersionedIncomingPacket::V5(p)),
                Err(DecodeError::InsufficientData) => Err(DecodeError::MalformedPacket),
                Err(e) => Err(e),
            },
            MqttVersion::V311 => match v311::packet::IncomingPacket::decode(buf) {
                Ok((p, _)) => Ok(VersionedIncomingPacket::V311(p)),
                Err(DecodeError::InsufficientData) => Err(DecodeError::MalformedPacket),
                Err(e) => Err(e),
            },
        }
    }
}
