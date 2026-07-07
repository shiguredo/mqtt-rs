//! MQTT パケットのエンコード／デコード時に発生するエラー型。

/// MQTT パケットのデコード中に発生しうるエラー。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    /// デコードを完了するのに十分なバイト列がない。
    InsufficientData,
    /// パケットが MQTT 仕様に準拠していない。
    MalformedPacket,
    /// パケット種別フィールドの値が無効である。
    InvalidPacketType,
    /// 仕様上有効なパケット種別だが、クライアントの受信方向（Server → Client）には
    /// 存在してはならないパケットを受信した。
    ///
    /// CONNECT / SUBSCRIBE / UNSUBSCRIBE / PINGREQ（および MQTT v3.1.1 の DISCONNECT）は
    /// Client → Server 専用種別であり（MQTT v5.0 §2.1.2 Table 2-1 / MQTT v3.1.1 §2.2.1 Table 2.1）、
    /// `Decoder` がクライアント受信方向でこれらを検出した場合に返す。
    /// 未定義の種別（MQTT v3.1.1 では 0x00 / 0xF0、MQTT v5.0 では 0x00 の Reserved）は
    /// 本バリアントではなく [`DecodeError::InvalidPacketType`] で表す。
    UnexpectedPacket {
        /// 固定ヘッダー先頭バイトの上位 4 ビット（Control Packet type）。
        /// `Decoder` は `buf[start] & 0xF0` を格納する（フラグビットは切り落とす）。
        /// CONNECT なら 0x10、SUBSCRIBE なら 0x80、UNSUBSCRIBE なら 0xA0、PINGREQ なら 0xC0、
        /// MQTT v3.1.1 の DISCONNECT なら 0xE0。
        packet_type: u8,
    },
    /// パケット種別に対してフラグフィールドの値が無効である。
    InvalidPacketFlags,
    /// UTF-8 エンコードされた文字列が正しい UTF-8 でない。
    InvalidUtf8,
    /// パケットが設定された最大サイズを超えた。
    PacketTooLarge {
        /// デコードされたパケットサイズ。
        size: usize,
        /// 設定されたサイズ制限。
        limit: usize,
    },
}

/// フレーム境界が確定した領域内のパースで発生した [`DecodeError::InsufficientData`] を
/// [`DecodeError::MalformedPacket`] に変換する。
///
/// Remaining Length で境界が確定した完全なフレーム内では、内部の長さフィールドが
/// 境界を超えて指していても追加入力では解決しない。このようなパケットは
/// MQTT v5.0 §1.2 の Malformed Packet の定義
/// （この仕様に従ってパースできないパケット）に該当するものとして扱う。
/// MQTT v3.1.1 §4.8 [MQTT-4.8.0-1]（protocol violation 時は接続を閉じる）に対応する。
///
/// フレーム長チェック自体（バッファがフレーム長より短い真のデータ不足）に
/// 適用してはならない。適用してよいのは、フレーム境界で切ったスライスに対する
/// 内部パースのみである。
pub(crate) fn malformed_if_insufficient(e: DecodeError) -> DecodeError {
    match e {
        DecodeError::InsufficientData => DecodeError::MalformedPacket,
        other => other,
    }
}

/// [`EncodeError::InvalidField`] の違反種別。
///
/// フィールド位置（PUBLISH Topic Name / Will Topic / Response Topic など）は区別せず、
/// 違反の種類だけを表す。全バリアントは unit のみとし、動的な文字列は載せない。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeInvalidField {
    /// Topic Name が空である（v5 では Topic Alias 無しのとき）。
    EmptyTopicName,
    /// Topic Name にワイルドカード文字 (`+` / `#`) が含まれる。
    WildcardInTopicName,
    /// QoS が 0 以外なのに Packet Identifier が欠落している。
    MissingPacketId,
    /// QoS 0 なのに Packet Identifier が付いている。
    UnexpectedPacketId,
    /// Packet Identifier が 0 である。
    ZeroPacketId,
    /// QoS 0 なのに DUP が立っている。
    DupWithQos0,
    /// SUBSCRIBE の subscriptions が空である。
    EmptySubscriptions,
    /// SUBSCRIBE / UNSUBSCRIBE の topic_filters が空である。
    EmptyTopicFilters,
    /// SUBACK / UNSUBACK の reason_codes が空である。
    EmptyReasonCodes,
    /// SUBACK の return_codes が空である。
    EmptyReturnCodes,
    /// Topic Filter の構文が不正である。
    InvalidTopicFilter,
    /// 共有サブスクリプションで No Local が指定されている。
    SharedSubscriptionNoLocal,
    /// プロパティの許可リスト・方向・共起などの検証に失敗した。
    PropertyValidationFailed,
    /// プロパティ識別子が重複している。
    DuplicatePropertyIdentifier,
    /// プロパティの値域が不正である。
    InvalidPropertyValue,
    /// Payload Format Indicator が 1 なのにペイロードが UTF-8 でない。
    InvalidPayloadUtf8,
    /// クライアントが送れない Reason Code である。
    InvalidReasonCode,
    /// Session Present が立っているのに成功以外の Return / Reason Code である。
    SessionPresentWithNonSuccess,
    /// UTF-8 文字列に U+0000 が含まれる。
    NullInUtf8String,
    /// password があるのに username がない（MQTT v3.1.1）。
    PasswordWithoutUsername,
    /// ClientId が空なのに Clean Session が 0 である（MQTT v3.1.1）。
    EmptyClientIdWithoutCleanSession,
}

/// MQTT パケットのエンコード中に発生しうるエラー。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeError {
    /// 提供されたバッファがエンコード後のパケットを格納するには小さすぎる。
    BufferTooSmall,
    /// パケットが許容される最大サイズを超えた。
    PacketTooLarge {
        /// エンコード対象のサイズ。
        size: usize,
        /// サイズ制限。
        limit: usize,
    },
    /// パケット内のフィールド値が指定されたコンテキストに対して無効である。
    InvalidField {
        /// 検証に失敗した理由。
        reason: EncodeInvalidField,
    },
}

impl core::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InsufficientData => write!(f, "insufficient data to decode packet"),
            Self::MalformedPacket => write!(f, "malformed packet"),
            Self::InvalidPacketType => write!(f, "invalid packet type"),
            Self::UnexpectedPacket { packet_type } => write!(
                f,
                "unexpected packet type 0x{packet_type:02X} in client-receive direction"
            ),
            Self::InvalidPacketFlags => write!(f, "invalid packet flags"),
            Self::InvalidUtf8 => write!(f, "invalid UTF-8 string"),
            Self::PacketTooLarge { size, limit } => {
                write!(f, "packet size {} exceeds limit {}", size, limit)
            }
        }
    }
}

impl core::fmt::Display for EncodeInvalidField {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::EmptyTopicName => write!(f, "empty topic name"),
            Self::WildcardInTopicName => write!(f, "wildcard in topic name"),
            Self::MissingPacketId => write!(f, "missing packet identifier"),
            Self::UnexpectedPacketId => write!(f, "unexpected packet identifier"),
            Self::ZeroPacketId => write!(f, "zero packet identifier"),
            Self::DupWithQos0 => write!(f, "dup with qos 0"),
            Self::EmptySubscriptions => write!(f, "empty subscriptions"),
            Self::EmptyTopicFilters => write!(f, "empty topic filters"),
            Self::EmptyReasonCodes => write!(f, "empty reason codes"),
            Self::EmptyReturnCodes => write!(f, "empty return codes"),
            Self::InvalidTopicFilter => write!(f, "invalid topic filter"),
            Self::SharedSubscriptionNoLocal => write!(f, "shared subscription with no local"),
            Self::PropertyValidationFailed => write!(f, "property validation failed"),
            Self::DuplicatePropertyIdentifier => write!(f, "duplicate property identifier"),
            Self::InvalidPropertyValue => write!(f, "invalid property value"),
            Self::InvalidPayloadUtf8 => write!(f, "invalid payload utf-8"),
            Self::InvalidReasonCode => write!(f, "invalid reason code"),
            Self::SessionPresentWithNonSuccess => {
                write!(f, "session present with non-success")
            }
            Self::NullInUtf8String => write!(f, "null in utf-8 string"),
            Self::PasswordWithoutUsername => write!(f, "password without username"),
            Self::EmptyClientIdWithoutCleanSession => {
                write!(f, "empty client id without clean session")
            }
        }
    }
}

impl core::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::BufferTooSmall => write!(f, "buffer too small for encoded packet"),
            Self::PacketTooLarge { size, limit } => {
                write!(f, "packet size {} exceeds limit {}", size, limit)
            }
            Self::InvalidField { reason } => write!(f, "invalid field: {reason}"),
        }
    }
}

impl core::error::Error for DecodeError {}

impl core::error::Error for EncodeError {}
