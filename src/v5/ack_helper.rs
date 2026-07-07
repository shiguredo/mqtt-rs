//! MQTT v5.0 の簡易 ack パケット用の共通ヘルパー関数。
//!
//! 固定ヘッダー検証、VBI デコード、残り長さに基づく終了位置計算などの
//! 定型部を共通化する。

use crate::codec::variable_byte_integer::VariableByteInteger;
use crate::error::{DecodeError, EncodeError};
use crate::v5::property::Properties;

/// 残り長さが 0 のパケットをエンコードする。
///
/// `packet_type` と `flags` を固定ヘッダーに書き込み、VBI で残り長さ 0 をエンコードする。
pub(crate) fn encode_empty(
    buf: &mut [u8],
    packet_type: u8,
    flags: u8,
) -> Result<usize, EncodeError> {
    encode_fixed_header(buf, packet_type, flags, 0)
}

/// 残り長さが 0 のパケットをデコードする。
pub(crate) fn decode_empty(buf: &[u8], packet_type: u8, flags: u8) -> Result<usize, DecodeError> {
    let (remaining_len, header_len, _) = decode_fixed_header(buf, packet_type, flags)?;
    if remaining_len != 0 {
        return Err(DecodeError::MalformedPacket);
    }
    Ok(header_len)
}

/// 理由コード + プロパティのみを持つパケット（DISCONNECT / AUTH）の残り長さを計算する。
///
/// 常に理由コードとプロパティ長を含む長形式の残り長さを返す（省略形は生成しない）。
/// `reason_code` は長さ計算に使わないが、呼び出し側との引数対応のため受け取る。
pub(crate) fn reason_code_only_packet_encoded_len(
    _reason_code: u8,
    properties: &Properties,
) -> usize {
    1usize.saturating_add(properties.encoded_len())
}

/// 理由コード + プロパティのみを持つパケット（DISCONNECT / AUTH）をエンコードする。
///
/// 戻り値は書き込んだ総バイト数。
pub(crate) fn encode_reason_code_only_packet(
    buf: &mut [u8],
    packet_type: u8,
    flags: u8,
    reason_code: u8,
    properties: &Properties,
) -> Result<usize, EncodeError> {
    let remaining_len = reason_code_only_packet_encoded_len(reason_code, properties);
    let mut offset = encode_fixed_header(buf, packet_type, flags, remaining_len)?;

    // 省略形は生成しないため remaining_len は常に 1 以上であり、
    // 理由コードとプロパティを無条件に書き込む。
    buf[offset] = reason_code;
    offset += 1;
    offset += properties.encode(&mut buf[offset..])?;

    Ok(offset)
}

/// DISCONNECT の理由コード + プロパティ部分をデコードする。
///
/// `remaining_len == 0` のときは MQTT v5.0 §3.14.2.1 に従い、
/// Reason Code `0x00`（Normal disconnection）と空の Properties を使う。
/// 戻り値は `(reason_code, properties, consumed_bytes)` である。
/// プロパティの検証は呼び出し側で行う。
///
/// AUTH はこのヘルパーを使わない (判定順制御のため。詳細は `Auth::decode` のコメント参照)。
pub(crate) fn decode_reason_code_only_packet(
    buf: &[u8],
    packet_type: u8,
    flags: u8,
) -> Result<(u8, Properties, usize), DecodeError> {
    let (remaining_len, header_len, end) = decode_fixed_header(buf, packet_type, flags)?;

    if remaining_len == 0 {
        return Ok((0x00, Properties::new(), end));
    }

    let mut offset = header_len;
    if offset >= end {
        return Err(DecodeError::MalformedPacket);
    }
    let reason_code = buf[offset];
    offset += 1;

    let (properties, n) = if offset == end {
        // remaining_len == 1 の場合、プロパティは空として扱う。
        (Properties::new(), 0)
    } else {
        let (p, n) = Properties::decode(&buf[offset..end])?;
        (p, n)
    };
    offset += n;

    if offset != end {
        return Err(DecodeError::MalformedPacket);
    }

    Ok((reason_code, properties, offset))
}

/// 固定ヘッダーをエンコードする。
///
/// `packet_type | flags` を書き込み、続けて `remaining_len` の VBI 表現を書き込む。
/// 戻り値は可変ヘッダー・ペイロードの書き込み開始オフセットである。
pub(crate) fn encode_fixed_header(
    buf: &mut [u8],
    packet_type: u8,
    flags: u8,
    remaining_len: usize,
) -> Result<usize, EncodeError> {
    if remaining_len > VariableByteInteger::MAX as usize {
        return Err(EncodeError::PacketTooLarge {
            size: remaining_len,
            limit: VariableByteInteger::MAX as usize,
        });
    }

    let vbi = VariableByteInteger(remaining_len as u32);
    let total_len = 1usize
        .checked_add(vbi.encoded_len())
        .and_then(|x| x.checked_add(remaining_len))
        .ok_or(EncodeError::PacketTooLarge {
            size: remaining_len,
            limit: VariableByteInteger::MAX as usize,
        })?;
    if buf.len() < total_len {
        return Err(EncodeError::BufferTooSmall);
    }

    buf[0] = packet_type | flags;
    let offset = 1 + vbi.encode(&mut buf[1..])?;
    Ok(offset)
}

/// 固定ヘッダーをデコードする。
///
/// パケット型とフラグを検証し、VBI から残り長さを読み取る。
/// 戻り値は `(remaining_len, header_len, end)` である。
pub(crate) fn decode_fixed_header(
    buf: &[u8],
    packet_type: u8,
    flags: u8,
) -> Result<(usize, usize, usize), DecodeError> {
    if buf.is_empty() {
        return Err(DecodeError::InsufficientData);
    }
    if buf[0] & 0xF0 != packet_type {
        return Err(DecodeError::InvalidPacketType);
    }
    if buf[0] & 0x0F != flags {
        return Err(DecodeError::InvalidPacketFlags);
    }

    let (remaining_len, vbi_len) = VariableByteInteger::decode(&buf[1..])?;
    let remaining_len = remaining_len.0 as usize;
    let header_len = 1 + vbi_len;
    if buf.len() < header_len + remaining_len {
        return Err(DecodeError::InsufficientData);
    }

    let end = header_len + remaining_len;
    Ok((remaining_len, header_len, end))
}

/// パケット識別子のみを含む ack パケットの可変ヘッダーをデコードする。
///
/// `end` は `decode_fixed_header` で得た終了位置である。
/// 戻り値は `(packet_id, offset)` である。
pub(crate) fn decode_packet_id(
    buf: &[u8],
    header_len: usize,
    end: usize,
) -> Result<(u16, usize), DecodeError> {
    let mut offset = header_len;

    if offset + 2 > end {
        return Err(DecodeError::MalformedPacket);
    }
    let packet_id = u16::from_be_bytes([buf[offset], buf[offset + 1]]);
    offset += 2;

    // MQTT v5.0 §2.2.1 [MQTT-2.2.1-3]: Packet Identifier は 0 以外でなければならない。
    if packet_id == 0 {
        return Err(DecodeError::MalformedPacket);
    }

    Ok((packet_id, offset))
}

/// パケット識別子 + 理由コード + プロパティを持つ ACK パケットの残り長さを計算する。
///
/// 常に packet_id・理由コード・プロパティ長を含む長形式の残り長さを返す（省略形は生成しない）。
/// `reason_code` は長さ計算に使わないが、呼び出し側との引数対応のため受け取る。
pub(crate) fn reason_code_packet_encoded_len(_reason_code: u8, properties: &Properties) -> usize {
    2usize
        .saturating_add(1)
        .saturating_add(properties.encoded_len())
}

/// パケット識別子 + 理由コード + プロパティを持つ ACK パケットをエンコードする。
///
/// 戻り値は書き込んだ総バイト数。
pub(crate) fn encode_reason_code_packet(
    buf: &mut [u8],
    packet_type: u8,
    flags: u8,
    packet_id: u16,
    reason_code: u8,
    properties: &Properties,
) -> Result<usize, EncodeError> {
    let remaining_len = reason_code_packet_encoded_len(reason_code, properties);
    let mut offset = encode_fixed_header(buf, packet_type, flags, remaining_len)?;

    // パケット識別子
    buf[offset..offset + 2].copy_from_slice(&packet_id.to_be_bytes());
    offset += 2;

    // 省略形は生成しないため remaining_len は常に 3 以上であり、
    // 理由コードとプロパティを無条件に書き込む。
    buf[offset] = reason_code;
    offset += 1;
    offset += properties.encode(&mut buf[offset..])?;

    Ok(offset)
}

/// パケット識別子 + 理由コード + プロパティを持つ ACK パケットをデコードする。
///
/// `default_reason_code` は remaining_len == 2 の場合に使用する理由コード。
/// 戻り値は `(packet_id, reason_code, properties, consumed_bytes)` である。
/// プロパティの検証は呼び出し側で行う。
pub(crate) fn decode_reason_code_packet(
    buf: &[u8],
    packet_type: u8,
    flags: u8,
    default_reason_code: u8,
) -> Result<(u16, u8, Properties, usize), DecodeError> {
    let (remaining_len, header_len, end) = decode_fixed_header(buf, packet_type, flags)?;
    let (packet_id, mut offset) = decode_packet_id(buf, header_len, end)?;

    let (reason_code, properties) = if remaining_len == 2 {
        (default_reason_code, Properties::new())
    } else {
        if offset >= end {
            return Err(DecodeError::MalformedPacket);
        }
        let reason_code = buf[offset];
        offset += 1;
        let (properties, n) = if offset < end {
            let (p, n) = Properties::decode(&buf[offset..end])?;
            (p, n)
        } else {
            // remaining_len == 3 の場合、Property Length プレフィックスは省略され、
            // プロパティは空として扱われる（MQTT v5.0 §2.2.2.1 / MQTT v5.0 §3.6.2.2.1 / MQTT v5.0 §3.7.2.2.1）。
            (Properties::new(), 0)
        };
        offset += n;
        (reason_code, properties)
    };

    if offset != end {
        return Err(DecodeError::MalformedPacket);
    }

    Ok((packet_id, reason_code, properties, offset))
}
