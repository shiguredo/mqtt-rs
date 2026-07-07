//! MQTT v3.1.1 の簡易 ack パケット用の共通ヘルパー関数。
//!
//! PUBACK / PUBREC / PUBCOMP / UNSUBACK / PINGREQ / PINGRESP / DISCONNECT
//! など、構造が単純なパケットの encode / decode 定型部を共通化する。

use crate::codec::variable_byte_integer::VariableByteInteger;
use crate::error::{DecodeError, EncodeError, EncodeInvalidField};

/// 残り長さが 0 のパケットをエンコードする。
///
/// `packet_type` と `flags` を固定ヘッダーに書き込み、VBI で残り長さ 0 をエンコードする。
pub(crate) fn encode_empty(
    buf: &mut [u8],
    packet_type: u8,
    flags: u8,
) -> Result<usize, EncodeError> {
    let remaining_len = 0;
    let vbi = VariableByteInteger(remaining_len);
    let total_len = 1 + vbi.encoded_len();
    if buf.len() < total_len {
        return Err(EncodeError::BufferTooSmall);
    }

    buf[0] = packet_type | flags;
    let offset = 1 + vbi.encode(&mut buf[1..])?;
    Ok(offset)
}

/// 残り長さが 0 のパケットをデコードする。
pub(crate) fn decode_empty(buf: &[u8], packet_type: u8, flags: u8) -> Result<usize, DecodeError> {
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
    if remaining_len.0 != 0 {
        return Err(DecodeError::MalformedPacket);
    }

    let offset = 1 + vbi_len;
    Ok(offset)
}

/// パケット識別子のみを含む ack パケットをエンコードする。
///
/// PUBACK / PUBREC / PUBCOMP / UNSUBACK などで使用する。
pub(crate) fn encode_simple_ack(
    buf: &mut [u8],
    packet_type: u8,
    flags: u8,
    packet_id: u16,
) -> Result<usize, EncodeError> {
    if packet_id == 0 {
        return Err(EncodeError::InvalidField {
            reason: EncodeInvalidField::ZeroPacketId,
        });
    }

    // MQTT v3.1.1 の簡易 ack パケットの Remaining Length はパケット識別子の
    // 2 バイトで固定のため、サイズ超過は起こり得ない。
    let remaining_len = 2;
    let vbi = VariableByteInteger(remaining_len as u32);
    let total_len = 1 + vbi.encoded_len() + remaining_len;
    if buf.len() < total_len {
        return Err(EncodeError::BufferTooSmall);
    }

    buf[0] = packet_type | flags;
    let mut offset = 1 + vbi.encode(&mut buf[1..])?;

    buf[offset..offset + 2].copy_from_slice(&packet_id.to_be_bytes());
    offset += 2;

    Ok(offset)
}

/// パケット識別子のみを含む ack パケットをデコードする。
///
/// 戻り値は (packet_id, consumed_bytes) である。
pub(crate) fn decode_simple_ack(
    buf: &[u8],
    packet_type: u8,
    flags: u8,
) -> Result<(u16, usize), DecodeError> {
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

    let mut offset = header_len;
    let end = header_len + remaining_len;

    if offset + 2 > end {
        return Err(DecodeError::MalformedPacket);
    }
    let packet_id = u16::from_be_bytes([buf[offset], buf[offset + 1]]);
    offset += 2;

    if packet_id == 0 {
        return Err(DecodeError::MalformedPacket);
    }

    // 残り長さ分を全て消費したことを検証する。
    if offset != end {
        return Err(DecodeError::MalformedPacket);
    }

    Ok((packet_id, offset))
}
