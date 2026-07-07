//! クライアント目線の送信・受信に分離した MQTT v5.0 パケット列挙型。
//!
//! MQTT v5.0 §2.1 を参照。
//!
//! クライアントから見て送信するパケットは [`OutgoingPacket`]、
//! クライアントから見て受信するパケットは [`IncomingPacket`] で表す。
//! 方向を跨いだ全種別を扱う低水準 codec は `codec` サブモジュールとして公開する。

use alloc::vec;
use alloc::vec::Vec;

use crate::codec::variable_byte_integer::VariableByteInteger;
use crate::error::{DecodeError, EncodeError};
use crate::v5::auth::Auth;
use crate::v5::connack::ConnAck;
use crate::v5::connect::Connect;
use crate::v5::disconnect::Disconnect;
use crate::v5::pingreq::PingReq;
use crate::v5::pingresp::PingResp;
use crate::v5::puback::PubAck;
use crate::v5::pubcomp::PubComp;
use crate::v5::publish::Publish;
use crate::v5::pubrec::PubRec;
use crate::v5::pubrel::PubRel;
use crate::v5::suback::SubAck;
use crate::v5::subscribe::Subscribe;
use crate::v5::unsuback::UnsubAck;
use crate::v5::unsubscribe::Unsubscribe;

/// クライアントから見て送信する（Client → Server）MQTT v5.0 制御パケット。
///
/// CONNACK / SUBACK / UNSUBACK / PINGRESP の Server → Client 専用種別は
/// バリアントとして存在せず、コンパイル時に送信不可能であることが保証される。
/// MQTT v5.0 §2.1.2 Table 2-1 を参照。
///
/// 双方向種別のうち方向別の検証が存在するもの
/// （PUBLISH / PUBACK / PUBREC / DISCONNECT / AUTH）の
/// Reason Code やプロパティの方向検証は型では表現できず、
/// 各 struct の `encode` が Client → Server 方向の検証として実行時に行う。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutgoingPacket {
    /// CONNECT パケット。
    Connect(Connect),
    /// PUBLISH パケット。
    Publish(Publish),
    /// PUBACK パケット。
    PubAck(PubAck),
    /// PUBREC パケット。
    PubRec(PubRec),
    /// PUBREL パケット。
    PubRel(PubRel),
    /// PUBCOMP パケット。
    PubComp(PubComp),
    /// SUBSCRIBE パケット。
    Subscribe(Subscribe),
    /// UNSUBSCRIBE パケット。
    Unsubscribe(Unsubscribe),
    /// PINGREQ パケット。
    PingReq(PingReq),
    /// DISCONNECT パケット。
    Disconnect(Disconnect),
    /// AUTH パケット。
    Auth(Auth),
}

impl OutgoingPacket {
    /// エンコード後のパケットの残り長さを返す（固定ヘッダー分は含まない）。
    pub fn encoded_len(&self) -> usize {
        match self {
            OutgoingPacket::Connect(p) => p.encoded_len(),
            OutgoingPacket::Publish(p) => p.encoded_len(),
            OutgoingPacket::PubAck(p) => p.encoded_len(),
            OutgoingPacket::PubRec(p) => p.encoded_len(),
            OutgoingPacket::PubRel(p) => p.encoded_len(),
            OutgoingPacket::PubComp(p) => p.encoded_len(),
            OutgoingPacket::Subscribe(p) => p.encoded_len(),
            OutgoingPacket::Unsubscribe(p) => p.encoded_len(),
            OutgoingPacket::PingReq(_) => 0,
            OutgoingPacket::Disconnect(p) => p.encoded_len(),
            OutgoingPacket::Auth(p) => p.encoded_len(),
        }
    }

    /// パケットを `buf` にエンコードし、書き込んだバイト数を返す。
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        match self {
            OutgoingPacket::Connect(p) => p.encode(buf),
            OutgoingPacket::Publish(p) => p.encode(buf),
            OutgoingPacket::PubAck(p) => p.encode(buf),
            OutgoingPacket::PubRec(p) => p.encode(buf),
            OutgoingPacket::PubRel(p) => p.encode(buf),
            OutgoingPacket::PubComp(p) => p.encode(buf),
            OutgoingPacket::Subscribe(p) => p.encode(buf),
            OutgoingPacket::Unsubscribe(p) => p.encode(buf),
            OutgoingPacket::PingReq(p) => p.encode(buf),
            OutgoingPacket::Disconnect(p) => p.encode(buf),
            OutgoingPacket::Auth(p) => p.encode(buf),
        }
    }

    /// 新しく確保した `Vec<u8>` にパケットをエンコードする。
    pub fn encode_to_vec(&self) -> Result<Vec<u8>, EncodeError> {
        let remaining_len = self.encoded_len();
        // MQTT の残り長さは VariableByteInteger::MAX を超えられない。
        // 飽和後に `as u32` で切り詰められて巨大アロケーションになる経路を防ぐため、
        // バッファ確保の前に事前チェックする。
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
                size: usize::MAX,
                limit: usize::MAX,
            })?;
        let mut buf = vec![0; total_len];
        self.encode(&mut buf)?;
        Ok(buf)
    }
}

/// クライアントから見て受信する（Server → Client）MQTT v5.0 制御パケット。
///
/// CONNECT / SUBSCRIBE / UNSUBSCRIBE / PINGREQ の Client → Server 専用種別は
/// バリアントとして存在せず、コンパイル時に受信不可能であることが保証される。
/// それらの種別のバイト列は `Decoder` 経由のデコード時に
/// [`DecodeError::UnexpectedPacket`] として拒否される。
/// MQTT v5.0 §2.1.2 Table 2-1 を参照。
///
/// 双方向種別のうち decode 時に方向別の検証が存在するもの
/// （PUBLISH / DISCONNECT / AUTH）の Reason Code やプロパティの検証は
/// 型では表現できず、各 struct の `decode` が Server → Client 方向の検証として
/// 実行時に行う。なお PUBACK / PUBREC の方向別検証
/// （NoMatchingSubscribers の拒否）は encode 側にのみ存在する。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncomingPacket {
    /// CONNACK パケット。
    ConnAck(ConnAck),
    /// PUBLISH パケット。
    Publish(Publish),
    /// PUBACK パケット。
    PubAck(PubAck),
    /// PUBREC パケット。
    PubRec(PubRec),
    /// PUBREL パケット。
    PubRel(PubRel),
    /// PUBCOMP パケット。
    PubComp(PubComp),
    /// SUBACK パケット。
    SubAck(SubAck),
    /// UNSUBACK パケット。
    UnsubAck(UnsubAck),
    /// PINGRESP パケット。
    PingResp(PingResp),
    /// DISCONNECT パケット。
    Disconnect(Disconnect),
    /// AUTH パケット。
    Auth(Auth),
}

impl IncomingPacket {
    /// `buf` からクライアント受信方向のパケットをデコードする。
    ///
    /// デコードしたパケットと消費したバイト数を返す。
    ///
    /// `Decoder` からのみ呼び出す。先頭に完全な 1 フレームを含むバッファを前提とする。
    /// バッファがフレーム長より短い場合の [`DecodeError::InsufficientData`] はそのまま返し、
    /// `Decoder::decode_packet` が [`DecodeError::MalformedPacket`] に変換する
    /// （変換責務は `decode_packet` 側に集約）。
    ///
    /// Client → Server 専用種別（CONNECT / SUBSCRIBE / UNSUBSCRIBE / PINGREQ）は
    /// 先頭バイトの種別判定時に [`DecodeError::UnexpectedPacket`] として拒否する。
    /// 逆方向種別かつ不正フラグの入力（例: フラグ 0x01 の CONNECT 先頭バイト 0x11）は
    /// 種別判定が先に行われるため `UnexpectedPacket` が優先される。
    pub(crate) fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        if buf.is_empty() {
            return Err(DecodeError::InsufficientData);
        }

        // 下位 4 ビットの固定ヘッダーフラグは各パケットの個別デコード関数側で検証する。
        // MQTT v5.0 §2.1.3 [MQTT-2.1.3-1] に基づき、各パケット種別の予約フラグは指定値である必要がある。
        match buf[0] & 0xF0 {
            0x20 => {
                let (p, n) = ConnAck::decode(buf)?;
                Ok((IncomingPacket::ConnAck(p), n))
            }
            0x30 => {
                let (p, n) = Publish::decode(buf)?;
                Ok((IncomingPacket::Publish(p), n))
            }
            0x40 => {
                let (p, n) = PubAck::decode(buf)?;
                Ok((IncomingPacket::PubAck(p), n))
            }
            0x50 => {
                let (p, n) = PubRec::decode(buf)?;
                Ok((IncomingPacket::PubRec(p), n))
            }
            0x60 => {
                let (p, n) = PubRel::decode(buf)?;
                Ok((IncomingPacket::PubRel(p), n))
            }
            0x70 => {
                let (p, n) = PubComp::decode(buf)?;
                Ok((IncomingPacket::PubComp(p), n))
            }
            0x90 => {
                let (p, n) = SubAck::decode(buf)?;
                Ok((IncomingPacket::SubAck(p), n))
            }
            0xB0 => {
                let (p, n) = UnsubAck::decode(buf)?;
                Ok((IncomingPacket::UnsubAck(p), n))
            }
            0xD0 => {
                let (p, n) = PingResp::decode(buf)?;
                Ok((IncomingPacket::PingResp(p), n))
            }
            0xE0 => {
                let (p, n) = Disconnect::decode(buf)?;
                Ok((IncomingPacket::Disconnect(p), n))
            }
            0xF0 => {
                let (p, n) = Auth::decode(buf)?;
                Ok((IncomingPacket::Auth(p), n))
            }
            // Client → Server 専用種別はクライアントの受信方向には存在してはならない。
            // MQTT v5.0 §2.1.2 Table 2-1 を参照。
            packet_type @ (0x10 | 0x80 | 0xA0 | 0xC0) => {
                Err(DecodeError::UnexpectedPacket { packet_type })
            }
            // 0x00 は Reserved 種別のため、方向拒否ではなく種別不正として拒否する。
            // MQTT v5.0 §2.1.2 Table 2-1 を参照。
            0x00 => Err(DecodeError::InvalidPacketType),
            // `buf[0] & 0xF0` の値域は上位 4 ビットの 16 通りであり上記で網羅済みのため、
            // このアームには到達しない。型上の網羅性を満たすため同じエラーを返す。
            _ => Err(DecodeError::InvalidPacketType),
        }
    }
}

/// 方向を跨いだ全種別のエンコード・デコードを扱う低水準 codec モジュール。
///
/// クライアント利用では送信に [`super::OutgoingPacket`]、受信に
/// [`Decoder`](crate::decoder::Decoder) 経由の [`super::IncomingPacket`] を使うこと。
/// 本モジュールは個別 struct を経由せず全種別を一括で扱う必要がある場合
/// （ラウンドトリップ検証・任意バイト列のデコードなど）向けの入口である。
/// 共通コーデック部品の `crate::codec` とは別モジュールであり、パス表記を混同しないこと。
pub mod codec {
    use super::*;

    /// 方向を跨いだ全種別を扱う低水準の MQTT v5.0 制御パケット。
    ///
    /// クライアント利用では [`super::OutgoingPacket`] / [`super::IncomingPacket`] を使うこと。
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum Packet {
        /// CONNECT パケット。
        Connect(Connect),
        /// CONNACK パケット。
        ConnAck(ConnAck),
        /// PUBLISH パケット。
        Publish(Publish),
        /// PUBACK パケット。
        PubAck(PubAck),
        /// PUBREC パケット。
        PubRec(PubRec),
        /// PUBREL パケット。
        PubRel(PubRel),
        /// PUBCOMP パケット。
        PubComp(PubComp),
        /// SUBSCRIBE パケット。
        Subscribe(Subscribe),
        /// SUBACK パケット。
        SubAck(SubAck),
        /// UNSUBSCRIBE パケット。
        Unsubscribe(Unsubscribe),
        /// UNSUBACK パケット。
        UnsubAck(UnsubAck),
        /// PINGREQ パケット。
        PingReq(PingReq),
        /// PINGRESP パケット。
        PingResp(PingResp),
        /// DISCONNECT パケット。
        Disconnect(Disconnect),
        /// AUTH パケット。
        Auth(Auth),
    }

    impl Packet {
        /// エンコード後のパケットの残り長さを返す（固定ヘッダー分は含まない）。
        pub fn encoded_len(&self) -> usize {
            match self {
                Packet::Connect(p) => p.encoded_len(),
                Packet::ConnAck(p) => p.encoded_len(),
                Packet::Publish(p) => p.encoded_len(),
                Packet::PubAck(p) => p.encoded_len(),
                Packet::PubRec(p) => p.encoded_len(),
                Packet::PubRel(p) => p.encoded_len(),
                Packet::PubComp(p) => p.encoded_len(),
                Packet::Subscribe(p) => p.encoded_len(),
                Packet::SubAck(p) => p.encoded_len(),
                Packet::Unsubscribe(p) => p.encoded_len(),
                Packet::UnsubAck(p) => p.encoded_len(),
                Packet::PingReq(_) => 0,
                Packet::PingResp(_) => 0,
                Packet::Disconnect(p) => p.encoded_len(),
                Packet::Auth(p) => p.encoded_len(),
            }
        }

        /// パケットを `buf` にエンコードし、書き込んだバイト数を返す。
        pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
            match self {
                Packet::Connect(p) => p.encode(buf),
                Packet::ConnAck(p) => p.encode(buf),
                Packet::Publish(p) => p.encode(buf),
                Packet::PubAck(p) => p.encode(buf),
                Packet::PubRec(p) => p.encode(buf),
                Packet::PubRel(p) => p.encode(buf),
                Packet::PubComp(p) => p.encode(buf),
                Packet::Subscribe(p) => p.encode(buf),
                Packet::SubAck(p) => p.encode(buf),
                Packet::Unsubscribe(p) => p.encode(buf),
                Packet::UnsubAck(p) => p.encode(buf),
                Packet::PingReq(p) => p.encode(buf),
                Packet::PingResp(p) => p.encode(buf),
                Packet::Disconnect(p) => p.encode(buf),
                Packet::Auth(p) => p.encode(buf),
            }
        }

        /// 新しく確保した `Vec<u8>` にパケットをエンコードする。
        pub fn encode_to_vec(&self) -> Result<Vec<u8>, EncodeError> {
            let remaining_len = self.encoded_len();
            // MQTT の残り長さは VariableByteInteger::MAX を超えられない。
            // 飽和後に `as u32` で切り詰められて巨大アロケーションになる経路を防ぐため、
            // バッファ確保の前に事前チェックする。
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
                    size: usize::MAX,
                    limit: usize::MAX,
                })?;
            let mut buf = vec![0; total_len];
            self.encode(&mut buf)?;
            Ok(buf)
        }

        /// `buf` からパケットをデコードする。
        ///
        /// デコードしたパケットと消費したバイト数を返す。
        ///
        /// 先頭に完全な 1 パケットを含むバッファを前提とする。
        /// バッファがフレーム長より短い場合は [`DecodeError::InsufficientData`] を返し、
        /// 完結したフレーム内の破損は [`DecodeError::MalformedPacket`] を返す。
        /// 分割入力のフレーミングには `Decoder` を使うこと。
        ///
        /// 方向を跨いだ全種別を受理するため、Client → Server 専用種別の
        /// デコードも成功する。クライアント受信方向のデコードには
        /// `Decoder` 経由の [`super::IncomingPacket`] を使うこと。
        pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
            if buf.is_empty() {
                return Err(DecodeError::InsufficientData);
            }

            // 下位 4 ビットの固定ヘッダーフラグは各パケットの個別デコード関数側で検証する。
            // MQTT v5.0 §2.1.3 [MQTT-2.1.3-1] に基づき、各パケット種別の予約フラグは指定値である必要がある。
            match buf[0] & 0xF0 {
                0x10 => {
                    let (p, n) = Connect::decode(buf)?;
                    Ok((Packet::Connect(p), n))
                }
                0x20 => {
                    let (p, n) = ConnAck::decode(buf)?;
                    Ok((Packet::ConnAck(p), n))
                }
                0x30 => {
                    let (p, n) = Publish::decode(buf)?;
                    Ok((Packet::Publish(p), n))
                }
                0x40 => {
                    let (p, n) = PubAck::decode(buf)?;
                    Ok((Packet::PubAck(p), n))
                }
                0x50 => {
                    let (p, n) = PubRec::decode(buf)?;
                    Ok((Packet::PubRec(p), n))
                }
                0x60 => {
                    let (p, n) = PubRel::decode(buf)?;
                    Ok((Packet::PubRel(p), n))
                }
                0x70 => {
                    let (p, n) = PubComp::decode(buf)?;
                    Ok((Packet::PubComp(p), n))
                }
                0x80 => {
                    let (p, n) = Subscribe::decode(buf)?;
                    Ok((Packet::Subscribe(p), n))
                }
                0x90 => {
                    let (p, n) = SubAck::decode(buf)?;
                    Ok((Packet::SubAck(p), n))
                }
                0xA0 => {
                    let (p, n) = Unsubscribe::decode(buf)?;
                    Ok((Packet::Unsubscribe(p), n))
                }
                0xB0 => {
                    let (p, n) = UnsubAck::decode(buf)?;
                    Ok((Packet::UnsubAck(p), n))
                }
                0xC0 => {
                    let (p, n) = PingReq::decode(buf)?;
                    Ok((Packet::PingReq(p), n))
                }
                0xD0 => {
                    let (p, n) = PingResp::decode(buf)?;
                    Ok((Packet::PingResp(p), n))
                }
                0xE0 => {
                    let (p, n) = Disconnect::decode(buf)?;
                    Ok((Packet::Disconnect(p), n))
                }
                0xF0 => {
                    let (p, n) = Auth::decode(buf)?;
                    Ok((Packet::Auth(p), n))
                }
                _ => Err(DecodeError::InvalidPacketType),
            }
        }
    }
}
