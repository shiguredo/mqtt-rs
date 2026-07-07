//! E2E クライアント用の共通送受信ユーティリティ。

use std::time::Duration;

use shiguredo_mqtt::decoder::{Decoder, VersionedIncomingPacket};
use shiguredo_mqtt::error::DecodeError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

use tokio::net::TcpStream;

/// 共通送受信ユーティリティで発生するエラー。
///
/// 各バージョンのクライアントは `From<TransportError>` で
/// 自身のエラー型に変換する。
#[derive(Debug)]
pub enum TransportError {
    /// I/O エラー。
    Io(std::io::Error),
    /// パケットのデコードに失敗した。
    Decode(DecodeError),
    /// タイムアウトした。
    Timeout,
    /// 接続が切断された。
    Disconnected,
}

/// エンコード済みのパケットをストリームに送信する。
///
/// エンコードはバージョンごとのパケット型が異なるため、呼び出し側が行う。
pub async fn send_bytes(stream: &mut TcpStream, buf: &[u8]) -> Result<(), TransportError> {
    stream.write_all(buf).await.map_err(TransportError::Io)?;
    stream.flush().await.map_err(TransportError::Io)?;
    Ok(())
}

/// 1 つの MQTT パケットを受信する。
///
/// バージョンの選別（`VersionedIncomingPacket` から特定バージョンのパケットを
/// 取り出す処理）は呼び出し側が行う。
pub async fn recv_packet(
    decoder: &mut Decoder,
    stream: &mut TcpStream,
    dur: Duration,
) -> Result<VersionedIncomingPacket, TransportError> {
    loop {
        if let Some(packet) = decoder.decode().map_err(TransportError::Decode)? {
            return Ok(packet);
        }
        let mut buf = [0u8; 1024];
        let n = timeout(dur, stream.read(&mut buf))
            .await
            .map_err(|_| TransportError::Timeout)?
            .map_err(TransportError::Io)?;
        if n == 0 {
            return Err(TransportError::Disconnected);
        }
        decoder.feed(&buf[..n]).map_err(TransportError::Decode)?;
    }
}
