use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

pub type FramedStream = Framed<TcpStream, LengthDelimitedCodec>;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

pub fn frame(stream: TcpStream) -> FramedStream {
    LengthDelimitedCodec::builder()
        .length_field_type::<u32>()
        .big_endian()
        .max_frame_length(MAX_FRAME_LEN)
        .new_framed(stream)
}

pub async fn send<S, T>(sink: &mut S, msg: &T) -> Result<(), TransportError>
where
    S: SinkExt<Bytes, Error = std::io::Error> + Unpin,
    T: Serialize,
{
    let payload = serde_json::to_vec(msg)?;
    sink.send(Bytes::from(payload)).await?;
    Ok(())
}

pub async fn recv<St, T>(stream: &mut St) -> Result<Option<T>, TransportError>
where
    St: StreamExt<Item = Result<bytes::BytesMut, std::io::Error>> + Unpin,
    T: DeserializeOwned,
{
    match stream.next().await {
        Some(Ok(frame)) => {
            let msg = serde_json::from_slice(&frame)?;
            Ok(Some(msg))
        }
        Some(Err(e)) => Err(e.into()),
        None => Ok(None),
    }
}