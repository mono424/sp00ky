//! A stand-in for the scheduler endpoint the generated DB functions post to
//! (`http::post($sp00ky_endpoint + '/view/register', ...)` and friends): it
//! answers every request at once with `{}`, so a function's cost is its own
//! database work, not a connection timeout.

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub async fn serve(port: u16) -> Result<JoinHandle<()>> {
    let listener = match TcpListener::bind(("0.0.0.0", port)).await {
        Ok(l) => l,
        // another run's stub (runs on several servers side by side) answers
        // the same way
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => return Ok(tokio::spawn(async {})),
        Err(e) => return Err(e.into()),
    };
    Ok(tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                continue;
            };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 64 * 1024];
                let mut seen = Vec::new();
                loop {
                    let n = match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    seen.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&seen);
                    let Some(head_end) = text.find("\r\n\r\n") else {
                        continue;
                    };
                    let len = text[..head_end]
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    if seen.len() < head_end + 4 + len {
                        continue;
                    }
                    let body = "{}";
                    let resp = format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
                    let _ = sock.write_all(resp.as_bytes()).await;
                    return;
                }
            });
        }
    }))
}
