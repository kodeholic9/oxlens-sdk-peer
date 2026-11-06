// author: kodeholic (powered by Claude)
//! 작은 HTTP/1.1 클라 — 토큰 발급(연:SES.http.TOKEN)과 방 재동기(연:ROOM.http.GET_ROOM)만 쓴다.
//! 요청마다 연결 하나(`Connection: close`) · `http://` 만(서버사이드 확장은 같은 망 안의 hub 에 붙는다 — TLS 는 첫 단계 밖).

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::Error;

/// `base`(앱이 준다 — 연:RULE-TRN-BASE-GIVEN) = `http://host:port/경로` → (host:port, 경로)
pub(crate) fn split_base(base: &str) -> Result<(String, String), Error> {
    let rest = base.strip_prefix("http://").ok_or_else(|| Error::transport(format!("base 는 http:// 로 시작한다(첫 단계): {base}")))?;
    let (auth, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    if auth.is_empty() {
        return Err(Error::transport(format!("base 에 host 가 없다: {base}")));
    }
    let auth = if auth.contains(':') { auth.to_string() } else { format!("{auth}:80") };
    Ok((auth, path.trim_end_matches('/').to_string()))
}

pub(crate) struct Reply {
    pub status: u16,
    pub body: Value,
}

pub(crate) async fn call(base: &str, method: &str, path: &str, headers: &[(&str, &str)], body: Option<&Value>) -> Result<Reply, Error> {
    let (auth, prefix) = split_base(base)?;
    let full = format!("{prefix}{path}");
    let mut s = TcpStream::connect(&auth).await.map_err(|e| Error::transport(format!("{method} {full}: 연결 {e}")))?;
    let payload = body.map(|b| serde_json::to_vec(b).unwrap_or_default()).unwrap_or_default();
    let mut req = format!("{method} {full} HTTP/1.1\r\nHost: {auth}\r\nConnection: close\r\nContent-Length: {}\r\n", payload.len());
    if body.is_some() {
        req.push_str("Content-Type: application/json\r\n");
    }
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).await.map_err(|e| Error::transport(e.to_string()))?;
    s.write_all(&payload).await.map_err(|e| Error::transport(e.to_string()))?;
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).await.map_err(|e| Error::transport(e.to_string()))?;
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n").ok_or_else(|| Error::transport(format!("{method} {full}: 머리 끝이 없다")))?;
    let head = String::from_utf8_lossy(&raw[..split]).to_string();
    let mut rest = raw[split + 4..].to_vec();
    let status: u16 = head.split_whitespace().nth(1).and_then(|c| c.parse().ok()).ok_or_else(|| Error::transport(format!("{method} {full}: 상태 줄 {head}")))?;
    if head.to_ascii_lowercase().contains("transfer-encoding: chunked") {
        rest = dechunk(&rest);
    }
    let body = if rest.is_empty() { Value::Null } else { serde_json::from_slice(&rest).unwrap_or(Value::String(String::from_utf8_lossy(&rest).to_string())) };
    Ok(Reply { status, body })
}

fn dechunk(mut b: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(nl) = b.windows(2).position(|w| w == b"\r\n") {
        let n = usize::from_str_radix(String::from_utf8_lossy(&b[..nl]).trim(), 16).unwrap_or(0);
        if n == 0 || b.len() < nl + 2 + n {
            break;
        }
        out.extend_from_slice(&b[nl + 2..nl + 2 + n]);
        b = &b[(nl + 4 + n).min(b.len())..];
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_split() {
        assert_eq!(split_base("http://127.0.0.1:19745/media").unwrap(), ("127.0.0.1:19745".into(), "/media".into()));
        assert_eq!(split_base("http://hub/").unwrap(), ("hub:80".into(), "".into()));
        assert!(split_base("https://hub").is_err());
    }

    #[test]
    fn chunked() {
        assert_eq!(dechunk(b"4\r\n{\"a\"\r\n3\r\n:1}\r\n0\r\n\r\n"), b"{\"a\":1}".to_vec());
    }
}
