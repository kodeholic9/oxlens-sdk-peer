// author: kodeholic (powered by Claude)
//! 이음매 연결(SDK:RULE-PLAT-AGENT-CONNECT · SDK:RULE-PLAT-AGENT-FAULT) — 브리지가 고객 WS 서버로 접속한다 · 끊기면 물러서며 다시.
//! ★AI 로 가는 통은 상한 — 넘치면 가장 오래된 음성부터 버리고 다음 음성 앞에 `gap(overflow)` 를 낸다(수신 루프 · 레코드 통을 막지 않는다).
//! `wss://` 는 rustls(ring · tls12 — oxpeer 와 같은 계열) · 뿌리는 webpki 또는 설정의 PEM.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};

use crate::seam;

/// 나가는 것 하나
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Out {
    Text(String),
    Audio { seq: u32, bytes: Vec<u8> },
}

/// AI 로 가는 통 — 음성만 상한을 센다(사건은 버리지 않는다)
pub struct OutQueue {
    q: VecDeque<Out>,
    cap_audio: usize,
    audio: usize,
    dropped: Option<(u32, u32)>,
    pub overflow_frames: u64,
}

impl OutQueue {
    pub fn new(cap_audio: usize) -> OutQueue {
        OutQueue { q: VecDeque::new(), cap_audio: cap_audio.max(1), audio: 0, dropped: None, overflow_frames: 0 }
    }

    pub fn text(&mut self, s: String) {
        self.q.push_back(Out::Text(s));
    }

    pub fn audio(&mut self, seq: u32, bytes: Vec<u8>) {
        if self.audio >= self.cap_audio
            && let Some(i) = self.q.iter().position(|o| matches!(o, Out::Audio { .. }))
            && let Some(Out::Audio { seq: s, .. }) = self.q.remove(i)
        {
            self.audio -= 1;
            self.overflow_frames += 1;
            self.dropped = Some(match self.dropped {
                Some((a, b)) => (a.min(s), b.max(s)),
                None => (s, s),
            });
        }
        self.q.push_back(Out::Audio { seq, bytes });
        self.audio += 1;
    }

    /// 다음 하나 — 버린 음성이 있으면 다음 음성 앞에 `gap(overflow)`
    pub fn pop(&mut self) -> Option<Out> {
        if let (Some((a, b)), Some(Out::Audio { .. })) = (self.dropped, self.q.front()) {
            self.dropped = None;
            return Some(Out::Text(seam::event("gap", json!({ "from_seq": a, "to_seq": b, "reason": "overflow" }))));
        }
        let o = self.q.pop_front()?;
        if matches!(o, Out::Audio { .. }) {
            self.audio -= 1;
        }
        Some(o)
    }

    /// 끊겼다 — 못 보낸 것을 다 버리고 그 음성 순번 범위(버린 넘침 포함)를 돌려준다
    pub fn cut(&mut self) -> Option<(u32, u32)> {
        let mut r = self.dropped.take();
        for o in self.q.drain(..) {
            if let Out::Audio { seq, .. } = o {
                r = Some(match r {
                    Some((a, b)) => (a.min(seq), b.max(seq)),
                    None => (seq, seq),
                });
            }
        }
        self.audio = 0;
        r
    }

    pub fn is_empty(&self) -> bool {
        self.q.is_empty() && self.dropped.is_none()
    }
}

/// AI 쪽에서 온 것
#[derive(Debug)]
pub enum In {
    Up,
    Down(String),
    Text(String),
    Binary(Vec<u8>),
}

pub struct Target {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub tls_root_pem: Option<Vec<u8>>,
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

fn tls_config(root_pem: Option<&[u8]>) -> Result<Arc<ClientConfig>, String> {
    let mut roots = RootCertStore::empty();
    match root_pem {
        Some(pem) => {
            for c in rustls_pemfile::certs(&mut std::io::BufReader::new(pem)) {
                roots.add(c.map_err(|e| format!("tls 뿌리 PEM: {e}"))?).map_err(|e| format!("tls 뿌리: {e}"))?;
            }
        }
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
    }
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let cfg = ClientConfig::builder_with_provider(provider).with_safe_default_protocol_versions().map_err(|e| format!("tls: {e}"))?.with_root_certificates(roots).with_no_client_auth();
    Ok(Arc::new(cfg))
}

async fn dial(t: &Target, tls: Option<&Arc<ClientConfig>>) -> Result<Pin<Box<dyn Io>>, String> {
    let rest = t.url.split_once("://").map(|x| x.1).ok_or("ai_url 에 스킴이 없다")?;
    let auth = rest.split('/').next().unwrap_or(rest);
    let secure = t.url.starts_with("wss://");
    let addr = if auth.contains(':') && !auth.ends_with(']') { auth.to_string() } else { format!("{auth}:{}", if secure { 443 } else { 80 }) };
    let tcp = TcpStream::connect(&addr).await.map_err(|e| format!("{addr}: 연결 {e}"))?;
    let _ = tcp.set_nodelay(true);
    let Some(cfg) = tls else { return Ok(Box::pin(tcp)) };
    let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(&addr).trim_start_matches('[').trim_end_matches(']').to_string();
    let name = ServerName::try_from(host.clone()).map_err(|e| format!("tls 이름 {host}: {e}"))?;
    let s = TlsConnector::from(cfg.clone()).connect(name, tcp).await.map_err(|e| format!("{addr}: tls {e}"))?;
    Ok(Box::pin(s))
}

/// 연결 일꾼 — 끊기면 물러서며(0.25초 → 두 배 · 5초 상한) 다시 · 나가는 것은 통에서 꺼낸다(기다리지 않는 쪽은 꺼내는 쪽뿐)
pub fn spawn(t: Target, outq: Arc<Mutex<OutQueue>>, kick: Arc<Notify>, tx: mpsc::UnboundedSender<In>) -> Result<(), String> {
    if !(t.url.starts_with("ws://") || t.url.starts_with("wss://")) {
        return Err(format!("ai_url {} — ws:// · wss:// 만", t.url));
    }
    let tls = if t.url.starts_with("wss://") { Some(tls_config(t.tls_root_pem.as_deref())?) } else { None };
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(250);
        loop {
            match session(&t, tls.as_ref(), &outq, &kick, &tx).await {
                Ok(()) => return,
                Err(e) => {
                    if tx.send(In::Down(e)).is_err() {
                        return;
                    }
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(5));
        }
    });
    Ok(())
}

/// 연결 하나 — 앱 쪽 통이 닫히면 Ok(끝) · 끊기면 Err(사유)
async fn session(t: &Target, tls: Option<&Arc<ClientConfig>>, outq: &Arc<Mutex<OutQueue>>, kick: &Arc<Notify>, tx: &mpsc::UnboundedSender<In>) -> Result<(), String> {
    let s = dial(t, tls).await?;
    let mut req = t.url.as_str().into_client_request().map_err(|e| format!("요청: {e}"))?;
    for (k, v) in &t.headers {
        let k = HeaderName::from_bytes(k.as_bytes()).map_err(|e| format!("머리 {k}: {e}"))?;
        let v = HeaderValue::from_str(v).map_err(|e| format!("머리 값: {e}"))?;
        req.headers_mut().insert(k, v);
    }
    let (ws, _) = tokio_tungstenite::client_async(req, s).await.map_err(|e| format!("핸드셰이크: {e}"))?;
    let (mut w, mut r) = ws.split();
    if tx.send(In::Up).is_err() {
        return Ok(());
    }
    loop {
        let next = outq.lock().map_err(|_| "통 잠금")?.pop();
        if let Some(o) = next {
            let m = match o {
                Out::Text(s) => Message::Text(s),
                Out::Audio { bytes, .. } => Message::Binary(bytes),
            };
            w.send(m).await.map_err(|e| format!("보내기: {e}"))?;
            continue;
        }
        tokio::select! {
            _ = kick.notified() => {}
            m = r.next() => match m {
                Some(Ok(Message::Text(s))) => if tx.send(In::Text(s)).is_err() { return Ok(()) },
                Some(Ok(Message::Binary(b))) => if tx.send(In::Binary(b)).is_err() { return Ok(()) },
                Some(Ok(Message::Close(_))) | None => return Err("닫힘".into()),
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(format!("받기: {e}")),
            },
        }
    }
}

/// 마지막 몇 줄을 내보내고 닫는다(끝날 때 — `stop` 사건)
pub async fn flush(outq: &Arc<Mutex<OutQueue>>, kick: &Arc<Notify>, within: Duration) {
    let deadline = tokio::time::Instant::now() + within;
    while tokio::time::Instant::now() < deadline {
        kick.notify_one();
        if outq.lock().map(|q| q.is_empty()).unwrap_or(true) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn texts(q: &mut OutQueue) -> Vec<Out> {
        std::iter::from_fn(|| q.pop()).collect()
    }

    #[test]
    fn overflow_drops_oldest_audio_and_reports_gap_before_next_audio() {
        let mut q = OutQueue::new(2);
        q.text("t0".into());
        for s in 0..5 {
            q.audio(s, vec![s as u8]);
        }
        let out = texts(&mut q);
        assert_eq!(out[0], Out::Text("t0".into()), "사건은 버리지 않는다");
        match &out[1] {
            Out::Text(s) => {
                let v: serde_json::Value = serde_json::from_str(s).unwrap();
                assert_eq!((v["type"].as_str(), v["from_seq"].as_u64(), v["to_seq"].as_u64(), v["reason"].as_str()), (Some("gap"), Some(0), Some(2), Some("overflow")));
            }
            o => panic!("gap 이 먼저 와야 한다: {o:?}"),
        }
        assert_eq!(&out[2..], &[Out::Audio { seq: 3, bytes: vec![3] }, Out::Audio { seq: 4, bytes: vec![4] }]);
        assert_eq!(q.overflow_frames, 3);
    }

    #[test]
    fn cut_returns_unsent_audio_range() {
        let mut q = OutQueue::new(2);
        for s in 10..14 {
            q.audio(s, vec![]);
        }
        q.text("x".into());
        assert_eq!(q.cut(), Some((10, 13)), "넘쳐 버린 것과 못 보낸 것을 합친다");
        assert!(q.is_empty());
    }
}
