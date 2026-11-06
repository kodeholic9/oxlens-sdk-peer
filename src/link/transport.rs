// author: kodeholic (powered by Claude)
//! 전송 하나(웹의 PeerConnection 한 개 자리) — UDP 소켓 하나 · 첫 바이트로 가른다(RFC 7983: STUN 0-3 · DTLS 20-63 · RTP/RTCP 128-191).
//! ICE(서버 ICE-lite · 클라 controlling · consent) → DTLS 능동(서버가 클라 인증서를 요구한다) → SRTP 키 → (발행 연결이면) SCTP + DCEP `"unreliable"`.
//! 층마다 사실을 따로 낸다(SDK:RULE-LINK-LAYERS — ICE · DTLS · SCTP · DC 를 섞지 않는다) · 판정은 SDK 루프의 연결 상태기가 한다.
//! 받은 RTP 는 SDK 루프를 거치지 않고 복호해 미디어 통로로 바로 낸다 — 어느 방 · 트랙인지는 루프가 고쳐 두는 경로표(ssrc → 트랙)로 가른다.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use dtls::config::{Config, ExtendedMasterSecretType};
use dtls::conn::DTLSConn;
use dtls::crypto::Certificate;
use dtls::extension::extension_use_srtp::SrtpProtectionProfile;
use sctp_proto::{AssociationHandle, ClientConfig, DatagramEvent, Endpoint, EndpointConfig, Event, Payload, PayloadProtocolIdentifier, ReliabilityType, StreamEvent, TransportConfig};
use tokio::net::UdpSocket;
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio::task::AbortHandle;
use webrtc_srtp::config::Config as SrtpConfig;
use webrtc_srtp::context::Context;
use webrtc_srtp::protection_profile::ProtectionProfile;
use webrtc_util::conn::Conn;

use super::stun;
use crate::wire::dc::CHANNEL_LABEL;

/// 검사 간격 — 서기 전(빠르게) · 선 뒤(consent · RFC 7675 의 5초보다 촘촘히)
const CHECK_EARLY: Duration = Duration::from_millis(500);
const CHECK_CONSENT: Duration = Duration::from_millis(2_500);
/// 응답이 이만큼 없으면 disconnected · failed(브라우저의 ICE 상태와 같은 뜻 — SDK:RULE-LINK-LAYERS)
const ICE_DISCONNECTED: Duration = Duration::from_millis(7_500);
const ICE_FAILED: Duration = Duration::from_secs(30);
const DTLS_TIMEOUT: Duration = Duration::from_secs(30);
const DCEP_OPEN: u8 = 0x03;
const DCEP_ACK: u8 = 0x02;
/// DCEP 채널 종류 — 순서 없음 · 재전송 횟수 한도(웹 `ordered:false · maxRetransmits:0` 과 같다)
const DCEP_PARTIAL_REXMIT_UNORDERED: u8 = 0x81;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Role {
    /// 발행용 연결 — DC 가 여기 붙는다(연:TRN.frame.DC 어디에 붙나)
    Publish,
    /// 구독용 연결 — 받는 미디어
    Subscribe,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Creds {
    pub ufrag: String,
    pub pwd: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IceState {
    Connected,
    Disconnected,
    Failed,
}

#[derive(Debug)]
pub(crate) enum Fact {
    Ice(IceState),
    /// DTLS 가 섰다(true) · 닫혔다 · 실패했다(false)
    Dtls(bool),
    /// SCTP 연관이 섰다(true) · 잃었다(false)
    Sctp(bool),
    /// DC 가 열렸다(true · DCEP ACK) · 닫혔다(false)
    Dc(bool),
    DcData(Vec<u8>),
    /// 이 ssrc 의 첫 복호 RTP 패킷(SDK:RULE-PLAT-SERVER `receiving`)
    FirstRtp(u32),
}

/// 사실 하나 — 어느 서버 · 몇째 전송 묶음 · 어느 연결
#[derive(Debug)]
pub(crate) struct LinkFact {
    pub server: String,
    pub sgen: u64,
    pub role: Role,
    pub fact: Fact,
}

/// 받는 트랙 하나의 경로 — 배정표 줄 + 트랙 보관본에서 루프가 짓는다
#[derive(Clone, Debug)]
pub(crate) struct Route {
    pub room: String,
    pub track_id: String,
    pub user_id: Option<String>,
    pub kind: String,
    pub codec: String,
    pub mid: String,
}

/// 받은 RTP 한 패킷(SRTP 를 푼 것 · 코덱 페이로드 그대로 — 디코딩은 앱 몫, SDK:RULE-PLAT-SERVER `mediaStreamTrack` 의 뜻)
#[derive(Clone, Debug)]
pub struct MediaPacket {
    pub room: String,
    pub track_id: String,
    /// 트랙 주인(개인 트랙) — 무전 슬롯은 없다(화자는 발언권 `speaker` 가 알린다)
    pub user_id: Option<String>,
    /// `audio` · `video`
    pub kind: String,
    pub codec: String,
    pub mid: String,
    pub ssrc: u32,
    pub payload_type: u8,
    pub sequence: u16,
    pub timestamp: u32,
    pub marker: bool,
    pub payload: Bytes,
    pub received_at: Instant,
}

/// 경로표와 미디어 통로 — 모든 구독 연결이 같이 쓴다
#[derive(Clone)]
pub(crate) struct MediaSink {
    pub routes: Arc<RwLock<HashMap<u32, Route>>>,
    pub tx: mpsc::Sender<MediaPacket>,
    /// 통로가 가득 차 버린 패킷 수(앱이 늦게 꺼낸다)
    pub dropped: Arc<AtomicU64>,
    /// 경로표에 없는 ssrc 로 온 패킷 수
    pub unrouted: Arc<AtomicU64>,
    pub decrypt_fail: Arc<AtomicU64>,
}

enum Ctl {
    Creds(Creds),
    SendDc(Vec<u8>),
    ReopenDc,
}

enum SctpCtl {
    Send(Vec<u8>),
    Reopen,
}

pub(crate) struct Transport {
    ctl: mpsc::UnboundedSender<Ctl>,
    tasks: Arc<Mutex<Vec<AbortHandle>>>,
}

impl Transport {
    /// 연다 — 곧바로 돌아오고 층마다의 사실은 `facts` 로 온다
    pub(crate) fn open(server: SocketAddr, creds: Creds, cert: Certificate, role: Role, tag: (String, u64), facts: mpsc::UnboundedSender<LinkFact>, sink: MediaSink) -> Transport {
        let (name, sgen) = tag;
        let (ctl, ctl_rx) = mpsc::unbounded_channel();
        let tasks: Arc<Mutex<Vec<AbortHandle>>> = Arc::default();
        let t2 = tasks.clone();
        let main = tokio::spawn(async move {
            let say = Say { facts, server: name, sgen, role };
            if let Err(why) = run(server, creds, cert, role, say.clone(), sink, ctl_rx, t2).await {
                say.send(Fact::Dtls(false));
                let _ = why;
            }
        });
        if let Ok(mut g) = tasks.lock() {
            g.push(main.abort_handle());
        }
        Transport { ctl, tasks }
    }

    /// ICE 재시작 — 새 자격으로 검사한다(DTLS · SRTP 는 그대로 — 지문 · setup 이 같다)
    pub(crate) fn set_creds(&self, c: Creds) {
        let _ = self.ctl.send(Ctl::Creds(c));
    }

    pub(crate) fn send_dc(&self, b: Vec<u8>) -> bool {
        self.ctl.send(Ctl::SendDc(b)).is_ok()
    }

    /// 같은 연관 위에 DC 를 새로 연다(SDK:RULE-LINK-LAYERS DC 다시 열기)
    pub(crate) fn reopen_dc(&self) {
        let _ = self.ctl.send(Ctl::ReopenDc);
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        if let Ok(g) = self.tasks.lock() {
            for h in g.iter() {
                h.abort();
            }
        }
    }
}

#[derive(Clone)]
struct Say {
    facts: mpsc::UnboundedSender<LinkFact>,
    server: String,
    sgen: u64,
    role: Role,
}

impl Say {
    fn send(&self, fact: Fact) {
        let _ = self.facts.send(LinkFact { server: self.server.clone(), sgen: self.sgen, role: self.role, fact });
    }
}

fn keep(tasks: &Arc<Mutex<Vec<AbortHandle>>>, h: AbortHandle) {
    if let Ok(mut g) = tasks.lock() {
        g.push(h);
    }
}

/// 선 DTLS 와 SRTP 문맥 둘(로컬 · 원격)
type Secured = (Arc<DTLSConn>, Context, Context);

enum Inner {
    StunOk([u8; 12]),
    Dtls(Box<Result<Secured, String>>),
}

#[allow(clippy::too_many_arguments)]
async fn run(server: SocketAddr, mut creds: Creds, cert: Certificate, role: Role, say: Say, sink: MediaSink, mut ctl: mpsc::UnboundedReceiver<Ctl>, tasks: Arc<Mutex<Vec<AbortHandle>>>) -> Result<(), String> {
    let sock = Arc::new(UdpSocket::bind("0.0.0.0:0").await.map_err(|e| format!("UDP bind: {e}"))?);
    let (inner_tx, mut inner_rx) = mpsc::unbounded_channel::<Inner>();
    let (dtls_tx, dtls_rx) = mpsc::channel::<Vec<u8>>(256);
    let rx_ctx: Arc<OnceLock<Mutex<Context>>> = Arc::default();

    let (s2, i2, rx2, say2, sink2) = (sock.clone(), inner_tx.clone(), rx_ctx.clone(), say.clone(), sink.clone());
    let demux = tokio::spawn(async move {
        let mut buf = vec![0u8; 2048];
        let mut seen: HashSet<u32> = HashSet::new();
        loop {
            let Ok((n, from)) = s2.recv_from(&mut buf).await else { break };
            if n == 0 || from != server {
                continue;
            }
            let b0 = buf[0];
            if b0 <= 3 {
                if let Some(tid) = stun::binding_success(&buf[..n]) {
                    let _ = i2.send(Inner::StunOk(tid));
                }
            } else if (20..=63).contains(&b0) {
                let _ = dtls_tx.try_send(buf[..n].to_vec());
            } else if (128..=191).contains(&b0) && n >= 2 && !(192..=223).contains(&buf[1]) {
                on_rtp(&buf[..n], &rx2, &sink2, &mut seen, &say2);
            }
        }
    });
    keep(&tasks, demux.abort_handle());

    let local = stun::random_ice_string(8);
    let mut tie_b = [0u8; 8];
    let _ = getrandom::fill(&mut tie_b);
    let tie = u64::from_be_bytes(tie_b);
    let mut pending: HashMap<[u8; 12], Instant> = HashMap::new();
    let started = Instant::now();
    let mut last_ok: Option<Instant> = None;
    let mut ice: Option<IceState> = None;
    let mut last_sent = Instant::now() - CHECK_CONSENT;
    let mut dtls_rx = Some(dtls_rx);
    let mut cert = Some(cert);
    let mut sctp: Option<mpsc::UnboundedSender<SctpCtl>> = None;
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let now = Instant::now();
                let every = if ice == Some(IceState::Connected) { CHECK_CONSENT } else { CHECK_EARLY };
                if now.duration_since(last_sent) >= every {
                    let (req, tid) = stun::binding_request(&creds.ufrag, &local, &creds.pwd, tie);
                    pending.retain(|_, t| now.duration_since(*t) < ICE_FAILED);
                    pending.insert(tid, now);
                    let _ = sock.send_to(&req, server).await;
                    last_sent = now;
                }
                let quiet = now.duration_since(last_ok.unwrap_or(started));
                let next = if quiet >= ICE_FAILED { Some(IceState::Failed) } else if quiet >= ICE_DISCONNECTED && last_ok.is_some() { Some(IceState::Disconnected) } else { ice };
                if next != ice && next.is_some() && next != Some(IceState::Connected) {
                    ice = next;
                    say.send(Fact::Ice(ice.unwrap_or(IceState::Failed)));
                }
            }
            Some(m) = inner_rx.recv() => match m {
                Inner::StunOk(tid) => {
                    if pending.remove(&tid).is_none() {
                        continue;
                    }
                    last_ok = Some(Instant::now());
                    if ice != Some(IceState::Connected) {
                        ice = Some(IceState::Connected);
                        say.send(Fact::Ice(IceState::Connected));
                    }
                    if let (Some(rx), Some(c)) = (dtls_rx.take(), cert.take()) {
                        let (s3, i3) = (sock.clone(), inner_tx.clone());
                        let hs = tokio::spawn(async move {
                            let _ = i3.send(Inner::Dtls(Box::new(handshake(s3, server, rx, c).await)));
                        });
                        keep(&tasks, hs.abort_handle());
                    }
                }
                Inner::Dtls(r) => {
                    let (conn, tx_ctx, rx) = match *r {
                        Ok(x) => x,
                        Err(why) => {
                            say.send(Fact::Dtls(false));
                            return Err(why);
                        }
                    };
                    drop(tx_ctx);
                    let _ = rx_ctx.set(Mutex::new(rx));
                    say.send(Fact::Dtls(true));
                    if role == Role::Publish {
                        let (stx, srx) = mpsc::unbounded_channel();
                        sctp = Some(stx);
                        let h = tokio::spawn(sctp_client(conn, say.clone(), srx));
                        keep(&tasks, h.abort_handle());
                    } else {
                        let say3 = say.clone();
                        let h = tokio::spawn(async move {
                            let mut buf = vec![0u8; 2048];
                            while conn.read(&mut buf, None).await.is_ok() {}
                            say3.send(Fact::Dtls(false));
                        });
                        keep(&tasks, h.abort_handle());
                    }
                }
            },
            c = ctl.recv() => match c {
                None => return Ok(()),
                Some(Ctl::Creds(c)) => {
                    creds = c;
                    pending.clear();
                    last_sent = Instant::now() - CHECK_CONSENT;
                }
                Some(Ctl::SendDc(b)) => {
                    if let Some(s) = &sctp {
                        let _ = s.send(SctpCtl::Send(b));
                    }
                }
                Some(Ctl::ReopenDc) => {
                    if let Some(s) = &sctp {
                        let _ = s.send(SctpCtl::Reopen);
                    }
                }
            },
        }
    }
}

fn on_rtp(pkt: &[u8], rx: &Arc<OnceLock<Mutex<Context>>>, sink: &MediaSink, seen: &mut HashSet<u32>, say: &Say) {
    let Some(ctx) = rx.get() else { return };
    let plain = ctx.lock().ok().and_then(|mut c| c.decrypt_rtp(pkt).ok());
    let Some(p) = plain else {
        sink.decrypt_fail.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let Some(h) = rtp_header(&p) else { return };
    let route = sink.routes.read().ok().and_then(|g| g.get(&h.ssrc).cloned());
    let Some(r) = route else {
        sink.unrouted.fetch_add(1, Ordering::Relaxed);
        return;
    };
    if seen.insert(h.ssrc) {
        say.send(Fact::FirstRtp(h.ssrc));
    }
    let m = MediaPacket {
        room: r.room,
        track_id: r.track_id,
        user_id: r.user_id,
        kind: r.kind,
        codec: r.codec,
        mid: r.mid,
        ssrc: h.ssrc,
        payload_type: h.pt,
        sequence: h.seq,
        timestamp: h.ts,
        marker: h.marker,
        payload: p.slice(h.start..h.end),
        received_at: Instant::now(),
    };
    if sink.tx.try_send(m).is_err() {
        sink.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) struct RtpHeader {
    pub marker: bool,
    pub pt: u8,
    pub seq: u16,
    pub ts: u32,
    pub ssrc: u32,
    pub start: usize,
    pub end: usize,
}

/// RTP 머리(RFC 3550) — CSRC · 확장 머리를 건너뛰고 채움을 뗀다
pub(crate) fn rtp_header(p: &[u8]) -> Option<RtpHeader> {
    if p.len() < 12 || p[0] >> 6 != 2 {
        return None;
    }
    let cc = usize::from(p[0] & 0x0f);
    let mut start = 12 + 4 * cc;
    if p[0] & 0x10 != 0 {
        if p.len() < start + 4 {
            return None;
        }
        let words = usize::from(u16::from_be_bytes([p[start + 2], p[start + 3]]));
        start += 4 + 4 * words;
    }
    let mut end = p.len();
    if p[0] & 0x20 != 0 {
        let pad = usize::from(*p.last()?);
        end = end.checked_sub(pad)?;
    }
    if start > end {
        return None;
    }
    Some(RtpHeader {
        marker: p[1] & 0x80 != 0,
        pt: p[1] & 0x7f,
        seq: u16::from_be_bytes([p[2], p[3]]),
        ts: u32::from_be_bytes([p[4], p[5], p[6], p[7]]),
        ssrc: u32::from_be_bytes([p[8], p[9], p[10], p[11]]),
        start,
        end,
    })
}

/// DTLS 능동 → SRTP 키(로컬 · 원격)
async fn handshake(sock: Arc<UdpSocket>, server: SocketAddr, rx: mpsc::Receiver<Vec<u8>>, cert: Certificate) -> Result<Secured, String> {
    let conn: Arc<dyn Conn + Send + Sync> = Arc::new(ChanConn { rx: AsyncMutex::new(rx), sock, peer: server });
    let cfg = Config {
        certificates: vec![cert],
        srtp_protection_profiles: vec![SrtpProtectionProfile::Srtp_Aes128_Cm_Hmac_Sha1_80],
        extended_master_secret: ExtendedMasterSecretType::Require,
        insecure_skip_verify: true,
        mtu: 1_200,
        ..Config::default()
    };
    let dtls = match tokio::time::timeout(DTLS_TIMEOUT, DTLSConn::new(conn, cfg, true, None)).await {
        Ok(Ok(c)) => Arc::new(c),
        Ok(Err(e)) => return Err(format!("DTLS: {e}")),
        Err(_) => return Err("DTLS 시한".into()),
    };
    let mut sc = SrtpConfig { profile: ProtectionProfile::Aes128CmHmacSha1_80, ..Default::default() };
    let state = dtls.connection_state().await;
    sc.extract_session_keys_from_dtls(state, true).await.map_err(|e| format!("SRTP 키: {e}"))?;
    let tx = Context::new(&sc.keys.local_master_key, &sc.keys.local_master_salt, sc.profile, None, None).map_err(|e| e.to_string())?;
    let rx = Context::new(&sc.keys.remote_master_key, &sc.keys.remote_master_salt, sc.profile, None, None).map_err(|e| e.to_string())?;
    Ok((dtls, tx, rx))
}

/// DTLS 를 UDP 소켓 위에 — 받는 것은 갈라 받은 DTLS 레코드만
struct ChanConn {
    rx: AsyncMutex<mpsc::Receiver<Vec<u8>>>,
    sock: Arc<UdpSocket>,
    peer: SocketAddr,
}

#[async_trait]
impl Conn for ChanConn {
    async fn connect(&self, _addr: SocketAddr) -> webrtc_util::Result<()> {
        Ok(())
    }
    async fn recv(&self, buf: &mut [u8]) -> webrtc_util::Result<usize> {
        let mut r = self.rx.lock().await;
        match r.recv().await {
            Some(p) => {
                let n = p.len().min(buf.len());
                buf[..n].copy_from_slice(&p[..n]);
                Ok(n)
            }
            None => Err(webrtc_util::Error::ErrUseClosedNetworkConn),
        }
    }
    async fn recv_from(&self, buf: &mut [u8]) -> webrtc_util::Result<(usize, SocketAddr)> {
        let n = self.recv(buf).await?;
        Ok((n, self.peer))
    }
    async fn send(&self, buf: &[u8]) -> webrtc_util::Result<usize> {
        self.sock.send_to(buf, self.peer).await.map_err(|e| webrtc_util::Error::Other(e.to_string()))
    }
    async fn send_to(&self, buf: &[u8], _target: SocketAddr) -> webrtc_util::Result<usize> {
        self.send(buf).await
    }
    fn local_addr(&self) -> webrtc_util::Result<SocketAddr> {
        self.sock.local_addr().map_err(|e| webrtc_util::Error::Other(e.to_string()))
    }
    fn remote_addr(&self) -> Option<SocketAddr> {
        Some(self.peer)
    }
    async fn close(&self) -> webrtc_util::Result<()> {
        Ok(())
    }
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        self
    }
}

fn dcep_open() -> Vec<u8> {
    let label = CHANNEL_LABEL.as_bytes();
    let mut m = vec![DCEP_OPEN, DCEP_PARTIAL_REXMIT_UNORDERED, 0, 0, 0, 0, 0, 0];
    m.extend_from_slice(&(label.len() as u16).to_be_bytes());
    m.extend_from_slice(&0u16.to_be_bytes());
    m.extend_from_slice(label);
    m
}

async fn write_payload(dtls: &DTLSConn, p: Payload) -> bool {
    if let Payload::RawEncode(chunks) = p {
        for c in chunks {
            if dtls.write(&c, None).await.is_err() {
                return false;
            }
        }
    }
    true
}

/// SCTP 연관(클라) — DTLS 클라는 짝수 스트림(RFC 8832 §6). 서면 DCEP OPEN · ACK 를 받으면 DC 열림 · 그 뒤 받은 이진 메시지를 낸다.
async fn sctp_client(dtls: Arc<DTLSConn>, say: Say, mut ctl: mpsc::UnboundedReceiver<SctpCtl>) {
    let (pkt_tx, mut pkt_rx) = mpsc::channel::<Vec<u8>>(256);
    let reader = dtls.clone();
    let rh = tokio::spawn(async move {
        let mut buf = vec![0u8; 65_536];
        while let Ok(n) = reader.read(&mut buf, None).await {
            if pkt_tx.send(buf[..n].to_vec()).await.is_err() {
                break;
            }
        }
    });
    let remote = SocketAddr::from(([127, 0, 0, 1], 5_000));
    let mut endpoint = Endpoint::new(Arc::new(EndpointConfig::new()), None);
    let mut cc = ClientConfig::new();
    cc.transport = Arc::new(TransportConfig::default().with_max_data_retransmits(None).with_max_init_retransmits(None));
    let Ok((h, mut a)) = endpoint.connect(cc, remote) else {
        rh.abort();
        say.send(Fact::Sctp(false));
        return;
    };
    let h: AssociationHandle = h;
    let mut sid: u16 = 0;
    let mut open = false;
    let open_stream = |a: &mut sctp_proto::Association, sid: u16| {
        if let Ok(mut s) = a.open_stream(sid, PayloadProtocolIdentifier::Dcep) {
            let _ = s.set_reliability_params(true, ReliabilityType::Rexmit, 0);
            let _ = s.write_sctp(&Bytes::from(dcep_open()), PayloadProtocolIdentifier::Dcep);
        }
    };
    loop {
        let now = Instant::now();
        let mut alive = true;
        while let Some(t) = endpoint.poll_transmit() {
            alive &= write_payload(&dtls, t.payload).await;
        }
        while let Some(t) = a.poll_transmit(now) {
            alive &= write_payload(&dtls, t.payload).await;
        }
        while let Some(ev) = a.poll_endpoint_event() {
            if let Some(back) = endpoint.handle_event(h, ev) {
                a.handle_event(back);
            }
        }
        while let Some(ev) = a.poll() {
            match ev {
                Event::Connected => {
                    say.send(Fact::Sctp(true));
                    open_stream(&mut a, sid);
                }
                Event::HandshakeFailed { .. } | Event::AssociationLost { .. } => {
                    rh.abort();
                    say.send(Fact::Sctp(false));
                    return;
                }
                Event::Stream(StreamEvent::Readable { id }) => {
                    if let Ok(mut s) = a.stream(id) {
                        while let Ok(Some(chunks)) = s.read_sctp() {
                            let mut buf = vec![0u8; chunks.len()];
                            let Ok(n) = chunks.read(&mut buf) else { continue };
                            buf.truncate(n);
                            if chunks.ppi == PayloadProtocolIdentifier::Dcep {
                                if id == sid && buf.first() == Some(&DCEP_ACK) && !open {
                                    open = true;
                                    say.send(Fact::Dc(true));
                                }
                            } else if matches!(chunks.ppi, PayloadProtocolIdentifier::Binary | PayloadProtocolIdentifier::String) {
                                say.send(Fact::DcData(buf));
                            }
                        }
                    }
                }
                Event::Stream(StreamEvent::Stopped { id, .. }) | Event::Stream(StreamEvent::Finished { id }) => {
                    if id == sid && open {
                        open = false;
                        say.send(Fact::Dc(false));
                    }
                }
                _ => {}
            }
        }
        let now = Instant::now();
        while let Some(t) = a.poll_transmit(now) {
            alive &= write_payload(&dtls, t.payload).await;
        }
        if !alive {
            rh.abort();
            say.send(Fact::Dtls(false));
            return;
        }
        let deadline = a.poll_timeout();
        let sleep = async {
            match deadline {
                Some(d) => tokio::time::sleep_until(tokio::time::Instant::from_std(d)).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            p = pkt_rx.recv() => {
                let Some(p) = p else {
                    say.send(Fact::Dtls(false));
                    return;
                };
                if let Some((_, DatagramEvent::AssociationEvent(e))) = endpoint.handle(Instant::now(), remote, None, None, Bytes::from(p)) {
                    a.handle_event(e);
                }
            }
            c = ctl.recv() => match c {
                None => {
                    rh.abort();
                    return;
                }
                Some(SctpCtl::Send(b)) => {
                    if open
                        && let Ok(mut s) = a.stream(sid) {
                            let _ = s.write_sctp(&Bytes::from(b), PayloadProtocolIdentifier::Binary);
                        }
                }
                Some(SctpCtl::Reopen) => {
                    if let Ok(mut s) = a.stream(sid) {
                        let _ = s.stop();
                    }
                    open = false;
                    sid = sid.wrapping_add(2);
                    open_stream(&mut a, sid);
                }
            },
            _ = sleep => a.handle_timeout(Instant::now()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rtp_header_skips_csrc_ext_padding() {
        let mut p = vec![0xB1, 0xE0, 0x00, 0x05, 0, 0, 0, 9, 0, 0, 0, 7];
        p.extend_from_slice(&[0, 0, 0, 1]);
        p.extend_from_slice(&[0xBE, 0xDE, 0, 1, 1, 2, 3, 4]);
        p.extend_from_slice(&[0xAA, 0xBB, 0, 2]);
        let h = rtp_header(&p).unwrap();
        assert!(h.marker);
        assert_eq!((h.pt, h.seq, h.ts, h.ssrc), (0x60, 5, 9, 7));
        assert_eq!(&p[h.start..h.end], &[0xAA, 0xBB]);
    }

    #[test]
    fn dcep_open_is_unordered_label() {
        let m = dcep_open();
        assert_eq!(&m[..2], &[DCEP_OPEN, DCEP_PARTIAL_REXMIT_UNORDERED]);
        assert_eq!(&m[12..], CHANNEL_LABEL.as_bytes());
    }
}
