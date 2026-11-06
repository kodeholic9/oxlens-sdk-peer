// author: kodeholic (powered by Claude)
//! SDK 루프 — 세션 · 방 · 연결 · 구독 · 트랙 · 발언권의 상태를 한 태스크가 쥔다(SDK:RULE-API-ASYNC 스레드 — 서버사이드 = SDK 루프 하나).
//! ★절차는 위(연동규격서)가 지은 영역 상태기(generated/*_client)가 정한다 — 이 루프는 입구 표(어느 사건이 어느 입구를) · 효과 표(위 효과를 어느 사건으로 —
//!   generated/*_binding) · 이음매(위가 받는 곳까지 정했다) 집행 · 위가 값을 두지 않은 손잡이(시계 · 사다리)만 한다.
//! 바깥 입력은 넷 — 앱 명령 · 소켓 사건 · 연결 사실 · 재동기 응답. 걸음 뒤 요청의 끝(응답 · 버림)과 미룬 일(협상)을 차례대로 비운다.

mod link;
mod room;
mod ses;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dtls::crypto::Certificate;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::api::{Config, Event, Stats};
use crate::error::Error;
use crate::generated::{floor_client as flr, session_client as sc};
use crate::http;
use crate::link::transport::{LinkFact, MediaPacket, MediaSink};
use crate::sig::{Outcome, Purpose, Sig, SockIn};

pub(crate) use link::Link;
pub(crate) use room::Room;

pub(crate) enum Cmd {
    Connect(oneshot::Sender<Result<(), Error>>),
    SetToken(String),
    Join(String, oneshot::Sender<Result<(), Error>>),
    Leave(String, oneshot::Sender<Result<(), Error>>),
    Close(oneshot::Sender<()>),
    Stats(oneshot::Sender<Stats>),
}

/// 재동기 응답(HTTP — 루프 밖 태스크가 받아 넘긴다)
pub(crate) struct HttpDone {
    room: String,
    epoch: u64,
    result: Result<http::Reply, Error>,
}

/// 루프가 거는 시계 — 이름 하나에 하나
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Tk {
    Ses(sc::TimerName),
    /// 끊긴 뒤 경과가 재개 창을 넘었나
    Window,
    /// 요청 기한(T-req)을 센다
    ReqCheck,
    /// 재구축 칸 첫 입장 분산(massJitterMs)
    Jitter,
    /// 과부하 하한 · 재시도 사다리 — 방마다
    RoomRetry(String),
    RoomResend(String),
    /// 재동기 실패 뒤 다시
    Resync(String),
    Floor(String, flr::TimerName),
    LinkGrace(String),
    LinkApply(String),
    LinkReopenTry(String),
    LinkReopenJudge(String),
    LinkStable(String),
    LinkCheck(String),
    LinkProbe(String),
}

#[derive(Default)]
pub(crate) struct Timers {
    at: BTreeMap<(Instant, u64), Tk>,
    by: HashMap<Tk, (Instant, u64)>,
    seq: u64,
}

impl Timers {
    pub(crate) fn start(&mut self, tk: Tk, after: Duration) {
        self.stop(&tk);
        self.seq += 1;
        let when = Instant::now() + after;
        self.at.insert((when, self.seq), tk.clone());
        self.by.insert(tk, (when, self.seq));
    }
    pub(crate) fn stop(&mut self, tk: &Tk) {
        if let Some(k) = self.by.remove(tk) {
            self.at.remove(&k);
        }
    }
    pub(crate) fn armed(&self, tk: &Tk) -> bool {
        self.by.contains_key(tk)
    }
    fn next(&self) -> Option<Instant> {
        self.at.keys().next().map(|(t, _)| *t)
    }
    fn due(&mut self, now: Instant) -> Vec<Tk> {
        let mut out = Vec::new();
        while let Some((&(t, s), _)) = self.at.iter().next() {
            if t > now {
                break;
            }
            if let Some(tk) = self.at.remove(&(t, s)) {
                self.by.remove(&tk);
                out.push(tk);
            }
        }
        out
    }
    pub(crate) fn stop_where(&mut self, pred: impl Fn(&Tk) -> bool) {
        let gone: Vec<Tk> = self.by.keys().filter(|k| pred(k)).cloned().collect();
        for k in gone {
            self.stop(&k);
        }
    }
}

/// 불투명 신원 → 작은 수(SDK:RULE-MOD-BINDING-VALUES) — 같은 문자열 ⇔ 같은 수 · 처음 본 차례로 1 부터 · 0 은 내지 않는다
#[derive(Default)]
pub(crate) struct Interner {
    to: HashMap<String, i64>,
    from: HashMap<i64, String>,
}

impl Interner {
    pub(crate) fn id(&mut self, s: &str) -> i64 {
        if let Some(n) = self.to.get(s) {
            return *n;
        }
        let n = self.to.len() as i64 + 1;
        self.to.insert(s.to_string(), n);
        self.from.insert(n, s.to_string());
        n
    }
    pub(crate) fn str(&self, n: i64) -> Option<&str> {
        self.from.get(&n).map(String::as_str)
    }
}

/// 루프 안에서 미룬 일 — 웹의 비동기 협상 · 다음 틱 자리
pub(crate) enum Job {
    Negotiate(String),
}

pub(crate) struct Core {
    pub(crate) base: String,
    pub(crate) token: String,
    pub(crate) ses: sc::St,
    pub(crate) user_id: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) reason: Option<(u16, String)>,
    pub(crate) failure: Option<Error>,
    pub(crate) app_closed: bool,
    pub(crate) resume_on_token: bool,
    pub(crate) resume_body: Option<Value>,
    pub(crate) dropped_at: Option<Instant>,
    pub(crate) last_info: Option<crate::api::SessionInfo>,
    pub(crate) sig: Sig,
    pub(crate) rooms: BTreeMap<String, Room>,
    pub(crate) links: BTreeMap<String, Link>,
    pub(crate) sids: Interner,
    pub(crate) epochs: Interner,
    pub(crate) jids: Interner,
    /// 배정표 줄의 SDP 재료 지문(SDK:RULE-MOD-BINDING-VALUES `rows`)
    pub(crate) materials: Interner,
    pub(crate) timers: Timers,
    pub(crate) ev: mpsc::UnboundedSender<Event>,
    pub(crate) sock_tx: mpsc::UnboundedSender<SockIn>,
    pub(crate) fact_tx: mpsc::UnboundedSender<LinkFact>,
    pub(crate) http_tx: mpsc::UnboundedSender<HttpDone>,
    pub(crate) sink: MediaSink,
    pub(crate) cert: Certificate,
    pub(crate) connect_waiters: Vec<oneshot::Sender<Result<(), Error>>>,
    /// ACTIVE 를 기다리는 입장(SDK:RULE-API-ASYNC 연결 전 요청)
    pub(crate) parked: Vec<(String, oneshot::Sender<Result<(), Error>>)>,
    pub(crate) jitter_rooms: Vec<String>,
    pub(crate) jobs: VecDeque<Job>,
    pub(crate) ended: Vec<(Purpose, Outcome)>,
    pub(crate) link_gen: u64,
    pub(crate) finished: bool,
    trace: bool,
}

pub(crate) fn spawn(cfg: Config, ev: mpsc::UnboundedSender<Event>, media: mpsc::Sender<MediaPacket>) -> Result<mpsc::UnboundedSender<Cmd>, Error> {
    let (auth, prefix) = http::split_base(&cfg.base)?;
    let cert = Certificate::generate_self_signed(vec!["oxpeer".to_string()]).map_err(|e| Error::transport(format!("인증서: {e}")))?;
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (sock_tx, sock_rx) = mpsc::unbounded_channel();
    let (fact_tx, fact_rx) = mpsc::unbounded_channel();
    let (http_tx, http_rx) = mpsc::unbounded_channel();
    let sink = MediaSink { routes: Arc::default(), tx: media, dropped: Arc::new(AtomicU64::new(0)), unrouted: Arc::new(AtomicU64::new(0)), decrypt_fail: Arc::new(AtomicU64::new(0)) };
    let req = Duration::from_millis(sc::timer_ms(sc::TimerName::TReq).unwrap_or(30_000) as u64);
    let core = Core {
        base: cfg.base.clone(),
        token: cfg.token,
        ses: sc::initial(),
        user_id: None,
        session_id: None,
        reason: None,
        failure: None,
        app_closed: false,
        resume_on_token: false,
        resume_body: None,
        dropped_at: None,
        last_info: None,
        sig: Sig::new(format!("ws://{auth}{prefix}/ws"), req),
        rooms: BTreeMap::new(),
        links: BTreeMap::new(),
        sids: Interner::default(),
        epochs: Interner::default(),
        jids: Interner::default(),
        materials: Interner::default(),
        timers: Timers::default(),
        ev,
        sock_tx,
        fact_tx,
        http_tx,
        sink,
        cert,
        connect_waiters: Vec::new(),
        parked: Vec::new(),
        jitter_rooms: Vec::new(),
        jobs: VecDeque::new(),
        ended: Vec::new(),
        link_gen: 0,
        finished: false,
        trace: std::env::var_os("OXPEER_TRACE").is_some(),
    };
    tokio::spawn(run(core, cmd_rx, sock_rx, fact_rx, http_rx));
    Ok(cmd_tx)
}

async fn run(mut core: Core, mut cmd: mpsc::UnboundedReceiver<Cmd>, mut sock: mpsc::UnboundedReceiver<SockIn>, mut facts: mpsc::UnboundedReceiver<LinkFact>, mut http: mpsc::UnboundedReceiver<HttpDone>) {
    loop {
        let deadline = core.timers.next();
        let sleep = async {
            match deadline {
                Some(d) => tokio::time::sleep_until(tokio::time::Instant::from_std(d)).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            c = cmd.recv() => match c {
                Some(c) => core.command(c),
                None => {
                    core.close_for_app();
                    core.drain();
                    break;
                }
            },
            Some(s) = sock.recv() => core.socket(s),
            Some(f) = facts.recv() => core.link_fact(f),
            Some(h) = http.recv() => core.resync_done(h.room, h.epoch, h.result),
            _ = sleep => {
                for tk in core.timers.due(Instant::now()) {
                    core.fire(tk);
                    core.drain();
                }
            }
        }
        core.drain();
        if core.finished {
            break;
        }
    }
}

impl Core {
    /// 전이 기록 — OXPEER_TRACE 가 있으면 표준 오류로(층 · 열쇠 · 앞 → 뒤 · 라벨 · 까닭)
    pub(crate) fn record(&self, layer: &str, key: &str, from: &str, to: &str, label: &str, cause: &str) {
        if self.trace {
            eprintln!("[oxpeer] {layer} {key} {from}→{to} {label} ({cause})");
        }
    }

    /// 받지 않는 사건 — 조용히 무시하지 않는다(SDK:IMPL-LINK-PROJECTION 받지 않는 사건)
    pub(crate) fn impossible(&self, layer: &str, key: &str, from: &str, entry: &str) {
        if self.trace {
            eprintln!("[oxpeer] {layer} {key} {from} — 입구 {entry} 를 받지 않는다");
        }
    }

    pub(crate) fn emit(&self, e: Event) {
        let _ = self.ev.send(e);
    }

    /// 요청 — 끝은 drain 이 쓰임새로 가른다
    pub(crate) fn request(&mut self, op: crate::generated::ws_wire::Op, body: Value, purpose: Purpose) {
        let mut ended = std::mem::take(&mut self.ended);
        self.sig.request(op, body, purpose, &mut ended);
        self.ended = ended;
        self.watch_requests();
    }

    pub(crate) fn watch_requests(&mut self) {
        if self.sig.counting() && !self.timers.armed(&Tk::ReqCheck) {
            self.timers.start(Tk::ReqCheck, Duration::from_secs(1));
        }
    }

    /// 걸음 뒤 — 요청의 끝과 미룬 일을 바닥까지
    pub(crate) fn drain(&mut self) {
        for _ in 0..10_000 {
            if !self.ended.is_empty() {
                let batch = std::mem::take(&mut self.ended);
                for (p, o) in batch {
                    self.outcome(p, o);
                }
                continue;
            }
            if let Some(j) = self.jobs.pop_front() {
                match j {
                    Job::Negotiate(server) => self.negotiate(&server),
                }
                continue;
            }
            break;
        }
        self.settle_joins();
    }

    fn command(&mut self, c: Cmd) {
        match c {
            Cmd::Connect(tx) => self.connect(tx),
            Cmd::SetToken(t) => self.set_token(t),
            Cmd::Join(room, tx) => self.join(room, tx),
            Cmd::Leave(room, tx) => self.leave(room, tx),
            Cmd::Close(tx) => {
                self.close_for_app();
                let _ = tx.send(());
            }
            Cmd::Stats(tx) => {
                let _ = tx.send(Stats {
                    media_dropped: self.sink.dropped.load(Ordering::Relaxed),
                    media_unrouted: self.sink.unrouted.load(Ordering::Relaxed),
                    decrypt_fail: self.sink.decrypt_fail.load(Ordering::Relaxed),
                });
            }
        }
    }

    fn socket(&mut self, s: SockIn) {
        match s {
            SockIn::Open(g) => self.sock_open(g),
            SockIn::Closed(g) => self.sock_closed(g),
            SockIn::Frame(g, f) => {
                let mut ended = std::mem::take(&mut self.ended);
                let n = self.sig.receive(g, f, &mut ended);
                self.ended = ended;
                if let Some((op, body)) = n {
                    self.notify(op, body);
                }
            }
        }
    }

    /// 통지 — 받는 영역으로 가른다
    fn notify(&mut self, op: crate::generated::ws_wire::Op, body: Value) {
        use crate::generated::ws_wire::Op;
        let room_of = |b: &Value| b.get("room_id").and_then(Value::as_str).unwrap_or("").to_string();
        match op {
            Op::ParticipantEvent | Op::ParticipantState | Op::TrackEvent | Op::TrackState => {
                let r = room_of(&body);
                if self.rooms.contains_key(&r) {
                    self.room_delta(&r, op, body);
                }
            }
            Op::RoomEvent => {
                let r = room_of(&body);
                if self.rooms.contains_key(&r) {
                    self.room_event(&r, &body);
                }
            }
            Op::MlineEvent => {
                if let Some(t) = body.get("table").cloned() {
                    self.mline_event(t);
                }
            }
            Op::LinkEvent => {
                let server = body.get("sfu_id").and_then(Value::as_str).unwrap_or("").to_string();
                let ufrag = body.get("ufrag").and_then(Value::as_str).unwrap_or("").to_string();
                self.link_event(&server, &ufrag);
            }
            _ => self.record("SES", "notify", "-", "-", op_name(op), "모르는 통지 — ACK 만 했다"),
        }
    }

    fn fire(&mut self, tk: Tk) {
        match tk {
            Tk::Ses(t) => self.ses_expired(t),
            Tk::Window => self.ses_window(),
            Tk::ReqCheck => {
                let mut ended = std::mem::take(&mut self.ended);
                let cut = self.sig.check_timeouts(&mut ended);
                self.ended = ended;
                if cut {
                    let mut ended = std::mem::take(&mut self.ended);
                    self.sig.close(&mut ended);
                    self.ended = ended;
                    let g = self.sig.generation();
                    self.sock_closed_now(g);
                }
                self.watch_requests();
            }
            Tk::Jitter => self.jitter_due(),
            Tk::RoomRetry(r) => self.room_retry_due(&r),
            Tk::RoomResend(r) => self.room_resend(&r),
            Tk::Resync(r) => self.resync(&r),
            Tk::Floor(r, t) => self.floor_expire(&r, t),
            Tk::LinkGrace(s) => self.link_grace(&s),
            Tk::LinkApply(s) => self.link_apply_timeout(&s),
            Tk::LinkReopenTry(s) => self.link_reopen_try(&s),
            Tk::LinkReopenJudge(s) => self.link_reopen_judge(&s),
            Tk::LinkStable(s) => self.link_stable(&s),
            Tk::LinkCheck(s) => self.link_check_send(&s),
            Tk::LinkProbe(s) => self.link_probe_due(&s),
        }
    }

    /// 요청의 끝 — 쓰임새대로 받는 곳에
    fn outcome(&mut self, p: Purpose, o: Outcome) {
        match p {
            Purpose::Bind => self.bind_reply(o),
            Purpose::Resume => self.resume_reply(o),
            Purpose::Heartbeat => {}
            Purpose::RoomJoin { room, epoch, jid } => self.join_reply(&room, epoch, jid, o),
            Purpose::RoomLeave { room, epoch } => self.leave_reply(&room, epoch, o),
            Purpose::Ready { room, epoch } => self.ready_reply(&room, epoch, o),
            Purpose::IceRestart { server, req } => self.restart_reply(&server, req, o),
        }
    }

    /// 재동기 — HTTP(세션 헤더 · 연:RULE-TRN-HTTP-AUTH-TWO) · 응답은 루프로 돌아온다
    pub(crate) fn spawn_resync(&self, room: &str, epoch: u64) {
        let (base, sid, token, tx, r) = (self.base.clone(), self.session_id.clone(), self.token.clone(), self.http_tx.clone(), room.to_string());
        tokio::spawn(async move {
            let path = format!("/rooms/{}?tracks=1", url_escape(&r));
            let header = match &sid {
                Some(s) => ("X-OxLens-Session", s.clone()),
                None => ("Authorization", format!("Bearer {token}")),
            };
            let result = http::call(&base, "GET", &path, &[(header.0, header.1.as_str())], None).await;
            let _ = tx.send(HttpDone { room: r, epoch, result });
        });
    }
}

pub(crate) fn op_name(op: crate::generated::ws_wire::Op) -> &'static str {
    use crate::generated::ws_wire::Op;
    match op {
        Op::ParticipantEvent => "PARTICIPANT_EVENT",
        Op::ParticipantState => "PARTICIPANT_STATE",
        Op::TrackEvent => "TRACK_EVENT",
        Op::TrackState => "TRACK_STATE",
        Op::RoomEvent => "ROOM_EVENT",
        Op::MlineEvent => "MLINE_EVENT",
        Op::LinkEvent => "LINK_EVENT",
        Op::Message => "MESSAGE",
        Op::Task => "TASK",
        _ => "OTHER",
    }
}

fn url_escape(s: &str) -> String {
    s.bytes().map(|b| if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) { (b as char).to_string() } else { format!("%{b:02X}") }).collect()
}
