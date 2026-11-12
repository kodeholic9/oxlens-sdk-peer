// author: kodeholic (powered by Claude)
//! AI 에이전트 브리지 배선(SDK:RULE-PLAT-AGENT) — 프로세스 하나 = 에이전트 세션 하나(방 하나).
//! 방 쪽: oxpeer 로 발언 방에 들어가 반이중 음성을 발행하고 · 라이브 레코드 통(지점 ① → 발언 정책 파이프라인 지점 ② ③)으로 턴을 짓는다.
//! AI 쪽: 고객 WS 서버로 접속해(끊기면 다시) 사건 · PCM 을 주고받는다 · 방 연결과 AI 연결의 수명은 따로다.
//!
//!   oxbridge --base http://127.0.0.1:1974/media --room ch-1 --ai-url ws://127.0.0.1:9000/agent \
//!            [--token T | --user agent-1 [--api-key K --api-secret S]] [--header 'Authorization: Bearer x'] [--custom persona=ops] \
//!            [--rate 16000] [--silence-ms 3000] [--ws-queue-ms 5000] [--secs N] [--tls-root ca.pem] [--ai-tls-root ca.pem]
//!
//! 끝나면 표준 출력에 요약 JSON 한 줄(SDK:RULE-PLAT-AGENT 끝 요약) · 종료 코드 0 정상 · 1 기동 실패 · 3 방을 잃음 · 4 레코드 통 넘침.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use oxbridge::codec::{self, Codec};
use oxbridge::reply::{Act, Limits, PttView, Replies};
use oxbridge::seam::{self, FromAi};
use oxbridge::turn::{TurnOut, Turns};
use oxbridge::ws::{self, In, OutQueue, Target};
use oxpeer::log::{AppKind, AppLine, Level};
use oxpeer::record::{Channel, Pipeline, Policy, RecordConfig};
use oxpeer::{Client, Config, Duplex, Event, Kind, OutFrame, TokenReq, TrackSpec};
use serde_json::{Value, json};
use tokio::sync::{Notify, mpsc};

struct Args {
    base: String,
    room: String,
    ai_url: String,
    token: Option<String>,
    user: String,
    api_key: String,
    api_secret: String,
    headers: Vec<(String, String)>,
    custom: serde_json::Map<String, Value>,
    rate: u32,
    silence_ms: Option<u64>,
    ws_queue_ms: u64,
    secs: Option<u64>,
    tls_root: Option<String>,
    ai_tls_root: Option<String>,
}

fn args() -> Args {
    let mut a = Args { base: String::new(), room: String::new(), ai_url: String::new(), token: None, user: "oxbridge-agent".into(), api_key: "ox_k_demo".into(), api_secret: "ox_s_demo".into(), headers: Vec::new(), custom: serde_json::Map::new(), rate: 16_000, silence_ms: None, ws_queue_ms: 5_000, secs: None, tls_root: None, ai_tls_root: None };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let mut v = || it.next().unwrap_or_default();
        match k.as_str() {
            "--base" => a.base = v(),
            "--room" => a.room = v(),
            "--ai-url" => a.ai_url = v(),
            "--token" => a.token = Some(v()),
            "--user" => a.user = v(),
            "--api-key" => a.api_key = v(),
            "--api-secret" => a.api_secret = v(),
            "--header" => {
                let h = v();
                if let Some((k, x)) = h.split_once(':') {
                    a.headers.push((k.trim().to_string(), x.trim().to_string()));
                }
            }
            "--custom" => {
                let c = v();
                if let Some((k, x)) = c.split_once('=') {
                    a.custom.insert(k.to_string(), json!(x));
                }
            }
            "--rate" => a.rate = v().parse().unwrap_or(0),
            "--silence-ms" => a.silence_ms = v().parse().ok(),
            "--ws-queue-ms" => a.ws_queue_ms = v().parse().unwrap_or(5_000),
            "--secs" => a.secs = v().parse().ok(),
            "--tls-root" => a.tls_root = Some(v()),
            "--ai-tls-root" => a.ai_tls_root = Some(v()),
            other => eprintln!("모르는 인자 {other}"),
        }
    }
    if a.base.is_empty() || a.room.is_empty() || a.ai_url.is_empty() {
        eprintln!("사용: oxbridge --base http://host:port/media --room <방> --ai-url ws://host:port/path [--token T | --user id] [--header 'K: V'] [--custom k=v] [--rate 16000] [--silence-ms N] [--secs N]");
        std::process::exit(2);
    }
    a
}

fn fail(what: &str, e: impl std::fmt::Display) -> ! {
    eprintln!("[oxbridge] {what}: {e}");
    std::process::exit(1);
}

fn wall_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// 방 쪽 송출 일꾼에 넘기는 것 — 차례가 지켜진다(프레임 → 놓기 앞에 통을 비움)
enum Tx {
    Frame(OutFrame),
    Press,
    Release,
}

/// 순번 범위 모음(이어진 것은 잇는다)
#[derive(Default)]
struct Range(Option<(u32, u32)>);

impl Range {
    fn add(&mut self, a: u32, b: u32) {
        self.0 = Some(match self.0 {
            Some((x, y)) => (x.min(a), y.max(b)),
            None => (a, b),
        });
    }
    fn take(&mut self) -> Option<(u32, u32)> {
        self.0.take()
    }
}

struct Bridge {
    client: Client,
    room: String,
    me: String,
    session: String,
    rate: u32,
    frame_n: usize,
    custom: serde_json::Map<String, Value>,
    codec: Codec,
    turns: Turns,
    replies: Replies,
    outq: Arc<Mutex<OutQueue>>,
    kick: Arc<Notify>,
    tx: mpsc::Sender<Tx>,
    ws_up: bool,
    ws_connects: u64,
    ws_downs: u64,
    /// 다음 프레임 순번 — 보낸 · 못 보낸 · 비운 프레임 모두 하나씩(사건의 순번과 같은 축)
    seq: u32,
    disconnected: Range,
    filled: Range,
    /// 턴 안 다음 프레임의 RTP 시각(48kHz) · 디코딩 남은 표본
    expect_ts: Option<u32>,
    carry: Vec<i16>,
    participants: BTreeSet<String>,
    /// 남의 허가(`ctl/floor` taken)를 본 때 — 잠깐 뒤에도 쥐고 있다고 믿으면 잃은 것(회수 사건과 레코드는 다른 통이라 차례를 기다린다)
    foreign: Option<(String, Instant)>,
    turn_end_at: Option<Instant>,
    reply_pcm_at: Option<Instant>,
    lat_ai_ms: Vec<u64>,
    lat_bridge_ms: Vec<u64>,
    frames_to_ai: u64,
    gaps_network: u64,
    tx_dropped: u64,
    orphan_diag: u64,
    pcm_carry: Vec<u8>,
    stop_reason: Option<String>,
    /// 무음 상한 `L`(ms — 요약에 싣는다)
    silence_ms: u64,
}

impl Bridge {
    fn send_text(&mut self, t: &str, v: Value) {
        if self.ws_up {
            if let Ok(mut q) = self.outq.lock() {
                q.text(seam::event(t, v));
            }
            self.kick.notify_one();
        }
    }

    fn diag(&self, level: Level, spec: &str, what: &str, detail: String) {
        self.client.log(AppLine::new(AppKind::Diag).level(level).spec(spec).room(&self.room).key("what", what).msg(detail));
    }

    fn start_event(&mut self, resumed: bool) {
        let parts: Vec<Value> = self.participants.iter().map(|u| json!({ "user_id": u })).collect();
        let v = json!({
            "version": seam::VERSION, "session": self.session, "room": self.room, "agent": { "user_id": self.me },
            "audio": { "encoding": "pcm_s16le", "rate": self.rate, "channels": 1, "frame_ms": seam::FRAME_MS },
            "participants": parts, "custom": Value::Object(self.custom.clone()), "resumed": resumed,
        });
        self.send_text("start", v);
    }

    /// 20ms PCM 한 프레임에 순번을 매겨 보낸다(끊겼으면 그 순번은 `gap(disconnected)` 몫)
    fn emit_audio(&mut self, pcm: &[i16], filled: bool) {
        let seq = self.seq;
        self.seq = self.seq.wrapping_add(1);
        if filled {
            self.filled.add(seq, seq);
        } else {
            self.flush_filled();
        }
        if self.ws_up {
            if let Ok(mut q) = self.outq.lock() {
                q.audio(seq, seam::audio_frame(seq, pcm));
            }
            self.kick.notify_one();
            self.frames_to_ai += 1;
        } else {
            self.disconnected.add(seq, seq);
        }
    }

    fn flush_filled(&mut self) {
        if let Some((a, b)) = self.filled.take() {
            self.gaps_network += u64::from(b.wrapping_sub(a)) + 1;
            self.send_text("gap", json!({ "from_seq": a, "to_seq": b, "reason": "network" }));
        }
    }

    fn turn_out(&mut self, o: TurnOut) {
        match o {
            TurnOut::Start { turn, speaker } => {
                self.expect_ts = None;
                self.carry.clear();
                self.send_text("turn.start", json!({ "turn": turn, "speaker": { "user_id": speaker }, "seq": self.seq }));
            }
            TurnOut::End { turn, reason } => {
                if !self.carry.is_empty() {
                    let mut f = std::mem::take(&mut self.carry);
                    f.resize(self.frame_n, 0);
                    self.emit_audio(&f, false);
                }
                self.flush_filled();
                self.send_text("turn.end", json!({ "turn": turn, "last_seq": self.seq.wrapping_sub(1), "reason": reason }));
                self.turn_end_at = Some(Instant::now());
            }
            TurnOut::Audio { data, filled, rtp_ts } => {
                // 메우지 않은 구멍(상한을 넘어 세그먼트가 갈림) — 그 길이만큼 순번을 비우고 gap(network)
                if let Some(e) = self.expect_ts {
                    let d = rtp_ts.wrapping_sub(e) as i32;
                    if d >= 960 {
                        let n = (d / 960) as u32;
                        let a = self.seq;
                        self.seq = self.seq.wrapping_add(n);
                        self.flush_filled();
                        self.gaps_network += u64::from(n);
                        self.send_text("gap", json!({ "from_seq": a, "to_seq": a.wrapping_add(n - 1), "reason": "network" }));
                    }
                }
                let Some(pcm) = self.codec.decode(&data) else {
                    self.diag(Level::Warn, "SD-RULE-PLAT-AGENT-AUDIO", "decode_error", format!("bytes={}", data.len()));
                    return;
                };
                self.expect_ts = Some(rtp_ts.wrapping_add((pcm.len() as u64 * 48_000 / u64::from(self.rate)) as u32));
                self.carry.extend_from_slice(&pcm);
                while self.carry.len() >= self.frame_n {
                    let f: Vec<i16> = self.carry.drain(..self.frame_n).collect();
                    self.emit_audio(&f, filled);
                }
            }
            TurnOut::Orphan { source, user } => {
                self.orphan_diag += 1;
                if self.orphan_diag <= 20 || self.orphan_diag.is_multiple_of(100) {
                    self.diag(Level::Warn, "SD-RULE-PLAT-AGENT-TURN", "orphan_frame", format!("source={source:?} user={user:?} total={}", self.orphan_diag));
                }
            }
        }
    }

    fn acts(&mut self, acts: Vec<Act>) {
        for a in acts {
            match a {
                Act::Press => self.room_tx(Tx::Press),
                Act::Release => self.room_tx(Tx::Release),
                Act::Send(pcm) => {
                    let level = codec::level_dbov(&pcm);
                    match self.codec.encode(&pcm) {
                        Some(pkt) => self.room_tx(Tx::Frame(OutFrame { data: pkt.into(), duration: Duration::from_millis(u64::from(seam::FRAME_MS)), keyframe: false, audio_level: Some(level) })),
                        None => self.diag(Level::Warn, "SD-RULE-PLAT-AGENT-AUDIO", "encode_error", String::new()),
                    }
                }
                Act::State(v) => {
                    if v["state"] != "floor_granted" && v["state"] != "floor_queued" && v["state"] != "done" {
                        self.diag(Level::Info, "SD-RULE-PLAT-AGENT-REPLY", "reply_state", v.to_string());
                    }
                    self.send_text("reply.state", v);
                }
                Act::Mark { name, played } => self.send_text("mark", json!({ "name": name, "status": if played { "played" } else { "cleared" } })),
                Act::Diag { what, spec, detail } => self.diag(Level::Warn, spec, what, detail),
            }
        }
        if let (Some(p), Some(s)) = (self.reply_pcm_at, self.replies.first_send) {
            self.lat_bridge_ms.push(s.saturating_duration_since(p).as_millis() as u64);
            self.reply_pcm_at = None;
        }
    }

    fn room_tx(&mut self, t: Tx) {
        if self.tx.try_send(t).is_err() {
            self.tx_dropped += 1;
        }
    }

    fn ai_in(&mut self, m: In) {
        let now = Instant::now();
        match m {
            In::Up => {
                let resumed = self.ws_connects > 0;
                self.ws_connects += 1;
                self.ws_up = true;
                self.diag(Level::Info, "SD-RULE-PLAT-AGENT-CONNECT", "ai_connected", format!("resumed={resumed}"));
                self.start_event(resumed);
                if let Some((a, b)) = self.disconnected.take() {
                    self.send_text("gap", json!({ "from_seq": a, "to_seq": b, "reason": "disconnected" }));
                }
            }
            In::Down(e) => {
                if self.ws_up {
                    self.ws_downs += 1;
                    self.diag(Level::Warn, "SD-RULE-PLAT-AGENT-FAULT", "ai_disconnected", e);
                }
                self.ws_up = false;
                if let Some((a, b)) = self.outq.lock().ok().and_then(|mut q| q.cut()) {
                    self.disconnected.add(a, b);
                }
                let acts = self.replies.ws_down();
                self.acts(acts);
            }
            In::Text(s) => match seam::parse_text(&s) {
                Ok(Some(FromAi::ReplyStart { reply })) => {
                    self.reply_pcm_at = None;
                    let acts = self.replies.reply_start(&reply);
                    self.acts(acts);
                }
                Ok(Some(FromAi::ReplyEnd { reply })) => {
                    let acts = self.replies.reply_end(&reply, now);
                    self.acts(acts);
                }
                Ok(Some(FromAi::Mark { name })) => {
                    let acts = self.replies.mark(&name);
                    self.acts(acts);
                }
                Ok(Some(FromAi::Clear)) => {
                    let acts = self.replies.clear();
                    self.acts(acts);
                }
                Ok(None) => {}
                Err(e) => self.diag(Level::Warn, "SD-RULE-PLAT-AGENT-EVENTS", "ai_bad_event", e),
            },
            In::Binary(b) => {
                let pcm = seam::pcm_from_bytes(&mut self.pcm_carry, &b);
                if self.replies.active().is_some() && self.reply_pcm_at.is_none() && self.replies.first_send.is_none() {
                    self.reply_pcm_at = Some(now);
                    if let Some(t) = self.turn_end_at.take() {
                        self.lat_ai_ms.push(now.saturating_duration_since(t).as_millis() as u64);
                    }
                }
                let acts = self.replies.pcm(&pcm);
                self.acts(acts);
            }
        }
    }

    fn participants(&mut self, list: &[oxpeer::MemberInfo]) {
        let now: BTreeSet<String> = list.iter().map(|m| m.user_id.clone()).filter(|u| *u != self.me).collect();
        let joined: Vec<String> = now.difference(&self.participants).cloned().collect();
        let left: Vec<String> = self.participants.difference(&now).cloned().collect();
        self.participants = now;
        for u in joined {
            self.send_text("participant", json!({ "action": "joined", "user_id": u }));
        }
        for u in left {
            self.send_text("participant", json!({ "action": "left", "user_id": u }));
        }
    }

    fn summary(&self, pipe: &Pipeline) -> Value {
        let c = &self.replies.counts;
        let t = &self.turns.counts;
        let p = pipe.counts();
        json!({
            "room": self.room, "user": self.me, "session": self.session, "rate": self.rate, "stop_reason": self.stop_reason, "silence_ms": self.silence_ms,
            "turns": t.turns, "self_utterances": t.self_utterances, "self_frames": t.self_frames,
            "orphan_sourceless": t.orphan_sourceless, "orphan_sourced": t.orphan_sourced,
            "pipeline": { "frames": p.frames, "filled": p.filled, "other_speaker": p.other_speaker, "unheld": p.unheld, "broken": p.broken },
            "frames_to_ai": self.frames_to_ai, "gaps_network": self.gaps_network, "next_seq": self.seq,
            "overflow_frames": self.outq.lock().map(|q| q.overflow_frames).unwrap_or(0),
            "ws_connects": self.ws_connects, "ws_downs": self.ws_downs,
            "replies": { "started": c.started, "done": c.done, "cleared": c.cleared, "granted": c.granted, "queued": c.queued, "denied": c.denied,
                         "preempted": c.preempted, "truncated_silence": c.truncated_silence, "truncated_buffer": c.truncated_buffer, "truncated_floor": c.truncated_floor,
                         "revoked": c.revoked, "lost": c.lost, "ws_cut": c.ws_cut, "overlap": c.overlap },
            "frames_sent": c.frames_sent, "ignored_samples": c.ignored_samples, "tx_dropped": self.tx_dropped,
            "decode_errors": self.codec.decode_errors, "encode_errors": self.codec.encode_errors,
            "lat_ai_ms": self.lat_ai_ms, "lat_bridge_ms": self.lat_bridge_ms, "end_wall_ms": wall_ms(),
        })
    }
}

#[tokio::main]
async fn main() {
    let a = args();
    if !seam::rate_ok(a.rate) {
        fail("표본률", format!("{} — 8000 · 12000 · 16000 · 24000 · 48000 중 하나", a.rate));
    }
    let lim = match a.silence_ms {
        Some(ms) => Limits::default_for(seam::FRAME_MS).with_silence(ms).unwrap_or_else(|e| fail("무음 상한", e)),
        None => Limits::default_for(seam::FRAME_MS),
    };
    let codec = Codec::new(a.rate).unwrap_or_else(|e| fail("코덱", e));
    let root = a.tls_root.as_ref().map(|p| std::fs::read(p).unwrap_or_else(|e| fail("tls 뿌리", e)));
    let token = match &a.token {
        Some(t) => t.clone(),
        None => {
            let req = TokenReq { api_key: a.api_key.clone(), api_secret: a.api_secret.clone(), user_id: a.user.clone(), participant_type: Some(2), hidden: Some(false), metadata: None, permission: None };
            oxpeer::issue_token_with_root(&a.base, &req, root.as_deref()).await.unwrap_or_else(|e| fail("토큰 발급", e))
        }
    };
    let session = format!("{:x}-{}", wall_ms(), std::process::id());
    let mut cfg = Config::new(&a.base, token);
    cfg.tls_root_pem = root;
    // 라이브 레코드 통만(원시 파트 없음) · tick 을 짧게 — 닫기 유예가 다음 레코드에서 풀리므로 턴 끝이 tick 만큼 더 늦지 않게
    let mut rc = RecordConfig::live(format!("agent-{session}"));
    rc.tick = Duration::from_millis(50);
    cfg.record = Some(rc);
    let (client, mut events, _media, _frames) = Client::create(cfg).unwrap_or_else(|e| fail("만들기", e));
    let mut transitions = client.transitions();
    let mut records = client.records(8_192);
    client.connect().await.unwrap_or_else(|e| fail("접속", e));
    client.join_speaking(&a.room).await.unwrap_or_else(|e| fail("입장", e));
    let track = client.publish(&a.room, TrackSpec { kind: Kind::Audio, duplex: Duplex::Half, simulcast: false }).await.unwrap_or_else(|e| fail("발행", e));
    let snap = client.snapshot().await.unwrap_or_else(|e| fail("상태", e));
    let me = snap.session.and_then(|s| s.user_id).unwrap_or_default();
    eprintln!("[oxbridge] {} 에 들어왔다 — 발행 {} · AI {}", a.room, track.track_id, a.ai_url);

    // 방 쪽 송출 일꾼 — 프레임은 송신 시계로 · 놓기 전에 통을 비운다(SDK:RULE-PLAT-AGENT-REPLY)
    let (tx, mut tx_rx) = mpsc::channel::<Tx>(64);
    {
        let client = client.clone();
        let room = a.room.clone();
        tokio::spawn(async move {
            while let Some(t) = tx_rx.recv().await {
                match t {
                    Tx::Frame(f) => {
                        let _ = track.write(f).await;
                    }
                    Tx::Press => {
                        let _ = client.ptt_press(&room, 0).await;
                    }
                    Tx::Release => {
                        track.drain().await;
                        let _ = client.ptt_release(&room).await;
                    }
                }
            }
        });
    }

    let frame_n = seam::frame_samples(a.rate);
    let outq = Arc::new(Mutex::new(OutQueue::new((a.ws_queue_ms / u64::from(seam::FRAME_MS)).max(1) as usize)));
    let kick = Arc::new(Notify::new());
    let (ws_tx, mut ws_rx) = mpsc::unbounded_channel();
    let ai_root = a.ai_tls_root.as_ref().map(|p| std::fs::read(p).unwrap_or_else(|e| fail("AI tls 뿌리", e)));
    ws::spawn(Target { url: a.ai_url.clone(), headers: a.headers.clone(), tls_root_pem: ai_root }, outq.clone(), kick.clone(), ws_tx).unwrap_or_else(|e| fail("AI 연결", e));

    let mut b = Bridge {
        client: client.clone(), room: a.room.clone(), me: me.clone(), session, rate: a.rate, frame_n, custom: a.custom.clone(), codec,
        turns: Turns::new(me.clone()), replies: Replies::new(lim, frame_n), outq: outq.clone(), kick: kick.clone(), tx,
        ws_up: false, ws_connects: 0, ws_downs: 0, seq: 0, disconnected: Range::default(), filled: Range::default(), expect_ts: None, carry: Vec::new(),
        participants: BTreeSet::new(), foreign: None, turn_end_at: None, reply_pcm_at: None, lat_ai_ms: Vec::new(), lat_bridge_ms: Vec::new(),
        frames_to_ai: 0, gaps_network: 0, tx_dropped: 0, orphan_diag: 0, pcm_carry: Vec::new(), stop_reason: None, silence_ms: lim.silence.as_millis() as u64,
    };
    let mut pipe = Pipeline::new(Policy::Utterance);
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let secs = a.secs;
    let stop = async {
        match secs {
            Some(s) => tokio::time::sleep(Duration::from_secs(s)).await,
            None => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    };
    tokio::pin!(stop);
    let mut code = 0;
    loop {
        tokio::select! {
            _ = &mut stop => break,
            e = events.next() => {
                let Some(e) = e else { b.stop_reason = Some("sdk_closed".into()); code = 3; break };
                match e {
                    Event::Ptt { room, event, ptt } if room == b.room => {
                        let cause = match event.as_str() {
                            "denied" => ptt.last_deny.as_ref().map(|c| c.cause),
                            "revoked" => ptt.last_revoke.as_ref().map(|c| c.cause),
                            _ => None,
                        };
                        let acts = b.replies.ptt(&event, &PttView { cause, remaining_sec: ptt.remaining_sec, queue_position: ptt.queue_position }, Instant::now());
                        b.acts(acts);
                    }
                    Event::Participants { room, participants } if room == b.room => b.participants(&participants),
                    Event::Forced { room, cause } if room == b.room => { b.stop_reason = Some(format!("room_lost:{cause}")); code = 3; break }
                    Event::Left { room } if room == b.room => { b.stop_reason = Some("room_left".into()); code = 3; break }
                    Event::Closed { name, .. } => { b.stop_reason = Some(format!("session_closed:{name}")); code = 3; break }
                    Event::RecordConsumerStopped { reason } => { b.stop_reason = Some(reason); code = 4; break }
                    _ => {}
                }
            }
            t = transitions.next() => {
                if let Some(t) = t && t.area == "FLR" && t.key == b.room && let Some(to) = t.to {
                    let acts = b.replies.peer_state(&to);
                    b.acts(acts);
                }
            }
            r = records.next() => {
                let Some(r) = r else { b.stop_reason = Some("records_closed".into()); code = 4; break };
                if r.channel == Channel::Floor && let Ok(v) = serde_json::from_slice::<Value>(&r.data)
                    && v["room"] == b.room.as_str() && v["event"] == "taken" && let Some(u) = v["user_id"].as_str() && u != b.me {
                    b.foreign = Some((u.to_string(), Instant::now()));
                }
                for o in pipe.push(&r) {
                    for t in b.turns.push(&o) {
                        b.turn_out(t);
                    }
                }
            }
            m = ws_rx.recv() => {
                let Some(m) = m else { break };
                b.ai_in(m);
            }
            _ = tick.tick() => {
                let now = Instant::now();
                if let Some((u, at)) = b.foreign.clone() && now.saturating_duration_since(at) >= Duration::from_millis(150) {
                    b.foreign = None;
                    let acts = b.replies.foreign_taken(&u);
                    b.acts(acts);
                }
                let acts = b.replies.tick(now);
                b.acts(acts);
            }
        }
    }
    // 끝 — 쥔 발언권을 놓고 · stop 을 알리고 · 요약 한 줄
    if b.replies.active().is_some() {
        let acts = b.replies.clear();
        b.acts(acts);
    }
    let reason = b.stop_reason.clone().unwrap_or_else(|| "shutdown".into());
    b.send_text("stop", json!({ "reason": reason }));
    ws::flush(&outq, &kick, Duration::from_millis(500)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    println!("{}", b.summary(&pipe));
    client.close().await;
    std::process::exit(code);
}
