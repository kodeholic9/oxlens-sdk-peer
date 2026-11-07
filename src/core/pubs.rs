// author: kodeholic (powered by Claude)
//! 말하는 쪽(20261107c 에이전트 단계) — 발행 · 소속(발언 방) · 발언 누름 · 메시지. 절차는 위 발행(PUB) · 소속(AFF) · 발언권(FLR) 상태기(생성본)가 정하고
//! 이 파일은 입구 표 · 효과 표 · 이음매(SDK:IMPL-PUB-API · SDK:IMPL-FLR-BUTTON)만 집행한다.
//! ★서버사이드엔 SDP 가 없다 — 발행의 협상 이음매(`sdp.rebuild`)는 곧바로 성공으로 돌아오고(발행 m-line = PUBLISH_TRACKS 의 줄 하나),
//!   트랙을 송신 시계에 얹는 것(`attach`)도 곧바로다(SDK:RULE-PLAT-SERVER — 앱이 인코딩한 프레임을 넣는다).
//! 송신 시계(SDK:RULE-PLAT-SERVER 송신 시계)는 트랙마다 태스크 하나 — 프레임 길이대로 내보내고 · 게이트(허가 · 전이중 송신)가 닫혀 있으면 버리고 센다.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime};

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio::task::AbortHandle;

use super::{Cmd, Core};
use crate::api::{Event, PttInfo, RoomView, Snapshot, TrackSpec};
use crate::error::{Category, Error};
use crate::generated::ws_wire::{AffiliationReq, AffiliationRes, Duplex, Kind, MessageBody, MessageReq, Op, PublishTrack, PublishTracksReq, PublishTracksReqAddAction, PublishTracksReqRemoveAction, Source};
use crate::generated::{aff_binding as ab, aff_client as ac, floor_client as fc, pub_binding as pbd, publish_client as pc};
use crate::link::depack::Codec;
use crate::link::send::{ExtIds, OutFrame, SendTrack};
use crate::link::transport::PubOut;
use crate::sig::{Outcome, Purpose};

/// 서버 선언 확장의 이름(연:LINK.type.ServerConfig) — 번호는 그 선언에서 찾는다
const URI_MID: &str = "urn:ietf:params:rtp-hdrext:sdes:mid";
const URI_AUDIO_LEVEL: &str = "urn:ietf:params:rtp-hdrext:ssrc-audio-level";
/// 송신 통 — 앱이 실시간보다 빨리 넣으면 이만큼 앞서 쌓이고 그 뒤 `write` 가 기다린다(역압)
pub(crate) const FRAME_QUEUE: usize = 25;
const SR_EVERY: Duration = Duration::from_secs(1);

/// 송신 시계와 SDK 루프가 같이 보는 것 — 게이트 · 게이트가 열린 뒤 첫 패킷 표시 · 계수 · 지금 붙은 서버
pub(crate) struct TrackIo {
    pub gate: AtomicBool,
    pub fresh: AtomicBool,
    pub gated: AtomicU64,
    pub sent: AtomicU64,
    pub qa_dropped: AtomicU64,
    pub server: Mutex<String>,
}

pub(crate) struct PubTrack {
    pub(crate) id: u64,
    pub(crate) room: String,
    pub(crate) st: pc::St,
    pub(crate) kind: Kind,
    pub(crate) half: bool,
    pub(crate) codec: String,
    pub(crate) fmtp: Option<String>,
    pub(crate) pt: u8,
    pub(crate) rtx: Option<(u32, u8)>,
    pub(crate) ssrc: u32,
    pub(crate) mid: String,
    pub(crate) track_id: Option<String>,
    pub(crate) io: Arc<TrackIo>,
    waiters: Vec<oneshot::Sender<Result<(u64, String), Error>>>,
    /// 마지막 거절 응답(앱에 실패 사유로 준다)
    fault: Option<Value>,
    dup_waiters: Vec<oneshot::Sender<Result<(), Error>>>,
    pacer: AbortHandle,
}

impl Drop for PubTrack {
    fn drop(&mut self) {
        self.pacer.abort();
    }
}

/// 송신 시계 — 프레임을 그 길이대로 낸다. 늦게 온 프레임(앱이 쉬었다)은 지금부터 다시 센다 · RTP 시각은 보낸 때에서 나온다
#[allow(clippy::too_many_arguments)]
async fn pacer(id: u64, mut rx: mpsc::Receiver<OutFrame>, st: Arc<Mutex<SendTrack>>, io: Arc<TrackIo>, outs: Arc<RwLock<HashMap<String, Arc<OnceLock<PubOut>>>>>, cmd: mpsc::UnboundedSender<Cmd>) {
    let out = |io: &TrackIo| -> Option<PubOut> {
        let server = io.server.lock().ok()?.clone();
        outs.read().ok()?.get(&server)?.get().cloned()
    };
    let mut t0: Option<Instant> = None;
    let mut next: Option<Instant> = None;
    let qa_every: u64 = std::env::var("OXPEER_QA_DROP_SEND_EVERY").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let mut qa_n = 0u64;
    let video = st.lock().map(|t| t.clock == 90_000).unwrap_or(false);
    let mut sr = tokio::time::interval(SR_EVERY);
    sr.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            f = rx.recv() => {
                let Some(f) = f else { break };
                let now = Instant::now();
                let spurt = next.is_none_or(|n| now > n + f.duration);
                let due = match next { Some(n) if n > now => n, _ => now };
                if due > now {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await;
                }
                let base = *t0.get_or_insert(due);
                next = Some(due + f.duration);
                if !io.gate.load(Ordering::Acquire) {
                    io.gated.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let Some(o) = out(&io) else {
                    io.gated.fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                let pkts = match st.lock() {
                    Ok(mut t) => {
                        let ts = t.ts_at(due.duration_since(base));
                        t.frame(&f, ts, due, spurt)
                    }
                    Err(_) => continue,
                };
                for p in &pkts {
                    // ★QA 스위치(OXPEER_QA_DROP_SEND_EVERY) — 영상 패킷 N 개마다 하나를 보내지 않는다(보낸 기록엔 남는다 → 서버 NACK 에 RTX 로 답한다) · 시험 수단
                    if qa_every > 0 && video {
                        qa_n += 1;
                        if qa_n.is_multiple_of(qa_every) {
                            io.qa_dropped.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    }
                    o.rtp(p);
                }
                io.sent.fetch_add(1, Ordering::Relaxed);
                if io.fresh.swap(false, Ordering::AcqRel) {
                    let _ = cmd.send(Cmd::RtpTx(id));
                }
            }
            _ = sr.tick() => {
                if let Some(o) = out(&io)
                    && let Some(b) = st.lock().ok().and_then(|t| t.sender_report(Instant::now(), SystemTime::now()))
                {
                    o.rtcp(&b);
                }
            }
        }
    }
}

fn rand_u32() -> u32 {
    let mut b = [0u8; 4];
    let _ = getrandom::fill(&mut b);
    u32::from_be_bytes(b) | 1
}

fn str_or_none(v: Option<&str>) -> String {
    v.map(str::to_string).unwrap_or_else(|| "NONE".to_string())
}

impl Core {
    /// 서버마다 내보내는 손잡이 자리 — 연결을 세우거나 닫을 때 고친다(송신 시계가 서버 이름으로 찾는다)
    pub(super) fn sync_outs(&mut self) {
        let m: HashMap<String, Arc<OnceLock<PubOut>>> = self.links.iter().filter_map(|(k, l)| l.out_slot().map(|o| (k.clone(), o))).collect();
        if let Ok(mut g) = self.pub_outs.write() {
            *g = m;
        }
    }

    // ── 발행(PUB) ──────────────────────────────────────────────────────

    /// 발행 — 그 방의 미디어 코덱으로(연:RULE-PUB-PUBLISH-PROC — 보내기 전에 맞춘다) · 응답의 `track_id` 와 함께 돌아온다
    pub(super) fn publish(&mut self, room: String, spec: TrackSpec, frames: mpsc::Receiver<OutFrame>, tx: oneshot::Sender<Result<(u64, String), Error>>) {
        let Some(r) = self.rooms.get(&room).filter(|r| r.st.rs == crate::generated::room_client::RoomSt::JOINED) else {
            let _ = tx.send(Err(Error::sdk("NOT_JOINED", Category::State)));
            return;
        };
        let Some(server) = r.sfu.clone() else {
            let _ = tx.send(Err(Error::sdk("NOT_JOINED", Category::State)));
            return;
        };
        let video = spec.kind == Kind::Video;
        let codec = if video { r.media.as_ref().and_then(|m| m.video.clone()).unwrap_or_else(|| "H264".into()) } else { r.media.as_ref().and_then(|m| m.audio.clone()).unwrap_or_else(|| "opus".into()) };
        let c = Codec::of(&codec);
        let (pt, rtx_pt, fmtp) = match c {
            Codec::Opus => (111, None, None),
            Codec::Vp8 => (96, Some(97), None),
            Codec::H264 => (102, Some(103), Some("level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f".to_string())),
            Codec::Other => {
                let _ = tx.send(Err(Error::sdk("CODEC_UNSUPPORTED", Category::Negotiation)));
                return;
            }
        };
        let ext = self.links.get(&server).map(|l| {
            let id = |u: &str| l.sc.extmap.iter().find(|e| e.uri == u).map(|e| e.id).unwrap_or(0);
            ExtIds { mid: id(URI_MID), audio_level: if video { 0 } else { id(URI_AUDIO_LEVEL) } }
        });
        let ext = ext.unwrap_or_default();
        let n = self.pub_mids.entry(server.clone()).or_insert(0);
        let mid = n.to_string();
        *n += 1;
        self.pub_seq += 1;
        let id = self.pub_seq;
        let ssrc = rand_u32();
        let rtx = rtx_pt.map(|p| (rand_u32(), p));
        let st = Arc::new(Mutex::new(SendTrack::new(ssrc, pt, rtx, c, video, mid.clone(), ext)));
        if let Ok(mut g) = self.sink.senders.write() {
            g.insert(ssrc, st.clone());
        }
        let io = Arc::new(TrackIo { gate: AtomicBool::new(false), fresh: AtomicBool::new(false), gated: AtomicU64::new(0), sent: AtomicU64::new(0), qa_dropped: AtomicU64::new(0), server: Mutex::new(server) });
        let pacer = tokio::spawn(pacer(id, frames, st, io.clone(), self.pub_outs.clone(), self.cmd_tx.clone())).abort_handle();
        let half = spec.duplex == Duplex::Half;
        let t = PubTrack {
            id,
            room: room.clone(),
            st: pc::St { half, ..pc::initial() },
            kind: spec.kind,
            half,
            codec,
            fmtp,
            pt,
            rtx,
            ssrc,
            mid,
            track_id: None,
            io,
            waiters: vec![tx],
            fault: None,
            dup_waiters: Vec::new(),
            pacer,
        };
        self.pubs.insert(id, t);
        self.pub_enter(id, "on", pc::on, "publish");
    }

    pub(super) fn unpublish(&mut self, id: u64) {
        self.pub_enter(id, "off", |s| Some(pc::off(s)), "unpublish");
        self.pub_forget(id);
    }

    fn pub_forget(&mut self, id: u64) {
        // 기다리던 publish 는 통이 버려져 닫힘으로 끝난다
        if let Some(t) = self.pubs.remove(&id)
            && let Ok(mut g) = self.sink.senders.write()
        {
            g.remove(&t.ssrc);
        }
    }

    pub(super) fn pub_enter(&mut self, id: u64, entry: &'static str, f: impl Fn(&pc::St) -> Option<pc::Step>, cause: &str) {
        let Some(t) = self.pubs.get(&id) else { return };
        let Some(step) = f(&t.st) else {
            self.impossible("PUB", &id.to_string(), t.st.ps.as_str(), entry);
            return;
        };
        self.pub_apply(id, step, cause);
    }

    /// 한 걸음 적용(제네릭 아님 — 이 안에서 다른 입구를 다시 부른다)
    fn pub_apply(&mut self, id: u64, step: pc::Step, cause: &str) {
        let Some(t) = self.pubs.get(&id) else { return };
        let prev = t.st.ps;
        let plan = pbd::plan(step);
        let label = plan.label;
        let Some(t) = self.pubs.get_mut(&id) else { return };
        t.st = plan.state.clone();
        t.half = plan.state.half;
        self.record("PUB", &id.to_string(), prev.as_str(), plan.state.ps.as_str(), label, cause);
        for tx in &plan.send {
            match tx {
                pc::Tx::PublishTracks { action: pc::PublishTracksAction::add, .. } => self.send_publish(id),
                pc::Tx::PublishTracks { action: pc::PublishTracksAction::remove, .. } => self.send_unpublish(id),
                pc::Tx::TrackSet { duplex } => self.send_track_set(id, *duplex),
            }
        }
        for a in &plan.actions {
            let Some(t) = self.pubs.get(&id) else { break };
            match a {
                pbd::PubAction::GateOpen => {
                    t.io.fresh.store(true, Ordering::Release);
                    t.io.gate.store(true, Ordering::Release);
                }
                pbd::PubAction::GateClose => t.io.gate.store(false, Ordering::Release),
                pbd::PubAction::TransceiverInactive => {}
            }
        }
        for e in &plan.events {
            let Some(t) = self.pubs.get_mut(&id) else { break };
            let (room, track_id) = (t.room.clone(), t.track_id.clone());
            match e {
                pbd::PubEvent::Published => {
                    let tid = track_id.clone().unwrap_or_default();
                    for w in t.waiters.drain(..) {
                        let _ = w.send(Ok((id, tid.clone())));
                    }
                    self.emit(Event::Published { room, track_id: tid });
                }
                pbd::PubEvent::PublishFailed => {
                    let err = t.fault.as_ref().map(Error::from_failure).unwrap_or_else(|| Error::sdk("PUBLISH_FAILED", Category::State));
                    for w in t.waiters.drain(..) {
                        let _ = w.send(Err(err.clone()));
                    }
                    self.emit(Event::PublishFailed { room });
                }
                pbd::PubEvent::Ended => self.emit(Event::TrackEnded { room, track_id: track_id.unwrap_or_default() }),
                pbd::PubEvent::DuplexApplied => {
                    for w in t.dup_waiters.drain(..) {
                        let _ = w.send(Ok(()));
                    }
                }
                pbd::PubEvent::DuplexFailed => {
                    for w in t.dup_waiters.drain(..) {
                        let _ = w.send(Err(Error::sdk("DUPLEX_FAILED", Category::State)));
                    }
                }
            }
        }
        // 협상 이음매 — SDP 가 없으니 곧바로 성공(발행 m-line 은 PUBLISH_TRACKS 의 줄)
        if plan.seams.iter().any(|s| s.fx == pc::SeamFx::sdp_rebuild) {
            self.pub_enter(id, "nego", |s| pc::nego(s, true), "no-sdp");
        }
        // 등록됐다 — 송신 시계는 이미 섰다(트랙을 트랜시버에 붙인 것과 같다)
        if prev != pc::PubSt::REGISTERED && plan.state.ps == pc::PubSt::REGISTERED {
            self.pub_enter(id, "attach", |s| pc::attach(s, true), "pacer");
        }
        if plan.state.ps == pc::PubSt::IDLE && !plan.state.want {
            self.pub_forget_if_done(id);
        }
    }

    fn pub_forget_if_done(&mut self, id: u64) {
        if self.pubs.get(&id).is_some_and(|t| t.waiters.is_empty()) {
            self.pub_forget(id);
        }
    }

    fn send_publish(&mut self, id: u64) {
        let Some(t) = self.pubs.get(&id) else { return };
        let Some(join_id) = self.rooms.get(&t.room).and_then(|r| self.jids.str(r.st.jid)).map(str::to_string) else { return };
        let video = t.kind == Kind::Video;
        let (mid_ext, level_ext) = t.io.server.lock().ok().and_then(|s| self.links.get(&*s).map(|l| {
            let id = |u: &str| l.sc.extmap.iter().find(|e| e.uri == u).map(|e| e.id);
            (id(URI_MID), if video { None } else { id(URI_AUDIO_LEVEL) })
        })).unwrap_or((None, None));
        let track = PublishTrack {
            kind: t.kind,
            ssrc: t.ssrc,
            mid: t.mid.clone(),
            codec: Some(t.codec.clone()),
            fmtp: t.fmtp.clone(),
            pt: t.pt,
            rtx_pt: t.rtx.map(|r| r.1),
            rtx_ssrc: t.rtx.map(|r| r.0),
            // 오디오 항목에 source 를 실으면 거절이다(연:RULE-PUB-PUBLISH-FAIL 1002)
            source: if video { Some(Source::Camera) } else { None },
            duplex: Some(if t.half { Duplex::Half } else { Duplex::Full }),
            simulcast: if video { Some(false) } else { None },
        };
        let body = PublishTracksReq::Add {
            action: Some(PublishTracksReqAddAction::Add),
            join_id,
            room_id: t.room.clone(),
            tracks: vec![track],
            twcc_extmap_id: None,
            rid_extmap_id: None,
            repair_rid_extmap_id: None,
            mid_extmap_id: mid_ext,
            audio_level_extmap_id: level_ext,
        };
        let pid = t.st.req;
        if let Ok(v) = serde_json::to_value(&body) {
            self.request(Op::PublishTracks, v, Purpose::Publish { track: id, pid });
        }
    }

    fn send_unpublish(&mut self, id: u64) {
        let Some(t) = self.pubs.get(&id) else { return };
        let (Some(tid), Some(join_id)) = (t.track_id.clone(), self.rooms.get(&t.room).and_then(|r| self.jids.str(r.st.jid)).map(str::to_string)) else { return };
        let body = PublishTracksReq::Remove { action: PublishTracksReqRemoveAction::Remove, room_id: t.room.clone(), join_id, track_ids: vec![tid] };
        if let Ok(v) = serde_json::to_value(&body) {
            self.request(Op::PublishTracks, v, Purpose::Unpublish);
        }
    }

    fn send_track_set(&mut self, id: u64, d: pc::Duplex) {
        let Some(t) = self.pubs.get(&id) else { return };
        let Some(tid) = t.track_id.clone() else { return };
        let body = serde_json::json!({ "room_id": t.room, "track_id": tid, "duplex": d.as_str() });
        self.request(Op::TrackSet, body, Purpose::TrackSet { track: id });
    }

    pub(super) fn publish_reply(&mut self, id: u64, pid: i64, o: Outcome) {
        let m = match o {
            Outcome::Ok(b) => {
                if let Some(t) = self.pubs.get_mut(&id) {
                    let mid = t.mid.clone();
                    t.track_id = b.get("tracks").and_then(Value::as_array).and_then(|a| a.iter().find(|x| x.get("mid").and_then(Value::as_str) == Some(mid.as_str()))).and_then(|x| x.get("track_id")).and_then(Value::as_str).map(str::to_string);
                }
                pc::PubResMsg { action: Some(pc::PubAction::add), code: 0, part: pc::Part::res, pid }
            }
            Outcome::Fail(b) => {
                let code = b.get("code").and_then(Value::as_i64).unwrap_or(5003);
                if let Some(t) = self.pubs.get_mut(&id) {
                    t.fault = Some(b);
                }
                pc::PubResMsg { action: None, code, part: pc::Part::fail, pid }
            }
            Outcome::Expired => {
                self.pub_enter(id, "reqExpired", pc::req_expired, "T-req");
                return;
            }
            Outcome::Transport | Outcome::Closed => return,
        };
        self.pub_enter(id, "pubRes", |s| Some(pc::pub_res(s, &m)), if m.part == pc::Part::res { "PUBLISH_TRACKS.res" } else { "PUBLISH_TRACKS.fail" });
    }

    pub(super) fn track_set_reply(&mut self, id: u64, o: Outcome) {
        let m = match o {
            Outcome::Ok(b) => pc::SetDuplexResMsg {
                code: 0,
                duplex: b.get("duplex").and_then(Value::as_str).and_then(pc::Duplex::parse),
                noop: b.get("noop").and_then(Value::as_bool).unwrap_or(false),
                part: pc::Part::res,
            },
            Outcome::Fail(b) => pc::SetDuplexResMsg { code: b.get("code").and_then(Value::as_i64).unwrap_or(5003), duplex: None, noop: false, part: pc::Part::fail },
            Outcome::Expired => {
                self.pub_enter(id, "reqExpired", pc::req_expired, "T-req");
                return;
            }
            Outcome::Transport | Outcome::Closed => return,
        };
        self.pub_enter(id, "setDuplexRes", |s| Some(pc::set_duplex_res(s, &m)), "TRACK_SET");
    }

    pub(super) fn set_duplex(&mut self, id: u64, half: bool, tx: oneshot::Sender<Result<(), Error>>) {
        let Some(t) = self.pubs.get_mut(&id) else {
            let _ = tx.send(Err(Error::sdk("NO_TRACK", Category::State)));
            return;
        };
        t.dup_waiters.push(tx);
        let d = if half { pc::Duplex::half } else { pc::Duplex::full };
        self.pub_enter(id, "setDuplex", |s| pc::set_duplex(s, d), "app");
    }

    /// 발언권 효과 mic.open · mic.close 가 그 방의 반이중 발행에 닿는다(위 이음매 — 발행 floorOpen · floorClose)
    pub(super) fn floor_to_pub(&mut self, room: &str, open: bool) {
        let ids: Vec<u64> = self.pubs.values().filter(|t| t.room == room && t.half).map(|t| t.id).collect();
        for id in ids {
            if open {
                self.pub_enter(id, "floorOpen", |s| Some(pc::floor_open(s)), "mic.open");
            } else {
                self.pub_enter(id, "floorClose", |s| Some(pc::floor_close(s)), "mic.close");
            }
        }
    }

    /// 게이트가 열린 뒤 첫 반이중 RTP 가 나갔다 — 발언권 입구 rtpTx
    pub(super) fn rtp_tx(&mut self, id: u64) {
        let Some(t) = self.pubs.get(&id) else { return };
        if t.half {
            let room = t.room.clone();
            self.floor_enter(&room, "rtpTx", |s| Some(fc::rtp_tx(s)), "pacer");
        }
    }

    /// 방을 잃었다 · 떠난다 — 그 방의 발행(위 이음매 roomGone)
    pub(super) fn pub_room_gone(&mut self, room: &str, by_app: bool) {
        let ids: Vec<u64> = self.pubs.values().filter(|t| t.room == room).map(|t| t.id).collect();
        for id in ids {
            self.pub_enter(id, "roomGone", |s| Some(pc::room_gone(s, by_app)), if by_app { "leave" } else { "room_gone" });
        }
    }

    /// 새 세션 — 발행을 전부 다시 한다(위 이음매 pub.reregister_all)
    pub(super) fn pub_reregister_all(&mut self) {
        let ids: Vec<u64> = self.pubs.keys().copied().collect();
        for id in ids {
            self.pub_enter(id, "reregister", |s| Some(pc::reregister(s)), "rebuild");
        }
    }

    /// 그 서버 연결이 닫혔다(drop) · 재구축으로 다시 섰다(reregister) — 위 이음매 pub.drop_server · pub.reregister_server
    pub(super) fn pub_server(&mut self, server: &str, up: bool) {
        let ids: Vec<u64> = self.pubs.values().filter(|t| t.io.server.lock().is_ok_and(|s| *s == server)).map(|t| t.id).collect();
        for id in ids {
            if up {
                self.pub_enter(id, "reregister", |s| Some(pc::reregister(s)), "rebuild_server");
            } else {
                self.pub_enter(id, "dropServer", |s| Some(pc::drop_server(s)), "drop_server");
            }
        }
    }

    /// 이어받기 응답의 발행 목록과 맞댄다(위 이음매 pub.resume_check) — listed = 내 트랙이 목록에 있다 · shalf = 서버가 아는 반이중
    pub(super) fn pub_resume_check(&mut self, body: &Value) {
        let list: Vec<crate::generated::ws_wire::Publication> = body.get("publications").cloned().and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();
        let ids: Vec<(u64, bool, bool)> = self
            .pubs
            .values()
            .map(|t| {
                let p = t.track_id.as_ref().and_then(|x| list.iter().find(|p| &p.track_id == x));
                (t.id, p.is_some(), p.is_some_and(|p| p.duplex == Duplex::Half))
            })
            .collect();
        for (id, listed, shalf) in ids {
            self.pub_enter(id, "resumeCheck", |s| Some(pc::resume_check(s, listed, shalf)), "RESUME");
        }
    }

    /// 내 발행이 트랙 목록에서 빠졌다(TRACK_EVENT remove — 위 이음매 revoked)
    pub(super) fn pub_revoked(&mut self, track_ids: &[String]) {
        let ids: Vec<u64> = self.pubs.values().filter(|t| t.track_id.as_ref().is_some_and(|x| track_ids.contains(x))).map(|t| t.id).collect();
        for id in ids {
            self.pub_enter(id, "revoked", |s| Some(pc::revoked(s, &pc::RevokedMsg {})), "TRACK_EVENT.remove");
        }
    }

    /// 서버가 내 발행에 키프레임을 청했다 — 앱 몫(SDK 는 인코딩하지 않는다)
    pub(super) fn pli_in(&mut self, ssrc: u32) {
        if let Some(t) = self.pubs.values().find(|t| t.ssrc == ssrc) {
            self.emit(Event::KeyframeRequested { room: t.room.clone(), track_id: t.track_id.clone().unwrap_or_default() });
        }
    }

    // ── 발언 누름(FLR) ───────────────────────────────────────────────

    pub(super) fn ptt_press(&mut self, room: &str, priority: u8) -> Result<(), Error> {
        if !self.rooms.contains_key(room) {
            return Err(Error::sdk("NOT_JOINED", Category::State));
        }
        self.floor_enter(room, "press", |s| Some(fc::press(s, i64::from(priority))), "app");
        Ok(())
    }

    pub(super) fn ptt_release(&mut self, room: &str) -> Result<(), Error> {
        if !self.rooms.contains_key(room) {
            return Err(Error::sdk("NOT_JOINED", Category::State));
        }
        self.floor_enter(room, "release", fc::release, "app");
        Ok(())
    }

    // ── 소속(AFF — 발언 방) ──────────────────────────────────────────

    pub(super) fn aff_enter(&mut self, entry: &'static str, f: impl Fn(&ac::St) -> Option<ac::Step>, cause: &str) {
        let Some(step) = f(&self.aff) else {
            self.impossible("AFF", "session", &self.aff.r#pub, entry);
            return;
        };
        let prev = self.aff.r#pub.clone();
        let plan = ab::plan(step);
        self.aff = plan.state.clone();
        self.record("AFF", "session", &prev, &self.aff.r#pub.clone(), plan.label, cause);
        for tx in &plan.send {
            match tx {
                ac::Tx::Affiliation { pub_select, pub_deselect } => self.send_affiliation(pub_select.clone(), pub_deselect.clone()),
            }
        }
        for sm in &plan.seams {
            match sm.fx {
                ac::SeamFx::floor_pub_select => {
                    let r = self.aff.r#pub.clone();
                    if self.rooms.contains_key(&r) {
                        self.floor_enter(&r, "pubSelect", |s| Some(fc::pub_select(s)), "affiliation");
                    }
                }
                ac::SeamFx::floor_pub_away => {
                    if self.rooms.contains_key(&prev) {
                        self.floor_enter(&prev, "pubAway", |s| Some(fc::pub_away(s)), "affiliation");
                    }
                }
                ac::SeamFx::pub_move_out => {
                    let ids: Vec<u64> = self.pubs.values().filter(|t| t.room == prev).map(|t| t.id).collect();
                    for id in ids {
                        self.pub_enter(id, "moveOut", |s| Some(pc::move_out(s, true)), "affiliation");
                    }
                }
                ac::SeamFx::pub_move_in => {
                    let ids: Vec<u64> = self.pubs.keys().copied().collect();
                    for id in ids {
                        self.pub_enter(id, "moveIn", |s| Some(pc::move_in(s)), "affiliation");
                    }
                }
            }
        }
        for e in &plan.events {
            match e {
                ab::AffEvent::SpeakingRoom => {
                    let room = (self.aff.r#pub != "NONE").then(|| self.aff.r#pub.clone());
                    for w in self.aff_waiters.drain(..) {
                        let _ = w.send(Ok(()));
                    }
                    self.emit(Event::SpeakingRoom { room });
                }
                ab::AffEvent::SpeakingRoomFailed => {
                    for w in self.aff_waiters.drain(..) {
                        let _ = w.send(Err(Error::sdk("AFFILIATION_FAILED", Category::State)));
                    }
                }
            }
        }
    }

    fn send_affiliation(&mut self, sel: Option<String>, desel: Option<String>) {
        let jid = |c: &Core, r: &Option<String>| r.as_ref().and_then(|r| c.rooms.get(r)).and_then(|x| c.jids.str(x.st.jid)).map(str::to_string);
        let body = AffiliationReq { pub_select_join_id: jid(self, &sel), pub_deselect_join_id: jid(self, &desel), pub_select: sel.clone(), pub_deselect: desel.clone(), change_id: None };
        let server = sel.as_ref().or(desel.as_ref()).and_then(|r| self.rooms.get(r)).and_then(|r| r.sfu.clone());
        if let Ok(v) = serde_json::to_value(&body) {
            self.request(Op::Affiliation, v, Purpose::Affiliation { server });
        }
    }

    pub(super) fn affiliation_reply(&mut self, server: Option<String>, o: Outcome) {
        let (ok, p) = match &o {
            Outcome::Ok(b) => (true, serde_json::from_value::<AffiliationRes>(b.clone()).ok().and_then(|x| x.pub_room)),
            Outcome::Fail(_) => (false, None),
            Outcome::Expired | Outcome::Transport | Outcome::Closed => (false, None),
        };
        let p = str_or_none(p.as_deref());
        let sfu_of = |c: &Core, r: &str| c.rooms.get(r).and_then(|x| x.sfu.clone());
        let on_pub = server.is_some() && sfu_of(self, &self.aff.r#pub) == server;
        let on_want = server.is_some() && sfu_of(self, &self.aff.want) == server;
        let p_in = self.rooms.contains_key(&p);
        self.aff_enter("res", |s| Some(ac::res(s, ok, &p, on_pub, on_want, p_in)), if ok { "AFFILIATION.res" } else { "AFFILIATION.fail" });
    }

    /// 이 방에서 말하려고 들어간다 — 발행 방이 없으면 select:true 입장(위 소속 입구 joinSelect) · 있으면 select:false 로 들어간 뒤 소속 전환(연:ROOM 입장 절차 2)
    pub(super) fn join_speaking(&mut self, room: String, tx: oneshot::Sender<Result<(), Error>>) {
        if self.rooms.get(&room).is_some_and(|r| !r.closed) {
            return self.set_speaking_room(room, tx);
        }
        let r = room.clone();
        // 입장 요청이 나가기 전에 표시가 서야 한다(ROOM_JOIN 은 방을 만드는 걸음에서 곧 나간다)
        if self.aff.r#pub == "NONE" && ac::join_select(&self.aff, &r).is_some() {
            self.aff_enter("joinSelect", |s| ac::join_select(s, &r), "app");
            self.join_mode = Some((room.clone(), true));
        } else {
            self.join_mode = Some((room.clone(), false));
        }
        self.join(room, tx);
    }

    /// 발언 방을 이 방으로(SDK:IMPL-AFF-SPEAKING-ROOM `setSpeakingRoom`)
    pub(super) fn set_speaking_room(&mut self, room: String, tx: oneshot::Sender<Result<(), Error>>) {
        if !self.rooms.contains_key(&room) {
            let _ = tx.send(Err(Error::sdk("NOT_JOINED", Category::State)));
            return;
        }
        if self.aff.r#pub == room {
            let _ = tx.send(Ok(()));
            return;
        }
        let same = self.rooms.get(&room).and_then(|r| r.sfu.clone()) == self.rooms.get(&self.aff.r#pub).and_then(|r| r.sfu.clone());
        self.aff_waiters.push(tx);
        self.aff_enter("select", |s| ac::select(s, &room, same), "app");
    }

    /// 입장 응답의 affiliation — 입장이 곁들여 바꾼 발행 방(위 입구 implicit)
    pub(super) fn aff_implicit(&mut self, room: &str, sel: bool, pub_room: Option<String>) {
        let p = str_or_none(pub_room.as_deref());
        let server = self.rooms.get(room).and_then(|r| r.sfu.clone());
        let on_pub = server.is_some() && self.rooms.get(&self.aff.r#pub).and_then(|r| r.sfu.clone()) == server;
        let p_in = self.rooms.contains_key(&p);
        let r = room.to_string();
        self.aff_enter("implicit", |s| Some(ac::implicit(s, &r, sel, &p, on_pub, p_in)), "ROOM_JOIN.res");
    }

    // ── 메시지(AUX) · 지금 값 ─────────────────────────────────────────

    pub(super) fn send_message(&mut self, room: String, content: String, tx: oneshot::Sender<Result<String, Error>>) {
        if !self.rooms.contains_key(&room) {
            let _ = tx.send(Err(Error::sdk("NOT_JOINED", Category::State)));
            return;
        }
        self.msg_seq += 1;
        let id = self.msg_seq;
        self.msg_waiters.insert(id, tx);
        if let Ok(v) = serde_json::to_value(MessageReq { room_id: room, content }) {
            self.request(Op::Message, v, Purpose::Message { id });
        }
    }

    pub(super) fn message_reply(&mut self, id: u64, o: Outcome) {
        let Some(tx) = self.msg_waiters.remove(&id) else { return };
        let r = match o {
            Outcome::Ok(b) => Ok(b.get("msg_id").and_then(Value::as_str).unwrap_or_default().to_string()),
            Outcome::Fail(b) => Err(Error::from_failure(&b)),
            _ => Err(Error::closed("CLOSED")),
        };
        let _ = tx.send(r);
    }

    pub(super) fn message_in(&mut self, body: Value) {
        if let Ok(m) = serde_json::from_value::<MessageBody>(body)
            && self.rooms.contains_key(&m.room_id)
        {
            self.emit(Event::Message { room: m.room_id, user_id: m.user_id, content: m.content });
        }
    }

    pub(super) fn snapshot(&self) -> Snapshot {
        let rooms = self
            .rooms
            .values()
            .map(|r| RoomView {
                room: r.id.clone(),
                joined: r.st.rs == crate::generated::room_client::RoomSt::JOINED,
                server: r.sfu.clone(),
                speaker: r.flr.shown.clone(),
                ptt: PttInfo { state: r.flr.cs.as_str().to_string(), trusted: r.flr.sub.as_str() != "WAIT" },
                permission: r.me.as_ref().and_then(|m| m.permission.clone()),
                published: self.pubs.values().filter(|t| t.room == r.id).filter_map(|t| t.track_id.clone()).collect(),
            })
            .collect();
        Snapshot {
            session: self.last_info.clone(),
            speaking_room: (self.aff.r#pub != "NONE").then(|| self.aff.r#pub.clone()),
            rooms,
            gated: self.pubs.values().map(|t| t.io.gated.load(Ordering::Relaxed)).sum(),
            sent: self.pubs.values().map(|t| t.io.sent.load(Ordering::Relaxed)).sum(),
        }
    }
}
