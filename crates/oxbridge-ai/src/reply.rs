// author: kodeholic (powered by Claude)
//! reply · 발언권(SDK:RULE-PLAT-AGENT-REPLY · SDK:RULE-PLAT-AGENT-SILENCE) — 순수 상태기(시각은 넣는 쪽이 준다 · 1층이 그대로 몬다).
//! `reply.start` 에서 누르고 · 허가 뒤 쌓인 음성을 20ms 마다 한 프레임씩 내고 · `reply.end` 뒤 통이 비면 놓는다.
//! ★무음 상한 `L` — 허가를 받은 때 · 마지막 프레임을 낸 때 중 늦은 것에서 `L` 이 지나도록 낼 음성이 없으면 놓고 `truncated(silence)`(위 `T1` 거울).
//! ★누름은 위 클라 상태가 `NOPERM` 일 때만 닿는다(그 밖은 위가 버린다) — 아니면 `NOPERM` 이 될 때까지 미룬다.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::seam::FRAME_MS;

const FRAME: Duration = Duration::from_millis(FRAME_MS as u64);
/// 송신 시계가 이만큼 밀리면 몰아 내지 않고 지금부터 다시 잰다
const CATCH_UP: Duration = Duration::from_millis(100);

/// 위 회수 사유(연:RULE-FLR-TLV — 번호는 oxpeer 생성본)
const REVOKE_TOO_LONG: u16 = oxpeer::floor::revoke::TOO_LONG as u16;
const REVOKE_PREEMPTED: u16 = oxpeer::floor::revoke::PREEMPTED as u16;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// 무음 상한 `L`
    pub silence: Duration,
    /// reply 통 상한(프레임)
    pub buffer_frames: usize,
}

impl Limits {
    /// 기본 — `L` = 위 `T1` 에서 여유 1초를 뺀 값 · 통 = 위 발화 상한(`T2` 기본 30초)만큼
    pub fn default_for(frame_ms: u32) -> Limits {
        Limits { silence: Duration::from_millis(oxpeer::floor::T1_MS.saturating_sub(MARGIN_MS)), buffer_frames: (30_000 / frame_ms) as usize }
    }

    /// 설정은 줄이는 쪽만 받는다(SDK:RULE-PLAT-AGENT-SILENCE)
    pub fn with_silence(mut self, ms: u64) -> Result<Limits, String> {
        let max = oxpeer::floor::T1_MS.saturating_sub(MARGIN_MS);
        if ms == 0 || ms > max {
            return Err(format!("무음 상한 {ms}ms — 1 ~ {max}ms(위 T1 에서 여유 {MARGIN_MS}ms 를 뺀 값)만 받는다"));
        }
        self.silence = Duration::from_millis(ms);
        Ok(self)
    }
}

/// `L` 의 여유 — 허가가 오는 한쪽 길 · 첫 RTP 가 가는 한쪽 길 · 송신 시계 한 칸
pub const MARGIN_MS: u64 = 1_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Floor {
    None,
    Asking,
    Queued,
    Held,
}

/// 위 발언권 사건(oxpeer `Event::Ptt`)에서 쓰는 것
#[derive(Clone, Debug, Default)]
pub struct PttView {
    pub cause: Option<u16>,
    pub remaining_sec: Option<u32>,
    pub queue_position: Option<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Act {
    Press,
    Release,
    /// 20ms PCM 한 프레임을 인코딩해 낸다
    Send(Vec<i16>),
    /// `reply.state` 칸(`type` 뺀)
    State(Value),
    Mark { name: String, played: bool },
    /// 운영 로그 `diag` — 무엇 · 근거 라벨 · 덧말
    Diag { what: &'static str, spec: &'static str, detail: String },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub started: u64,
    pub done: u64,
    pub cleared: u64,
    pub granted: u64,
    pub queued: u64,
    pub denied: u64,
    pub preempted: u64,
    pub truncated_silence: u64,
    pub truncated_buffer: u64,
    pub truncated_floor: u64,
    pub revoked: u64,
    pub lost: u64,
    pub ws_cut: u64,
    /// 보낸 프레임 · reply 밖에서 받아 버린 표본 · 진행 중 reply 가 있는데 온 `reply.start`
    pub frames_sent: u64,
    pub ignored_samples: u64,
    pub overlap: u64,
}

enum Item {
    Frame(Vec<i16>),
    Mark(String),
}

struct Reply {
    id: String,
    items: VecDeque<Item>,
    frames: usize,
    ended: bool,
    carry: Vec<i16>,
}

pub struct Replies {
    lim: Limits,
    frame_n: usize,
    cur: Option<Reply>,
    floor: Floor,
    /// 위 클라 상태(전이 통의 FLR `to`) — 누름은 `NOPERM` 에서만
    peer_state: String,
    want_press: bool,
    since: Option<Instant>,
    next_due: Option<Instant>,
    /// 허가 → 첫 프레임 송출(응답 지연 · 브리지 몫의 끝)
    pub first_send: Option<Instant>,
    pub counts: Counts,
}

impl Replies {
    pub fn new(lim: Limits, frame_n: usize) -> Replies {
        Replies { lim, frame_n, cur: None, floor: Floor::None, peer_state: "NOPERM".into(), want_press: false, since: None, next_due: None, first_send: None, counts: Counts::default() }
    }

    pub fn floor(&self) -> Floor {
        self.floor
    }

    pub fn active(&self) -> Option<&str> {
        self.cur.as_ref().map(|r| r.id.as_str())
    }

    /// 위 클라 상태가 바뀌었다(전이 통) — 미룬 누름을 낸다
    pub fn peer_state(&mut self, to: &str) -> Vec<Act> {
        self.peer_state = to.to_string();
        if self.want_press && to == "NOPERM" {
            self.want_press = false;
            return vec![Act::Press];
        }
        vec![]
    }

    fn press(&mut self) -> Vec<Act> {
        self.floor = Floor::Asking;
        if self.peer_state == "NOPERM" {
            vec![Act::Press]
        } else {
            self.want_press = true;
            vec![]
        }
    }

    pub fn reply_start(&mut self, id: &str) -> Vec<Act> {
        if let Some(r) = &self.cur {
            self.counts.overlap += 1;
            return vec![Act::Diag { what: "reply_overlap", spec: "SD-RULE-PLAT-AGENT-REPLY", detail: format!("active={} new={id}", r.id) }];
        }
        self.counts.started += 1;
        self.cur = Some(Reply { id: id.to_string(), items: VecDeque::new(), frames: 0, ended: false, carry: Vec::new() });
        self.first_send = None;
        match self.floor {
            Floor::Held => {
                self.since = Some(self.since.unwrap_or_else(Instant::now));
                vec![]
            }
            Floor::Asking | Floor::Queued => vec![],
            Floor::None => self.press(),
        }
    }

    /// AI 음성 — reply 밖이면 버리고 센다 · 20ms 로 자른다
    pub fn pcm(&mut self, s: &[i16]) -> Vec<Act> {
        let n = self.frame_n;
        let Some(r) = self.cur.as_mut().filter(|r| !r.ended) else {
            self.counts.ignored_samples += s.len() as u64;
            return vec![];
        };
        r.carry.extend_from_slice(s);
        while r.carry.len() >= n {
            let f: Vec<i16> = r.carry.drain(..n).collect();
            r.items.push_back(Item::Frame(f));
            r.frames += 1;
        }
        if r.frames > self.lim.buffer_frames {
            self.counts.truncated_buffer += 1;
            return self.finish(json!({ "state": "truncated", "limit": "buffer" }), true);
        }
        vec![]
    }

    /// 지금까지 보낸 음성이 송출을 마치면 되돌린다 · reply 가 없으면 곧 `cleared`
    pub fn mark(&mut self, name: &str) -> Vec<Act> {
        match self.cur.as_mut() {
            Some(r) => {
                r.items.push_back(Item::Mark(name.to_string()));
                vec![]
            }
            None => vec![Act::Mark { name: name.to_string(), played: false }],
        }
    }

    pub fn reply_end(&mut self, id: &str, now: Instant) -> Vec<Act> {
        let n = self.frame_n;
        let Some(r) = self.cur.as_mut().filter(|r| r.id == id) else {
            return vec![Act::Diag { what: "reply_end_unknown", spec: "SD-RULE-PLAT-AGENT-REPLY", detail: format!("reply={id}") }];
        };
        r.ended = true;
        if !r.carry.is_empty() {
            let mut f = std::mem::take(&mut r.carry);
            f.resize(n, 0);
            r.items.push_back(Item::Frame(f));
            r.frames += 1;
        }
        self.tick(now)
    }

    /// 쌓인 음성을 버리고 reply 를 끝낸다
    pub fn clear(&mut self) -> Vec<Act> {
        if self.cur.is_none() {
            return vec![];
        }
        self.counts.cleared += 1;
        self.finish(json!({ "state": "done", "cleared": true }), true)
    }

    /// 이음매가 끊겼다 — 쥔 발언권은 즉시 비우고 놓는다(알릴 길이 없다)
    pub fn ws_down(&mut self) -> Vec<Act> {
        if self.cur.is_none() && self.floor == Floor::None {
            return vec![];
        }
        self.counts.ws_cut += 1;
        let mut acts = self.finish(json!({ "state": "done", "cleared": true }), true);
        acts.retain(|a| matches!(a, Act::Release | Act::Diag { .. }));
        acts
    }

    /// 위 발언권 사건(oxpeer `Event::Ptt`)
    pub fn ptt(&mut self, event: &str, v: &PttView, now: Instant) -> Vec<Act> {
        match (event, self.floor) {
            ("granted", Floor::Asking | Floor::Queued) => {
                if self.cur.is_none() {
                    self.floor = Floor::None;
                    return vec![Act::Release];
                }
                self.floor = Floor::Held;
                self.since = Some(now);
                self.next_due = Some(now);
                self.counts.granted += 1;
                let mut acts = vec![self.state(json!({ "state": "floor_granted", "remaining_sec": v.remaining_sec }))];
                acts.extend(self.tick(now));
                acts
            }
            ("queued", Floor::Asking) => {
                self.floor = Floor::Queued;
                self.counts.queued += 1;
                vec![self.state(json!({ "state": "floor_queued", "position": v.queue_position }))]
            }
            ("queued", Floor::Queued) => vec![self.state(json!({ "state": "floor_queued", "position": v.queue_position }))],
            ("denied", Floor::Asking | Floor::Queued) => {
                self.floor = Floor::None;
                self.counts.denied += 1;
                self.finish(json!({ "state": "floor_denied", "cause": v.cause }), false)
            }
            ("revoked", Floor::Held | Floor::Asking | Floor::Queued) => {
                self.floor = Floor::None;
                let st = match v.cause {
                    Some(REVOKE_PREEMPTED) => {
                        self.counts.preempted += 1;
                        json!({ "state": "preempted", "cause": v.cause })
                    }
                    Some(REVOKE_TOO_LONG) => {
                        self.counts.truncated_floor += 1;
                        json!({ "state": "truncated", "limit": "floor_max", "cause": v.cause })
                    }
                    _ => {
                        self.counts.revoked += 1;
                        json!({ "state": "revoked", "cause": v.cause })
                    }
                };
                self.finish(st, false)
            }
            // 요청 재전송을 다 쓰고 위가 내렸다(허가 · 거절 · 대기 응답이 없었다)
            ("state", Floor::Asking | Floor::Queued) if self.peer_state == "NOPERM" && !self.want_press => {
                self.floor = Floor::None;
                self.counts.denied += 1;
                self.finish(json!({ "state": "floor_denied", "cause": Value::Null }), false)
            }
            _ => vec![],
        }
    }

    /// 방에 남의 허가가 났다(`ctl/floor` `taken`) — 내가 쥐었다고 믿는 동안이면 앞선 회수 없이 잃은 것이다
    pub fn foreign_taken(&mut self, user: &str) -> Vec<Act> {
        if self.floor != Floor::Held {
            return vec![];
        }
        self.floor = Floor::None;
        self.counts.lost += 1;
        let mut acts = vec![Act::Diag { what: "floor_lost_silent", spec: "SD-RULE-PLAT-AGENT-SILENCE", detail: format!("taken_by={user}") }];
        acts.extend(self.finish(json!({ "state": "lost", "taken_by": user }), false));
        acts
    }

    /// 시계 — 허가 동안 20ms 마다 한 프레임 · 끝 · 무음 상한
    pub fn tick(&mut self, now: Instant) -> Vec<Act> {
        let mut acts = Vec::new();
        if self.floor != Floor::Held {
            return acts;
        }
        let Some(r) = self.cur.as_mut() else {
            self.floor = Floor::None;
            return vec![Act::Release];
        };
        loop {
            while let Some(Item::Mark(_)) = r.items.front() {
                if let Some(Item::Mark(name)) = r.items.pop_front() {
                    acts.push(Act::Mark { name, played: true });
                }
            }
            let due = self.next_due.unwrap_or(now);
            if !matches!(r.items.front(), Some(Item::Frame(_))) || now < due {
                break;
            }
            if let Some(Item::Frame(f)) = r.items.pop_front() {
                acts.push(Act::Send(f));
                self.counts.frames_sent += 1;
                self.first_send.get_or_insert(now);
                self.since = Some(now);
                let next = due + FRAME;
                self.next_due = Some(if now.saturating_duration_since(next) > CATCH_UP { now + FRAME } else { next });
            }
        }
        if r.items.is_empty() && r.ended {
            self.counts.done += 1;
            acts.extend(self.finish(json!({ "state": "done" }), true));
            return acts;
        }
        let quiet = self.since.map(|s| now.saturating_duration_since(s)).unwrap_or_default();
        if !matches!(r.items.front(), Some(Item::Frame(_))) && quiet >= self.lim.silence {
            self.counts.truncated_silence += 1;
            acts.push(Act::Diag { what: "reply_truncated_silence", spec: "SD-RULE-PLAT-AGENT-SILENCE", detail: format!("quiet_ms={}", quiet.as_millis()) });
            acts.extend(self.finish(json!({ "state": "truncated", "limit": "silence" }), true));
        }
        acts
    }

    fn state(&self, mut v: Value) -> Act {
        if let (Some(o), Some(r)) = (v.as_object_mut(), &self.cur) {
            o.insert("reply".into(), json!(r.id));
        }
        Act::State(v)
    }

    /// reply 를 끝낸다 — 상태 알림 · 남은 mark 는 `cleared` · `release` 면 쥔 · 청한 발언권을 놓는다(위가 이미 내린 거절 · 회수는 놓지 않는다)
    fn finish(&mut self, st: Value, release: bool) -> Vec<Act> {
        let mut acts = vec![self.state(st)];
        if let Some(r) = self.cur.take() {
            for it in r.items {
                if let Item::Mark(name) = it {
                    acts.push(Act::Mark { name, played: false });
                }
            }
        }
        if release && self.floor != Floor::None {
            acts.push(Act::Release);
        }
        if release && self.want_press {
            self.want_press = false;
        }
        self.floor = Floor::None;
        self.since = None;
        self.next_due = None;
        acts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const N: usize = 320;

    fn lim() -> Limits {
        Limits::default_for(FRAME_MS)
    }

    fn frames(k: usize) -> Vec<i16> {
        vec![7; N * k]
    }

    fn states(a: &[Act]) -> Vec<String> {
        a.iter().filter_map(|x| if let Act::State(v) = x { v["state"].as_str().map(str::to_string) } else { None }).collect()
    }

    fn sends(a: &[Act]) -> usize {
        a.iter().filter(|x| matches!(x, Act::Send(_))).count()
    }

    fn granted(r: &mut Replies, t: Instant) -> Vec<Act> {
        r.ptt("granted", &PttView { remaining_sec: Some(30), ..Default::default() }, t)
    }

    #[test]
    fn happy_path_press_send_paced_then_release_done() {
        let t0 = Instant::now();
        let mut r = Replies::new(lim(), N);
        assert_eq!(r.reply_start("r1"), vec![Act::Press]);
        assert!(r.pcm(&frames(3)).is_empty());
        r.mark("m1");
        let a = granted(&mut r, t0);
        assert_eq!(states(&a), vec!["floor_granted"]);
        assert_eq!(sends(&a), 1, "허가 순간 한 프레임");
        assert_eq!(sends(&r.tick(t0 + Duration::from_millis(5))), 0, "20ms 전엔 다음 프레임 없음");
        assert_eq!(sends(&r.tick(t0 + Duration::from_millis(20))), 1);
        let a = r.reply_end("r1", t0 + Duration::from_millis(40));
        assert_eq!(sends(&a), 1);
        assert!(a.contains(&Act::Mark { name: "m1".into(), played: true }), "mark 는 앞 음성을 다 낸 뒤 played");
        assert!(a.contains(&Act::Release));
        assert_eq!(states(&a), vec!["done"]);
        assert_eq!((r.counts.frames_sent, r.counts.done, r.floor()), (3, 1, Floor::None));
    }

    #[test]
    fn first_audio_wait_counts_from_grant_and_truncates_at_silence() {
        let t0 = Instant::now();
        let mut r = Replies::new(lim(), N);
        r.reply_start("r1");
        r.mark("m0");
        let a = granted(&mut r, t0);
        assert!(a.contains(&Act::Mark { name: "m0".into(), played: true }), "앞 음성이 없는 mark 는 닿자마자 played(Twilio 의미)");
        assert!(r.tick(t0 + lim().silence - Duration::from_millis(1)).iter().all(|a| !matches!(a, Act::Release)));
        let a = r.tick(t0 + lim().silence);
        assert_eq!(states(&a), vec!["truncated"]);
        assert!(a.contains(&Act::Release));
        assert_eq!(r.counts.truncated_silence, 1);
        assert_eq!(r.counts.frames_sent, 0);
    }

    #[test]
    fn silence_inside_reply_counts_from_last_frame() {
        let t0 = Instant::now();
        let mut r = Replies::new(lim(), N);
        r.reply_start("r1");
        r.pcm(&frames(1));
        granted(&mut r, t0);
        let t1 = t0 + Duration::from_millis(2_500);
        r.pcm(&frames(1));
        assert_eq!(sends(&r.tick(t1)), 1);
        assert!(states(&r.tick(t1 + lim().silence - Duration::from_millis(1))).is_empty(), "마지막 프레임부터 잰다");
        assert_eq!(states(&r.tick(t1 + lim().silence)), vec!["truncated"]);
    }

    #[test]
    fn silence_limit_is_below_t1_and_only_lowered() {
        assert!(lim().silence.as_millis() as u64 + MARGIN_MS <= oxpeer::floor::T1_MS);
        assert!(lim().with_silence(oxpeer::floor::T1_MS).is_err(), "T1 을 넘기는 설정 거부");
        assert!(lim().with_silence(1_500).is_ok());
    }

    #[test]
    fn revoke_causes_map_to_reply_states() {
        let t0 = Instant::now();
        for (cause, want) in [(oxpeer::floor::revoke::PREEMPTED, "preempted"), (oxpeer::floor::revoke::TOO_LONG, "truncated"), (oxpeer::floor::revoke::NO_PERMISSION, "revoked")] {
            let mut r = Replies::new(lim(), N);
            r.reply_start("r1");
            r.pcm(&frames(5));
            r.mark("m1");
            granted(&mut r, t0);
            let a = r.ptt("revoked", &PttView { cause: Some(u16::from(cause)), ..Default::default() }, t0);
            assert_eq!(states(&a), vec![want]);
            assert!(!a.contains(&Act::Release), "회수는 위가 이미 놓았다");
            assert!(a.contains(&Act::Mark { name: "m1".into(), played: false }));
            assert_eq!(sends(&r.tick(t0 + Duration::from_millis(100))), 0, "낡은 답을 늦게 내지 않는다");
        }
    }

    #[test]
    fn denied_and_queued_then_granted() {
        let t0 = Instant::now();
        let mut r = Replies::new(lim(), N);
        r.reply_start("r1");
        let a = r.ptt("denied", &PttView { cause: Some(u16::from(oxpeer::floor::deny::RETRY_AFTER)), ..Default::default() }, t0);
        assert_eq!(states(&a), vec!["floor_denied"]);
        assert_eq!(r.active(), None);
        r.reply_start("r2");
        assert_eq!(states(&r.ptt("queued", &PttView { queue_position: Some(1), ..Default::default() }, t0)), vec!["floor_queued"]);
        r.pcm(&frames(1));
        assert!(r.tick(t0 + Duration::from_secs(10)).is_empty(), "기다리는 동안 무음 상한은 돌지 않는다(위 T1 은 허가 뒤)");
        assert_eq!(sends(&granted(&mut r, t0 + Duration::from_secs(10))), 1);
    }

    #[test]
    fn foreign_taken_while_held_is_lost() {
        let t0 = Instant::now();
        let mut r = Replies::new(lim(), N);
        r.reply_start("r1");
        granted(&mut r, t0);
        let a = r.foreign_taken("u-human");
        assert_eq!(states(&a), vec!["lost"]);
        assert!(a.iter().any(|x| matches!(x, Act::Diag { what: "floor_lost_silent", .. })));
        assert_eq!(r.counts.lost, 1);
        assert!(r.foreign_taken("u-human").is_empty(), "쥐지 않았으면 남의 허가는 정상");
    }

    #[test]
    fn press_waits_for_noperm() {
        let t0 = Instant::now();
        let mut r = Replies::new(lim(), N);
        r.peer_state("PREL");
        assert!(r.reply_start("r1").is_empty(), "놓는 중엔 누르지 않는다(위가 버린다)");
        assert_eq!(r.peer_state("NOPERM"), vec![Act::Press]);
        assert_eq!(states(&granted(&mut r, t0)), vec!["floor_granted"]);
    }

    #[test]
    fn clear_drops_and_releases() {
        let t0 = Instant::now();
        let mut r = Replies::new(lim(), N);
        r.reply_start("r1");
        r.pcm(&frames(4));
        granted(&mut r, t0);
        r.mark("m");
        let a = r.clear();
        assert_eq!(states(&a), vec!["done"]);
        assert!(a.contains(&Act::Release) && a.contains(&Act::Mark { name: "m".into(), played: false }));
        assert!(r.pcm(&frames(1)).is_empty());
        assert_eq!(r.counts.ignored_samples, N as u64, "reply 밖 음성은 버리고 센다");
    }

    #[test]
    fn buffer_over_limit_truncates() {
        let mut r = Replies::new(Limits { buffer_frames: 3, ..lim() }, N);
        r.reply_start("r1");
        assert!(r.pcm(&frames(3)).is_empty());
        let a = r.pcm(&frames(1));
        assert_eq!(states(&a), vec!["truncated"]);
        assert!(a.contains(&Act::Release), "청한 발언권도 놓는다");
    }

    #[test]
    fn ws_down_releases_without_events() {
        let t0 = Instant::now();
        let mut r = Replies::new(lim(), N);
        r.reply_start("r1");
        r.pcm(&frames(2));
        granted(&mut r, t0);
        assert_eq!(r.ws_down(), vec![Act::Release]);
        assert_eq!(r.floor(), Floor::None);
    }

    #[test]
    fn overlap_is_counted_not_started() {
        let mut r = Replies::new(lim(), N);
        r.reply_start("r1");
        assert!(matches!(r.reply_start("r2").as_slice(), [Act::Diag { what: "reply_overlap", .. }]));
        assert_eq!((r.counts.started, r.counts.overlap, r.active()), (1, 1, Some("r1")));
    }

    #[test]
    fn grant_after_reply_gone_releases() {
        let t0 = Instant::now();
        let mut r = Replies::new(lim(), N);
        r.reply_start("r1");
        r.clear();
        r.floor = Floor::Asking;
        assert_eq!(granted(&mut r, t0), vec![Act::Release]);
    }
}
