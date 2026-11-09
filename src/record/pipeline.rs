// author: kodeholic (powered by Claude)
//! 녹음 파이프라인(SDK:RULE-PLAT-REC-PIPE · REC-SPLIT) — 레코드 하나를 받아 출력 열을 돌려주는 ★순수 상태기.
//! 분기(줄 → 트랙 · CSRC → 출처) → 조립(차례가 선 패킷 · `gap` 은 깨진 프레임 · 영상은 키프레임부터) → 시간축(SR → UTC) → 분할 정책.
//! ★시스템 시계를 읽지 않는다 — 시간은 레코드의 `log_time` 과 RTP · SR 뿐이다. 그래서 라이브 레코드열과 파일 레코드열이 같으면 출력도 같다.

use std::collections::{BTreeMap, HashMap};

use serde_json::Value;

use super::{Channel, Record};
use crate::link::depack::{self, Codec};
use crate::link::transport::rtp_header;

/// NTP(1900) → 유닉스(1970)
const NTP_UNIX: u64 = 2_208_988_800;

/// 분할 정책(SDK:RULE-PLAT-REC-SPLIT)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    /// 줄마다 출처가 바뀌면 새 세그먼트 · 출처 없는 프레임은 세그먼트 밖(`Output::Orphan`)
    Utterance,
    /// 트랙마다 시간(ns) · 크기(bytes)가 지난 뒤 영상은 첫 키프레임 · 음성은 아무 프레임에서 새 세그먼트
    Span { time_ns: u64, bytes: u64 },
}

/// 프레임(지점 ②)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub segment: Option<u64>,
    pub room: String,
    pub track_id: String,
    /// 트랙 주인(개인 트랙) — 무전 슬롯은 없다
    pub owner: Option<String>,
    pub kind: String,
    pub codec: String,
    /// 줄(다운링크) SSRC
    pub ssrc: u32,
    /// 출처 번호(CSRC) · 그 주인 — 없으면 서버가 지은 프레임
    pub source: Option<u32>,
    pub user_id: Option<String>,
    pub keyframe: bool,
    pub rtp_ts: u32,
    /// SR 로 이은 UTC(ns) — 첫 SR 전이면 없다
    pub utc_ns: Option<u64>,
    /// 첫 패킷 레코드의 시각
    pub log_time: u64,
    pub seq_first: u16,
    pub seq_last: u16,
    pub rec_first: u32,
    pub rec_last: u32,
    pub data: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentKind {
    Opened,
    Closed,
}

/// 세그먼트 사건(지점 ③)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    pub event: SegmentKind,
    pub id: u64,
    pub room: String,
    pub track_id: String,
    pub owner: Option<String>,
    pub kind: String,
    pub codec: String,
    pub source: Option<u32>,
    pub user_id: Option<String>,
    pub first_utc_ns: Option<u64>,
    pub last_utc_ns: Option<u64>,
    pub first_log_time: u64,
    pub last_log_time: u64,
    pub frames: u64,
    pub bytes: u64,
    /// 닫힌 까닭 — `source` · `no_source` · `span` · `track` · `end`
    pub reason: Option<&'static str>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output {
    Frame(Frame),
    Segment(Segment),
    /// 발언 정책이 담지 않은 출처 없는 프레임(서버가 지은 침묵)
    Orphan(Frame),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Row {
    room: String,
    track_id: String,
    kind: String,
    codec: String,
}

struct Asm {
    ts: u32,
    data: Vec<u8>,
    key: bool,
    broken: bool,
    source: Option<u32>,
    log_time: u64,
    seq_first: u16,
    seq_last: u16,
    rec_first: u32,
    rec_last: u32,
}

#[derive(Default)]
struct Line {
    asm: Option<Asm>,
    need_key: bool,
}

/// 파이프라인 계수 — 결과물 요약 · 시험이 읽는다
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub records: u64,
    pub packets: u64,
    /// 줄을 모르는 패킷(`ctl/track` 전)
    pub unrouted: u64,
    pub frames: u64,
    pub broken: u64,
    /// 키프레임 전이라 버린 영상 프레임
    pub before_key: u64,
}

pub struct Pipeline {
    policy: Policy,
    rows: HashMap<u32, Row>,
    owners: HashMap<String, Option<String>>,
    sources: HashMap<u32, String>,
    lines: HashMap<u32, Line>,
    sr: HashMap<u32, (u64, u32)>,
    open: BTreeMap<String, Segment>,
    next_id: u64,
    counts: Counts,
}

impl Pipeline {
    pub fn new(policy: Policy) -> Pipeline {
        Pipeline { policy, rows: HashMap::new(), owners: HashMap::new(), sources: HashMap::new(), lines: HashMap::new(), sr: HashMap::new(), open: BTreeMap::new(), next_id: 0, counts: Counts::default() }
    }

    pub fn counts(&self) -> &Counts {
        &self.counts
    }

    /// 레코드 하나 → 출력 열
    pub fn push(&mut self, r: &Record) -> Vec<Output> {
        self.counts.records += 1;
        let mut out = Vec::new();
        match &r.channel {
            Channel::Rtp(ssrc) => self.rtp(*ssrc, r, &mut out),
            Channel::Track => self.track(&r.data, &mut out),
            Channel::Floor => self.floor(&r.data),
            Channel::Sr => {
                if let Ok(v) = serde_json::from_slice::<Value>(&r.data)
                    && let (Some(s), Some(n), Some(t)) = (v["ssrc"].as_u64(), v["ntp"].as_u64(), v["rtp"].as_u64())
                {
                    self.sr.insert(s as u32, (n, t as u32));
                }
            }
            Channel::Gap => {
                if let Ok(v) = serde_json::from_slice::<Value>(&r.data)
                    && let Some(s) = v["ssrc"].as_u64()
                {
                    let video = self.rows.get(&(s as u32)).is_some_and(|x| x.kind == "video");
                    let line = self.lines.entry(s as u32).or_insert_with(|| Line { asm: None, need_key: video });
                    if let Some(a) = line.asm.as_mut() {
                        a.broken = true;
                    }
                    if video {
                        line.need_key = true;
                    }
                }
            }
            Channel::Session | Channel::Tick => {}
        }
        out
    }

    /// 끝 — 열린 세그먼트를 닫는다(번호 차례) · 조립 중인 프레임은 끝이 없어 버린다
    pub fn finish(&mut self) -> Vec<Output> {
        let mut segs: Vec<Segment> = std::mem::take(&mut self.open).into_values().collect();
        segs.sort_by_key(|s| s.id);
        segs.into_iter()
            .map(|mut s| {
                s.event = SegmentKind::Closed;
                s.reason = Some("end");
                Output::Segment(s)
            })
            .collect()
    }

    fn track(&mut self, data: &[u8], out: &mut Vec<Output>) {
        let Ok(v) = serde_json::from_slice::<Value>(data) else { return };
        let Some(room) = v["room"].as_str() else { return };
        for t in v["tracks"].as_array().into_iter().flatten() {
            let Some(tid) = t["track_id"].as_str() else { continue };
            self.owners.insert(tid.to_string(), t["user_id"].as_str().map(str::to_string));
            if let (Some(c), Some(u)) = (t["csrc"].as_u64(), t["user_id"].as_str()) {
                self.sources.insert(c as u32, u.to_string());
            }
        }
        let mut now: HashMap<u32, Row> = HashMap::new();
        for (k, x) in v["rows"].as_object().into_iter().flatten() {
            let Ok(ssrc) = k.parse::<u32>() else { continue };
            let row = match x {
                Value::String(tid) => Row { room: room.to_string(), track_id: tid.clone(), kind: String::new(), codec: String::new() },
                o => Row {
                    room: room.to_string(),
                    track_id: o["track_id"].as_str().unwrap_or_default().to_string(),
                    kind: o["kind"].as_str().unwrap_or_default().to_string(),
                    codec: o["codec"].as_str().unwrap_or_default().to_string(),
                },
            };
            now.insert(ssrc, row);
        }
        // 그 방의 줄 — 빠진 줄 · 트랙이 바뀐 줄은 조립을 버리고 세그먼트를 닫는다
        let mut gone: Vec<u32> = self.rows.iter().filter(|(k, r)| r.room == room && now.get(k) != Some(r)).map(|(k, _)| *k).collect();
        gone.sort_unstable();
        for k in gone {
            if let Some(old) = self.rows.remove(&k) {
                self.lines.remove(&k);
                let key = self.seg_key(k, &old.track_id);
                self.close(&key, "track", out);
            }
        }
        for (k, r) in now {
            self.rows.entry(k).or_insert(r);
        }
    }

    fn floor(&mut self, data: &[u8]) {
        let Ok(v) = serde_json::from_slice::<Value>(data) else { return };
        if v["event"].as_str() != Some("taken") {
            return;
        }
        let Some(u) = v["user_id"].as_str() else { return };
        for c in v["sources"].as_array().into_iter().flatten().filter_map(Value::as_u64) {
            self.sources.insert(c as u32, u.to_string());
        }
    }

    fn rtp(&mut self, ssrc: u32, r: &Record, out: &mut Vec<Output>) {
        self.counts.packets += 1;
        let Some(row) = self.rows.get(&ssrc).cloned() else {
            self.counts.unrouted += 1;
            return;
        };
        let Some(h) = rtp_header(&r.data) else { return };
        let codec = Codec::of(&row.codec);
        let video = row.kind == "video";
        let line = self.lines.entry(ssrc).or_insert_with(|| Line { asm: None, need_key: video });
        let mut done = Vec::new();
        if line.asm.as_ref().is_some_and(|a| a.ts != h.ts)
            && let Some(a) = line.asm.take()
        {
            done.push(a);
        }
        let payload = &r.data[h.start..h.end];
        if !payload.is_empty() {
            let piece = depack::unpack(codec, payload);
            let a = line.asm.get_or_insert_with(|| Asm {
                ts: h.ts,
                data: Vec::new(),
                key: false,
                broken: piece.as_ref().is_some_and(|x| !x.start),
                source: h.csrc,
                log_time: r.log_time,
                seq_first: h.seq,
                seq_last: h.seq,
                rec_first: r.sequence,
                rec_last: r.sequence,
            });
            if a.source != h.csrc {
                a.broken = true;
            }
            a.seq_last = h.seq;
            a.rec_last = r.sequence;
            match piece {
                Some(x) => {
                    a.data.extend_from_slice(&x.data);
                    a.key |= x.key;
                }
                None => a.broken = true,
            }
            if (h.marker || !video)
                && let Some(a) = line.asm.take()
            {
                done.push(a);
            }
        }
        for a in done {
            self.finish_frame(ssrc, &row, a, out);
        }
    }

    fn finish_frame(&mut self, ssrc: u32, row: &Row, a: Asm, out: &mut Vec<Output>) {
        let video = row.kind == "video";
        let Some(line) = self.lines.get_mut(&ssrc) else { return };
        if a.broken {
            self.counts.broken += 1;
            if video {
                line.need_key = true;
            }
            return;
        }
        if video {
            if a.key {
                line.need_key = false;
            } else if line.need_key {
                self.counts.before_key += 1;
                return;
            }
        }
        self.counts.frames += 1;
        let clock = u64::from(Codec::of(&row.codec).clock(video));
        let utc_ns = self.sr.get(&ssrc).map(|(ntp, rtp)| {
            let base = ((ntp >> 32).saturating_sub(NTP_UNIX)) * 1_000_000_000 + (((ntp & 0xffff_ffff) * 1_000_000_000) >> 32);
            let d = i64::from(a.ts.wrapping_sub(*rtp) as i32) * 1_000_000_000 / clock as i64;
            (base as i64 + d).max(0) as u64
        });
        let frame = Frame {
            segment: None,
            room: row.room.clone(),
            track_id: row.track_id.clone(),
            owner: self.owners.get(&row.track_id).cloned().flatten(),
            kind: row.kind.clone(),
            codec: row.codec.clone(),
            ssrc,
            source: a.source,
            user_id: a.source.and_then(|c| self.sources.get(&c).cloned()),
            keyframe: a.key,
            rtp_ts: a.ts,
            utc_ns,
            log_time: a.log_time,
            seq_first: a.seq_first,
            seq_last: a.seq_last,
            rec_first: a.rec_first,
            rec_last: a.rec_last,
            data: a.data,
        };
        self.split(frame, out);
    }

    fn seg_key(&self, ssrc: u32, track_id: &str) -> String {
        match self.policy {
            Policy::Utterance => format!("row/{ssrc}"),
            Policy::Span { .. } => format!("track/{track_id}"),
        }
    }

    fn split(&mut self, mut f: Frame, out: &mut Vec<Output>) {
        let key = self.seg_key(f.ssrc, &f.track_id);
        match self.policy {
            Policy::Utterance => {
                let Some(src) = f.source else {
                    self.close(&key, "no_source", out);
                    out.push(Output::Orphan(f));
                    return;
                };
                if self.open.get(&key).is_some_and(|s| s.source != Some(src)) {
                    self.close(&key, "source", out);
                }
            }
            Policy::Span { time_ns, bytes } => {
                let cut = self.open.get(&key).is_some_and(|s| f.log_time.saturating_sub(s.first_log_time) >= time_ns || s.bytes >= bytes);
                if cut && (f.kind != "video" || f.keyframe) {
                    self.close(&key, "span", out);
                }
            }
        }
        if !self.open.contains_key(&key) {
            let s = Segment {
                event: SegmentKind::Opened,
                id: self.next_id,
                room: f.room.clone(),
                track_id: f.track_id.clone(),
                owner: f.owner.clone(),
                kind: f.kind.clone(),
                codec: f.codec.clone(),
                source: f.source,
                user_id: f.user_id.clone().or_else(|| f.owner.clone()),
                first_utc_ns: f.utc_ns,
                last_utc_ns: f.utc_ns,
                first_log_time: f.log_time,
                last_log_time: f.log_time,
                frames: 0,
                bytes: 0,
                reason: None,
            };
            self.next_id += 1;
            out.push(Output::Segment(s.clone()));
            self.open.insert(key.clone(), s);
        }
        if let Some(s) = self.open.get_mut(&key) {
            s.frames += 1;
            s.bytes += f.data.len() as u64;
            s.last_log_time = f.log_time;
            if f.utc_ns.is_some() {
                s.last_utc_ns = f.utc_ns;
                s.first_utc_ns = s.first_utc_ns.or(f.utc_ns);
            }
            if s.user_id.is_none() {
                s.user_id = f.user_id.clone();
            }
            f.segment = Some(s.id);
        }
        out.push(Output::Frame(f));
    }

    fn close(&mut self, key: &str, why: &'static str, out: &mut Vec<Output>) {
        if let Some(mut s) = self.open.remove(key) {
            s.event = SegmentKind::Closed;
            s.reason = Some(why);
            out.push(Output::Segment(s));
        }
    }
}
