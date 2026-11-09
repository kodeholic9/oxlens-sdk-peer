// author: kodeholic (powered by Claude)
//! 녹음 원시 기록(SDK:RULE-PLAT-REC-TAP · REC-RAW · REC-CLOCK · REC-PART · context 202611/20261109c) — 정리층 출구의 RTP 원본과 그 시점의 제어 사건을
//! 받은 차례대로 MCAP 파트에 적는다. 이 판(단계 1)은 기록기와 다시 읽기(`FileSource`)까지다 — 조립 · 분할 · Sink 는 다음 단계가 이 레코드 위에 짓는다.
//!
//! 채취는 직접 소유다 — 정리층이 `Out::Raw` · `Out::Gap` 을 내면 `deliver` 가 `MediaSink` 가 쥔 기록기를 부르고, core 는 사건을 낼 때 같은 기록기를 부른다.
//! `sequence` · `log_time` 은 기록기 잠금 안 한 곳에서 매긴다(REC-CLOCK) — 그래서 파일 차례로 둘 다 오른다.

mod file;

pub use file::{FileSource, PartScan, scan_part};

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mcap::records::{MessageHeader, Metadata};
use mcap::{WriteOptions, Writer};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::api::Event;

/// 원시 포맷 판(SDK:RULE-PLAT-REC-RAW `oxlens.recording` `format`)
pub const FORMAT: u32 = 1;

/// 청크 압축(실측 M9 — 기본은 없음)
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Compression {
    #[default]
    None,
    #[cfg(feature = "record-lz4")]
    Lz4,
    #[cfg(feature = "record-zstd")]
    Zstd,
}

/// 녹음 설정(SDK:RULE-PLAT-REC-PART 손잡이)
#[derive(Clone, Debug)]
pub struct RecordConfig {
    /// 파트를 둘 디렉터리
    pub dir: PathBuf,
    /// 녹음 세션 ID — 파트 이름 `<recording_id>-<파트 4자리>.mcap`
    pub recording_id: String,
    /// 파트 크기 S · 시간 T — 먼저 닿는 쪽에서 회전
    pub part_bytes: u64,
    pub part_time: Duration,
    /// 청크를 닫는 주기 F = 비정상 종료 손실 상한 — `tick` 경계에서 닫는다
    pub flush: Duration,
    /// `tick` 주기
    pub tick: Duration,
    /// 이 밑이면 기록을 멈추고 사건
    pub min_free_bytes: u64,
    /// 청크를 닫을 때마다 fsync(파트를 닫을 때는 늘 한다)
    pub fsync_on_flush: bool,
    pub compression: Compression,
}

impl RecordConfig {
    pub fn new(dir: impl Into<PathBuf>, recording_id: impl Into<String>) -> RecordConfig {
        RecordConfig {
            dir: dir.into(),
            recording_id: recording_id.into(),
            part_bytes: 1 << 30,
            part_time: Duration::from_secs(3_600),
            flush: Duration::from_secs(2),
            tick: Duration::from_millis(500),
            min_free_bytes: 512 << 20,
            fsync_on_flush: false,
            compression: Compression::None,
        }
    }
}

/// 채널(SDK:RULE-PLAT-REC-RAW)
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Channel {
    /// `rtp/<줄 SSRC 10진>`
    Rtp(u32),
    Track,
    Floor,
    Session,
    Sr,
    Gap,
    Tick,
}

impl Channel {
    pub fn topic(&self) -> String {
        match self {
            Channel::Rtp(s) => format!("rtp/{s}"),
            Channel::Track => "ctl/track".into(),
            Channel::Floor => "ctl/floor".into(),
            Channel::Session => "ctl/session".into(),
            Channel::Sr => "clock/sr".into(),
            Channel::Gap => "gap".into(),
            Channel::Tick => "tick".into(),
        }
    }

    pub fn parse(topic: &str) -> Option<Channel> {
        Some(match topic {
            "ctl/track" => Channel::Track,
            "ctl/floor" => Channel::Floor,
            "ctl/session" => Channel::Session,
            "clock/sr" => Channel::Sr,
            "gap" => Channel::Gap,
            "tick" => Channel::Tick,
            t => Channel::Rtp(t.strip_prefix("rtp/")?.parse().ok()?),
        })
    }

    fn encoding(&self) -> &'static str {
        match self {
            Channel::Rtp(_) => "oxlens.rtp",
            _ => "json",
        }
    }
}

/// 레코드 하나(공개 지점 ① — 소유한 바이트 · 평범한 값)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub sequence: u32,
    /// UTC ns(REC-CLOCK — 세션 닻 + 단조 경과)
    pub log_time: u64,
    pub channel: Channel,
    pub data: Vec<u8>,
}

/// 기록기 — `MediaSink` · core 가 같이 쥔다
pub(crate) struct Recorder {
    inner: Mutex<Inner>,
}

struct Inner {
    cfg: RecordConfig,
    anchor_ns: u64,
    mono0: Instant,
    seq: u32,
    part_no: u32,
    prev_last: Option<u32>,
    part: Option<Part>,
    last_flush: u64,
    stopped: bool,
    snap: Snap,
    ev: mpsc::UnboundedSender<Event>,
}

struct Part {
    w: Writer<BufWriter<File>>,
    /// fsync 몫 — 쓰기 손잡이와 같은 파일
    file: File,
    channels: HashMap<Channel, u16>,
    start: u64,
    bytes: u64,
    last_seq: Option<u32>,
}

/// 파트 머리 스냅숏 재료(REC-PART) — 방마다 마지막 트랙 목록 · 발언권 · 마지막 세션 · 연결 사건 · 줄마다 마지막 SR
#[derive(Default)]
struct Snap {
    track: BTreeMap<String, Vec<u8>>,
    floor: BTreeMap<String, Vec<u8>>,
    session: BTreeMap<String, Vec<u8>>,
    sr: BTreeMap<u32, Vec<u8>>,
}

impl Recorder {
    /// 만들기 — 첫 파트는 첫 레코드에서 연다 · `tick` 태스크를 띄운다(기록기가 사라지면 끝난다)
    pub(crate) fn start(cfg: RecordConfig, ev: mpsc::UnboundedSender<Event>) -> std::io::Result<Arc<Recorder>> {
        fs::create_dir_all(&cfg.dir)?;
        let anchor_ns = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
        let tick = cfg.tick;
        let r = Arc::new(Recorder { inner: Mutex::new(Inner { cfg, anchor_ns, mono0: Instant::now(), seq: 0, part_no: 0, prev_last: None, part: None, last_flush: anchor_ns, stopped: false, snap: Snap::default(), ev }) });
        let weak: Weak<Recorder> = Arc::downgrade(&r);
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(tick);
            iv.tick().await;
            loop {
                iv.tick().await;
                let Some(r) = weak.upgrade() else { break };
                r.push(Channel::Tick, b"{}".to_vec());
            }
        });
        Ok(r)
    }

    /// 레코드 하나를 적는다 — 순번 · 시각을 여기서 매긴다(REC-CLOCK)
    pub(crate) fn push(&self, ch: Channel, data: Vec<u8>) {
        let Ok(mut g) = self.inner.lock() else { return };
        g.push(ch, data);
    }

    /// 트랙 목록 — 방마다 앞과 같으면 적지 않는다(줄 재배정 · 목록 바뀜만 남긴다)
    pub(crate) fn track(&self, room: &str, body: Value) {
        let Ok(mut g) = self.inner.lock() else { return };
        let data = body.to_string().into_bytes();
        if g.snap.track.get(room) == Some(&data) {
            return;
        }
        g.push(Channel::Track, data);
    }

    pub(crate) fn close(&self) {
        if let Ok(mut g) = self.inner.lock() {
            g.close_part("close");
        }
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        self.close();
    }
}

impl Inner {
    fn now(&self) -> u64 {
        self.anchor_ns.saturating_add(self.mono0.elapsed().as_nanos() as u64)
    }

    fn push(&mut self, ch: Channel, data: Vec<u8>) {
        if self.stopped {
            return;
        }
        let t = self.now();
        if let Some(p) = &self.part
            && (p.bytes >= self.cfg.part_bytes || t.saturating_sub(p.start) >= self.cfg.part_time.as_nanos() as u64)
        {
            let why = if p.bytes >= self.cfg.part_bytes { "size" } else { "time" };
            self.close_part(why);
        }
        if self.part.is_none() && !self.open_part(t) {
            return;
        }
        self.remember(&ch, &data);
        self.write(ch.clone(), &data, t);
        // 청크 닫기는 tick 경계 — 문턱을 반 tick 앞당긴다(tick 이 조금 일찍 와도 한 주기 밀리지 않게 · 정:IMPL-ROOM-STALL 과 같은 꼴)
        let due = (self.cfg.flush.as_nanos() as u64).saturating_sub(self.cfg.tick.as_nanos() as u64 / 2);
        if ch == Channel::Tick && t.saturating_sub(self.last_flush) >= due {
            self.last_flush = t;
            self.flush();
        }
    }

    fn remember(&mut self, ch: &Channel, data: &[u8]) {
        let key = |k: &str| serde_json::from_slice::<Value>(data).ok().and_then(|v| v.get(k).and_then(|x| x.as_str().map(str::to_string).or_else(|| x.as_u64().map(|n| n.to_string()))));
        match ch {
            Channel::Track => {
                if let Some(r) = key("room") {
                    self.snap.track.insert(r, data.to_vec());
                }
            }
            Channel::Floor => {
                if let Some(r) = key("room") {
                    self.snap.floor.insert(r, data.to_vec());
                }
            }
            Channel::Session => {
                if let Some(e) = key("event") {
                    let at = key("server").or_else(|| key("room")).unwrap_or_default();
                    self.snap.session.insert(format!("{e}/{at}"), data.to_vec());
                }
            }
            Channel::Sr => {
                if let Some(s) = key("ssrc").and_then(|s| s.parse().ok()) {
                    self.snap.sr.insert(s, data.to_vec());
                }
            }
            _ => {}
        }
    }

    fn write(&mut self, ch: Channel, data: &[u8], t: u64) {
        self.seq = self.seq.wrapping_add(1);
        let seq = self.seq;
        let Some(p) = self.part.as_mut() else { return };
        let id = match p.channels.get(&ch) {
            Some(id) => *id,
            None => match p.w.add_channel(0, &ch.topic(), ch.encoding(), &BTreeMap::new()) {
                Ok(id) => {
                    p.channels.insert(ch.clone(), id);
                    id
                }
                Err(_) => return,
            },
        };
        let h = MessageHeader { channel_id: id, sequence: seq, log_time: t, publish_time: t };
        if p.w.write_to_known_channel(&h, data).is_ok() {
            p.bytes += 31 + data.len() as u64;
            p.last_seq = Some(seq);
        }
    }

    fn open_part(&mut self, t: u64) -> bool {
        if free_bytes(&self.cfg.dir).is_some_and(|f| f < self.cfg.min_free_bytes) {
            self.stop();
            return false;
        }
        let path = self.cfg.dir.join(format!("{}-{:04}.mcap", self.cfg.recording_id, self.part_no));
        let Some((f, file)) = File::create(&path).ok().and_then(|f| f.try_clone().ok().map(|c| (f, c))) else {
            self.stop();
            return false;
        };
        let opts = WriteOptions::new().use_chunks(true).chunk_size(None).compression(compression(self.cfg.compression)).profile("oxlens");
        let Ok(mut w) = opts.create(BufWriter::new(f)) else {
            self.stop();
            return false;
        };
        let mut rec = BTreeMap::new();
        rec.insert("format".to_string(), FORMAT.to_string());
        rec.insert("recording_id".to_string(), self.cfg.recording_id.clone());
        rec.insert("sdk".to_string(), format!("oxpeer {}", env!("CARGO_PKG_VERSION")));
        let mut part = BTreeMap::new();
        part.insert("part".to_string(), self.part_no.to_string());
        if let Some(l) = self.prev_last {
            part.insert("prev_last_sequence".to_string(), l.to_string());
        }
        if w.write_metadata(&Metadata { name: "oxlens.recording".into(), metadata: rec }).is_err() || w.write_metadata(&Metadata { name: "oxlens.part".into(), metadata: part }).is_err() {
            self.stop();
            return false;
        }
        self.part = Some(Part { w, file, channels: HashMap::new(), start: t, bytes: 0, last_seq: None });
        self.part_no += 1;
        // 파트 머리 스냅숏(REC-PART) — 트랙 · 발언권 · 세션 · SR
        let head: Vec<(Channel, Vec<u8>)> = self
            .snap
            .track
            .values()
            .map(|d| (Channel::Track, d.clone()))
            .chain(self.snap.floor.values().map(|d| (Channel::Floor, d.clone())))
            .chain(self.snap.session.values().map(|d| (Channel::Session, d.clone())))
            .chain(self.snap.sr.values().map(|d| (Channel::Sr, d.clone())))
            .collect();
        for (ch, d) in head {
            self.write(ch, &d, t);
        }
        true
    }

    fn flush(&mut self) {
        let Some(p) = self.part.as_mut() else { return };
        let _ = p.w.flush();
        if self.cfg.fsync_on_flush {
            let _ = p.file.sync_data();
        }
        if free_bytes(&self.cfg.dir).is_some_and(|f| f < self.cfg.min_free_bytes) {
            self.stop();
        }
    }

    fn stop(&mut self) {
        self.close_part("disk_low");
        self.stopped = true;
        let _ = self.ev.send(Event::RecordStopped { reason: "disk_low".into() });
    }

    fn close_part(&mut self, reason: &str) {
        let Some(mut p) = self.part.take() else { return };
        let mut end = BTreeMap::new();
        if let Some(l) = p.last_seq {
            end.insert("last_sequence".to_string(), l.to_string());
        }
        end.insert("reason".to_string(), reason.to_string());
        let _ = p.w.write_metadata(&Metadata { name: "oxlens.part_end".into(), metadata: end });
        let _ = p.w.finish();
        let mut bw = p.w.into_inner();
        let _ = bw.flush();
        let _ = bw.get_ref().sync_all();
        self.prev_last = p.last_seq.or(self.prev_last);
    }
}

fn compression(c: Compression) -> Option<mcap::Compression> {
    match c {
        Compression::None => None,
        #[cfg(feature = "record-lz4")]
        Compression::Lz4 => Some(mcap::Compression::Lz4),
        #[cfg(feature = "record-zstd")]
        Compression::Zstd => Some(mcap::Compression::Zstd),
    }
}

/// 남은 공간(bytes) — 유닉스 계열만(statvfs)
fn free_bytes(dir: &Path) -> Option<u64> {
    let s = rustix::fs::statvfs(dir).ok()?;
    Some(s.f_bavail.saturating_mul(s.f_frsize))
}

/// 제어 사건 본문 — `ctl/session`(REC-RAW)
pub(crate) fn session_body(e: &Event) -> Option<Value> {
    Some(match e {
        Event::Session(s) => json!({ "event": "session", "state": format!("{:?}", s.state), "recovering": s.recovering, "reason": s.reason }),
        Event::Link(l) => json!({ "event": "link", "server": l.server, "state": format!("{:?}", l.state) }),
        Event::Joined { room } => json!({ "event": "joined", "room": room }),
        Event::JoinFailed { room, error } => json!({ "event": "join_failed", "room": room, "error": error.to_string() }),
        Event::Left { room } => json!({ "event": "left", "room": room }),
        Event::Forced { room, cause } => json!({ "event": "forced", "room": room, "cause": cause }),
        Event::Rebuilt => json!({ "event": "rebuilt" }),
        Event::Closed { code, name, retryable } => json!({ "event": "closed", "code": code, "name": name, "retryable": retryable }),
        _ => return None,
    })
}

#[cfg(test)]
mod tests;
