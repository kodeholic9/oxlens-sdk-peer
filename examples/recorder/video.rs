// author: kodeholic (powered by Claude)
//! 녹화 갈래(`--video` · 20261107a B4 · B5 → ★202611/20261110a 레코드 흐름 위로) — 녹음 갈래와 같은 파이프라인(SDK:RULE-PLAT-REC-PIPE) · 결과물만 Matroska(`MkvSink` — SDK:RULE-PLAT-REC-MKV).
//! 정책 기본 = 발언(SDK:RULE-PLAT-REC-SPLIT) — 개인 트랙은 출처가 안 바뀌어 녹화 내내 파일 하나 · 무전 슬롯은 발언 한 건 = 파일 하나 · 영상은 키프레임부터.
//!   `--span-secs N` 이면 길이 정책(트랙마다 N 초 뒤 영상은 첫 키프레임 · 음성은 아무 프레임에서 새 파일).
//! `--hls`(202611/20261110b) — 결과물을 HLS(fMP4 · `HlsSink` — SDK:RULE-PLAT-REC-HLS)로. 파이프라인은 잇는 길이 정책(끝없음) · 조각 경계 1초 전에 영상 키프레임을 요청한다.
//! 층은 앱이 고른다(B3) — 시뮬캐스트 트랙은 `--spatial` 이 없으면 최고 단을 상한으로 · `--layer-plan` 이 있으면 그 초에 바꾼다.

use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use oxpeer::record::{FileSource, HlsConfig, HlsSink, MkvConfig, MkvSink, Output, Pipeline, Policy, Record, Records};
use oxpeer::{Client, Event, Events, Layer, Media, TrackEntry};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::{Args, Flow, records_summary};

fn unix_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// 시뮬캐스트 · SVC 트랙의 최고 공간 단(`L{S}T{T}` 의 S − 1)
fn top_spatial(t: &TrackEntry) -> Option<u8> {
    let s = t.scalability.as_deref()?.strip_prefix('L')?.split_once('T')?.0.parse::<u8>().ok()?;
    s.checked_sub(1)
}

/// 층이 있는 트랙인가(시뮬캐스트 · SVC) — 층 상한은 이것에만 낸다
fn layered(t: &TrackEntry) -> bool {
    t.scalability.is_some() || t.simulcast == Some(true)
}

/// `at_rtp` = 낸 순간 그 트랙 파일의 마지막 RTP 시각 — 상한과 받은 층을 같은 시각축(그 파일의 RTP)에서 견주는 재료
fn ask_layer(client: &Client, tx: &mpsc::UnboundedSender<Value>, track: String, spatial: u8, at_rtp: Option<u32>) {
    let (c, tx) = (client.clone(), tx.clone());
    tokio::spawn(async move {
        let at = unix_ms();
        let r = c.set_layer(&track, Layer { spatial: Some(spatial), ..Layer::default() }).await;
        let _ = tx.send(json!({ "at_ms": at, "done_ms": unix_ms(), "at_rtp": at_rtp, "track_id": track, "spatial": spatial, "ok": r.is_ok(), "error": r.err().map(|e| e.to_string()) }));
    });
}

/// 결과물 — Matroska(사람마다 · 방 슬롯마다 영상 · 음성 한 파일 — 202611/20261110e) · HLS(묶음마다 스트림)
pub enum Out {
    Mkv(MkvSink),
    Hls(HlsSink),
}

impl Out {
    fn open(a: &Args, dir: &std::path::Path) -> std::io::Result<Out> {
        Ok(if a.hls {
            Out::Hls(HlsSink::new(dir, HlsConfig::default())?)
        } else {
            Out::Mkv(MkvSink::with(dir, MkvConfig { span: a.span_secs.map(Duration::from_secs), ..MkvConfig::default() })?)
        })
    }

    fn write(&mut self, o: &Output) -> std::io::Result<()> {
        match self {
            Out::Mkv(s) => s.write(o),
            Out::Hls(s) => s.write(o),
        }
    }

    fn last_rtp(&self, track: &str) -> Option<u32> {
        match self {
            Out::Mkv(s) => s.last_rtp(track),
            Out::Hls(_) => None,
        }
    }

    fn finish(&mut self) {
        let r = match self {
            Out::Mkv(s) => s.finish(),
            Out::Hls(s) => s.finish(),
        };
        if let Err(e) = r {
            eprintln!("[recorder] 결과물 마무리 실패: {e}");
        }
    }

    /// 결과물이 낸 키프레임 요청(조각 · 파일 경계 1초 전) — 앱이 SDK 에 넘긴다(결과물 바이트와 무관)
    fn take_keyframe_requests(&mut self) -> Vec<String> {
        match self {
            Out::Mkv(s) => s.take_keyframe_requests(),
            Out::Hls(s) => s.take_keyframe_requests(),
        }
    }

    fn before_key(&self) -> u64 {
        match self {
            Out::Mkv(s) => s.before_key(),
            Out::Hls(_) => 0,
        }
    }

    /// 요약 — Matroska 는 `files` · HLS 는 `hls`(묶음마다) + `hls_files`(라이브 = 파일 대조 재료 — 모든 파일)
    fn summary(&self, out: &mut Value) {
        match self {
            Out::Mkv(s) => {
                out["files"] = json!(files(s));
                out["unknown_codec"] = json!(s.unknown());
            }
            Out::Hls(s) => {
                let name = |p: &std::path::Path| p.display().to_string();
                out["hls"] = json!(s.streams().iter().map(|st| json!({
                    "key": st.key, "dir": name(&st.dir), "room": st.room, "owner": st.owner, "base": st.base, "refused": st.refused,
                    "over_target": st.over_target, "keyframe_requests": st.keyframe_requests, "replaced": st.replaced,
                    "tracks": st.tracks, "inits": st.inits.iter().map(|p| name(p)).collect::<Vec<_>>(),
                    "event": name(&st.event_playlist), "live": name(&st.live_playlist),
                    "segments": st.segments.iter().map(|x| json!({ "file": name(&x.file), "init": name(&x.init), "discontinuity": x.discontinuity, "duration": x.duration })).collect::<Vec<_>>(),
                })).collect::<Vec<_>>());
                out["hls_files"] = json!(hls_files(s).iter().map(|f| json!({ "file": f })).collect::<Vec<_>>());
            }
        }
    }
}

fn hls_files(s: &HlsSink) -> Vec<String> {
    let mut v: Vec<String> = Vec::new();
    for st in s.streams() {
        v.extend(st.inits.iter().map(|p| p.display().to_string()));
        v.extend(st.segments.iter().map(|x| x.file.display().to_string()));
        if !st.segments.is_empty() {
            v.push(st.event_playlist.display().to_string());
            v.push(st.live_playlist.display().to_string());
        }
    }
    v
}

pub fn policy(a: &Args) -> Policy {
    // HLS · Matroska 길이 정책은 결과물 안에서 자른다(영상 · 음성을 같은 시각에서) — 파이프라인은 잇는 길이 정책
    if a.hls || a.span_secs.is_some() {
        return Policy::Span { time_ns: u64::MAX, bytes: u64::MAX };
    }
    Policy::Utterance
}

/// 녹화 갈래의 셈 — 그 방 프레임만 결과물로 · 출처 없는 프레임 수 · 트랙마다 첫 프레임이 실린 때(시작 뒤 ms — 커버리지)
#[derive(Default)]
pub struct Tally {
    orphan: u64,
    loaded_at: BTreeMap<String, u64>,
}

impl Tally {
    pub fn feed(&mut self, room: &str, pipe: &mut Pipeline, sink: &mut Out, r: &Record, since: Option<Instant>) {
        for o in pipe.push(r) {
            self.out(room, sink, &o, since);
        }
    }

    pub fn out(&mut self, room: &str, sink: &mut Out, o: &Output, since: Option<Instant>) {
        match o {
            Output::Frame(f) if f.room == room => {
                if let Some(t) = since {
                    self.loaded_at.entry(f.track_id.clone()).or_insert(t.elapsed().as_millis() as u64);
                }
            }
            Output::Orphan(f) if f.room == room => self.orphan += 1,
            Output::Segment(s) if s.room != room => return,
            Output::Frame(_) | Output::Orphan(_) => return,
            Output::Segment(_) => {}
        }
        if let Err(e) = sink.write(o) {
            eprintln!("[recorder] 파일 쓰기 실패: {e}");
        }
    }
}

fn files(sink: &MkvSink) -> Vec<Value> {
    sink.files()
        .iter()
        .map(|f| {
            json!({
                "file": f.path, "group": f.group, "index": f.index, "who": f.user_id, "owner": f.owner, "slot": f.owner.is_none(), "source": f.source,
                "base": f.base, "date": f.date, "date_track": f.date_track, "start_wall_ms": f.start_ms, "duration_ms": f.duration_ms, "reason": f.reason,
                "tracks": f.tracks.iter().map(|t| json!({
                    "kind": t.kind, "track_id": t.track_id, "codec": t.codec, "num": t.num, "frames": t.frames, "keyframes": t.keyframes,
                    "synced": t.synced, "filled": t.filled, "first_rtp": t.first_rtp, "last_rtp": t.last_rtp, "first_ms": t.first_ms, "rebased": t.rebased,
                })).collect::<Vec<_>>(),
            })
        })
        .collect()
}

/// 원시 파트로 다시 짓기 — 라이브와 같은 파이프라인 · 같은 결과물(파일 이름이 같다 — SDK:RULE-PLAT-REC-PIPE)
pub fn rebuild(a: &Args, out: &std::path::Path) -> Value {
    let Some(dir) = &a.raw else { return Value::Null };
    let Ok(mut src) = FileSource::open(dir, &a.user) else { return json!({ "error": "원시 파트를 못 열었다" }) };
    let Ok(mut sink) = Out::open(a, out) else { return json!({ "error": "다시 짓기 폴더" }) };
    let mut pipe = Pipeline::new(policy(a));
    let mut tally = Tally::default();
    src.run(|r| tally.feed(&a.room, &mut pipe, &mut sink, &r, None));
    for o in pipe.finish() {
        tally.out(&a.room, &mut sink, &o, None);
    }
    sink.finish();
    let list: Vec<Value> = match &sink {
        Out::Mkv(s) => files(s).iter().map(|f| json!({ "file": f["file"], "segment": f["segment"], "frames": f["frames"] })).collect(),
        Out::Hls(s) => hls_files(s).iter().map(|f| json!({ "file": f })).collect(),
    };
    json!({ "dir": out, "parts": src.parts().len(), "files": list, "records": records_summary(&pipe, false) })
}

pub async fn run(a: &Args, client: Client, mut events: Events, mut media: Media, mut records: Records) -> Value {
    let stop = async {
        match a.secs {
            Some(s) => tokio::time::sleep(Duration::from_secs(s)).await,
            None => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    };
    tokio::pin!(stop);
    let started = Instant::now();
    let mut pipe = Pipeline::new(policy(a));
    let mut sink = Out::open(a, &a.out).expect("출력 폴더");
    let mut tally = Tally::default();
    let mut video: BTreeMap<String, TrackEntry> = BTreeMap::new();
    // 커버리지(20261107b Q4) — 영상 트랙이 방 목록에 처음 보인 때 · 첫 프레임이 실린 때(시작 뒤 ms). 줄 상한 밖이면 실린 때가 늦거나 없다
    let mut listed_at: BTreeMap<String, u64> = BTreeMap::new();
    let mut asked: HashSet<String> = HashSet::new();
    let mut prioritized: HashSet<String> = HashSet::new();
    let mut plan = a.layer_plan.clone();
    plan.sort();
    let mut plan_at = 0usize;
    let mut cap: Option<u8> = a.spatial;
    let (ltx, mut lrx) = mpsc::unbounded_channel::<Value>();
    let mut layer_calls: Vec<Value> = Vec::new();
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let mut flow = Flow::default();
    let mut seen_events: Vec<String> = Vec::new();
    let mut ended = false;
    loop {
        tokio::select! {
            _ = &mut stop => break,
            _ = tick.tick() => {
                while plan_at < plan.len() && started.elapsed() >= Duration::from_secs(plan[plan_at].0) {
                    let s = plan[plan_at].1;
                    cap = Some(s);
                    for (id, t) in &video {
                        if layered(t) {
                            ask_layer(&client, &ltx, id.clone(), s, sink.last_rtp(id));
                        }
                    }
                    plan_at += 1;
                }
                // HLS 조각 경계 — 결과물이 낸 키프레임 요청을 SDK 에 넘긴다(SDK:RULE-PLAT-REC-HLS · 결과물 바이트와 무관)
                for t in sink.take_keyframe_requests() {
                    let _ = client.request_keyframe(t);
                }
            }
            Some(v) = lrx.recv() => layer_calls.push(v),
            e = events.next() => {
                let Some(e) = e else { break };
                match &e {
                    Event::Tracks { room, tracks } if *room == a.room => {
                        for t in tracks.iter().filter(|t| t.kind == oxpeer::Kind::Video) {
                            video.insert(t.track_id.clone(), t.clone());
                            listed_at.entry(t.track_id.clone()).or_insert(started.elapsed().as_millis() as u64);
                            // 층 상한 — 앱이 준 값 · 계획의 지금 값 · 없으면 그 트랙의 최고 단(B3)
                            if layered(t)
                                && asked.insert(t.track_id.clone())
                                && let Some(s) = cap.or_else(|| top_spatial(t))
                            {
                                ask_layer(&client, &ltx, t.track_id.clone(), s, sink.last_rtp(&t.track_id));
                            }
                            // 줄 우선순위 — 앱이 고른 사람의 영상은 줄 상한을 넘는 방에서도 늘 받는다(연:RULE-SUB-FILL)
                            if let Some((_, n)) = a.priority.iter().find(|(u, _)| t.user_id.as_deref() == Some(u.as_str()))
                                && prioritized.insert(t.track_id.clone())
                            {
                                let (c, tx, id, n) = (client.clone(), ltx.clone(), t.track_id.clone(), *n);
                                tokio::spawn(async move {
                                    let r = c.set_layer(&id, Layer { priority: Some(n), ..Layer::default() }).await;
                                    let _ = tx.send(json!({ "at_ms": unix_ms(), "track_id": id, "priority": n, "ok": r.is_ok(), "error": r.err().map(|e| e.to_string()) }));
                                });
                            }
                        }
                    }
                    _ => {
                        if flow.on(&e, &a.room) {
                            ended = true;
                            break;
                        }
                    }
                }
                seen_events.push(format!("{e:?}").chars().take(160).collect());
            }
            // 미디어 통은 비워 둔다(이 갈래는 레코드 흐름으로 짓는다 — 통이 차 버리면 계수가 오른다)
            p = media.next() => if p.is_none() { break },
            r = records.next() => {
                let Some(r) = r else { break };
                tally.feed(&a.room, &mut pipe, &mut sink, &r, Some(started));
            }
        }
    }
    while let Ok(v) = lrx.try_recv() {
        layer_calls.push(v);
    }
    let stats = client.stats().await;
    client.close().await;
    // Client 를 놓아야 SDK 루프가 끝나고 기록기가 닫힌다 — 통에 남은 레코드를 다 짓는다(라이브 = 파일 — 같은 레코드열)
    drop(client);
    if !ended {
        while let Some(r) = records.next().await {
            tally.feed(&a.room, &mut pipe, &mut sink, &r, Some(started));
        }
    }
    for o in pipe.finish() {
        tally.out(&a.room, &mut sink, &o, Some(started));
    }
    sink.finish();
    let c = pipe.counts().clone();
    let mut out = json!({
        "room": a.room,
        "orphan_frames": tally.orphan,
        "skipped_until_key": c.before_key + sink.before_key(),
        "late_frames": c.late,
        "layer_calls": layer_calls,
        "events": seen_events,
        "records": records_summary(&pipe, records.degraded()),
        "coverage": video.iter().filter(|(_, t)| t.user_id.is_some()).map(|(id, t)| json!({
            "track_id": id, "user_id": t.user_id, "listed_ms": listed_at.get(id), "loaded_ms": tally.loaded_at.get(id),
        })).collect::<Vec<_>>(),
        "not_recorded": video.iter().filter(|(id, t)| t.user_id.is_some() && !tally.loaded_at.contains_key(*id)).map(|(id, _)| id.clone()).collect::<Vec<_>>(),
    });
    sink.summary(&mut out);
    if a.raw.is_some() {
        out["rebuild"] = rebuild(a, &a.out.join("rebuild"));
    }
    flow.fill(&mut out, &stats);
    // 조립은 파이프라인 몫이다 — 깨진 프레임은 파이프라인 계수(SDK 프레임 통은 쓰지 않는다)
    out["frames_broken"] = json!(stats.frames_broken + c.broken);
    out
}
