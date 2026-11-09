// author: kodeholic (powered by Claude)
//! 녹화 갈래(`--video` · 20261107a B4 · B5) — 받는 트랙마다 Matroska 파일 하나. SDK 가 조립한 프레임(영상은 키프레임부터 · 깨진 프레임 없음)을 그대로 담는다.
//! 개인 트랙은 녹화 내내 파일 하나 · 무전 슬롯은 발언 한 건 = 파일 하나(녹음과 같은 경계 — ★프레임의 출처 · SDK:RULE-PLAT-SERVER 「출처 표지」).
//! 슬롯 파일은 출처가 바뀌면 닫고 새로 연다 — 영상은 새 출처의 첫 키프레임부터. 출처 없는 프레임(서버가 지은 침묵)은 경계 밖이다.
//! 층은 앱이 고른다(B3) — 시뮬캐스트 트랙은 `--spatial` 이 없으면 최고 단을 상한으로 · `--layer-plan` 이 있으면 그 초에 바꾼다.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use oxpeer::{Client, Event, Events, Frames, Layer, MediaFrame, TrackEntry};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::mkv::{Codec, Mkv};
use crate::{Args, Flow};

struct Rec {
    /// 지금 이 트랙을 싣는 다운링크 ssrc(줄) · 마지막 프레임을 받은 때 — 트랙이 다른 줄로 옮겨 가면 시각축을 받은 시각으로 다시 잇는다
    ssrc: u32,
    last_at: Instant,
    rebased: u64,
    /// 슬롯 파일의 출처 번호(CSRC) — 바뀌면 그 파일을 닫는다
    csrc: Option<u32>,
    file: PathBuf,
    track_id: String,
    kind: String,
    codec: String,
    who: String,
    slot: bool,
    m: Mkv,
}

fn safe(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' }).collect()
}

fn unix_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn close(r: Rec, done: &mut Vec<Value>) {
    let (frames, keyframes, synced, first_rtp, last_rtp, dur, rebased) = (r.m.frames, r.m.keyframes, r.m.synced, r.m.first_rtp, r.m.last_rtp, r.m.last_ms, r.rebased);
    let start = r.m.finish().ok().flatten();
    done.push(json!({
        "file": r.file, "track_id": r.track_id, "kind": r.kind, "codec": r.codec, "who": r.who, "slot": r.slot,
        "frames": frames, "keyframes": keyframes, "synced": synced, "first_rtp": first_rtp, "last_rtp": last_rtp,
        "duration_ms": dur, "start_wall_ms": start, "rebased": rebased,
    }));
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

pub async fn run(a: &Args, client: Client, mut events: Events, mut frames: Frames) -> Value {
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
    let mut recs: BTreeMap<String, Rec> = BTreeMap::new();
    let mut done: Vec<Value> = Vec::new();
    let mut n = 0u32;
    let (mut orphan_frames, mut until_key, mut unknown_codec) = (0u64, 0u64, 0u64);
    let mut video: BTreeMap<String, TrackEntry> = BTreeMap::new();
    // 커버리지(20261107b Q4) — 영상 트랙이 방 목록에 처음 보인 때 · 첫 프레임이 실린 때(시작 뒤 ms). 줄 상한 밖이면 실린 때가 늦거나 없다
    let mut listed_at: BTreeMap<String, u64> = BTreeMap::new();
    let mut loaded_at: BTreeMap<String, u64> = BTreeMap::new();
    let mut asked: HashSet<String> = HashSet::new();
    let mut plan = a.layer_plan.clone();
    plan.sort();
    let mut plan_at = 0usize;
    let mut cap: Option<u8> = a.spatial;
    let (ltx, mut lrx) = mpsc::unbounded_channel::<Value>();
    let mut layer_calls: Vec<Value> = Vec::new();
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let mut flow = Flow::default();
    let mut seen_events: Vec<String> = Vec::new();
    loop {
        tokio::select! {
            _ = &mut stop => break,
            _ = tick.tick() => {
                while plan_at < plan.len() && started.elapsed() >= Duration::from_secs(plan[plan_at].0) {
                    let s = plan[plan_at].1;
                    cap = Some(s);
                    for (id, t) in &video {
                        if layered(t) {
                            ask_layer(&client, &ltx, id.clone(), s, recs.get(id).map(|r| r.m.last_rtp));
                        }
                    }
                    plan_at += 1;
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
                                ask_layer(&client, &ltx, t.track_id.clone(), s, recs.get(&t.track_id).map(|r| r.m.last_rtp));
                            }
                        }
                    }
                    _ => {
                        if flow.on(&e, &a.room) {
                            break;
                        }
                    }
                }
                seen_events.push(format!("{e:?}").chars().take(160).collect());
            }
            f = frames.next() => {
                let Some(f) = f else { break };
                on_frame(a, f, &mut recs, &mut done, &mut n, &mut orphan_frames, &mut until_key, &mut unknown_codec);
                for k in recs.keys() {
                    loaded_at.entry(k.clone()).or_insert(started.elapsed().as_millis() as u64);
                }
            }
        }
    }
    for (_, r) in std::mem::take(&mut recs) {
        close(r, &mut done);
    }
    while let Ok(v) = lrx.try_recv() {
        layer_calls.push(v);
    }
    let stats = client.stats().await;
    client.close().await;
    let mut out = json!({
        "room": a.room,
        "files": done,
        "orphan_frames": orphan_frames,
        "skipped_until_key": until_key,
        "unknown_codec": unknown_codec,
        "layer_calls": layer_calls,
        "events": seen_events,
        "coverage": video.iter().filter(|(_, t)| t.user_id.is_some()).map(|(id, t)| json!({
            "track_id": id, "user_id": t.user_id, "listed_ms": listed_at.get(id), "loaded_ms": loaded_at.get(id),
        })).collect::<Vec<_>>(),
        "not_recorded": video.iter().filter(|(id, t)| t.user_id.is_some() && !loaded_at.contains_key(*id)).map(|(id, _)| id.clone()).collect::<Vec<_>>(),
    });
    flow.fill(&mut out, &stats);
    out
}

#[allow(clippy::too_many_arguments)]
fn on_frame(a: &Args, f: MediaFrame, recs: &mut BTreeMap<String, Rec>, done: &mut Vec<Value>, n: &mut u32, orphan: &mut u64, until_key: &mut u64, unknown: &mut u64) {
    if f.room != a.room {
        return;
    }
    let Some(codec) = Codec::of(&f.codec) else {
        *unknown += 1;
        return;
    };
    let slot = f.user_id.is_none();
    // 파일 열쇠 — 개인 트랙은 ★출처의 `track_id`(줄의 지금 트랙이 아니라 · 재배정 직후 늦은 옛 트랙 패킷도 제 파일로) · 슬롯은 슬롯 트랙
    let key = if slot { f.track_id.clone() } else { f.source.as_ref().and_then(|s| s.track_id.clone()).unwrap_or_else(|| f.track_id.clone()) };
    if slot {
        let Some(src) = f.source.as_ref() else {
            if let Some(r) = recs.remove(&key) {
                close(r, done);
            }
            *orphan += 1;
            return;
        };
        if recs.get(&key).is_some_and(|r| r.csrc != Some(src.csrc))
            && let Some(r) = recs.remove(&key)
        {
            close(r, done);
        }
    }
    if !recs.contains_key(&key) {
        if f.kind == "video" && !f.keyframe {
            *until_key += 1;
            return;
        }
        *n += 1;
        let who = if slot { f.source.as_ref().and_then(|s| s.user_id.clone()).unwrap_or_default() } else { f.user_id.clone().unwrap_or_default() };
        let file = a.out.join(format!("{}-{:03}-{}-{}.mkv", safe(&a.room), n, safe(&who), f.kind));
        match Mkv::create(&file, codec, &f.data, u64::from(*n)) {
            Ok(m) => {
                recs.insert(key.clone(), Rec { ssrc: f.ssrc, last_at: f.received_at, rebased: 0, csrc: f.source.as_ref().map(|s| s.csrc), file, track_id: key.clone(), kind: f.kind.clone(), codec: f.codec.clone(), who, slot, m });
            }
            Err(e) => {
                eprintln!("파일을 못 만들었다 {}: {e}", file.display());
                return;
            }
        }
    }
    if let Some(r) = recs.get_mut(&key) {
        // ★줄마다 RTP 시각 기준이 다르다(연:RULE-SUB-FILL 재배정 — 줄의 SSRC · seq · ts 는 줄 것) — 다른 줄로 옮겨 온 첫 프레임은 RTP 시각 차 대신 받은 시각 차로 잇는다
        if r.ssrc != f.ssrc {
            r.m.rebase(f.timestamp, f.received_at.saturating_duration_since(r.last_at));
            r.ssrc = f.ssrc;
            r.rebased += 1;
        }
        r.last_at = f.received_at;
        let _ = r.m.push(f.timestamp, f.keyframe, &f.data, f.wallclock, f.synced);
    }
}
