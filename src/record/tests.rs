// author: kodeholic (powered by Claude)
//! 1층 — 녹음 원시 기록(SDK:RULE-PLAT-REC-RAW · REC-CLOCK · REC-PART): 왕복 · 회전 · 사슬 · 머리 스냅숏 · 잘린 파트 · 디스크 하한

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::sync::mpsc;

use std::sync::Arc;

use super::*;

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("oxpeer-rec-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn cfg(dir: &Path) -> RecordConfig {
    let mut c = RecordConfig::new(dir, "r1");
    c.tick = Duration::from_secs(3_600);
    c.min_free_bytes = 0;
    c
}

fn rtp(seq: u16) -> Vec<u8> {
    let mut v = vec![0x80, 0x6f, 0, 0, 0, 0, 0, 1, 0, 0, 0, 7];
    v[2..4].copy_from_slice(&seq.to_be_bytes());
    v.extend_from_slice(&[seq as u8; 20]);
    v
}

fn all(dir: &Path) -> (Vec<Record>, Vec<PartScan>) {
    let mut src = FileSource::open(dir, "r1").unwrap();
    let mut got = Vec::new();
    src.run(|r| got.push(r));
    (got, src.scans().to_vec())
}

#[tokio::test]
async fn roundtrip_keeps_order_sequence_and_time() {
    let d = tmp("rt");
    let (ev, _rx) = mpsc::unbounded_channel();
    let r = Recorder::start(cfg(&d), ev).unwrap();
    r.track("room1", serde_json::json!({ "room": "room1", "tracks": [], "rows": {} }));
    for s in 0..50u16 {
        r.push(Channel::Rtp(1000 + (s % 2) as u32), rtp(s));
    }
    r.push(Channel::Sr, br#"{"ssrc":1000,"ntp":1,"rtp":2}"#.to_vec());
    r.close();
    let (got, scans) = all(&d);
    assert_eq!(scans.len(), 1);
    assert!(scans[0].complete, "정상으로 닫은 파트는 Footer 가 있다");
    assert_eq!(scans[0].recording.get("format").map(String::as_str), Some("1"));
    assert_eq!(got.len(), 52);
    assert_eq!(got[0].channel, Channel::Track);
    for (i, x) in got.iter().enumerate() {
        assert_eq!(x.sequence, i as u32 + 1, "순번은 파일 차례로 하나씩");
    }
    assert!(got.windows(2).all(|w| w[0].log_time <= w[1].log_time), "시각도 파일 차례로 오른다");
    assert_eq!(got[1].data, rtp(0));
    assert_eq!(got[2].channel, Channel::Rtp(1001));
    let end = scans[0].part_end.clone().unwrap();
    assert_eq!(end.get("reason").map(String::as_str), Some("close"));
    assert_eq!(end.get("last_sequence").map(String::as_str), Some("52"));
}

#[tokio::test]
async fn same_track_list_is_written_once() {
    let d = tmp("tr");
    let (ev, _rx) = mpsc::unbounded_channel();
    let r = Recorder::start(cfg(&d), ev).unwrap();
    let b = serde_json::json!({ "room": "a", "tracks": [], "rows": { "5": "t1" } });
    r.track("a", b.clone());
    r.track("a", b);
    r.track("a", serde_json::json!({ "room": "a", "tracks": [], "rows": { "5": "t2" } }));
    r.close();
    let (got, _) = all(&d);
    assert_eq!(got.iter().filter(|x| x.channel == Channel::Track).count(), 2, "줄 재배정만 다시 적는다");
}

#[tokio::test]
async fn rotation_by_size_chains_parts_and_repeats_head_snapshot() {
    let d = tmp("rot");
    let (ev, _rx) = mpsc::unbounded_channel();
    let mut c = cfg(&d);
    c.part_bytes = 2_000;
    let r = Recorder::start(c, ev).unwrap();
    r.track("room1", serde_json::json!({ "room": "room1", "tracks": [], "rows": {} }));
    r.push(Channel::Floor, br#"{"room":"room1","event":"taken","user_id":"u1","sources":[7],"seq":3}"#.to_vec());
    r.push(Channel::Sr, br#"{"ssrc":1000,"ntp":9,"rtp":8}"#.to_vec());
    for s in 0..200u16 {
        r.push(Channel::Rtp(1000), rtp(s));
    }
    r.close();
    let (got, scans) = all(&d);
    assert!(scans.len() >= 5, "작은 S 로 여러 파트(파트 {})", scans.len());
    for (k, sc) in scans.iter().enumerate() {
        assert!(sc.complete, "파트 {k} 는 Footer 가 있다");
        assert_eq!(sc.part.get("part").map(String::as_str), Some(k.to_string().as_str()));
        if k == 0 {
            assert!(!sc.part.contains_key("prev_last_sequence"));
            continue;
        }
        let prev: u32 = sc.part["prev_last_sequence"].parse().unwrap();
        assert_eq!(prev, scans[k - 1].last_sequence.unwrap(), "사슬 — 직전 마지막 순번");
        assert_eq!(prev + 1, sc.first_sequence.unwrap(), "사슬 — 직전 마지막 + 1 = 첫 순번");
        assert_eq!(scans[k - 1].part_end.as_ref().unwrap()["reason"], "size");
    }
    // 파트 1 을 따로 읽으면 머리가 트랙 · 발언권 · SR 상태를 다시 세운다
    let mut one = FileSource::of(vec![scans[1].path.clone()]);
    let mut head = Vec::new();
    one.run(|r| head.push(r.channel));
    assert_eq!(&head[..3], &[Channel::Track, Channel::Floor, Channel::Sr]);
    let rtp_n = got.iter().filter(|x| matches!(x.channel, Channel::Rtp(_))).count();
    assert_eq!(rtp_n, 200, "파트 사이에 패킷이 빠지지 않는다");
}

#[tokio::test]
async fn rotation_by_time() {
    let d = tmp("rott");
    let (ev, _rx) = mpsc::unbounded_channel();
    let mut c = cfg(&d);
    c.part_time = Duration::from_millis(30);
    let r = Recorder::start(c, ev).unwrap();
    for s in 0..4u16 {
        r.push(Channel::Rtp(1), rtp(s));
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    r.close();
    let (_, scans) = all(&d);
    assert_eq!(scans.len(), 4);
    assert!(scans[..3].iter().all(|s| s.part_end.as_ref().unwrap()["reason"] == "time"));
}

#[tokio::test]
async fn unclosed_part_reads_up_to_last_flushed_chunk() {
    let d = tmp("crash");
    let (ev, _rx) = mpsc::unbounded_channel();
    let mut c = cfg(&d);
    c.flush = Duration::ZERO;
    let r = Recorder::start(c, ev).unwrap();
    for s in 0..10u16 {
        r.push(Channel::Rtp(1), rtp(s));
    }
    r.push(Channel::Tick, b"{}".to_vec());
    for s in 10..20u16 {
        r.push(Channel::Rtp(1), rtp(s));
    }
    // 프로세스가 죽은 것처럼 — 닫지 않은 파일을 그대로 베낀다
    let live = d.join("r1-0000.mcap");
    let cut = d.join("cut.mcap");
    std::fs::copy(&live, &cut).unwrap();
    std::mem::forget(r);
    let sc = scan_part(&cut);
    assert!(!sc.complete, "Footer 가 없다");
    assert_eq!(sc.messages, 11, "닫힌 청크(tick 까지)만 읽힌다");
    assert_eq!(sc.last_sequence, Some(11));
}

#[tokio::test]
async fn disk_low_stops_with_event() {
    let d = tmp("disk");
    let (ev, mut rx) = mpsc::unbounded_channel();
    let mut c = cfg(&d);
    c.min_free_bytes = u64::MAX;
    let r = Recorder::start(c, ev).unwrap();
    r.push(Channel::Rtp(1), rtp(0));
    r.push(Channel::Rtp(1), rtp(1));
    match rx.try_recv() {
        Ok(Event::RecordStopped { reason }) => assert_eq!(reason, "disk_low"),
        x => panic!("멈춤 사건이 와야 한다: {x:?}"),
    }
    assert!(rx.try_recv().is_err(), "멈춘 뒤 다시 알리지 않는다");
    drop(r);
}

#[test]
fn channel_topics_round_trip() {
    for ch in [Channel::Rtp(4_000_000_001), Channel::Track, Channel::Floor, Channel::Session, Channel::Sr, Channel::Gap, Channel::Tick] {
        assert_eq!(Channel::parse(&ch.topic()), Some(ch));
    }
    assert_eq!(Channel::parse("rtp/x"), None);
}

// ── 2단계 — 파이프라인 · Ogg · 라이브 통(SDK:RULE-PLAT-REC-PIPE · REC-SPLIT · REC-API) ──

fn pkt(seq: u16, ts: u32, marker: bool, csrc: Option<u32>, payload: &[u8]) -> Vec<u8> {
    let mut v = vec![if csrc.is_some() { 0x81 } else { 0x80 }, if marker { 0xEF } else { 0x6F }, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    v[2..4].copy_from_slice(&seq.to_be_bytes());
    v[4..8].copy_from_slice(&ts.to_be_bytes());
    if let Some(c) = csrc {
        v.extend_from_slice(&c.to_be_bytes());
    }
    v.extend_from_slice(payload);
    v
}

fn rec(seq: u32, t: u64, ch: Channel, data: Vec<u8>) -> Record {
    Record { sequence: seq, log_time: t, channel: ch, data }
}

fn tracks(room: &str) -> Vec<u8> {
    serde_json::json!({
        "room": room,
        "tracks": [{ "track_id": "slot-a", "kind": "audio" }, { "track_id": "cam-u1", "kind": "video", "user_id": "u1", "csrc": 9 }],
        "rows": { "100": { "track_id": "slot-a", "kind": "audio", "codec": "opus", "pt": 111 }, "200": { "track_id": "cam-u1", "kind": "video", "codec": "VP8", "pt": 96 } }
    })
    .to_string()
    .into_bytes()
}

/// 무전 슬롯: 출처 없음(예열 침묵) · A(7) 둘 · B(8) 하나 · 출처 없음 / 영상: 델타(버림) · 키 · 델타 · gap · 델타(버림) · 키
fn stream() -> Vec<Record> {
    let ms = 1_000_000u64;
    let mut v = vec![rec(1, 0, Channel::Track, tracks("r")), rec(2, ms, Channel::Floor, br#"{"room":"r","event":"taken","user_id":"A","sources":[7],"seq":1}"#.to_vec())];
    let mut n = 3u32;
    let mut put = |v: &mut Vec<Record>, ch: Channel, d: Vec<u8>| {
        v.push(rec(n, u64::from(n) * 20 * ms, ch, d));
        n += 1;
    };
    put(&mut v, Channel::Rtp(100), pkt(1, 0, true, None, &[0xF8, 0xFF, 0xFE]));
    put(&mut v, Channel::Rtp(100), pkt(2, 960, true, Some(7), &[0x78, 1]));
    put(&mut v, Channel::Rtp(100), pkt(3, 1920, true, Some(7), &[0x78, 2]));
    put(&mut v, Channel::Floor, br#"{"room":"r","event":"taken","user_id":"B","sources":[8],"seq":2}"#.to_vec());
    put(&mut v, Channel::Rtp(100), pkt(4, 2880, true, Some(8), &[0x78, 3]));
    put(&mut v, Channel::Rtp(100), pkt(5, 3840, true, None, &[0xF8, 0xFF, 0xFE]));
    put(&mut v, Channel::Sr, br#"{"ssrc":200,"ntp":16045690984503098046,"rtp":3000}"#.to_vec());
    put(&mut v, Channel::Rtp(200), pkt(1, 3000, true, Some(9), &[0x10, 0x01, 1]));
    put(&mut v, Channel::Rtp(200), pkt(2, 6000, true, Some(9), &[0x10, 0x00, 2]));
    put(&mut v, Channel::Rtp(200), pkt(3, 9000, true, Some(9), &[0x10, 0x01, 3]));
    put(&mut v, Channel::Gap, br#"{"ssrc":200,"from":4,"to":4,"count":1,"reason":"nack_exhausted"}"#.to_vec());
    put(&mut v, Channel::Rtp(200), pkt(5, 15000, true, Some(9), &[0x10, 0x01, 5]));
    put(&mut v, Channel::Rtp(200), pkt(6, 18000, true, Some(9), &[0x10, 0x00, 6]));
    v
}

fn run_pipe(p: Policy, recs: &[Record]) -> Vec<Output> {
    let mut pl = Pipeline::new(p);
    let mut out: Vec<Output> = recs.iter().flat_map(|r| pl.push(r)).collect();
    out.extend(pl.finish());
    out
}

#[test]
fn pipeline_is_deterministic() {
    assert_eq!(run_pipe(Policy::Utterance, &stream()), run_pipe(Policy::Utterance, &stream()));
}

#[test]
fn utterance_policy_splits_by_source_and_leaves_server_silence_out() {
    let out = run_pipe(Policy::Utterance, &stream());
    let audio: Vec<(Option<u32>, Option<u64>, Option<String>)> = out
        .iter()
        .filter_map(|o| match o {
            Output::Frame(f) if f.kind == "audio" => Some((f.source, f.segment, f.user_id.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(audio, vec![(Some(7), Some(0), Some("A".into())), (Some(7), Some(0), Some("A".into())), (Some(8), Some(1), Some("B".into()))]);
    let orphans = out.iter().filter(|o| matches!(o, Output::Orphan(f) if f.data == [0xF8, 0xFF, 0xFE])).count();
    assert_eq!(orphans, 2, "서버 침묵은 세그먼트 밖");
    let closed: Vec<(u64, Option<&str>)> = out.iter().filter_map(|o| if let Output::Segment(s) = o { (s.event == SegmentKind::Closed).then_some((s.id, s.reason)) } else { None }).collect();
    assert_eq!(&closed[..2], &[(0, Some("source")), (1, Some("no_source"))]);
}

#[test]
fn video_starts_at_keyframe_and_gap_breaks_until_next_key() {
    let out = run_pipe(Policy::Utterance, &stream());
    let video: Vec<(u32, bool)> = out.iter().filter_map(|o| if let Output::Frame(f) = o { (f.kind == "video").then_some((f.rtp_ts, f.keyframe)) } else { None }).collect();
    assert_eq!(video, vec![(6000, true), (9000, false), (18000, true)], "첫 델타 · gap 뒤 델타는 버린다");
    let mut pl = Pipeline::new(Policy::Utterance);
    for r in stream() {
        pl.push(&r);
    }
    assert_eq!((pl.counts().before_key, pl.counts().broken), (2, 0));
}

#[test]
fn sr_maps_rtp_time_to_utc() {
    let out = run_pipe(Policy::Utterance, &stream());
    let utc: Vec<Option<u64>> = out.iter().filter_map(|o| if let Output::Frame(f) = o { (f.kind == "video").then_some(f.utc_ns) } else { None }).collect();
    // NTP 16045690984503098046 = 유닉스 1527000000.5 초 무렵 · RTP 3000 기준 · 90kHz
    let base = (16045690984503098046u64 >> 32) - 2_208_988_800;
    let frac = ((16045690984503098046u64 & 0xffff_ffff) * 1_000_000_000) >> 32;
    let t0 = base * 1_000_000_000 + frac;
    assert_eq!(utc, vec![Some(t0 + 3_000 * 1_000_000_000 / 90_000), Some(t0 + 6_000 * 1_000_000_000 / 90_000), Some(t0 + 15_000 * 1_000_000_000 / 90_000)]);
    let audio_utc = out.iter().any(|o| matches!(o, Output::Frame(f) if f.kind == "audio" && f.utc_ns.is_some()));
    assert!(!audio_utc, "SR 전 · SR 없는 줄은 UTC 가 없다(받은 시각으로 메우지 않는다)");
}

#[test]
fn span_policy_cuts_audio_anywhere_and_video_at_keyframe() {
    let out = run_pipe(Policy::Span { time_ns: 30_000_000, bytes: u64::MAX }, &stream());
    let segs: Vec<(String, u64, Option<&str>)> = out.iter().filter_map(|o| if let Output::Segment(s) = o { (s.event == SegmentKind::Closed).then(|| (s.track_id.clone(), s.frames, s.reason)) } else { None }).collect();
    assert!(segs.iter().filter(|s| s.0 == "slot-a").count() >= 2, "음성은 T 뒤 아무 프레임에서 자른다: {segs:?}");
    let v: Vec<_> = segs.iter().filter(|s| s.0 == "cam-u1").collect();
    assert_eq!(v.len(), 2, "영상은 T 뒤 첫 키프레임(18000)에서: {segs:?}");
    let silence_in = out.iter().filter(|o| matches!(o, Output::Frame(f) if f.data == [0xF8, 0xFF, 0xFE] && f.segment.is_some())).count();
    assert_eq!(silence_in, 2, "연속 정책은 서버 침묵도 담는다");
}

#[test]
fn ogg_sink_bytes_are_identical_for_identical_records() {
    let d1 = tmp("ogg1");
    let d2 = tmp("ogg2");
    for d in [&d1, &d2] {
        let mut sink = OggSink::new(d).unwrap();
        for o in run_pipe(Policy::Utterance, &stream()) {
            sink.write(&o).unwrap();
        }
        assert_eq!(sink.files().len(), 2);
        assert_eq!(sink.files()[0].packets, 2);
        assert_eq!(sink.files()[0].samples, 1_920);
    }
    for name in ["r-0000-A.ogg", "r-0001-B.ogg"] {
        assert_eq!(std::fs::read(d1.join(name)).unwrap(), std::fs::read(d2.join(name)).unwrap(), "{name}");
    }
}

#[tokio::test]
async fn live_tap_equals_file_records() {
    let d = tmp("tap");
    let (ev, _rx) = mpsc::unbounded_channel();
    let mut c = cfg(&d);
    c.part_bytes = 300;
    let r = Recorder::start(c, ev).unwrap();
    let (tx, rx) = mpsc::channel(10_000);
    let shared = Arc::new(Shared::default());
    r.subscribe(tx, shared.clone());
    let mut live = Records::new(rx, shared);
    for x in stream() {
        r.push(x.channel, x.data);
    }
    r.close();
    let mut got = Vec::new();
    while let Some(x) = live.next().await {
        got.push(x);
    }
    let (file, scans) = all(&d);
    assert!(scans.len() > 1, "회전이 끼어도");
    assert_eq!(got, file, "라이브 통 = 파일(머리 스냅숏 포함 · 같은 순번 · 시각)");
}

#[tokio::test]
async fn overflow_downgrades_to_tail_read_without_loss_or_duplicates() {
    let d = tmp("tail");
    let (ev, _rx) = mpsc::unbounded_channel();
    let mut c = cfg(&d);
    c.part_bytes = 3_000;
    c.flush = Duration::ZERO;
    c.tick = Duration::from_millis(20);
    let r = Recorder::start(c, ev).unwrap();
    let (tx, rx) = mpsc::channel(16);
    let shared = Arc::new(Shared::default());
    r.subscribe(tx, shared.clone());
    let mut live = Records::new(rx, shared);
    for s in 0..300u16 {
        r.push(Channel::Rtp(1), rtp(s));
        if s % 10 == 0 {
            r.push(Channel::Tick, b"{}".to_vec());
        }
    }
    let reader = tokio::spawn(async move {
        let mut got = Vec::new();
        while let Some(x) = live.next().await {
            got.push(x.sequence);
        }
        (got, live.degraded())
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    for s in 300..320u16 {
        r.push(Channel::Rtp(1), rtp(s));
    }
    r.close();
    let (got, degraded) = tokio::time::timeout(Duration::from_secs(5), reader).await.unwrap().unwrap();
    assert!(degraded, "16 칸 통이 넘쳤다");
    let (file, _) = all(&d);
    let want: Vec<u32> = file.iter().map(|x| x.sequence).collect();
    assert_eq!(got, want, "넘친 뒤도 빠짐 · 겹침 없이 파일과 같은 차례");
}

#[tokio::test]
async fn overflow_without_raw_stops_with_event() {
    let (ev, mut evs) = mpsc::unbounded_channel();
    let mut c = RecordConfig::live("live");
    c.tick = Duration::from_secs(3_600);
    let r = Recorder::start(c, ev).unwrap();
    let (tx, rx) = mpsc::channel(16);
    let shared = Arc::new(Shared::default());
    r.subscribe(tx, shared.clone());
    let mut live = Records::new(rx, shared);
    for s in 0..40u16 {
        r.push(Channel::Rtp(1), rtp(s));
    }
    let mut n = 0;
    while live.next().await.is_some() {
        n += 1;
    }
    assert_eq!(n, 16, "넘치기 전까지만 · 섞인 결과를 몰래 내지 않는다");
    assert!(live.stopped());
    assert!(matches!(evs.try_recv(), Ok(Event::RecordConsumerStopped { .. })));
}

#[tokio::test]
async fn closed_recorder_takes_nothing_and_opens_no_new_part() {
    // 닫힌 뒤 tick 이 밀어 넣어도 새 파트를 열지 않는다(꼬리 읽기가 끝을 볼 수 있게 — 20261109d 에서 드러남)
    let d = tmp("closed");
    let (ev, _rx) = mpsc::unbounded_channel();
    let r = Recorder::start(cfg(&d), ev).unwrap();
    r.push(Channel::Rtp(1), rtp(0));
    r.close();
    r.push(Channel::Tick, b"{}".to_vec());
    r.push(Channel::Rtp(1), rtp(1));
    let (got, scans) = all(&d);
    assert_eq!(scans.len(), 1);
    assert_eq!(got.len(), 1);
}

#[tokio::test]
async fn late_subscriber_gets_current_state_first() {
    // 늦게 연 통 — 트랙 · 발언권 · SR 의 마지막 레코드를 원래 순번 · 시각으로 먼저 받아 줄 → 트랙을 안다(20261109d 2층에서 드러남 — 입장 뒤 연 통이 줄 표를 몰라 패킷 전부가 「줄 모름」)
    let (ev, _rx) = mpsc::unbounded_channel();
    let mut c = RecordConfig::live("late");
    c.tick = Duration::from_secs(3_600);
    let r = Recorder::start(c, ev).unwrap();
    let recs = stream();
    for x in &recs[..2] {
        r.push(x.channel.clone(), x.data.clone());
    }
    let (tx, rx) = mpsc::channel(1_000);
    let shared = Arc::new(Shared::default());
    r.subscribe(tx, shared.clone());
    let mut live = Records::new(rx, shared);
    for x in &recs[2..] {
        r.push(x.channel.clone(), x.data.clone());
    }
    r.close();
    let mut got = Vec::new();
    while let Some(x) = live.next().await {
        got.push(x);
    }
    assert_eq!((got[0].channel.clone(), got[0].sequence), (Channel::Track, 1));
    assert_eq!((got[1].channel.clone(), got[1].sequence), (Channel::Floor, 2));
    let mut pl = Pipeline::new(Policy::Utterance);
    for x in &got {
        pl.push(x);
    }
    assert_eq!(pl.counts().unrouted, 0);
}
