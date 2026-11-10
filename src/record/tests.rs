// author: kodeholic (powered by Claude)
//! 1층 — 녹음 원시 기록(SDK:RULE-PLAT-REC-RAW · REC-CLOCK · REC-PART): 왕복 · 회전 · 사슬 · 머리 스냅숏 · 잘린 파트 · 디스크 하한

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::sync::mpsc;

use std::sync::Arc;

use super::*;

fn quiet() -> Arc<crate::log::Logger> {
    crate::log::Logger::start(crate::log::LogConfig { enabled: false, ..crate::log::LogConfig::default() })
}

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
    let r = Recorder::start(cfg(&d), ev, quiet()).unwrap();
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
    let r = Recorder::start(cfg(&d), ev, quiet()).unwrap();
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
    let r = Recorder::start(c, ev, quiet()).unwrap();
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
    let r = Recorder::start(c, ev, quiet()).unwrap();
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
    let r = Recorder::start(c, ev, quiet()).unwrap();
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
    let r = Recorder::start(c, ev, quiet()).unwrap();
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
    let r = Recorder::start(c, ev, quiet()).unwrap();
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
    let r = Recorder::start(c, ev, quiet()).unwrap();
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
    let r = Recorder::start(c, ev, quiet()).unwrap();
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
    let r = Recorder::start(cfg(&d), ev, quiet()).unwrap();
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
    let r = Recorder::start(c, ev, quiet()).unwrap();
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

// ── 3단계 — 음성 구멍 PLC(SDK:RULE-PLAT-REC-GAP) ──

fn opus_stream(seqs_ts: &[(u16, u32)], toc: u8) -> Vec<Record> {
    let mut v = vec![rec(1, 0, Channel::Track, tracks("r"))];
    for (i, (seq, ts)) in seqs_ts.iter().enumerate() {
        v.push(rec(2 + i as u32, (i as u64 + 1) * 20_000_000, Channel::Rtp(100), pkt(*seq, *ts, true, Some(7), &[toc, 9, 9])));
    }
    v
}

#[test]
fn audio_hole_is_filled_with_plc_and_remainder_carries() {
    // 20ms 프레임(960) · seq 3 · 4 를 잃음(구멍 1,920) · 그다음 구멍 1,000(= 960 하나 + 40 넘김) · 그다음 920(+40 = 960 하나)
    let out = run_pipe(Policy::Utterance, &opus_stream(&[(1, 0), (2, 960), (5, 3_840), (6, 5_800), (7, 7_680)], 0x78));
    let frames: Vec<(u32, bool, usize)> = out.iter().filter_map(|o| if let Output::Frame(f) = o { Some((f.rtp_ts, f.filled, f.data.len())) } else { None }).collect();
    assert_eq!(frames, vec![(0, false, 3), (960, false, 3), (1_920, true, 1), (2_880, true, 1), (3_840, false, 3), (4_800, true, 1), (5_800, false, 3), (6_760, true, 1), (7_680, false, 3)]);
    let plc: Vec<u8> = out.iter().filter_map(|o| if let Output::Frame(f) = o { f.filled.then(|| f.data[0]) } else { None }).collect();
    assert!(plc.iter().all(|t| *t == 0x78 & 0xFC), "앞 TOC 의 config · code 0");
    let closed = out.iter().find_map(|o| if let Output::Segment(s) = o { (s.event == SegmentKind::Closed).then_some(s.filled) } else { None });
    assert_eq!(closed, Some(4));
}

#[test]
fn hole_over_cap_opens_new_segment_without_fill() {
    let mut pl = Pipeline::new(Policy::Utterance).with_gap_max(Duration::from_millis(100));
    let mut out = Vec::new();
    for r in opus_stream(&[(1, 0), (2, 960), (3, 48_000)], 0x78) {
        out.extend(pl.push(&r));
    }
    out.extend(pl.finish());
    let closes: Vec<Option<&str>> = out.iter().filter_map(|o| if let Output::Segment(s) = o { (s.event == SegmentKind::Closed).then_some(s.reason) } else { None }).collect();
    assert_eq!(closes, vec![Some("gap"), Some("end")]);
    assert!(!out.iter().any(|o| matches!(o, Output::Frame(f) if f.filled)));
}

#[test]
fn video_is_never_filled() {
    let out = run_pipe(Policy::Utterance, &stream());
    assert!(!out.iter().any(|o| matches!(o, Output::Frame(f) if f.filled && f.kind == "video")));
}

#[test]
fn ogg_with_filled_frames_spans_rtp_time() {
    let d = tmp("oggfill");
    let mut sink = OggSink::new(&d).unwrap();
    for o in run_pipe(Policy::Utterance, &opus_stream(&[(1, 0), (2, 960), (5, 3_840)], 0x78)) {
        sink.write(&o).unwrap();
    }
    let f = &sink.files()[0];
    assert_eq!((f.packets, f.filled), (5, 2));
    assert_eq!(u64::from(f.end_ts.unwrap() - f.first_ts.unwrap()), f.samples, "메운 뒤 표본 수 = RTP 폭");
}

// ── 4단계(202611/20261110a) — 분기 = 출처 트랙 · 열쇠 = 트랙 · 영상은 키프레임에서 연다 · Matroska 결과물 ──

fn track_list(room: &str, tracks: serde_json::Value, rows: serde_json::Value) -> Vec<u8> {
    serde_json::json!({ "room": room, "tracks": tracks, "rows": rows }).to_string().into_bytes()
}

const KEY: &[u8] = &[0x10, 0x00, 0x9d, 0x01, 0x2a];
const DELTA: &[u8] = &[0x10, 0x01, 7];

fn vrow(tid: &str) -> serde_json::Value {
    serde_json::json!({ "track_id": tid, "kind": "video", "codec": "VP8", "pt": 96 })
}

fn segs(out: &[Output]) -> Vec<(SegmentKind, u64, String, Option<&'static str>)> {
    out.iter()
        .filter_map(|o| match o {
            Output::Segment(s) => Some((s.event, s.id, s.track_id.clone(), s.reason)),
            _ => None,
        })
        .collect()
}

#[test]
fn late_packet_of_departed_track_is_not_mixed_into_new_track() {
    let ms = 1_000_000u64;
    let u1 = serde_json::json!({ "track_id": "cam-u1", "kind": "video", "user_id": "u1", "csrc": 9 });
    let u2 = serde_json::json!({ "track_id": "cam-u2", "kind": "video", "user_id": "u2", "csrc": 10 });
    let recs = [
        rec(1, 0, Channel::Track, track_list("r", serde_json::json!([u1]), serde_json::json!({ "200": vrow("cam-u1") }))),
        rec(2, ms, Channel::Rtp(200), pkt(1, 3000, true, Some(9), KEY)),
        // 재배정 — 줄 200 이 u2 로 · u1 은 목록에서 빠짐
        rec(3, 2 * ms, Channel::Track, track_list("r", serde_json::json!([u2]), serde_json::json!({ "200": vrow("cam-u2") }))),
        rec(4, 3 * ms, Channel::Rtp(200), pkt(2, 6000, true, Some(9), DELTA)),
        rec(5, 4 * ms, Channel::Rtp(200), pkt(3, 9000, true, Some(10), KEY)),
    ];
    let mut pl = Pipeline::new(Policy::Utterance);
    let mut out: Vec<Output> = recs.iter().flat_map(|r| pl.push(r)).collect();
    out.extend(pl.finish());
    let frames: Vec<(String, u32)> = out.iter().filter_map(|o| if let Output::Frame(f) = o { Some((f.track_id.clone(), f.rtp_ts)) } else { None }).collect();
    assert_eq!(frames, vec![("cam-u1".into(), 3000), ("cam-u2".into(), 9000)], "늦은 u1 패킷은 u2 에 섞이지 않는다");
    assert_eq!(pl.counts().late, 1);
    assert_eq!(segs(&out)[1], (SegmentKind::Closed, 0, "cam-u1".into(), Some("track")), "목록에서 빠질 때 닫는다");
}

#[test]
fn track_moving_rows_stays_one_segment() {
    let ms = 1_000_000u64;
    let u1 = serde_json::json!([{ "track_id": "cam-u1", "kind": "video", "user_id": "u1", "csrc": 9 }]);
    let recs = [
        rec(1, 0, Channel::Track, track_list("r", u1.clone(), serde_json::json!({ "200": vrow("cam-u1") }))),
        rec(2, ms, Channel::Rtp(200), pkt(1, 3000, true, Some(9), KEY)),
        rec(3, 2 * ms, Channel::Track, track_list("r", u1, serde_json::json!({ "300": vrow("cam-u1") }))),
        rec(4, 40 * ms, Channel::Rtp(300), pkt(77, 500_000, true, Some(9), KEY)),
    ];
    for p in [Policy::Utterance, Policy::Span { time_ns: u64::MAX, bytes: u64::MAX }] {
        let out = run_pipe(p, &recs);
        assert_eq!(segs(&out), vec![(SegmentKind::Opened, 0, "cam-u1".into(), None), (SegmentKind::Closed, 0, "cam-u1".into(), Some("end"))], "{p:?}");
    }
}

#[test]
fn slot_video_segment_opens_only_at_keyframe_of_new_source() {
    let ms = 1_000_000u64;
    let recs = [
        rec(1, 0, Channel::Track, track_list("r", serde_json::json!([{ "track_id": "slot-v", "kind": "video" }]), serde_json::json!({ "400": vrow("slot-v") }))),
        rec(2, ms, Channel::Rtp(400), pkt(1, 3000, true, Some(7), KEY)),
        rec(3, 2 * ms, Channel::Rtp(400), pkt(2, 6000, true, Some(7), DELTA)),
        rec(4, 3 * ms, Channel::Rtp(400), pkt(3, 9000, true, Some(8), DELTA)),
        rec(5, 4 * ms, Channel::Rtp(400), pkt(4, 12000, true, Some(8), KEY)),
    ];
    let mut pl = Pipeline::new(Policy::Utterance);
    let mut out: Vec<Output> = recs.iter().flat_map(|r| pl.push(r)).collect();
    out.extend(pl.finish());
    let s = segs(&out);
    assert_eq!(s[1], (SegmentKind::Closed, 0, "slot-v".into(), Some("source")));
    assert_eq!(s[2], (SegmentKind::Opened, 1, "slot-v".into(), None));
    let first_of_1 = out.iter().find_map(|o| if let Output::Frame(f) = o && f.segment == Some(1) { Some((f.rtp_ts, f.keyframe)) } else { None });
    assert_eq!(first_of_1, Some((12000, true)), "새 화자의 첫 키프레임부터");
    assert_eq!(pl.counts().before_key, 1);
}

/// 같은 레코드열 → 같은 Matroska 바이트 · 줄이 바뀐 세그먼트는 `log_time` 차로 잇는다 · UTC 없으면 첫 프레임 레코드 시각
fn mkv_build(recs: &[Record], dir: &Path) -> Vec<MkvFile> {
    let mut pl = Pipeline::new(Policy::Utterance);
    let mut sink = MkvSink::new(dir).unwrap();
    for r in recs {
        for o in pl.push(r) {
            sink.write(&o).unwrap();
        }
    }
    for o in pl.finish() {
        sink.write(&o).unwrap();
    }
    sink.files().to_vec()
}

#[test]
fn mkv_sink_is_deterministic_and_rebases_across_rows() {
    let ms = 1_000_000u64;
    let t0 = 1_800_000_000_000_000_000u64;
    let u1 = serde_json::json!([{ "track_id": "cam-u1", "kind": "video", "user_id": "u1", "csrc": 9 }]);
    let recs = [
        rec(1, t0, Channel::Track, track_list("r", u1.clone(), serde_json::json!({ "200": vrow("cam-u1") }))),
        rec(2, t0 + ms, Channel::Rtp(200), pkt(1, 3000, true, Some(9), KEY)),
        rec(3, t0 + 34 * ms, Channel::Rtp(200), pkt(2, 6000, true, Some(9), DELTA)),
        rec(4, t0 + 40 * ms, Channel::Track, track_list("r", u1, serde_json::json!({ "300": vrow("cam-u1") }))),
        rec(5, t0 + 534 * ms, Channel::Rtp(300), pkt(9, 4_000_000_000, true, Some(9), KEY)),
    ];
    let (a, b) = (tmp("mkv-a"), tmp("mkv-b"));
    let fa = mkv_build(&recs, &a);
    let fb = mkv_build(&recs, &b);
    assert_eq!(fa.len(), 1);
    assert_eq!(std::fs::read(&fa[0].path).unwrap(), std::fs::read(&fb[0].path).unwrap(), "같은 레코드열이면 같은 바이트");
    assert_eq!(fa[0].path.file_name().unwrap(), "r-0000-u1-video.mkv");
    assert_eq!((fa[0].frames, fa[0].keyframes, fa[0].rebased, fa[0].synced), (3, 2, 1, 0));
    assert_eq!(fa[0].duration_ms, 33 + 500, "줄을 옮긴 뒤는 레코드 시각 차(500ms)로 잇는다");
    assert_eq!(fa[0].start_ms, Some((t0 + ms) / 1_000_000), "UTC 가 없으면 첫 프레임 레코드 시각");
    std::fs::remove_dir_all(&a).ok();
    std::fs::remove_dir_all(&b).ok();
}

#[test]
fn mkv_start_is_back_computed_from_first_synced_frame() {
    let ms = 1_000_000u64;
    let u1 = serde_json::json!([{ "track_id": "cam-u1", "kind": "video", "user_id": "u1", "csrc": 9 }]);
    // SR — NTP 초 3_000_000_000(1900 기준) ↔ RTP 9000. 첫 프레임(3000)은 SR 전이라 UTC 없음 · 셋째(9000)가 첫 동기
    let ntp = 3_000_000_000u64 << 32;
    let recs = [
        rec(1, 0, Channel::Track, track_list("r", u1, serde_json::json!({ "200": vrow("cam-u1") }))),
        rec(2, ms, Channel::Rtp(200), pkt(1, 3000, true, Some(9), KEY)),
        rec(3, 2 * ms, Channel::Rtp(200), pkt(2, 6000, true, Some(9), DELTA)),
        rec(4, 3 * ms, Channel::Sr, format!(r#"{{"ssrc":200,"ntp":{ntp},"rtp":9000}}"#).into_bytes()),
        rec(5, 4 * ms, Channel::Rtp(200), pkt(3, 9000, true, Some(9), DELTA)),
    ];
    let d = tmp("mkv-sr");
    let f = mkv_build(&recs, &d);
    let utc_ms = (3_000_000_000u64 - 2_208_988_800) * 1_000;
    assert_eq!(f[0].start_ms, Some(utc_ms - 66), "첫 동기 프레임 UTC − 그때까지 흐른 블록 시각(6000 틱 = 66.6ms)");
    assert_eq!(f[0].synced, 1);
    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn mkv_rtp_time_wrap_keeps_blocks_moving_forward() {
    let ms = 1_000_000u64;
    let u1 = serde_json::json!([{ "track_id": "cam-u1", "kind": "video", "user_id": "u1", "csrc": 9 }]);
    let mut recs = vec![rec(1, 0, Channel::Track, track_list("r", u1, serde_json::json!({ "200": vrow("cam-u1") })))];
    let start = u32::MAX - 3 * 3000 + 1;
    for i in 0..8u32 {
        recs.push(rec(2 + i, u64::from(i + 1) * 33 * ms, Channel::Rtp(200), pkt(i as u16, start.wrapping_add(i * 3000), true, Some(9), if i % 4 == 0 { KEY } else { DELTA })));
    }
    let d = tmp("mkv-wrap");
    let f = mkv_build(&recs, &d);
    assert_eq!((f[0].frames, f[0].first_rtp, f[0].last_rtp), (8, Some(start), Some(start.wrapping_add(7 * 3000))));
    assert_eq!(f[0].duration_ms, 7 * 3000 / 90, "넘침을 지나도 앞으로만 간다");
    std::fs::remove_dir_all(&d).ok();
}

// ── 5단계(202611/20261110b) — HLS 결과물(fMP4 · 재생 목록) ──

const SPS_A: &[u8] = &[0x67, 0x42, 0xc0, 0x1f, 0xda, 0x01, 0x40, 0x16, 0xe4];
const SPS_B: &[u8] = &[0x67, 0x42, 0xc0, 0x1f, 0xda, 0x02, 0x80, 0x2d, 0xc8];
const PPS: &[u8] = &[0x68, 0xce, 0x3c, 0x80];

/// 사람 u1 — 영상(H264 · 줄 200 · CSRC 9) · 음성(Opus · 줄 100 · CSRC 10) · 같은 시계에서 `secs` 초 · 영상 키 1초마다 · SR 은 0.5초에(둘 다)
fn av_stream(secs: u32, start_v: u32, sps_switch_at: Option<u32>) -> Vec<Record> {
    let ms = 1_000_000u64;
    let t0 = 1_800_000_000_000_000_000u64;
    let tracks = serde_json::json!([
        { "track_id": "cam-u1", "kind": "video", "user_id": "u1", "csrc": 9 },
        { "track_id": "mic-u1", "kind": "audio", "user_id": "u1", "csrc": 10 }
    ]);
    let rows = serde_json::json!({ "200": { "track_id": "cam-u1", "kind": "video", "codec": "H264", "pt": 102 }, "100": { "track_id": "mic-u1", "kind": "audio", "codec": "opus", "pt": 111 } });
    let mut v = vec![rec(1, t0, Channel::Track, track_list("r", tracks, rows))];
    let mut n = 2u32;
    let (mut vseq, mut aseq) = (0u16, 0u16);
    // 음성은 20ms 마다(960 틱) · 영상은 33ms 마다 한 프레임(3000 틱 = 1/30 초 — 받은 시각만 ms 로 근사)
    let end_ms = secs * 1000;
    let mut tv = 0u32;
    let mut ta = 0u32;
    let ntp = |unix_ns: u64| -> u64 { (((unix_ns / 1_000_000_000) + 2_208_988_800) << 32) | (((unix_ns % 1_000_000_000) << 32) / 1_000_000_000) };
    let mut sr_done = false;
    while tv.min(ta) < end_ms {
        if !sr_done && tv.min(ta) >= 500 {
            sr_done = true;
            let at = t0 + 500 * ms;
            v.push(rec(n, at, Channel::Sr, format!(r#"{{"ssrc":200,"ntp":{},"rtp":{}}}"#, ntp(at), start_v.wrapping_add(500 * 90)).into_bytes()));
            n += 1;
            v.push(rec(n, at, Channel::Sr, format!(r#"{{"ssrc":100,"ntp":{},"rtp":{}}}"#, ntp(at), 500 * 48).into_bytes()));
            n += 1;
        }
        if tv <= ta {
            let i = tv / 33;
            let key = i.is_multiple_of(30);
            let ts = start_v.wrapping_add(i * 3000);
            let sps = if sps_switch_at.is_some_and(|s| tv >= s) { SPS_B } else { SPS_A };
            let (idr, delta) = ([0x65, 0x88, 0x84, i as u8], [0x41, 0x9a, i as u8]);
            let nal: Vec<&[u8]> = if key { vec![sps, PPS, &idr] } else { vec![&delta] };
            for (k, x) in nal.iter().enumerate() {
                v.push(rec(n, t0 + u64::from(tv) * ms, Channel::Rtp(200), pkt(vseq, ts, k + 1 == nal.len(), Some(9), x)));
                n += 1;
                vseq = vseq.wrapping_add(1);
            }
            tv += 33;
        } else {
            v.push(rec(n, t0 + u64::from(ta) * ms, Channel::Rtp(100), pkt(aseq, ta * 48, true, Some(10), &[0x78, aseq as u8])));
            n += 1;
            aseq = aseq.wrapping_add(1);
            ta += 20;
        }
    }
    v
}

fn hls_build(recs: &[Record], dir: &Path) -> (Vec<HlsStream>, Vec<String>) {
    let mut pl = Pipeline::new(Policy::Span { time_ns: u64::MAX, bytes: u64::MAX });
    let mut sink = HlsSink::new(dir, HlsConfig::default()).unwrap();
    let mut asked = Vec::new();
    for r in recs {
        for o in pl.push(r) {
            sink.write(&o).unwrap();
        }
        asked.extend(sink.take_keyframe_requests());
    }
    for o in pl.finish() {
        sink.write(&o).unwrap();
    }
    sink.finish().unwrap();
    (sink.streams().to_vec(), asked)
}

/// 트랙 번호 · tfdt · [(길이, 크기, 플래그)]
type MoofTraf = (u32, u64, Vec<(u32, u32, u32)>);

/// fMP4 조각 → 트랙마다 (tfdt, [(길이, 크기, 플래그)]) — 상자를 걸어 읽는다
fn read_moof(b: &[u8]) -> Vec<MoofTraf> {
    fn boxes(b: &[u8]) -> Vec<([u8; 4], &[u8])> {
        let mut out = Vec::new();
        let mut i = 0;
        while i + 8 <= b.len() {
            let n = u32::from_be_bytes(b[i..i + 4].try_into().unwrap()) as usize;
            out.push((b[i + 4..i + 8].try_into().unwrap(), &b[i + 8..i + n]));
            i += n;
        }
        out
    }
    let mut out = Vec::new();
    for (t, body) in boxes(b) {
        if &t != b"moof" {
            continue;
        }
        for (t, traf) in boxes(body) {
            if &t != b"traf" {
                continue;
            }
            let (mut id, mut tfdt, mut rows) = (0, 0, Vec::new());
            for (t, x) in boxes(traf) {
                match &t {
                    b"tfhd" => id = u32::from_be_bytes(x[4..8].try_into().unwrap()),
                    b"tfdt" => tfdt = u64::from_be_bytes(x[4..12].try_into().unwrap()),
                    b"trun" => {
                        let n = u32::from_be_bytes(x[4..8].try_into().unwrap()) as usize;
                        for k in 0..n {
                            let o = 12 + k * 12;
                            let r = |a: usize| u32::from_be_bytes(x[a..a + 4].try_into().unwrap());
                            rows.push((r(o), r(o + 4), r(o + 8)));
                        }
                    }
                    _ => {}
                }
            }
            out.push((id, tfdt, rows));
        }
    }
    out
}

#[test]
fn hls_cuts_at_keyframe_after_target_with_continuous_decode_time() {
    let d = tmp("hls-av");
    let (st, asked) = hls_build(&av_stream(11, 1000, None), &d);
    assert_eq!(st.len(), 1, "사람 하나 = 묶음 하나");
    let s = &st[0];
    assert_eq!(s.base, Some("utc"), "두 트랙 모두 SR 이 있다");
    assert_eq!(s.inits.len(), 1);
    assert_eq!(s.segments.iter().map(|x| x.duration.round() as u64).collect::<Vec<_>>(), vec![4, 4, 3], "4초 지난 첫 키(1초마다)에서");
    assert!(s.segments.iter().all(|x| !x.discontinuity));
    assert_eq!(s.over_target, 0);
    assert_eq!(asked.len(), 3, "조각마다 T − 1초에 한 번");
    let mut next: HashMap<u32, u64> = HashMap::new();
    for x in &s.segments {
        let m = read_moof(&std::fs::read(&x.file).unwrap());
        let v = m.iter().find(|t| t.0 == 1).unwrap();
        assert_eq!(v.2[0].2, 0x0200_0000, "조각의 첫 영상 샘플은 키");
        for (id, tfdt, rows) in &m {
            if let Some(want) = next.get(id) {
                assert_eq!(tfdt, want, "트랙 {id} 디코드 시각이 앞 조각 끝에서 잇는다");
            }
            next.insert(*id, tfdt + rows.iter().map(|r| u64::from(r.0)).sum::<u64>());
        }
    }
    let ev = std::fs::read_to_string(&s.event_playlist).unwrap();
    assert!(ev.contains("#EXT-X-PLAYLIST-TYPE:EVENT") && ev.contains("#EXT-X-TARGETDURATION:6") && ev.trim_end().ends_with("#EXT-X-ENDLIST"));
    assert_eq!(ev.matches("#EXTINF").count(), 3);
    assert_eq!(ev.matches("#EXT-X-MAP").count(), 1);
    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn hls_samples_drop_parameter_sets_and_init_carries_them() {
    let d = tmp("hls-ps");
    let (st, _) = hls_build(&av_stream(5, 1000, None), &d);
    let init = std::fs::read(&st[0].inits[0]).unwrap();
    assert!(init.windows(SPS_A.len()).any(|w| w == SPS_A), "avcC 에 SPS");
    let seg = std::fs::read(&st[0].segments[0].file).unwrap();
    assert!(!seg.windows(SPS_A.len()).any(|w| w == SPS_A), "샘플에는 SPS 가 없다(avc1)");
    assert!(seg.windows(8).any(|w| w == [0, 0, 0, 4, 0x65, 0x88, 0x84, 0]), "IDR 은 길이 앞붙임");
    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn hls_is_deterministic_for_same_records() {
    let (a, b) = (tmp("hls-d1"), tmp("hls-d2"));
    let recs = av_stream(9, 1000, None);
    let (sa, _) = hls_build(&recs, &a);
    let (sb, _) = hls_build(&recs, &b);
    let files = |s: &HlsStream| -> Vec<PathBuf> { s.inits.iter().chain(s.segments.iter().map(|x| &x.file)).chain([&s.event_playlist, &s.live_playlist]).cloned().collect() };
    for (x, y) in files(&sa[0]).iter().zip(files(&sb[0]).iter()) {
        assert_eq!(x.file_name(), y.file_name());
        assert_eq!(std::fs::read(x).unwrap(), std::fs::read(y).unwrap(), "{}", x.display());
    }
    std::fs::remove_dir_all(&a).ok();
    std::fs::remove_dir_all(&b).ok();
}

#[test]
fn hls_parameter_change_opens_new_init_with_discontinuity() {
    let d = tmp("hls-res");
    let (st, _) = hls_build(&av_stream(10, 1000, Some(5000)), &d);
    let s = &st[0];
    assert_eq!(s.inits.len(), 2, "SPS 가 바뀐 키프레임에서 새 init");
    let k = s.segments.iter().position(|x| x.discontinuity).expect("끊김 조각");
    assert_ne!(s.segments[k].init, s.segments[k - 1].init);
    let ev = std::fs::read_to_string(&s.event_playlist).unwrap();
    assert_eq!(ev.matches("#EXT-X-DISCONTINUITY\n").count(), 1);
    assert_eq!(ev.matches("#EXT-X-MAP").count(), 2);
    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn hls_rtp_wrap_is_unfolded_not_a_discontinuity() {
    let d = tmp("hls-wrap");
    let (st, _) = hls_build(&av_stream(9, u32::MAX - 90_000 * 3, None), &d);
    let s = &st[0];
    assert!(s.segments.iter().all(|x| !x.discontinuity), "넘침은 펼친다");
    let mut last = 0;
    for x in &s.segments {
        let v = read_moof(&std::fs::read(&x.file).unwrap()).into_iter().find(|t| t.0 == 1).unwrap();
        assert!(v.1 >= last, "영상 디코드 시각이 앞으로만");
        last = v.1;
    }
    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn hls_refuses_non_h264_video_with_reason() {
    let d = tmp("hls-vp8");
    let ms = 1_000_000u64;
    let u1 = serde_json::json!([{ "track_id": "cam-u1", "kind": "video", "user_id": "u1", "csrc": 9 }]);
    let recs = [
        rec(1, 0, Channel::Track, track_list("r", u1, serde_json::json!({ "200": vrow("cam-u1") }))),
        rec(2, ms, Channel::Rtp(200), pkt(1, 3000, true, Some(9), KEY)),
    ];
    let (st, _) = hls_build(&recs, &d);
    assert!(st[0].refused.as_deref().is_some_and(|r| r.contains("VP8")));
    assert!(st[0].segments.is_empty() && !d.join("r-u1").exists(), "거절한 묶음은 아무것도 쓰지 않는다");
    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn hls_parameter_change_before_first_segment_keeps_old_parameters_for_earlier_frames() {
    // 첫 조각(4초)을 쓰기 전(2초)에 SPS 가 바뀐다 — 앞 프레임이 새 SPS 의 init 에 들면 디코드가 깨진다(media_check 실측 20261110)
    let d = tmp("hls-res-early");
    let (st, _) = hls_build(&av_stream(8, 1000, Some(2000)), &d);
    let s = &st[0];
    assert_eq!(s.inits.len(), 2);
    assert!(std::fs::read(&s.inits[0]).unwrap().windows(SPS_A.len()).any(|w| w == SPS_A), "첫 init 은 앞 SPS");
    assert!(std::fs::read(&s.inits[1]).unwrap().windows(SPS_B.len()).any(|w| w == SPS_B), "둘째 init 은 바뀐 SPS");
    assert!(s.segments[1].discontinuity && (s.segments[0].duration - 3.0).abs() < 0.01, "바뀐 첫 키프레임(2초 뒤 첫 키 = 90번째 프레임 · 3.0초)에서 자른다");
    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn hls_audio_decode_time_is_cumulative_across_segments_despite_plc_remainder() {
    // 무전 슬롯 화자 바뀜처럼 RTP 시각이 프레임 배수가 아닌 폭(+672)으로 뛰면 PLC 가 메우고 나머지가 남는다 —
    // 조각 사이 디코드 시각은 견본 길이 누적으로 잇는다(2층 peer_record_hls_ptt 실측 20261110 — 672 표본 끊김)
    let ms = 1_000_000u64;
    let tracks = serde_json::json!([{ "track_id": "slot-a", "kind": "audio" }]);
    let rows = serde_json::json!({ "100": { "track_id": "slot-a", "kind": "audio", "codec": "opus", "pt": 111 } });
    let mut recs = vec![rec(1, 0, Channel::Track, track_list("r", tracks, rows))];
    let mut ts = 0u32;
    for i in 0..500u32 {
        if i == 100 {
            ts += 672 + 960 * 3;
        }
        recs.push(rec(2 + i, u64::from(i) * 20 * ms, Channel::Rtp(100), pkt(i as u16, ts, true, Some(7), &[0x78, i as u8])));
        ts += 960;
    }
    let d = tmp("hls-plc");
    let (st, _) = hls_build(&recs, &d);
    let s = &st[0];
    assert!(s.segments.len() >= 2);
    let mut next: Option<u64> = None;
    for x in &s.segments {
        let m = read_moof(&std::fs::read(&x.file).unwrap());
        let a = m.iter().find(|t| t.0 == 2).unwrap();
        if let Some(n) = next {
            assert_eq!(a.1, n, "{} 음성 디코드 시각이 앞 조각 끝에서 잇는다", x.file.display());
        }
        next = Some(a.1 + a.2.iter().map(|r| u64::from(r.0)).sum::<u64>());
    }
    std::fs::remove_dir_all(&d).ok();
}


// ── 시험 단계 T2(202611/20261110d) — RTP seq 넘침(설계 §10-3 「seq 넘침도 같은 방식」) ──

#[test]
fn pipeline_assembles_video_frame_across_seq_wrap_and_ogg_keeps_range() {
    let ms = 1_000_000u64;
    let u1 = serde_json::json!([{ "track_id": "cam-u1", "kind": "video", "user_id": "u1", "csrc": 9 }]);
    let mut recs = vec![rec(1, 0, Channel::Track, track_list("r", u1, serde_json::json!({ "200": vrow("cam-u1") })))];
    // 키프레임 한 장이 조각 셋 — seq 65535 · 0 · 1(marker) · 다음 델타 2
    recs.push(rec(2, ms, Channel::Rtp(200), pkt(65535, 3000, false, Some(9), &[0x10, 0x00, 0x9d, 1])));
    recs.push(rec(3, ms, Channel::Rtp(200), pkt(0, 3000, false, Some(9), &[0x00, 2])));
    recs.push(rec(4, ms, Channel::Rtp(200), pkt(1, 3000, true, Some(9), &[0x00, 3])));
    recs.push(rec(5, 2 * ms, Channel::Rtp(200), pkt(2, 6000, true, Some(9), DELTA)));
    let mut pl = Pipeline::new(Policy::Utterance);
    let out: Vec<Output> = recs.iter().flat_map(|r| pl.push(r)).collect();
    let fr: Vec<(u16, u16, bool, Vec<u8>)> = out.iter().filter_map(|o| if let Output::Frame(f) = o { Some((f.seq_first, f.seq_last, f.keyframe, f.data.clone())) } else { None }).collect();
    assert_eq!(fr[0], (65535, 1, true, vec![0x00, 0x9d, 1, 2, 3]), "넘침을 가로지른 조각 셋이 한 프레임");
    assert_eq!((fr[1].0, fr[1].1), (2, 2));
    assert_eq!(pl.counts().broken, 0);

    // 발언 파일(Ogg) — seq 범위가 넘침을 가로질러도 첫 · 끝이 그대로
    let tracks = serde_json::json!([{ "track_id": "slot-a", "kind": "audio" }]);
    let rows = serde_json::json!({ "100": { "track_id": "slot-a", "kind": "audio", "codec": "opus", "pt": 111 } });
    let mut a = vec![rec(1, 0, Channel::Track, track_list("r", tracks, rows))];
    for (i, sq) in [65533u16, 65534, 65535, 0, 1, 2].into_iter().enumerate() {
        a.push(rec(2 + i as u32, i as u64 * 20 * ms, Channel::Rtp(100), pkt(sq, i as u32 * 960, true, Some(7), &[0x78, i as u8])));
    }
    let d = tmp("seq-wrap-ogg");
    let mut pl = Pipeline::new(Policy::Utterance);
    let mut sink = OggSink::new(&d).unwrap();
    for r in &a {
        for o in pl.push(r) {
            sink.write(&o).unwrap();
        }
    }
    for o in pl.finish() {
        sink.write(&o).unwrap();
    }
    let f = &sink.files()[0];
    assert_eq!((f.packets, f.first_seq, f.last_seq, f.filled), (6, Some(65533), Some(2), 0));
    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn hls_same_time_frame_on_layer_switch_replaces_previous_sample() {
    // 시뮬캐스트 층 바꿈 — 4초 지점 h 키프레임(T 로 자르는 프레임) 바로 뒤(+90 틱 = 1ms · 2층 실측)에 l 키프레임(다른 SPS)이 온다.
    // 같은 순간의 그림이니 앞 견본을 바꿔 끼워 1ms 조각이 없어야 한다(2층 peer_record_hls_simulcast 실측 20261110 — seg-00001 0.001초)
    let ms = 1_000_000u64;
    let tracks = serde_json::json!([{ "track_id": "cam-u1", "kind": "video", "user_id": "u1", "csrc": 9 }]);
    let rows = serde_json::json!({ "200": { "track_id": "cam-u1", "kind": "video", "codec": "H264", "pt": 102 } });
    for off in [0u32, 90] {
        let mut recs = vec![rec(1, 0, Channel::Track, track_list("r", tracks.clone(), rows.clone()))];
        let (mut n, mut seq) = (2u32, 0u16);
        let mut put = |recs: &mut Vec<Record>, ts: u32, i: u32, sps: Option<&[u8]>| {
            let idr = [0x65, 0x88, i as u8];
            let delta = [0x41, 0x9a, i as u8];
            let nal: Vec<&[u8]> = match sps {
                Some(s) => vec![s, PPS, &idr],
                None => vec![&delta],
            };
            for (k, x) in nal.iter().enumerate() {
                recs.push(rec(n, u64::from(i) * 33 * ms, Channel::Rtp(200), pkt(seq, ts, k + 1 == nal.len(), Some(9), x)));
                n += 1;
                seq = seq.wrapping_add(1);
            }
        };
        let mut base = 0u32;
        for i in 0..240u32 {
            let key = i.is_multiple_of(30);
            if i == 120 {
                put(&mut recs, i * 3000, i, Some(SPS_A));
                base = off;
            }
            put(&mut recs, i * 3000 + base, i, if key { Some(if i < 120 { SPS_A } else { SPS_B }) } else { None });
        }
        let d = tmp(&format!("hls-dup-{off}"));
        let (st, _) = hls_build(&recs, &d);
        let s = &st[0];
        assert_eq!(s.replaced, 1, "+{off} 틱 — 같은 순간 앞 견본을 바꿔 꼈다");
        assert!(s.segments.iter().all(|x| x.duration > 0.5), "+{off} 틱 — 견본 하나짜리 조각이 없다 {:?}", s.segments.iter().map(|x| x.duration).collect::<Vec<_>>());
        assert_eq!((s.inits.len(), s.segments.iter().filter(|x| x.discontinuity).count()), (2, 1));
        std::fs::remove_dir_all(&d).ok();
    }
}
