// author: kodeholic (powered by Claude)
//! 1층 — 녹음 원시 기록(SDK:RULE-PLAT-REC-RAW · REC-CLOCK · REC-PART): 왕복 · 회전 · 사슬 · 머리 스냅숏 · 잘린 파트 · 디스크 하한

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::sync::mpsc;

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
