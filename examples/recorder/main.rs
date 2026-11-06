// author: kodeholic (powered by Claude)
//! 예제 녹음 · 녹화기 — 서버사이드 SDK(oxpeer)로 방에 녹음 참가자(`participant_type:1`)로 들어간다.
//! 기본(녹음) — ★발언 한 건마다 Ogg Opus 파일 하나. 발언 경계는 발언권 화자 사건(`Speaker`)이 정한다 — 화자가 서면 새 파일 · 사라지거나 바뀌면 닫는다.
//!   무전 슬롯 오디오(PTT)만 담는다(개인 트랙은 센다).
//! `--video`(녹화 · 20261107a) — ★트랙마다 Matroska 파일 하나(영상 · 음성 모두 · SDK 가 조립한 프레임 · 영상은 키프레임부터). 무전 슬롯은 발언 한 건 = 파일 하나.
//!   `--spatial N` 은 영상 트랙의 층 상한(기본은 최고 단) · `--layer-plan 4:0,8:1` 은 입장 뒤 그 초에 상한을 바꾼다.
//!
//!   cargo run --example recorder -- --base http://127.0.0.1:1974/media --room ch-1 --out ./rec [--secs 30] [--hidden] [--user rec-1] [--video [--spatial N] [--layer-plan S:N,..]]
//!
//! 끝나면 표준 출력에 한 줄 JSON(파일마다 화자 · 수 · 경계 밖 수 · 정리층 계수 · 복구 사실)을 쓴다 — 2층 시험이 이것과 파일을 읽는다.

mod mkv;
mod ogg;
mod video;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use oxpeer::{Client, Config, Event, TokenReq, Unit};
use serde_json::{Value, json};

struct Args {
    base: String,
    room: String,
    out: PathBuf,
    secs: Option<u64>,
    hidden: bool,
    user: String,
    api_key: String,
    api_secret: String,
    video: bool,
    spatial: Option<u8>,
    layer_plan: Vec<(u64, u8)>,
}

fn args() -> Args {
    let mut a = Args { base: String::new(), room: String::new(), out: PathBuf::from("."), secs: None, hidden: false, user: "oxpeer-recorder".into(), api_key: "ox_k_demo".into(), api_secret: "ox_s_demo".into(), video: false, spatial: None, layer_plan: Vec::new() };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let mut v = || it.next().unwrap_or_default();
        match k.as_str() {
            "--base" => a.base = v(),
            "--room" => a.room = v(),
            "--out" => a.out = PathBuf::from(v()),
            "--secs" => a.secs = v().parse().ok(),
            "--hidden" => a.hidden = true,
            "--user" => a.user = v(),
            "--api-key" => a.api_key = v(),
            "--api-secret" => a.api_secret = v(),
            "--video" => a.video = true,
            "--spatial" => a.spatial = v().parse().ok(),
            "--layer-plan" => {
                a.layer_plan = v().split(',').filter_map(|x| x.split_once(':')).filter_map(|(t, n)| Some((t.parse().ok()?, n.parse().ok()?))).collect();
            }
            other => eprintln!("모르는 인자 {other}"),
        }
    }
    if a.base.is_empty() || a.room.is_empty() {
        eprintln!("사용: recorder --base http://host:port/media --room <방> [--out 폴더] [--secs N] [--hidden] [--user id] [--video [--spatial N] [--layer-plan S:N,..]]");
        std::process::exit(2);
    }
    a
}

struct Utterance {
    file: PathBuf,
    speaker: String,
    w: ogg::OggOpus,
    first_seq: Option<u16>,
    last_seq: u16,
}

fn close(cur: &mut Option<Utterance>, done: &mut Vec<Value>) {
    if let Some(u) = cur.take() {
        let packets = u.w.packets;
        let granule = u.w.finish().unwrap_or(0);
        done.push(json!({ "file": u.file, "speaker": u.speaker, "packets": packets, "samples": granule, "first_seq": u.first_seq, "last_seq": u.last_seq }));
    }
}

#[tokio::main]
async fn main() {
    let a = args();
    std::fs::create_dir_all(&a.out).expect("출력 폴더");
    let req = TokenReq { api_key: a.api_key.clone(), api_secret: a.api_secret.clone(), user_id: a.user.clone(), participant_type: Some(1), hidden: Some(a.hidden), metadata: None, permission: None };
    let token = match oxpeer::issue_token(&a.base, &req).await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("토큰 발급 실패: {e}");
            std::process::exit(1);
        }
    };
    let mut cfg = Config::new(&a.base, token);
    if a.video {
        cfg.unit = Unit::Frame;
    }
    let (client, mut events, mut media, frames) = match Client::connect(cfg).await {
        Ok(x) => x,
        Err(e) => {
            eprintln!("접속 실패: {e}");
            std::process::exit(1);
        }
    };
    if let Err(e) = client.join(&a.room).await {
        eprintln!("입장 실패: {e}");
        std::process::exit(1);
    }
    eprintln!("[recorder] {} 에 들어왔다 — 발언을 기다린다", a.room);
    if a.video {
        let out = video::run(&a, client, events, frames).await;
        println!("{out}");
        return;
    }
    let stop = async {
        match a.secs {
            Some(s) => tokio::time::sleep(Duration::from_secs(s)).await,
            None => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    };
    tokio::pin!(stop);
    let mut cur: Option<Utterance> = None;
    let mut done: Vec<Value> = Vec::new();
    let mut n = 0u32;
    let mut orphan = 0u64;
    let mut other_audio = 0u64;
    // 경계 밖 패킷마다 — seq · 서버 침묵 프레임인가 · 마지막 화자 사건(taken · idle · none)과 그 뒤 경과
    let mut orphans: Vec<Value> = Vec::new();
    let mut last_speaker: (&str, Instant) = ("none", Instant::now());
    let mut seen_events: Vec<String> = Vec::new();
    let mut flow = Flow::default();
    loop {
        tokio::select! {
            _ = &mut stop => break,
            e = events.next() => {
                let Some(e) = e else { break };
                match &e {
                    Event::Speaker { room, user_id } if *room == a.room => {
                        last_speaker = (if user_id.is_some() { "taken" } else { "idle" }, Instant::now());
                        close(&mut cur, &mut done);
                        if let Some(u) = user_id {
                            n += 1;
                            let file = a.out.join(format!("{}-{n:03}-{u}.ogg", a.room));
                            match ogg::OggOpus::create(&file, n) {
                                Ok(w) => cur = Some(Utterance { file, speaker: u.clone(), w, first_seq: None, last_seq: 0 }),
                                Err(err) => eprintln!("파일을 못 만들었다 {}: {err}", file.display()),
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
            p = media.next() => {
                let Some(p) = p else { break };
                if p.room != a.room || p.kind != "audio" || !p.codec.eq_ignore_ascii_case("opus") {
                    continue;
                }
                // 무전 슬롯 오디오만 담는다(주인 없는 트랙 — 화자는 발언권이 알린다) · 개인 트랙(전이중)은 세기만
                if p.user_id.is_some() {
                    other_audio += 1;
                    continue;
                }
                match cur.as_mut() {
                    Some(u) => {
                        let _ = u.w.push(&p.payload);
                        u.first_seq.get_or_insert(p.sequence);
                        u.last_seq = p.sequence;
                    }
                    None => {
                        orphan += 1;
                        orphans.push(json!({ "seq": p.sequence, "silence": p.payload.as_ref() == [0xF8, 0xFF, 0xFE], "last": last_speaker.0, "since_ms": last_speaker.1.elapsed().as_millis() as u64 }));
                    }
                }
            }
        }
    }
    close(&mut cur, &mut done);
    let stats = client.stats().await;
    client.close().await;
    let mut out = json!({
        "room": a.room,
        "utterances": done,
        "orphan_packets": orphan,
        "orphans": orphans,
        "other_audio": other_audio,
        "events": seen_events,
    });
    flow.fill(&mut out, &stats);
    println!("{out}");
}

/// 복구 사실 — 세션 · 연결 상태가 바뀐 차례(같은 값이 이어지면 한 번) · 다시 세움 · 입장 · 재동기 횟수(2층 복구 시나리오가 읽는다)
#[derive(Default)]
pub struct Flow {
    session: Vec<String>,
    link: Vec<String>,
    rebuilt: u32,
    joined: u32,
    resync: u32,
    join_failed: Vec<u16>,
}

impl Flow {
    /// 사건 하나를 센다 — 세션이 끝났으면 true
    pub fn on(&mut self, e: &Event, room: &str) -> bool {
        match e {
            Event::Session(i) => {
                let s = format!("{:?}", i.state);
                if self.session.last() != Some(&s) {
                    self.session.push(s);
                }
            }
            Event::Link(l) => {
                let s = format!("{:?}", l.state);
                if self.link.last() != Some(&s) {
                    self.link.push(s);
                }
            }
            Event::Rebuilt => self.rebuilt += 1,
            Event::Joined { room: r } if r == room => self.joined += 1,
            Event::Resync { room: r } if r == room => self.resync += 1,
            Event::JoinFailed { room: r, error } if r == room => self.join_failed.push(error.code),
            Event::Closed { code, name, .. } => {
                eprintln!("[recorder] 세션이 끝났다 {code} {name}");
                return true;
            }
            _ => {}
        }
        false
    }

    pub fn fill(&self, out: &mut Value, st: &oxpeer::Stats) {
        let m = json!({
            "media_dropped": st.media_dropped,
            "media_unrouted": st.media_unrouted,
            "decrypt_fail": st.decrypt_fail,
            "frames_dropped": st.frames_dropped,
            "nack_sent": st.nack_sent,
            "rtx_recovered": st.rtx_recovered,
            "lost": st.lost,
            "pli_sent": st.pli_sent,
            "rr_sent": st.rr_sent,
            "frames_broken": st.frames_broken,
            "frames_skipped": st.frames_skipped,
            "qa_dropped": st.qa_dropped,
            "session_flow": self.session,
            "link_flow": self.link,
            "rebuilt": self.rebuilt,
            "joined": self.joined,
            "resync": self.resync,
            "join_failed": self.join_failed,
        });
        if let (Some(o), Some(m)) = (out.as_object_mut(), m.as_object()) {
            for (k, v) in m {
                o.insert(k.clone(), v.clone());
            }
        }
    }
}
