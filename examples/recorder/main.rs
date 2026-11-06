// author: kodeholic (powered by Claude)
//! 예제 녹음기 — 서버사이드 SDK(oxpeer)로 방에 녹음 참가자(`participant_type:1`)로 들어가 ★발언 한 건마다 Ogg Opus 파일 하나를 쓴다.
//! 발언 경계는 발언권 화자 사건(`Speaker`)이 정한다 — 화자가 서면 새 파일 · 사라지거나 바뀌면 닫는다. 무전 슬롯 오디오(PTT)를 받는다.
//!
//!   cargo run --example recorder -- --base http://127.0.0.1:1974/media --room ch-1 --out ./rec [--secs 30] [--hidden] [--user rec-1]
//!
//! 끝나면 표준 출력에 한 줄 JSON(발언마다 파일 · 화자 · 패킷 수 · 경계 밖 패킷 수 · 미디어 계수)을 쓴다 — 2층 시험이 이것과 파일을 읽는다.

mod ogg;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use oxpeer::{Client, Config, Event, TokenReq};
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
}

fn args() -> Args {
    let mut a = Args { base: String::new(), room: String::new(), out: PathBuf::from("."), secs: None, hidden: false, user: "oxpeer-recorder".into(), api_key: "ox_k_demo".into(), api_secret: "ox_s_demo".into() };
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
            other => eprintln!("모르는 인자 {other}"),
        }
    }
    if a.base.is_empty() || a.room.is_empty() {
        eprintln!("사용: recorder --base http://host:port/media --room <방> [--out 폴더] [--secs N] [--hidden] [--user id]");
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
    let (client, mut events, mut media) = match Client::connect(Config::new(&a.base, token)).await {
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
    // 경계 밖 패킷마다 — seq · 서버 침묵 프레임인가 · 마지막 화자 사건(taken · idle · none)과 그 뒤 경과
    let mut orphans: Vec<Value> = Vec::new();
    let mut last_speaker: (&str, Instant) = ("none", Instant::now());
    let mut seen_events: Vec<String> = Vec::new();
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
                    Event::Closed { code, name, .. } => {
                        eprintln!("[recorder] 세션이 끝났다 {code} {name}");
                        break;
                    }
                    _ => {}
                }
                seen_events.push(format!("{e:?}").chars().take(160).collect());
            }
            p = media.next() => {
                let Some(p) = p else { break };
                if p.room != a.room || p.kind != "audio" || !p.codec.eq_ignore_ascii_case("opus") {
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
    let out = json!({
        "room": a.room,
        "utterances": done,
        "orphan_packets": orphan,
        "orphans": orphans,
        "media_dropped": stats.media_dropped,
        "media_unrouted": stats.media_unrouted,
        "decrypt_fail": stats.decrypt_fail,
        "events": seen_events,
    });
    println!("{out}");
}
