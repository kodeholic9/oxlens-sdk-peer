// author: kodeholic (powered by Claude)
//! 예제 에이전트(context 202611/20261107c C9) — 서버사이드 SDK(oxpeer)로 방에 봇 참가자(`participant_type:2`)로 들어가 ★말한다.
//! `echo`(기본) — 무전방 되울림: 화자의 발언(무전 슬롯 오디오)을 받아 두었다가 그가 놓으면 발언권을 눌러 허가 사이에 그대로 되돌려 말하고 놓는다.
//!   받기 · 누르기 · 말하기(반이중 발행 · 송신 시계) · 놓기를 한 판에 다 지난다. 서버 침묵 프레임(`f8 ff fe`)은 화자 소리가 아니라 담지 않는다.
//! `beacon` — 전이중 발행 시험판: 표지 프레임(오디오 20ms · 영상 30fps · 2층 봇과 같은 본문 꼴 — 보낸 표지 · 번호)을 낸다. 키프레임 요청(PLI)이 오면 다음 영상 프레임을 키프레임으로.
//!   ★표지 프레임은 코덱 본문이 아니다(서버는 본문을 읽지 않는다) — 2층 시험이 「보낸 것 = 받은 것」을 견주는 재료다.
//!
//!   cargo run --example agent -- --base http://127.0.0.1:1974/media --room ch-1 [--secs 30] [--user agent-1] [--mode echo|beacon] [--video]
//!
//! 끝나면 표준 출력에 한 줄 JSON(되울림마다 · 발언권 사건 · 키프레임 요청 · 보내는 쪽 계수)을 쓴다 — 2층 시험이 읽는다.

use std::time::{Duration, Instant};

use bytes::Bytes;
use oxpeer::{Client, Config, Duplex, Event, Kind, LocalTrack, OutFrame, TokenReq, TrackSpec};
use serde_json::{Value, json};

struct Args {
    base: String,
    room: String,
    secs: Option<u64>,
    user: String,
    mode: String,
    video: bool,
    api_key: String,
    api_secret: String,
}

fn args() -> Args {
    let mut a = Args { base: String::new(), room: String::new(), secs: None, user: "oxpeer-agent".into(), mode: "echo".into(), video: false, api_key: "ox_k_demo".into(), api_secret: "ox_s_demo".into() };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let mut v = || it.next().unwrap_or_default();
        match k.as_str() {
            "--base" => a.base = v(),
            "--room" => a.room = v(),
            "--secs" => a.secs = v().parse().ok(),
            "--user" => a.user = v(),
            "--mode" => a.mode = v(),
            "--video" => a.video = true,
            "--api-key" => a.api_key = v(),
            "--api-secret" => a.api_secret = v(),
            other => eprintln!("모르는 인자 {other}"),
        }
    }
    if a.base.is_empty() || a.room.is_empty() {
        eprintln!("사용: agent --base http://host:port/media --room <방> [--secs N] [--user id] [--mode echo|beacon] [--video]");
        std::process::exit(2);
    }
    a
}

const OPUS_20MS: Duration = Duration::from_millis(20);
const SILENCE: [u8; 3] = [0xF8, 0xFF, 0xFE];
/// 표지 본문의 「보낸 이」 — 에이전트는 자기 ssrc 를 모른다(SDK 몫) · 표지 쪽 고정값
const BEACON_TAG_AUDIO: u32 = 0xA6E7_0001;
const BEACON_TAG_VIDEO: u32 = 0xA6E7_0002;

fn beacon_audio(n: u32) -> Bytes {
    let mut b = SILENCE.to_vec();
    b.extend(BEACON_TAG_AUDIO.to_be_bytes());
    b.extend((n as u16).to_be_bytes());
    b.extend([0u8; 14]);
    Bytes::from(b)
}

fn beacon_video(n: u32, key: bool) -> Bytes {
    let mut b = vec![if key { 0x10 } else { 0x11 }, 0x00];
    b.extend(BEACON_TAG_VIDEO.to_be_bytes());
    b.extend(n.to_be_bytes());
    b.extend([0u8; 51]);
    Bytes::from(b)
}

#[tokio::main]
async fn main() {
    let a = args();
    let req = TokenReq { api_key: a.api_key.clone(), api_secret: a.api_secret.clone(), user_id: a.user.clone(), participant_type: Some(2), hidden: Some(false), metadata: None, permission: None };
    let token = match oxpeer::issue_token(&a.base, &req).await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("토큰 발급 실패: {e}");
            std::process::exit(1);
        }
    };
    let (client, mut events, mut media, _frames) = match Client::connect(Config::new(&a.base, token)).await {
        Ok(x) => x,
        Err(e) => {
            eprintln!("접속 실패: {e}");
            std::process::exit(1);
        }
    };
    if let Err(e) = client.join_speaking(&a.room).await {
        eprintln!("입장 실패: {e}");
        std::process::exit(1);
    }
    let duplex = if a.mode == "beacon" { Duplex::Full } else { Duplex::Half };
    let audio = match client.publish(&a.room, TrackSpec { kind: Kind::Audio, duplex }).await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("발행 실패: {e}");
            std::process::exit(1);
        }
    };
    let video = if a.mode == "beacon" && a.video {
        match client.publish(&a.room, TrackSpec { kind: Kind::Video, duplex: Duplex::Full }).await {
            Ok(t) => Some(t),
            Err(e) => {
                eprintln!("영상 발행 실패: {e}");
                std::process::exit(1);
            }
        }
    } else {
        None
    };
    let me = client.snapshot().await.ok().and_then(|s| s.session.and_then(|x| x.user_id)).unwrap_or_default();
    eprintln!("[agent] {} 에 들어왔다({}) — 발행 {}", a.room, a.mode, audio.track_id);
    let stop = async {
        match a.secs {
            Some(s) => tokio::time::sleep(Duration::from_secs(s)).await,
            None => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    };
    tokio::pin!(stop);
    let mut out = json!({ "room": a.room, "mode": a.mode, "user": me, "audio_track": audio.track_id, "video_track": video.as_ref().map(|v| v.track_id.clone()) });
    if a.mode == "beacon" {
        beacon(&client, &mut events, audio, video, &mut stop, &mut out).await;
    } else {
        echo(&client, &mut events, &mut media, audio, &me, &a.room, &mut stop, &mut out).await;
    }
    let st = client.stats().await;
    if let Some(o) = out.as_object_mut() {
        o.insert("stats".into(), json!({ "frames_sent": st.frames_sent, "frames_gated": st.frames_gated, "rtx_sent": st.rtx_sent, "remb_bps": st.remb_bps, "qa_send_dropped": st.qa_send_dropped, "media_dropped": st.media_dropped }));
    }
    client.close().await;
    println!("{out}");
}

/// 되울림 — 남의 발언을 받아 두고 · 그가 놓으면(화자 없음) 눌러서 허가 뒤 그대로 말하고 놓는다
#[allow(clippy::too_many_arguments)]
async fn echo(client: &Client, events: &mut oxpeer::Events, media: &mut oxpeer::Media, track: LocalTrack, me: &str, room: &str, stop: &mut std::pin::Pin<&mut impl std::future::Future<Output = ()>>, out: &mut Value) {
    let mut speaker: Option<String> = None;
    let mut heard: Vec<Bytes> = Vec::new();
    // 들은 패킷의 출처 주인(SDK:RULE-PLAT-SERVER 「출처 표지」) — 「누가 말했나」를 화자 사건이 아니라 패킷이 알린다
    let mut heard_from: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut echoes: Vec<Value> = Vec::new();
    let mut ptt: Vec<Value> = Vec::new();
    let t0 = Instant::now();
    loop {
        tokio::select! {
            _ = &mut *stop => break,
            e = events.next() => {
                let Some(e) = e else { break };
                match e {
                    Event::Speaker { room: r, user_id } if r == room => {
                        let was = speaker.take();
                        speaker = user_id.filter(|u| u != me);
                        // 남의 발언이 끝났다 — 되울린다
                        if speaker.is_none() && let Some(from) = was && !heard.is_empty() {
                            let packets = std::mem::take(&mut heard);
                            let sources: Vec<String> = std::mem::take(&mut heard_from).into_iter().collect();
                            let at = t0.elapsed().as_millis() as u64;
                            let granted = press_and_wait(client, events, room, &mut ptt, t0).await;
                            let mut sent = 0u32;
                            if granted {
                                for p in &packets {
                                    if track.write(OutFrame { data: p.clone(), duration: OPUS_20MS, keyframe: true, audio_level: None }).await.is_err() {
                                        break;
                                    }
                                    sent += 1;
                                }
                                // 송신 시계가 다 내보낼 때까지(통에 쌓인 만큼) 기다렸다 놓는다
                                track.drain().await;
                            }
                            let _ = client.ptt_release(room).await;
                            echoes.push(json!({ "from": from, "sources": sources, "heard": packets.len(), "sent": sent, "granted": granted, "at_ms": at, "released_ms": t0.elapsed().as_millis() as u64 }));
                        }
                    }
                    Event::Ptt { room: r, event, state } if r == room => ptt.push(json!({ "event": event, "state": state, "ms": t0.elapsed().as_millis() as u64 })),
                    Event::Closed { .. } => break,
                    _ => {}
                }
            }
            p = media.next() => {
                let Some(p) = p else { break };
                // 무전 슬롯 오디오(주인 없음)만 · 남이 말하는 동안만 · ★출처가 남인 패킷만(출처 없음 = 서버가 지은 침묵 · 예열)
                let from = p.source.as_ref().and_then(|s| s.user_id.clone());
                if p.room == room && p.kind == "audio" && p.user_id.is_none() && speaker.is_some() && p.source.is_some() && from.as_deref() != Some(me) {
                    if let Some(u) = from {
                        heard_from.insert(u);
                    }
                    heard.push(p.payload.clone());
                }
            }
        }
    }
    if let Some(o) = out.as_object_mut() {
        o.insert("echoes".into(), json!(echoes));
        o.insert("ptt".into(), json!(ptt));
    }
}

/// 누르고 허가(또는 거절 · 시한)를 기다린다
async fn press_and_wait(client: &Client, events: &mut oxpeer::Events, room: &str, ptt: &mut Vec<Value>, t0: Instant) -> bool {
    if client.ptt_press(room, 0).await.is_err() {
        return false;
    }
    let deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => return false,
            e = events.next() => match e {
                Some(Event::Ptt { room: r, event, state }) if r == room => {
                    ptt.push(json!({ "event": event, "state": state, "ms": t0.elapsed().as_millis() as u64 }));
                    match event.as_str() {
                        "granted" => return true,
                        "denied" | "revoked" => return false,
                        _ => {}
                    }
                }
                Some(_) => {}
                None => return false,
            },
        }
    }
}

/// 전이중 표지 — 오디오 20ms · 영상 33ms(30 장마다 키프레임 · 키프레임 요청이 오면 다음 장)
async fn beacon(client: &Client, events: &mut oxpeer::Events, audio: LocalTrack, video: Option<LocalTrack>, stop: &mut std::pin::Pin<&mut impl std::future::Future<Output = ()>>, out: &mut Value) {
    let _ = client;
    let (mut an, mut vn) = (0u32, 0u32);
    let mut want_key = false;
    let mut kreq: Vec<u64> = Vec::new();
    let t0 = Instant::now();
    let mut vt = tokio::time::interval(Duration::from_millis(33));
    loop {
        tokio::select! {
            _ = &mut *stop => break,
            r = audio.write(OutFrame { data: beacon_audio(an), duration: OPUS_20MS, keyframe: true, audio_level: Some(20) }) => {
                if r.is_err() { break }
                an += 1;
            }
            _ = vt.tick(), if video.is_some() => {
                let key = vn % 30 == 0 || std::mem::take(&mut want_key);
                if let Some(v) = &video
                    && v.write(OutFrame { data: beacon_video(vn, key), duration: Duration::from_millis(33), keyframe: key, audio_level: None }).await.is_err() {
                        break;
                    }
                vn += 1;
            }
            e = events.next() => match e {
                Some(Event::KeyframeRequested { .. }) => {
                    want_key = true;
                    kreq.push(t0.elapsed().as_millis() as u64);
                }
                Some(Event::Closed { .. }) | None => break,
                _ => {}
            },
        }
    }
    if let Some(o) = out.as_object_mut() {
        o.insert("audio_frames".into(), json!(an));
        o.insert("video_frames".into(), json!(vn));
        o.insert("keyframe_requests".into(), json!(kreq));
        o.insert("beacon_tags".into(), json!({ "audio": BEACON_TAG_AUDIO, "video": BEACON_TAG_VIDEO }));
    }
}
