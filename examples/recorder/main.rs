// author: kodeholic (powered by Claude)
//! 예제 녹음 · 녹화기 — 서버사이드 SDK(oxpeer)로 방에 녹음 참가자(`participant_type:1`)로 들어간다.
//! 기본(녹음) — ★발언 한 건마다 Ogg Opus 파일 하나. 발언 경계는 ★패킷의 출처(CSRC — SDK:RULE-PLAT-SERVER 「출처 표지」)가 정한다 —
//!   출처가 바뀌면 새 파일 · 출처 없는 패킷(표지를 끈 연결 — 서버는 미디어 프레임을 짓지 않는다)은 경계 밖이고 열린 파일을 닫는다. 화자 사건과 도착 시각을 맞대지 않는다.
//!   무전 슬롯 오디오(PTT)만 담는다(개인 트랙은 센다).
//! `--video`(녹화 · 20261107a → 202611/20261110a) — ★녹음과 같은 파이프라인 · 세그먼트마다 Matroska 파일 하나(영상 · 음성 모두 · 영상은 키프레임부터).
//!   개인 트랙은 녹화 내내 파일 하나 · 무전 슬롯은 발언 한 건 = 파일 하나 · `--span-secs N` 이면 트랙마다 N 초 길이 정책.
//!   `--spatial N` 은 영상 트랙의 층 상한(기본은 최고 단) · `--layer-plan 4:0,8:1` 은 입장 뒤 그 초에 상한을 바꾼다.
//! `--records-capacity N` · `--consume-stall-ms N`(시험 — 라이브 통 칸 수 · 입장 뒤 N ms 통을 안 읽는 느린 소비자 → 넘치면 꼬리 읽기로 강등 · 요약 `records.degraded` · SDK:RULE-PLAT-REC-API)
//! `--raw DIR`(녹음 원시 기록 · context 202611/20261109c) — 받는 줄 전부를 MCAP 파트(`<user>-NNNN.mcap`)로 함께 적는다(SDK:RULE-PLAT-REC-TAP).
//!   손잡이 `--raw-part-ms` · `--raw-part-bytes` · `--raw-flush-ms` · `--raw-tick-ms` · `--raw-min-free-bytes`(디스크 하한)(기본은 `RecordConfig::new`).
//!
//!   cargo run --example recorder -- --base http://127.0.0.1:1974/media --room ch-1 --out ./rec [--secs 30] [--hidden] [--user rec-1] [--video [--span-secs N] [--spatial N] [--layer-plan S:N,..]]
//!
//! 끝나면 표준 출력에 한 줄 JSON(파일마다 화자 · 수 · 경계 밖 수 · 정리층 계수 · 복구 사실)을 쓴다 — 2층 시험이 이것과 파일을 읽는다.

mod video;

use std::path::PathBuf;
use std::time::Duration;

use oxpeer::record::{Channel, FileSource, OggSink, Output, Pipeline, Policy, Record, RecordConfig};
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
    /// `https://` hub 의 사설 CA 뿌리(PEM 파일 — context 202611/20261108d E2)
    tls_root: Option<PathBuf>,
    api_secret: String,
    video: bool,
    spatial: Option<u8>,
    layer_plan: Vec<(u64, u8)>,
    /// 영상 줄 우선순위(`--priority <user_id>=<n>` · 여러 번) — 줄 상한을 넘는 방에서 누구를 늘 받을지 앱이 고른다(연:RULE-SUB-FILL video 「고르는 이 = 클라」 · 202611/20261110d T5)
    priority: Vec<(String, u8)>,
    /// 녹화 길이 정책(초) — 없으면 발언 정책(SDK:RULE-PLAT-REC-SPLIT)
    span_secs: Option<u64>,
    /// 결과물을 HLS 로(SDK:RULE-PLAT-REC-HLS · 202611/20261110b)
    hls: bool,
    raw: Option<PathBuf>,
    raw_part_ms: Option<u64>,
    raw_part_bytes: Option<u64>,
    raw_flush_ms: Option<u64>,
    raw_tick_ms: Option<u64>,
    /// 원시 기록 디스크 하한(`--raw-min-free-bytes` · 기본 `RecordConfig::new` — SDK:RULE-PLAT-REC-PART)
    raw_min_free_bytes: Option<u64>,
    /// close() 뒤 Client 를 놓기 전 이만큼 살려 두고 그동안 쓴 CPU 를 잰다(context 202611/20261109d — 닫힌 전송로의 DTLS 읽기 고리 돌기 재발 검사)
    linger_ms: Option<u64>,
    /// 라이브 레코드 통 칸 수(`--records-capacity` · 기본 65,536)
    records_capacity: usize,
    /// ★시험 — 입장 뒤 이 ms 동안 레코드 통을 읽지 않는다(`--consume-stall-ms` · 느린 소비자 · 통이 넘치면 꼬리 읽기로 강등 — SDK:RULE-PLAT-REC-API)
    consume_stall_ms: Option<u64>,
}

fn args() -> Args {
    let mut a = Args { base: String::new(), room: String::new(), out: PathBuf::from("."), secs: None, hidden: false, user: "oxpeer-recorder".into(), api_key: "ox_k_demo".into(), api_secret: "ox_s_demo".into(), tls_root: None, video: false, spatial: None, layer_plan: Vec::new(), priority: Vec::new(), span_secs: None, hls: false, raw: None, raw_part_ms: None, raw_part_bytes: None, raw_flush_ms: None, raw_tick_ms: None, raw_min_free_bytes: None, linger_ms: None, records_capacity: 1 << 16, consume_stall_ms: None };
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
            "--tls-root" => a.tls_root = Some(PathBuf::from(v())),
            "--api-key" => a.api_key = v(),
            "--api-secret" => a.api_secret = v(),
            "--video" => a.video = true,
            "--spatial" => a.spatial = v().parse().ok(),
            "--span-secs" => a.span_secs = v().parse().ok(),
            "--priority" => {
                if let Some((u, n)) = v().rsplit_once('=')
                    && let Ok(n) = n.parse()
                {
                    a.priority.push((u.to_string(), n));
                }
            }
            "--hls" => {
                a.hls = true;
                a.video = true;
            }
            "--raw" => a.raw = Some(PathBuf::from(v())),
            "--raw-part-ms" => a.raw_part_ms = v().parse().ok(),
            "--raw-part-bytes" => a.raw_part_bytes = v().parse().ok(),
            "--raw-flush-ms" => a.raw_flush_ms = v().parse().ok(),
            "--raw-tick-ms" => a.raw_tick_ms = v().parse().ok(),
            "--raw-min-free-bytes" => a.raw_min_free_bytes = v().parse().ok(),
            "--linger-ms" => a.linger_ms = v().parse().ok(),
            "--records-capacity" => a.records_capacity = v().parse().unwrap_or(1 << 16),
            "--consume-stall-ms" => a.consume_stall_ms = v().parse().ok(),
            "--layer-plan" => {
                a.layer_plan = v().split(',').filter_map(|x| x.split_once(':')).filter_map(|(t, n)| Some((t.parse().ok()?, n.parse().ok()?))).collect();
            }
            other => eprintln!("모르는 인자 {other}"),
        }
    }
    if a.base.is_empty() || a.room.is_empty() {
        eprintln!("사용: recorder --base http://host:port/media --room <방> [--out 폴더] [--secs N] [--hidden] [--user id] [--video [--span-secs N] [--spatial N] [--layer-plan S:N,..]] [--hls]");
        std::process::exit(2);
    }
    a
}

#[tokio::main]
async fn main() {
    let a = args();
    std::fs::create_dir_all(&a.out).expect("출력 폴더");
    let req = TokenReq { api_key: a.api_key.clone(), api_secret: a.api_secret.clone(), user_id: a.user.clone(), participant_type: Some(1), hidden: Some(a.hidden), metadata: None, permission: None };
    let root = a.tls_root.as_ref().map(|p| std::fs::read(p).expect("tls 뿌리 PEM"));
    let token = match oxpeer::issue_token_with_root(&a.base, &req, root.as_deref()).await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("토큰 발급 실패: {e}");
            std::process::exit(1);
        }
    };
    let mut cfg = Config::new(&a.base, token);
    cfg.tls_root_pem = root;
    if let Some(dir) = &a.raw {
        let mut rc = RecordConfig::new(dir, &a.user);
        if let Some(ms) = a.raw_part_ms {
            rc.part_time = Duration::from_millis(ms);
        }
        if let Some(b) = a.raw_part_bytes {
            rc.part_bytes = b;
        }
        if let Some(ms) = a.raw_flush_ms {
            rc.flush = Duration::from_millis(ms);
        }
        if let Some(ms) = a.raw_tick_ms {
            rc.tick = Duration::from_millis(ms);
        }
        if let Some(b) = a.raw_min_free_bytes {
            rc.min_free_bytes = b;
        }
        cfg.record = Some(rc);
    } else {
        // 녹음 · 녹화 갈래는 늘 레코드 흐름 위에서 짓는다(SDK:RULE-PLAT-REC-API) — 원시 파트 없이 라이브 통만
        cfg.record = Some(RecordConfig::live(&a.user));
    }
    let (client, mut events, mut media, _frames) = match Client::open(cfg).await {
        Ok(x) => x,
        Err(e) => {
            eprintln!("접속 실패: {e}");
            std::process::exit(1);
        }
    };
    // ★입장 전에 연다 — 입장 뒤 레코드 전량을 받아야 파일로 다시 지은 것과 같다(늦게 연 통도 해석 상태는 받는다 — SDK:RULE-PLAT-REC-API)
    let mut records = client.records(a.records_capacity);
    if let Err(e) = client.join(&a.room).await {
        eprintln!("입장 실패: {e}");
        std::process::exit(1);
    }
    eprintln!("[recorder] {} 에 들어왔다 — 발언을 기다린다", a.room);
    if a.video {
        let out = video::run(&a, client, events, media, records).await;
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
    let stall = tokio::time::sleep(Duration::from_millis(a.consume_stall_ms.unwrap_or(0)));
    tokio::pin!(stall);
    let mut stalled = a.consume_stall_ms.is_some();
    // ★발언 경계는 파이프라인이 정한다(SDK:RULE-PLAT-REC-SPLIT 발언 — 줄마다 출처가 바뀌면 새 세그먼트 · 출처 없는 프레임은 밖)
    let mut pipe = Pipeline::new(Policy::Utterance);
    let mut sink = OggSink::new(&a.out).expect("출력 폴더");
    let mut tally = Tally::default();
    let mut seen_events: Vec<String> = Vec::new();
    let mut flow = Flow::default();
    let mut ended = false;
    loop {
        tokio::select! {
            _ = &mut stop => break,
            e = events.next() => {
                let Some(e) = e else { break };
                if !matches!(e, Event::Speaker { .. }) && flow.on(&e, &a.room) {
                    ended = true;
                    break;
                }
                seen_events.push(format!("{e:?}").chars().take(160).collect());
            }
            // 미디어 통은 비워 둔다(이 갈래는 레코드 흐름으로 짓는다 — 통이 차 버리면 계수가 오른다)
            p = media.next() => if p.is_none() { break },
            _ = &mut stall, if stalled => stalled = false,
            r = records.next(), if !stalled => {
                let Some(r) = r else { break };
                tally.feed(&a.room, &mut pipe, &mut sink, &r);
            }
        }
    }
    let stats = client.stats().await;
    client.close().await;
    let linger = match a.linger_ms {
        Some(ms) => {
            let c0 = cpu_ms();
            tokio::time::sleep(Duration::from_millis(ms)).await;
            Some(json!({ "linger_ms": ms, "cpu_ms": cpu_ms().zip(c0).map(|(b, a)| b.saturating_sub(a)) }))
        }
        None => None,
    };
    // Client 를 놓아야 SDK 루프가 끝나고 기록기가 닫힌다(같은 객체로 다시 connect 할 수 있어 close 는 기록기를 닫지 않는다)
    drop(client);
    // 닫힌 뒤 — 통에 남은 레코드를 다 짓는다(라이브 = 파일 — 같은 레코드열)
    if !ended {
        while let Some(r) = records.next().await {
            tally.feed(&a.room, &mut pipe, &mut sink, &r);
        }
    }
    for o in pipe.finish() {
        let _ = sink.write(&o);
    }
    let done: Vec<Value> = sink.files().iter().map(|f| json!({ "file": f.path, "speaker": f.user_id, "csrc": f.source, "packets": f.packets, "samples": f.samples, "first_seq": f.first_seq, "last_seq": f.last_seq, "segment": f.segment, "reason": f.reason, "filled": f.filled, "first_ts": f.first_ts, "end_ts": f.end_ts })).collect();
    let mut out = json!({
        "room": a.room,
        "utterances": done,
        "orphan_packets": tally.orphan,
        "orphans": tally.orphans,
        "other_audio": tally.other_audio,
        "events": seen_events,
        "records": records_summary(&pipe, records.degraded()),
    });
    // 「라이브 = 파일」(context 202611/20261109d) — 원시 파트로 같은 파이프라인을 다시 돌려 `<out>/rebuild/` 에 짓는다
    if let Some(dir) = &a.raw {
        out["rebuild"] = rebuild(dir, &a.user, &a.room, &a.out.join("rebuild"));
    }
    if let Some(l) = linger {
        out["linger"] = l;
    }
    flow.fill(&mut out, &stats);
    println!("{out}");
}

/// 이 프로세스가 쓴 CPU 시간(ms) — `ps -o time=`(유닉스 계열 · `[[H:]M]M:SS.ss`)
fn cpu_ms() -> Option<u64> {
    let o = std::process::Command::new("ps").args(["-o", "time=", "-p", &std::process::id().to_string()]).output().ok()?;
    let t = String::from_utf8_lossy(&o.stdout).trim().to_string();
    let mut ms = 0f64;
    for (i, p) in t.split([':', '-']).rev().enumerate() {
        let v: f64 = p.parse().ok()?;
        ms += v * [1_000.0, 60_000.0, 3_600_000.0, 86_400_000.0].get(i)?;
    }
    Some(ms as u64)
}

/// 녹음 갈래의 셈 — 무전 슬롯 음성만 담는다(개인 트랙은 세기만) · 경계 밖 프레임마다 seq · 서버 침묵인가 · 마지막 발언권 사건과 그 뒤 경과(레코드 시각)
#[derive(Default)]
struct Tally {
    orphan: u64,
    orphans: Vec<Value>,
    other_audio: u64,
    last_floor: (&'static str, u64),
}

impl Tally {
    fn feed(&mut self, room: &str, pipe: &mut Pipeline, sink: &mut OggSink, r: &Record) {
        if r.channel == Channel::Floor
            && let Ok(v) = serde_json::from_slice::<Value>(&r.data)
            && v["room"].as_str() == Some(room)
        {
            self.last_floor = (if v["event"].as_str() == Some("taken") { "taken" } else { "idle" }, r.log_time);
        }
        for o in pipe.push(r) {
            let f = match &o {
                Output::Frame(f) | Output::Orphan(f) => Some(f),
                Output::Segment(_) => None,
            };
            if let Some(f) = f {
                if f.room != room || f.kind != "audio" || !f.codec.eq_ignore_ascii_case("opus") {
                    continue;
                }
                if f.owner.is_some() {
                    self.other_audio += 1;
                    continue;
                }
                if let Output::Orphan(f) = &o {
                    self.orphan += 1;
                    let last = if self.last_floor.1 == 0 { "none" } else { self.last_floor.0 };
                    self.orphans.push(json!({ "seq": f.seq_first, "silence": f.data == [0xF8, 0xFF, 0xFE], "last": last, "since_ms": f.log_time.saturating_sub(self.last_floor.1) / 1_000_000 }));
                    continue;
                }
            }
            if let Output::Segment(s) = &o
                && (s.room != room || s.owner.is_some())
            {
                continue;
            }
            let _ = sink.write(&o);
        }
    }
}

fn records_summary(pipe: &Pipeline, degraded: bool) -> Value {
    let c = pipe.counts();
    json!({ "records": c.records, "packets": c.packets, "unrouted": c.unrouted, "frames": c.frames, "broken": c.broken, "before_key": c.before_key, "degraded": degraded })
}

/// 원시 파트로 다시 짓기 — 라이브와 같은 파이프라인 · 같은 결과물(파일 이름이 같다)
fn rebuild(dir: &std::path::Path, id: &str, room: &str, out: &std::path::Path) -> Value {
    let Ok(mut src) = FileSource::open(dir, id) else { return json!({ "error": "원시 파트를 못 열었다" }) };
    let Ok(mut sink) = OggSink::new(out) else { return json!({ "error": "다시 짓기 폴더" }) };
    let mut pipe = Pipeline::new(Policy::Utterance);
    let mut tally = Tally::default();
    src.run(|r| tally.feed(room, &mut pipe, &mut sink, &r));
    for o in pipe.finish() {
        let _ = sink.write(&o);
    }
    json!({ "dir": out, "parts": src.parts().len(), "files": sink.files().iter().map(|f| json!({ "file": f.path, "segment": f.segment, "packets": f.packets })).collect::<Vec<_>>(), "records": records_summary(&pipe, false) })
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
