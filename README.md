# oxlens-sdk-peer — `oxpeer`

OxLens **서버사이드 SDK**. 녹음 · 녹화 · AI 에이전트 같은 서버 쪽 프로그램이 보통 클라와 같은 계약(연동규격서)으로 방에 들어가
미디어를 받고 발언권 · 방 사건을 듣는 Rust 라이브러리다(SDK 정의서 `SDK:RULE-PLAT-SERVER`).

- 서버 코어(hub · sfud)는 이 SDK 를 모른다 — 뒷문이 없다. 참가자 종류(`participant_type` 1 녹음 · 2 봇)와 투명(`hidden`)은 앱이 발급받은 토큰의 클레임이 정한다.
- 미디어 엔진은 서버와 같은 webrtc-rs 계열(DTLS · SRTP · SCTP)이다. 장치 · 재생이 없어 libwebrtc 를 쓰지 않는다 — 받은 것은 SRTP 를 푼 RTP(코덱 페이로드 그대로)이고 디코딩은 앱 몫이다.
- ★브라우저에서 libwebrtc 수신 쪽이 하던 일은 SDK 의 **수신 정리층**이 한다(`SDK:RULE-PLAT-SERVER` 수신 정리층) — 재정렬 · 영상만 NACK(RTX 를 원래 seq 로 푼다) · PLI · RR · SR 로 벽시계 잇기. 무엇을 보낼지는 그 줄의 `rtcp_fb` 가 정한다. 앱은 RTCP 를 짓지 않는다.
- 받는 단위는 트랙마다 고른다 — **패킷**(`Media` · 받은 순서 그대로) 또는 **프레임**(`Frames` · 순서대로 조립한 코덱 프레임 — H264 Annex-B · VP8 서술자 뗀 것 · Opus 패킷 · 영상은 키프레임부터 · SR 로 이은 벽시계). 조립은 디코딩이 아니다.
- 2pc 로 붙는다(능력표에 `oxpeer` 행이 없다 — `SDK:RULE-PLAT-PC-MODE`).

## 쓰는 법

```rust
let req = oxpeer::TokenReq { api_key: "…".into(), api_secret: "…".into(), user_id: "rec-1".into(),
                             participant_type: Some(1), hidden: Some(true), metadata: None, permission: None };
let token = oxpeer::issue_token("http://127.0.0.1:1974/media", &req).await?;
let mut cfg = oxpeer::Config::new("http://127.0.0.1:1974/media", token);
cfg.unit = oxpeer::Unit::Frame;              // 받는 단위 기본(트랙마다는 client.set_unit)
// cfg.tls_root_pem = Some(std::fs::read("ca.pem")?);   // base 가 https:// 면 wss — 같은 망 hub 의 사설 CA(기본은 공용 뿌리)
let (client, mut events, mut media, mut frames) = oxpeer::Client::open(cfg).await?;   // = Client::create(cfg) + client.connect()
client.join("ch-1").await?;                 // 듣기(select:false)로 들어간다
// events.next() — Session · Joined · Tracks · Speaker{user_id} · Link · Closed …(SDK 정의서 효과 표의 이름)
// media.next()  — MediaPacket{room, track_id, kind, codec, sequence, timestamp, payload …}(단위 Packet 인 트랙)
// frames.next() — MediaFrame{room, track_id, kind, codec, timestamp, keyframe, data, wallclock, synced …}(단위 Frame 인 트랙)
client.set_layer("t…", oxpeer::Layer { spatial: Some(0), ..Default::default() }).await?;   // 층 상한(실제 선택은 서버)
let mut tr = client.transitions();          // 전이 통(진단) — 생성 상태기의 걸음 · 받지 않는 사건(impossible) 전량(SDK:RULE-DIAG-EVENTS) · 앱 사건 통과 따로

// 말하기(에이전트 — 20261107c) — 발언 방으로 들어가 발행 · 무전이면 누르고 허가 사이에 넣는다
client.join_speaking("ch-1").await?;                                                         // select:true 입장(발언 방)
let mic = client.publish("ch-1", oxpeer::TrackSpec { kind: oxpeer::Kind::Audio, duplex: oxpeer::Duplex::Half, simulcast: false }).await?;   // mic.codec = 그 방 미디어 코덱
client.ptt_press("ch-1", 0).await?;                                                          // 허가는 사건 Ptt{event:"granted"}
mic.write(oxpeer::OutFrame { data: opus, duration: std::time::Duration::from_millis(20), keyframe: true, audio_level: Some(25) }).await?;
mic.drain().await;                                                                           // 송신 시계가 다 낸 뒤
client.ptt_release("ch-1").await?;
client.send_message("ch-1", "안녕하세요").await?;                                             // 방 문자(받기는 사건 Message)
mic.set_duplex(oxpeer::Duplex::Full).await?;                                                 // 반이중 ↔ 전이중(TRACK_SET)
client.set_speaking_room("ch-2").await?;                                                     // 발언 방 바꾸기(서버를 건너면 옛 서버 내림 · 새 서버 발행)
let cam = client.publish("ch-1", oxpeer::TrackSpec { kind: oxpeer::Kind::Video, duplex: oxpeer::Duplex::Full, simulcast: true }).await?;
cam.write_layer(0, low).await?; cam.write_layer(1, high).await?;                             // 시뮬캐스트 — 층마다 앱이 인코딩한 프레임(l = 0 · h = 1)
client.close().await;
client.connect().await?;                                                                     // ★같은 객체로 다시 — 새 세션(방은 앱이 다시 join)
```

보내는 쪽 — ★SDK 는 인코딩하지 않는다(앱이 Opus · VP8 · H264 프레임을 넣는다). 송신 시계가 프레임 길이대로 고르게 낸다(앱이 TTS 를 실시간보다 빨리 넣어도 몰아 보내지 않고 통이 차면 `write` 가 기다린다).
오디오엔 `ssrc-audio-level` 을 싣는다(서버가 줄 상한을 넘는 방에서 음량으로 고른다 · 안 주면 말소리 30 dBov). SR 1초 · 영상 NACK 엔 RTX · 서버의 키프레임 요청은 사건 `KeyframeRequested{layer}`(앱이 인코더에 시킨다).
★대역 추정 — 패킷마다 transport-cc 를 싣고 서버 TWCC 로 목표 비트레이트를 셈한다(손실 · 지연 기울기 · REMB 의 최솟값) → 사건 `TargetBitrate{bps}` · `Stats::target_bps`. 인코더를 맞추는 것은 앱 몫이다(SDK 는 버리거나 늦추지 않는다).

예제 — 녹음(발언 한 건마다 Ogg Opus 파일 하나 · 패킷 출처가 경계)과 녹화(`--video` — 트랙마다 Matroska 하나 · 무전 슬롯은 발언마다) · 에이전트(`agent` — `echo` 무전 되울림 · `beacon` 전이중 표지 발행 시험판(`--video [--simulcast]`) · `duplex` · `move --room2` · `reconnect --reconnect-at N` 걸음표 · `file --opus a.ogg --h264 v.h264` 진짜 표본) · 둘 다 `--tls-root ca.pem`:

```sh
cargo run --example recorder -- --base http://127.0.0.1:1974/media --room ch-1 --out ./rec --secs 30 --hidden
cargo run --example recorder -- --base http://127.0.0.1:1974/media --room conf-1 --out ./rec --video [--spatial 0] [--layer-plan 4:0,8:1]
cargo run --example agent -- --base http://127.0.0.1:1974/media --room ch-1 --mode echo
```

### 녹음 원시 기록(기능 `record` — context 202611/20261109c)

`Config.record = Some(RecordConfig::new(dir, recording_id))` 면 받는 줄 전부를 ★수신 정리층 출구에서 MCAP 파트(`<recording_id>-NNNN.mcap`)로 적는다 —
복호한 RTP 원본(RTX 로 메운 것은 원래 패킷으로) · 빈 번호(`gap`) · SR · 트랙 목록 · 발언권 · 세션 사건 · `tick`(SDK:RULE-PLAT-REC-TAP · REC-RAW · REC-CLOCK · REC-PART).
청크는 tick 경계에서 F(기본 2초)마다 닫는다 — 프로세스가 죽어도 잃는 것은 닫히지 않은 청크뿐이다. 파트는 크기(기본 1GiB) · 시간(기본 1시간) 중 먼저 닿는 쪽에서 돈다 · 파트마다 머리 스냅숏이 있어 따로 읽힌다.
다시 읽기는 `record::FileSource`(파일 차례 그대로 · Footer 없는 파트는 마지막 닫힌 청크까지). 예제 `recorder --raw DIR [--raw-part-ms N] [--raw-flush-ms N]` · 실측 `cargo run --release --example rawbench --features record-lz4,record-zstd`.

파이프라인(context 202611/20261109d · SDK:RULE-PLAT-REC-PIPE · REC-SPLIT · REC-API) — 레코드 흐름 하나 위에 결과물을 짓는다:

| 지점 | 무엇 | 쓰는 법 |
|---|---|---|
| ① 레코드 | 원시 레코드 그대로 | 라이브 `Client::records(capacity)`(★열 때 지금 해석 상태를 먼저 받는다 · 넘치면 원시 파트 꼬리 읽기로 갈아탄다 · 원시 기록을 끈 구성이면 멈추고 `Event::RecordConsumerStopped`) · 파일 `record::FileSource` |
| ② 프레임 | 조립 · 시간축 뒤(트랙 · 출처 · 키프레임 · RTP 시각 · SR 로 이은 UTC) | `record::Pipeline::push(&record)` → `Output::Frame` |
| ③ 세그먼트 | 분할 정책(`Policy::Utterance` 출처가 바뀌면 · `Policy::Span` 시간 · 크기 뒤 키프레임)의 열림 · 닫힘 | `Output::Segment` |

파이프라인은 시스템 시계를 읽지 않는 순수 상태기다 — 같은 레코드열이면 같은 바이트 결과(라이브로 지은 것 = 원시 파트로 다시 지은 것). 내장 결과물 `record::OggSink` 도 ② ③ 만 쓴다.
`rawtool frames <dir> <id> [--part K | --ref]` 는 원시 파트에서 프레임 목록을 낸다(`--ref` = webrtc-media SampleBuilder 대조군 · 기능 `record-ref`).

녹화 파일은 트랙마다 따로다 — 사람 · 트랙 합치기(음성 + 영상)는 예제 밖이다(ffmpeg 등). 블록 시각은 RTP 시각 차 · 파일 시작 벽시계(`DateUTC`)는 첫 SR 동기 프레임에서 거꾸로 셈한다 — 트랙끼리 그 값으로 맞춘다.

## 구조

| 자리 | 무엇 |
|---|---|
| `src/generated/` | ★**생성본 — 손으로 고치지 않는다.** 영역 클라 상태기(세션 · 방 · 연결 · 구독 · 트랙 · 발언권) · 결합 · WS wire · 발언권 코덱 · 정책 SDK 기본값. 규격 저장소에서 `npm run sync:peer` 로 옮긴다 |
| `src/core/` | SDK 루프 하나 — 입구 표 · 효과 표 · 이음매 집행 · 시계(절차는 생성 상태기가 정한다) |
| `src/link/` | 전송(UDP · ICE consent · DTLS · SRTP · SCTP/DCEP) — 층마다 사실을 낸다 · `recv.rs` 수신 정리층 · `depack.rs` RTP 꼴 → 코덱 꼴 |
| `src/sig.rs` · `src/wire/` | WS 소켓 · 대기표 · 프레임 머리(WS · DC) |
| `tests/vectors/` | 규격 짓기의 재생 시험이 TS 판 상태기에 넣은 호출과 결과 — Rust 판이 같은 결과를 내는지 1층이 맞댄다 |
| `src/record/` | 녹음(기능 `record`) — 기록기 · 파트 회전 · `FileSource` · 라이브 통(`live.rs` 꼬리 강등) · `Pipeline` · `OggSink` |
| `examples/recorder/` | 예제 녹음 · 녹화기(Ogg Opus · `--video` Matroska · `--raw` MCAP 원시) |
| `examples/rawbench.rs` | 원시 기록 실측(합성 RTP — F · fsync · 압축 · 시간당 크기) |
| `examples/rawtool.rs` | 원시 파트 → 프레임 목록(전체 · 파트 단독 · 대조군) — 2층 「파트 자족」 「조립 대조」 재료 |
| `examples/agent/` | 예제 에이전트(무전 되울림 · 전이중 표지 발행) |
| `src/core/pubs.rs` · `src/link/send.rs` | 말하는 쪽 — 발행 · 소속 · 누름 · 메시지 결합 / 패킷화 · 확장 · 보낸 기록 · RTX · SR |

## 게이트

```sh
scripts/gate.sh        # 생성본 어긋남(peer_sync --check) · clippy -D warnings · cargo test
scripts/media_check.sh # 재생기 호환 실측 — ffmpeg(libvpx · libx264 · libopus)가 있어야 돈다
```

`media_check.sh` — ffmpeg 가 진짜 VP8 · H264 · Opus 를 짓고(해상도가 640x360 → 320x180 으로 바뀌는 판 — 시뮬캐스트 층 바뀜 흉내) →
lib 시험(`media_fixture_*` · `#[ignore]`)이 libwebrtc 꼴 RTP(VP8 서술자 · H264 STAP-A · FU-A)로 쪼개 뒤바뀜 · RTX 복구를 섞어 수신 정리층에 넣고 원본과 바이트 대조 →
예제 `mkvcheck` 가 녹화기의 MKV 쓰개로 파일을 짓고 → ffprobe · ffmpeg 가 디코드 오류 0 · 해상도 바뀜 · 길이 · 시간 이동을 본다.

2층은 labs(`oxlens-labs/oxe2epy`)가 예제 녹음 · 녹화기를 띄워 「봇이 보낸 것 = 파일」로 견준다(봇 본문 표지 — 보낸 ssrc · 번호) — 재는 쪽은 labs 다.

| 시나리오 | 자극 | 복구 길 |
|---|---|---|
| `peer_recorder` | 없음(2봇 화자 전환) | — |
| `peer_recorder_ws_cut` | 하네스 중계가 WS 만 끊는다(발언 중) | 이어받기(RESUME) — 녹음이 끊기지 않는다 |
| `peer_recorder_session_cut` | 운영 절단(`/admin/users/{user}/cut`) | 세션 재구축 — 방 다시 들어가기 · 연결 새로 |
| `peer_recorder_resync` | 발행 봇 송신 멈춤 → 서버 `sync_required` | 재동기(HTTP `GET /rooms`) |
| `peer_recorder_freeze` | 프로세스 35초 얼림 | 이어받기 + ICE 재시작 |
| `peer_recorder_freeze_long` | 프로세스 95초 얼림(서버 재개 창 밖) | 새 세션 — 다시 세움 |
| `peer_recorder_unit_down` | 방을 든 미디어 유닛 내림 | 그 서버 재구축 → 방 없음(3001)으로 끝 · 세션은 산다 |
| `peer_record_raw` | 없음(무전 화자 전환 + 원시 기록 T 2초 · F 1초) | 사슬 · 회전 · 라이브 = 파일(Ogg 바이트) · 파트 자족 · 조립 대조 |
| `peer_record_raw_vp8` · `peer_record_raw_h264` | 없음(2봇 회의 음성 + 영상 + 원시 기록) | 사슬 · 회전 · 파트 자족(영상은 첫 키프레임부터) · 조립 대조(webrtc-media) |
| `peer_record_raw_kill` | 발언 중 SIGKILL | 비정상 종료 — 마지막 log_time ≥ 죽인 시각 − F |

DC 만 닫히는 경우(같은 연관 위 DC 다시 열기)는 바깥에서 걸 자극이 없어(서버 운영 표면에 그 손잡이가 없다) 1층 벡터(상태기)만 지난다.

녹화(context 202611/20261107a):

| 시나리오 | 보는 것 |
|---|---|
| `peer_record_video` · `peer_record_video_h264` | 봇이 세 조각으로 보낸 프레임(VP8 이어지는 조각 · H264 FU-A)을 이어 키프레임부터 번호가 빠짐없이 · 파일 시작 벽시계 = 봇 SR 로 셈한 값(±3ms) |
| `peer_record_simulcast` | `set_layer` 상한(1 → 0 → 1)대로 받은 층 표지(h/l)가 바뀐다 — 그 파일의 RTP 시각축에서 견준다 |
| `peer_record_loss` | ★QA 스위치 `OXPEER_QA_DROP_VIDEO_EVERY=N`(받은 영상 원본 N 개마다 하나를 정리층 앞에서 버림 · RTX 는 안 버림 · 시험 수단)로 흉내 낸 손실을 NACK → RTX 로 다 메운다 |
| `peer_record_ptt_video` | 무전 슬롯 — 종류마다 발언 한 건 = 파일 하나 · 화자 차례 = 허가 차례 |

에이전트(context 202611/20261107c):

| 시나리오 | 보는 것 |
|---|---|
| `peer_agent_echo` | 무전 되울림 — botA 의 발언을 받아 그가 놓으면 눌러 허가 사이에 그대로 말한다 · 청취 봇이 같은 표지를 원래 뒤에 한 번 더 받고(연속 · 보낸 수 그대로) 화자 통지가 에이전트 |
| `peer_agent_publish` | 전이중 표지 발행(오디오 · VP8) — 받은 봇마다 번호가 빠짐 없이 · 영상 첫 도착은 키프레임 · 늦게 든 봇 → 서버 PLI → 사건 `KeyframeRequested` |
| `peer_agent_loss` | ★보내는 쪽 QA 스위치 `OXPEER_QA_DROP_SEND_EVERY=N`(영상 델타 프레임 패킷 N 개마다 하나를 안 보냄 · transport-cc 번호는 매김 · 보낸 기록엔 남김)로 흉내 낸 업링크 손실 → 서버 NACK → RTX 로 다 메움 |
| `peer_agent_bwe_loss` | 같은 스위치로 손실 약 16% → TWCC 손실 기반 제어가 목표를 300k 아래로 내린다 |
| `peer_agent_simulcast` | 발행 시뮬캐스트 — 녹화기가 층 상한을 바꾸면 그 층 표지가 온다 · 깨끗한 판에서 목표가 오른다 |
| `peer_agent_duplex` · `peer_agent_move` · `peer_agent_reconnect` | 걸음표 — 전이중↔반이중 · 서버를 건너는 발언 방 전환(옛 서버 내림 · 새 서버 발행) · 닫고 같은 객체로 다시 connect |
| `peer_agent_ws_cut` · `peer_agent_session_cut` | WS 만 끊음 = 발행 그대로(번호 끊김 없음) · 운영 절단 = 재구축 뒤 방이 다시 선 다음 다시 발행(새 track_id) |
| `peer_agent_wss` | 하네스 TLS 종단 중계(그 판의 사설 CA)만 지나 https · wss 로 붙는다 |
| `peer_record_reassign` | 영상 발행 26 > 줄 상한 25 · 하나가 내리면 줄 밖 트랙이 그 줄을 받는다 — 녹화기는 출처로 파일을 가른다 |

3층(`oxlens-sdk-web/qa/live` `peer_mixed`) — 브라우저 실 인코더 → oxpeer 녹화 파일이 ffmpeg 로 끝까지 디코드 · oxpeer 에이전트의 진짜 Opus · H264 를 브라우저가 디코드(프레임 · 샘플 차분 · 폭) · 갈래B = 표지 본문이면 패킷은 와도 디코드 0.

## 범위

세션 · 연결 · 방 · 받는 트랙 · 구독 + 발언권 듣기(첫 단계) · 수신 정리층 · 프레임 단위 수신 · 층 고르기(녹화 단계) · 발행 · 발언 방 · 발언 누름 · 메시지 · 지금 값(에이전트 단계) ·
발행 신원 = 출처 표지(CSRC — context 20261107b) · https/wss · 같은 객체로 다시 connect · 발행 시뮬캐스트 · 송신 대역 추정(context 20261108d).
**없는 것** — 인코딩 · 디코딩(앱 몫) · 대역 추정에 따른 송신 조절(목표만 알린다 — 인코더는 앱이 맞춘다) · TCP 후보(UDP 만).
