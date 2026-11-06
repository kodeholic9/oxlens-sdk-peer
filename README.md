# oxlens-sdk-peer — `oxpeer`

OxLens **서버사이드 SDK**. 녹음 · 녹화 · AI 에이전트 같은 서버 쪽 프로그램이 보통 클라와 같은 계약(연동규격서)으로 방에 들어가
미디어를 받고 발언권 · 방 사건을 듣는 Rust 라이브러리다(SDK 정의서 `SDK:RULE-PLAT-SERVER`).

- 서버 코어(hub · sfud)는 이 SDK 를 모른다 — 뒷문이 없다. 참가자 종류(`participant_type` 1 녹음 · 2 봇)와 투명(`hidden`)은 앱이 발급받은 토큰의 클레임이 정한다.
- 미디어 엔진은 서버와 같은 webrtc-rs 계열(DTLS · SRTP · SCTP)이다. 장치 · 재생이 없어 libwebrtc 를 쓰지 않는다 — 받은 것은 SRTP 를 푼 RTP(코덱 페이로드 그대로)이고 디코딩은 앱 몫이다.
- 2pc 로 붙는다(능력표에 `oxpeer` 행이 없다 — `SDK:RULE-PLAT-PC-MODE`).

## 쓰는 법

```rust
let req = oxpeer::TokenReq { api_key: "…".into(), api_secret: "…".into(), user_id: "rec-1".into(),
                             participant_type: Some(1), hidden: Some(true), metadata: None, permission: None };
let token = oxpeer::issue_token("http://127.0.0.1:1974/media", &req).await?;
let (client, mut events, mut media) = oxpeer::Client::connect(oxpeer::Config::new("http://127.0.0.1:1974/media", token)).await?;
client.join("ch-1").await?;                 // 듣기(select:false)로 들어간다
// events.next() — Session · Joined · Tracks · Speaker{user_id} · Link · Closed …(SDK 정의서 효과 표의 이름)
// media.next()  — MediaPacket{room, track_id, kind, codec, sequence, timestamp, payload …}
client.close().await;
```

예제 녹음기 — 발언 한 건마다 Ogg Opus 파일 하나(화자 사건 `Speaker` 가 경계):

```sh
cargo run --example recorder -- --base http://127.0.0.1:1974/media --room ch-1 --out ./rec --secs 30 --hidden
```

## 구조

| 자리 | 무엇 |
|---|---|
| `src/generated/` | ★**생성본 — 손으로 고치지 않는다.** 영역 클라 상태기(세션 · 방 · 연결 · 구독 · 트랙 · 발언권) · 결합 · WS wire · 발언권 코덱 · 정책 SDK 기본값. 규격 저장소에서 `npm run sync:peer` 로 옮긴다 |
| `src/core/` | SDK 루프 하나 — 입구 표 · 효과 표 · 이음매 집행 · 시계(절차는 생성 상태기가 정한다) |
| `src/link/` | 전송(UDP · ICE consent · DTLS · SRTP · SCTP/DCEP) — 층마다 사실을 낸다 |
| `src/sig.rs` · `src/wire/` | WS 소켓 · 대기표 · 프레임 머리(WS · DC) |
| `tests/vectors/` | 규격 짓기의 재생 시험이 TS 판 상태기에 넣은 호출과 결과 — Rust 판이 같은 결과를 내는지 1층이 맞댄다 |
| `examples/recorder/` | 예제 녹음기(Ogg Opus) |

## 게이트

```sh
scripts/gate.sh        # 생성본 어긋남(peer_sync --check) · clippy -D warnings · cargo test
```

2층은 labs(`oxlens-labs/oxe2epy` 시나리오 `peer_recorder`)가 예제 녹음기를 띄워 「봇이 보낸 Opus = 녹음 파일」로 견준다 — 재는 쪽은 labs 다.

## 첫 단계 범위

세션 · 연결 · 방 · 받는 트랙 · 구독 + 발언권 듣기. **아직 없는 것** — 발행 · 발언 요청 · 레이어 고르기(`SUBSCRIBE_LAYER`) · RTCP 보내기(RR · NACK · PLI) · `https`/`wss` · 같은 `Client` 로 다시 `connect`(닫으면 새로 만든다).
