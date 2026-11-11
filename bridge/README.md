# oxbridge — AI 에이전트 브리지

무전 방 하나에 에이전트(`participant_type:2`)로 들어가서 두 가지를 합니다.
- 사람의 발언을 고객 AI 서비스(WebSocket)로 보냅니다.
- AI 가 낸 음성은 발언권을 잡아 방에 냅니다.

계약의 정본은 규격 `SDK:RULE-PLAT-AGENT-*`(context `spec/sdk/areas/plat/plat.md` 「서버사이드 — AI 에이전트 이음매」)이고, 이 파일은 그 계약을 쓰는 법만 적습니다.

## 경계

| 브리지(사실) | 고객 AI(추론) |
|---|---|
| 누가 말하나 · 발언의 시작과 끝(발언권 사건) · Opus ↔ PCM · 발언권 잡기와 놓기 · 송신 시계 | 알아듣기 · 답할지 · 무엇을 · 목소리 · 끼어들린 뒤 생성을 멈출지 |

## 실행

```sh
cargo run -p oxbridge -- --base https://hub.example/media --room ch-1 --ai-url wss://ai.example/agent \
  --token "$TOKEN" --header 'Authorization: Bearer …' --custom persona=dispatcher [--rate 16000] [--silence-ms 3000]
```

- 토큰은 도메인 백엔드가 hub 에서 `participant_type:2` 로 받아 넘깁니다. `--token` 이 없으면 `--user` 와 데모 API 키로 직접 받습니다(시험용).
- 프로세스 하나가 에이전트 세션 하나입니다. 끝나면 표준 출력에 요약 JSON 한 줄을 냅니다.
- 종료 코드: 0 = 정상, 1 = 기동 실패, 3 = 방을 잃음, 4 = 레코드 통 넘침.

## 이음매(판 1)

- 접속 방향: 브리지가 `--ai-url` 로 접속하고, 끊기면 다시 접속합니다. 접속할 때마다 첫 사건은 `start` 입니다(재접속이면 `resumed:true` 와 `gap(disconnected)` 가 뒤따릅니다).
- 브리지 → AI 텍스트 사건: `start` · `participant` · `turn.start` · `turn.end` · `gap` · `reply.state` · `mark` · `stop`.
- 브리지 → AI 바이너리: 머리 8바이트(`kind=1` · `stream=0` · `reserved` · `seq u32 LE`) 뒤에 PCM16LE 모노 20ms 가 붙습니다.
- AI → 브리지 텍스트 사건: `reply.start{reply}` · `mark{name}` · `reply.end{reply}` · `clear`.
- AI → 브리지 바이너리: 머리 없는 PCM16LE 모노입니다. `reply.start` 와 `reply.end` 사이에만 받습니다.
- 표본률: 8 · 12 · 16 · 24 · 48kHz 중 하나이고, 기본은 16kHz 입니다. 리샘플러는 없고 libopus 가 직접 변환합니다.

## 꼭 알아야 할 것

- **턴은 발언권이 정합니다.** 브리지는 VAD 로 짐작하지 않습니다. `turn.end` 는 발언권 사건보다 닫기 유예(200ms)만큼 늦게 옵니다.
- **`reply.start` 는 첫 음성을 무음 상한(기본 3초) 안에 보낼 수 있을 때 보냅니다.** 서버는 허가 뒤 RTP 가 4초(`T1`) 동안 없으면 알리지 않고 발언권을 거둡니다. 브리지는 그보다 먼저 놓고 `truncated(silence)` 를 알립니다. TTS 가 느리면 첫 음성이 준비된 뒤에 `reply.start` 를 보내십시오.
- **사람이 에이전트 발화를 끊으려면 사람 앱이 `ptt.priority ≥ 1` 로 눌러야 합니다.** 에이전트는 늘 우선순위 0 으로 누릅니다. 사람 앱이 전원 0 이면 선점이 없고, 사람은 큐에서 기다립니다.
- **`mark` 는 Twilio 와 같습니다.** 송출을 마치면 `played` 로, 비워지면 `cleared` 로 되돌아옵니다. 대화 이력은 `played` 까지만 남기면 됩니다.
- reply 결과(`reply.state`):

  | 상태 | 뜻 |
  |---|---|
  | `floor_granted` · `floor_queued` | 허가를 받음 · 큐에서 기다림 |
  | `floor_denied{cause}` | 거절 |
  | `preempted` | 사람이 선점 |
  | `truncated{limit:silence\|floor_max\|buffer}` | 무음 상한 · 발화 상한 · 통 상한에서 자름 |
  | `revoked` | 권한 회수 |
  | `lost` | 알림 없이 발언권을 잃음 — 0 이어야 합니다 |
  | `done` | 정상 끝 |

  거절 · 선점 · 자름 뒤에 다시 할지는 AI 가 정합니다.

## 코드 지도

| 파일 | 맡는 것 |
|---|---|
| `src/seam.rs` | 이음매 꼴(사건 · 바이너리 머리 · 표본률) |
| `src/codec.rs` | Opus ↔ PCM(opusic-c — libopus 1.6.1 정적 번들 · 이 크레이트에 `unsafe` 없음) · RFC 6464 음량 |
| `src/turn.rs` | 녹음 파이프라인 발언 정책의 발언 번호를 턴으로 · 버그 계수(유예 창 밖 프레임) |
| `src/reply.rs` | reply · 발언권 상태기(순수 — 시각을 넣어 1층이 몬다) · 무음 상한 · 잃음 탐지 |
| `src/ws.rs` | AI 연결 · 재접속 · 넘침 `gap(overflow)` · `wss://` |
| `src/main.rs` | 배선(oxpeer 공개 표면만) |

빌드에는 cmake 가 필요합니다(libopus 정적 빌드).
