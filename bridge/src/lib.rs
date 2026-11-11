// author: kodeholic (powered by Claude)
//! AI 에이전트 브리지(SDK:RULE-PLAT-AGENT) — 조각마다 순수 · 1층이 그대로 몬다. 배선은 `main.rs`.
//! ★oxpeer 공개 표면만 쓴다 · `unsafe` 없음(libopus FFI 는 opusic-c 안쪽 — SDK:RULE-PLAT-AGENT-AUDIO 코덱 경계).
#![forbid(unsafe_code)]

pub mod codec;
pub mod reply;
pub mod seam;
pub mod turn;
pub mod ws;
