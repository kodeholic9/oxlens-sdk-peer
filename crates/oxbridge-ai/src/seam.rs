// author: kodeholic (powered by Claude)
//! 이음매 꼴(SDK:RULE-PLAT-AGENT-EVENTS · SDK:RULE-PLAT-AGENT-AUDIO) — 텍스트 프레임 = JSON 사건 하나(`type`) · 바이너리 = 머리 8바이트 + PCM16LE(브리지 → AI) · 머리 없는 PCM(AI → 브리지).

use serde_json::{Value, json};

/// 이음매 판 — 없던 사건 · 칸을 더할 때만 올린다
pub const VERSION: u32 = 1;
/// 프레임 길이(ms)
pub const FRAME_MS: u32 = 20;
/// 머리 `kind` — 음성
pub const KIND_AUDIO: u8 = 1;

/// libopus 가 직접 내고 받는 표본률(리샘플러 없음)
pub fn rate_ok(rate: u32) -> bool {
    matches!(rate, 8_000 | 12_000 | 16_000 | 24_000 | 48_000)
}

/// 20ms 표본 수
pub fn frame_samples(rate: u32) -> usize {
    (rate * FRAME_MS / 1_000) as usize
}

/// 브리지 → AI 바이너리 프레임 — `kind u8` · `stream u8`(1판 0) · `reserved u16` · `seq u32` · PCM16LE(리틀엔디언)
pub fn audio_frame(seq: u32, pcm: &[i16]) -> Vec<u8> {
    let mut b = Vec::with_capacity(8 + pcm.len() * 2);
    b.extend([KIND_AUDIO, 0, 0, 0]);
    b.extend(seq.to_le_bytes());
    for s in pcm {
        b.extend(s.to_le_bytes());
    }
    b
}

/// AI → 브리지 사건
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FromAi {
    ReplyStart { reply: String },
    ReplyEnd { reply: String },
    Mark { name: String },
    Clear,
}

/// 텍스트 프레임 하나를 읽는다 — 모르는 사건은 `Ok(None)`(판 규칙: 받는 쪽은 모르는 사건 · 칸을 버린다)
pub fn parse_text(s: &str) -> Result<Option<FromAi>, String> {
    let v: Value = serde_json::from_str(s).map_err(|e| format!("json: {e}"))?;
    let t = v.get("type").and_then(Value::as_str).ok_or("type 칸이 없다")?;
    let field = |k: &str| v.get(k).and_then(Value::as_str).map(str::to_string).ok_or_else(|| format!("{t}: {k} 칸이 없다"));
    Ok(Some(match t {
        "reply.start" => FromAi::ReplyStart { reply: field("reply")? },
        "reply.end" => FromAi::ReplyEnd { reply: field("reply")? },
        "mark" => FromAi::Mark { name: field("name")? },
        "clear" => FromAi::Clear,
        _ => return Ok(None),
    }))
}

/// 머리 없는 PCM16LE → 표본 · 홀수 바이트는 남긴다(다음 프레임과 잇는다)
pub fn pcm_from_bytes(carry: &mut Vec<u8>, b: &[u8]) -> Vec<i16> {
    carry.extend_from_slice(b);
    let n = carry.len() / 2;
    let out = (0..n).map(|i| i16::from_le_bytes([carry[2 * i], carry[2 * i + 1]])).collect();
    carry.drain(..n * 2);
    out
}

/// 사건 하나(브리지 → AI) — `type` 칸 + 사건 칸
pub fn event(t: &str, fields: Value) -> String {
    let mut o = serde_json::Map::new();
    o.insert("type".into(), json!(t));
    if let Value::Object(m) = fields {
        o.extend(m);
    }
    Value::Object(o).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audio_frame_header_is_little_endian() {
        let b = audio_frame(0x0102_0304, &[1, -2]);
        assert_eq!(b, vec![1, 0, 0, 0, 4, 3, 2, 1, 1, 0, 0xFE, 0xFF]);
    }

    #[test]
    fn parse_known_and_unknown() {
        assert_eq!(parse_text(r#"{"type":"reply.start","reply":"r1"}"#), Ok(Some(FromAi::ReplyStart { reply: "r1".into() })));
        assert_eq!(parse_text(r#"{"type":"clear"}"#), Ok(Some(FromAi::Clear)));
        assert_eq!(parse_text(r#"{"type":"text.out","x":1}"#), Ok(None), "모르는 사건은 버린다");
        assert!(parse_text(r#"{"type":"mark"}"#).is_err(), "칸이 없으면 오류");
        assert!(parse_text("not json").is_err());
    }

    #[test]
    fn odd_bytes_carry_to_next_chunk() {
        let mut c = Vec::new();
        assert_eq!(pcm_from_bytes(&mut c, &[1, 0, 2]), vec![1]);
        assert_eq!(pcm_from_bytes(&mut c, &[0]), vec![2]);
        assert!(c.is_empty());
    }

    #[test]
    fn rates_are_the_libopus_set() {
        assert!([8_000, 12_000, 16_000, 24_000, 48_000].iter().all(|r| rate_ok(*r)));
        assert!(!rate_ok(44_100) && !rate_ok(22_050));
        assert_eq!(frame_samples(16_000), 320);
    }

    #[test]
    fn event_carries_type_and_fields() {
        let v: Value = serde_json::from_str(&event("mark", json!({ "name": "a", "status": "played" }))).unwrap();
        assert_eq!(v, json!({ "type": "mark", "name": "a", "status": "played" }));
    }
}
