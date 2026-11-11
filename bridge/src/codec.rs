// author: kodeholic (powered by Claude)
//! 코덱(SDK:RULE-PLAT-AGENT-AUDIO) — Opus ↔ PCM16 모노를 이음매 표본률로 직접(리샘플러 없음 · RTP 시계는 늘 48kHz).
//! ★코덱 경계 — libopus(C)는 opusic-c 안쪽에서만 부른다 · 이 크레이트에 `unsafe` 는 없다(`#![forbid(unsafe_code)]`).
//! 입력이 망에서 오는 FFI 라 출력 버퍼는 최대 프레임(120ms)으로 · 오류 반환은 그 프레임을 버리고 센다.

use opusic_c::{Application, Channels, Decoder, Encoder, SampleRate};

/// Opus 최대 패킷 길이(ms) — 출력 버퍼 크기
const MAX_PACKET_MS: usize = 120;
/// 인코딩 결과 상한(바이트 — RFC 6716 한 프레임 상한 1275 · 넉넉히)
const MAX_PACKET_BYTES: usize = 1_500;

pub struct Codec {
    dec: Decoder,
    enc: Encoder,
    rate: u32,
    /// 디코딩 · 인코딩 오류로 버린 프레임
    pub decode_errors: u64,
    pub encode_errors: u64,
}

fn sample_rate(rate: u32) -> Option<SampleRate> {
    Some(match rate {
        8_000 => SampleRate::Hz8000,
        12_000 => SampleRate::Hz12000,
        16_000 => SampleRate::Hz16000,
        24_000 => SampleRate::Hz24000,
        48_000 => SampleRate::Hz48000,
        _ => return None,
    })
}

impl Codec {
    pub fn new(rate: u32) -> Result<Codec, String> {
        let sr = sample_rate(rate).ok_or_else(|| format!("표본률 {rate} — libopus 집합(8 · 12 · 16 · 24 · 48kHz) 밖"))?;
        let dec = Decoder::new(Channels::Mono, sr).map_err(|e| format!("디코더: {e:?}"))?;
        let enc = Encoder::new(Channels::Mono, sr, Application::Voip).map_err(|e| format!("인코더: {e:?}"))?;
        Ok(Codec { dec, enc, rate, decode_errors: 0, encode_errors: 0 })
    }

    /// 패킷 하나 → PCM. 메운 프레임(TOC 한 바이트 · 길이 0 프레임)은 디코더가 은닉한다 · 오류는 None + 계수
    pub fn decode(&mut self, pkt: &[u8]) -> Option<Vec<i16>> {
        let mut out = vec![0u16; self.rate as usize * MAX_PACKET_MS / 1_000];
        match self.dec.decode_to_slice(pkt, &mut out, false) {
            Ok(n) => Some(out[..n].iter().map(|s| *s as i16).collect()),
            Err(_) => {
                self.decode_errors += 1;
                None
            }
        }
    }

    /// PCM 20ms 한 프레임 → 패킷 · 오류는 None + 계수
    pub fn encode(&mut self, pcm: &[i16]) -> Option<Vec<u8>> {
        let input: Vec<u16> = pcm.iter().map(|s| *s as u16).collect();
        let mut out = vec![0u8; MAX_PACKET_BYTES];
        match self.enc.encode_to_slice(&input, &mut out) {
            Ok(n) => {
                out.truncate(n);
                Some(out)
            }
            Err(_) => {
                self.encode_errors += 1;
                None
            }
        }
    }
}

/// 음량(RFC 6464 — 0 ~ 127 · −dBov · 0 이 가장 크다 · 127 은 무음)
pub fn level_dbov(pcm: &[i16]) -> u8 {
    if pcm.is_empty() {
        return 127;
    }
    let sum: f64 = pcm.iter().map(|s| {
        let x = f64::from(*s) / 32_768.0;
        x * x
    }).sum();
    let rms = (sum / pcm.len() as f64).sqrt();
    if rms <= 0.0 {
        return 127;
    }
    let db = -20.0 * rms.log10();
    db.round().clamp(0.0, 127.0) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tone(rate: u32, n: usize, amp: f64) -> Vec<i16> {
        (0..n).map(|i| (amp * (2.0 * std::f64::consts::PI * 440.0 * i as f64 / f64::from(rate)).sin()) as i16).collect()
    }

    #[test]
    fn round_trip_keeps_frame_length_at_every_rate() {
        for rate in [8_000, 12_000, 16_000, 24_000, 48_000] {
            let mut c = Codec::new(rate).unwrap();
            let n = crate::seam::frame_samples(rate);
            let pkt = c.encode(&tone(rate, n, 8_000.0)).expect("인코딩");
            assert!(!pkt.is_empty() && pkt.len() < MAX_PACKET_BYTES);
            let pcm = c.decode(&pkt).expect("디코딩");
            assert_eq!(pcm.len(), n, "{rate}Hz 20ms");
        }
    }

    #[test]
    fn filled_frame_is_concealed_not_an_error() {
        let mut c = Codec::new(16_000).unwrap();
        let pkt = c.encode(&tone(16_000, 320, 8_000.0)).unwrap();
        let _ = c.decode(&pkt).unwrap();
        // 파이프라인이 메운 프레임 — 앞 TOC 의 config · code 0 만(SDK:RULE-PLAT-REC-GAP)
        let plc = c.decode(&[pkt[0] & 0xFC]).expect("메운 프레임은 은닉");
        assert_eq!(plc.len(), 320);
        assert_eq!(c.decode_errors, 0);
    }

    #[test]
    fn garbage_never_kills_the_process() {
        let mut c = Codec::new(16_000).unwrap();
        let mut x: u32 = 0x1234_5678;
        for len in 0..400usize {
            let pkt: Vec<u8> = (0..len)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 17;
                    x ^= x << 5;
                    x as u8
                })
                .collect();
            let _ = c.decode(&pkt);
        }
        // 잘린 패킷(코드 3 · 프레임 수만 있고 몸이 없다)
        let _ = c.decode(&[0xFB]);
        let _ = c.decode(&[0xFB, 0x30]);
        assert!(c.decode_errors > 0, "무작위 입력 일부는 오류로 센다");
    }

    #[test]
    fn rate_outside_the_libopus_set_is_refused() {
        assert!(Codec::new(44_100).is_err());
    }

    #[test]
    fn level_is_rfc6464() {
        assert_eq!(level_dbov(&[0; 320]), 127);
        assert_eq!(level_dbov(&[i16::MAX; 320]), 0);
        let quiet = level_dbov(&tone(16_000, 320, 100.0));
        let loud = level_dbov(&tone(16_000, 320, 16_000.0));
        assert!(quiet > 50 && loud < 10, "작은 소리는 큰 값 · 큰 소리는 작은 값 ({quiet} · {loud})");
    }
}
