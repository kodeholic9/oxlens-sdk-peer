// author: kodeholic (powered by Claude)
//! Matroska 파일 하나에 트랙 하나(20261107a B4) — SDK 가 조립한 코덱 프레임을 그대로 담는다(다시 인코딩하지 않는다).
//! VP8 은 그대로 · H264 는 Annex-B 를 길이 앞붙임(avcC)으로 바꾼다(Matroska `V_MPEG4/ISO/AVC`) · Opus 는 그대로(`A_OPUS`).
//! 블록 시각은 RTP 시각 차(그 코덱 시계)로 센다 — SR 이 늦게 와도 뒤로 가지 않는다. 파일 시작 벽시계(DateUTC)는 첫 SR 동기 프레임에서
//! 거꾸로 셈해 닫을 때 고쳐 쓴다(동기 프레임이 없으면 첫 프레임을 받은 시각).
//! Segment · Cluster 는 크기를 모르는 꼴(라이브 Matroska)로 쓴다 — 쓰는 동안 앞으로 돌아가 크기를 고치지 않는다.

use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const ID_EBML: u32 = 0x1A45_DFA3;
const ID_SEGMENT: u32 = 0x1853_8067;
const ID_INFO: u32 = 0x1549_A966;
const ID_TRACKS: u32 = 0x1654_AE6B;
const ID_CLUSTER: u32 = 0x1F43_B675;
const UNKNOWN_SIZE: [u8; 8] = [0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
/// Matroska 날짜의 바닥(2001-01-01T00:00:00Z) — 유닉스 초
const MKV_EPOCH: u64 = 978_307_200;
/// 클러스터 하나의 블록 상대 시각은 i16(ms) — 넉넉히 끊는다
const CLUSTER_SPAN_MS: u64 = 30_000;
const AUDIO_CLUSTER_MS: u64 = 5_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    Vp8,
    H264,
    Opus,
}

impl Codec {
    pub fn of(name: &str) -> Option<Codec> {
        match name.to_ascii_lowercase().as_str() {
            "vp8" => Some(Codec::Vp8),
            "h264" => Some(Codec::H264),
            "opus" => Some(Codec::Opus),
            _ => None,
        }
    }

    fn clock(self) -> u64 {
        if self == Codec::Opus { 48_000 } else { 90_000 }
    }
}

fn vint_size(n: u64) -> Vec<u8> {
    for len in 1..=8u32 {
        if n < (1u64 << (7 * len)) - 1 {
            let mut b = n.to_be_bytes()[8 - len as usize..].to_vec();
            b[0] |= 0x80 >> (len - 1);
            return b;
        }
    }
    UNKNOWN_SIZE.to_vec()
}

fn id_bytes(id: u32) -> Vec<u8> {
    let b = id.to_be_bytes();
    let skip = b.iter().take_while(|x| **x == 0).count();
    b[skip..].to_vec()
}

fn el(id: u32, body: &[u8]) -> Vec<u8> {
    let mut v = id_bytes(id);
    v.extend(vint_size(body.len() as u64));
    v.extend_from_slice(body);
    v
}

fn uint(id: u32, n: u64) -> Vec<u8> {
    let b = n.to_be_bytes();
    let skip = b.iter().take_while(|x| **x == 0).count().min(7);
    el(id, &b[skip..])
}

fn float(id: u32, f: f64) -> Vec<u8> {
    el(id, &f.to_be_bytes())
}

fn text(id: u32, s: &str) -> Vec<u8> {
    el(id, s.as_bytes())
}

/// Annex-B → NAL 목록
pub fn nals(b: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut starts = Vec::new();
    let mut i = 0;
    while i + 3 <= b.len() {
        if b[i] == 0 && b[i + 1] == 0 && b[i + 2] == 1 {
            starts.push(i + 3);
            i += 3;
        } else {
            i += 1;
        }
    }
    for (k, &s) in starts.iter().enumerate() {
        let mut e = starts.get(k + 1).map(|n| n - 3).unwrap_or(b.len());
        if k + 1 < starts.len() && e > s && b[e - 1] == 0 {
            e -= 1;
        }
        if e > s {
            out.push(&b[s..e]);
        }
    }
    out
}

/// avcC(ISO 14496-15 §5.2.4.1) — SPS · PPS 가 없으면 줄의 profile(기본 42e0 · level 1f)로 빈 목록
fn avcc(sps: Option<&[u8]>, pps: Option<&[u8]>) -> Vec<u8> {
    let (p, c, l) = match sps {
        Some(s) if s.len() >= 4 => (s[1], s[2], s[3]),
        _ => (0x42, 0xe0, 0x1f),
    };
    let mut v = vec![1, p, c, l, 0xff];
    match sps {
        Some(s) => {
            v.push(0xe1);
            v.extend((s.len() as u16).to_be_bytes());
            v.extend_from_slice(s);
        }
        None => v.push(0xe0),
    }
    match pps {
        Some(s) => {
            v.push(1);
            v.extend((s.len() as u16).to_be_bytes());
            v.extend_from_slice(s);
        }
        None => v.push(0),
    }
    v
}

/// VP8 키프레임 머리(RFC 6386 §9.1) — 시작 부호 9d 01 2a 뒤 14비트 너비 · 높이
fn vp8_dims(f: &[u8]) -> Option<(u64, u64)> {
    if f.len() < 10 || f[0] & 1 != 0 || f[3..6] != [0x9d, 0x01, 0x2a] {
        return None;
    }
    let w = u64::from(u16::from_le_bytes([f[6], f[7]]) & 0x3fff);
    let h = u64::from(u16::from_le_bytes([f[8], f[9]]) & 0x3fff);
    Some((w, h))
}

/// 비트 읽개(지수 골롬 — H.264 §9.1) · 에뮬레이션 방지 바이트를 뗀 RBSP 위에서
struct Bits {
    b: Vec<u8>,
    i: usize,
}

impl Bits {
    fn new(nal: &[u8]) -> Bits {
        let mut b = Vec::with_capacity(nal.len());
        let mut zeros = 0;
        for &x in nal {
            if zeros >= 2 && x == 3 {
                zeros = 0;
                continue;
            }
            zeros = if x == 0 { zeros + 1 } else { 0 };
            b.push(x);
        }
        Bits { b, i: 0 }
    }

    fn bit(&mut self) -> Option<u64> {
        let byte = *self.b.get(self.i / 8)?;
        let v = (byte >> (7 - self.i % 8)) & 1;
        self.i += 1;
        Some(u64::from(v))
    }

    fn bits(&mut self, n: u32) -> Option<u64> {
        let mut v = 0;
        for _ in 0..n {
            v = (v << 1) | self.bit()?;
        }
        Some(v)
    }

    fn ue(&mut self) -> Option<u64> {
        let mut z = 0;
        while self.bit()? == 0 {
            z += 1;
            if z > 31 {
                return None;
            }
        }
        Some((1u64 << z) - 1 + self.bits(z)?)
    }

    fn se(&mut self) -> Option<i64> {
        let k = self.ue()? as i64;
        Some(if k % 2 == 1 { (k + 1) / 2 } else { -(k / 2) })
    }
}

/// SPS(H.264 §7.3.2.1.1) → 잘라 낸 뒤 화면 크기
fn h264_dims(sps: &[u8]) -> Option<(u64, u64)> {
    let mut r = Bits::new(sps.get(1..)?);
    let profile = r.bits(8)?;
    r.bits(16)?;
    r.ue()?;
    let mut chroma = 1;
    if [100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135].contains(&profile) {
        chroma = r.ue()?;
        if chroma == 3 {
            r.bit()?;
        }
        r.ue()?;
        r.ue()?;
        r.bit()?;
        if r.bit()? == 1 {
            for k in 0..if chroma == 3 { 12 } else { 8 } {
                if r.bit()? == 1 {
                    let n = if k < 6 { 16 } else { 64 };
                    let (mut last, mut next) = (8i64, 8i64);
                    for _ in 0..n {
                        if next != 0 {
                            next = (last + r.se()? + 256) % 256;
                        }
                        last = if next == 0 { last } else { next };
                    }
                }
            }
        }
    }
    r.ue()?;
    match r.ue()? {
        0 => {
            r.ue()?;
        }
        1 => {
            r.bit()?;
            r.se()?;
            r.se()?;
            for _ in 0..r.ue()? {
                r.se()?;
            }
        }
        _ => {}
    }
    r.ue()?;
    r.bit()?;
    let w_mbs = r.ue()? + 1;
    let h_units = r.ue()? + 1;
    let frame_mbs_only = r.bit()?;
    if frame_mbs_only == 0 {
        r.bit()?;
    }
    r.bit()?;
    let (mut cl, mut cr, mut ct, mut cb) = (0, 0, 0, 0);
    if r.bit()? == 1 {
        cl = r.ue()?;
        cr = r.ue()?;
        ct = r.ue()?;
        cb = r.ue()?;
    }
    let (sx, sy) = match chroma {
        0 | 3 => (1, 2 - frame_mbs_only),
        2 => (2, 2 - frame_mbs_only),
        _ => (2, 2 * (2 - frame_mbs_only)),
    };
    let w = (w_mbs * 16).checked_sub(sx * (cl + cr))?;
    let h = ((2 - frame_mbs_only) * h_units * 16).checked_sub(sy * (ct + cb))?;
    Some((w, h))
}

/// Annex-B 프레임 → 4바이트 길이 앞붙임
fn length_prefixed(b: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(b.len() + 8);
    for n in nals(b) {
        v.extend((n.len() as u32).to_be_bytes());
        v.extend_from_slice(n);
    }
    v
}

pub struct Mkv {
    f: File,
    codec: Codec,
    date_at: u64,
    first_ts: Option<u32>,
    unwrapped: u64,
    last_ts: u32,
    cluster_at: Option<u64>,
    start_wall: Option<SystemTime>,
    pub frames: u64,
    pub keyframes: u64,
    pub synced: u64,
    pub first_rtp: Option<u32>,
    pub last_rtp: u32,
    pub last_ms: u64,
}

impl Mkv {
    /// 첫 프레임(영상은 키프레임)을 보고 연다 — 머리(코덱 사적 자료 · 화면 크기)가 그 프레임에서 나온다
    pub fn create(path: &Path, codec: Codec, first: &[u8], uid: u64) -> io::Result<Mkv> {
        let mut f = File::create(path)?;
        let ebml = [uint(0x4286, 1), uint(0x42F7, 1), uint(0x42F2, 4), uint(0x42F3, 8), text(0x4282, "matroska"), uint(0x4287, 4), uint(0x4285, 2)].concat();
        f.write_all(&el(ID_EBML, &ebml))?;
        let mut seg = id_bytes(ID_SEGMENT);
        seg.extend_from_slice(&UNKNOWN_SIZE);
        f.write_all(&seg)?;
        // Info — DateUTC 는 8바이트 자리를 두고 닫을 때 고쳐 쓴다
        let head = [uint(0x2AD7B1, 1_000_000), text(0x4D80, "oxpeer recorder"), text(0x5741, "oxpeer recorder")].concat();
        let date = el(0x4461, &[0u8; 8]);
        let info_body_len = head.len() + date.len();
        let mut info = id_bytes(ID_INFO);
        info.extend(vint_size(info_body_len as u64));
        info.extend(&head);
        let date_at = f.stream_position()? + info.len() as u64 + (date.len() - 8) as u64;
        info.extend(&date);
        f.write_all(&info)?;
        let (id, kind, private, media) = match codec {
            Codec::Vp8 => {
                let (w, h) = vp8_dims(first).unwrap_or((0, 0));
                ("V_VP8", 1, None, el(0xE0, &[uint(0xB0, w), uint(0xBA, h)].concat()))
            }
            Codec::H264 => {
                let ns = nals(first);
                let sps = ns.iter().find(|n| n[0] & 0x1f == 7).copied();
                let pps = ns.iter().find(|n| n[0] & 0x1f == 8).copied();
                let (w, h) = sps.and_then(h264_dims).unwrap_or((0, 0));
                ("V_MPEG4/ISO/AVC", 1, Some(avcc(sps, pps)), el(0xE0, &[uint(0xB0, w), uint(0xBA, h)].concat()))
            }
            Codec::Opus => {
                let mut head = b"OpusHead".to_vec();
                head.push(1);
                head.push(1);
                head.extend_from_slice(&0u16.to_le_bytes());
                head.extend_from_slice(&48_000u32.to_le_bytes());
                head.extend_from_slice(&0i16.to_le_bytes());
                head.push(0);
                ("A_OPUS", 2, Some(head), el(0xE1, &[float(0xB5, 48_000.0), uint(0x9F, 1)].concat()))
            }
        };
        let mut entry = [uint(0xD7, 1), uint(0x73C5, uid), uint(0x83, kind), text(0x86, id), uint(0x9C, 0)].concat();
        if let Some(p) = private {
            entry.extend(el(0x63A2, &p));
        }
        entry.extend(media);
        f.write_all(&el(ID_TRACKS, &el(0xAE, &entry)))?;
        Ok(Mkv { f, codec, date_at, first_ts: None, unwrapped: 0, last_ts: 0, cluster_at: None, start_wall: None, frames: 0, keyframes: 0, synced: 0, first_rtp: None, last_rtp: 0, last_ms: 0 })
    }

    /// 프레임 하나 — RTP 시각 · 키프레임 · 벽시계(SR 로 이은 것이면 synced)
    pub fn push(&mut self, ts: u32, key: bool, data: &[u8], wall: SystemTime, synced: bool) -> io::Result<()> {
        let clock = self.codec.clock();
        match self.first_ts {
            None => {
                self.first_ts = Some(ts);
                self.first_rtp = Some(ts);
            }
            Some(_) => {
                let d = ts.wrapping_sub(self.last_ts) as i32;
                self.unwrapped = self.unwrapped.saturating_add_signed(i64::from(d));
            }
        }
        self.last_ts = ts;
        self.last_rtp = ts;
        let ms = self.unwrapped * 1_000 / clock;
        // 시작 벽시계 = 이 프레임 벽시계 − 지금까지 흐른 RTP 시간 — 동기된 것이 오면 그것으로 고친다
        if self.start_wall.is_none() || (synced && self.synced == 0) {
            self.start_wall = Some(wall - std::time::Duration::from_millis(ms));
        }
        if synced {
            self.synced += 1;
        }
        let video = self.codec != Codec::Opus;
        let new_cluster = match self.cluster_at {
            None => true,
            Some(c) => ms < c || ms - c >= CLUSTER_SPAN_MS || (video && key) || (!video && ms - c >= AUDIO_CLUSTER_MS),
        };
        if new_cluster {
            let mut cl = id_bytes(ID_CLUSTER);
            cl.extend_from_slice(&UNKNOWN_SIZE);
            cl.extend(uint(0xE7, ms));
            self.f.write_all(&cl)?;
            self.cluster_at = Some(ms);
        }
        let rel = (ms - self.cluster_at.unwrap_or(ms)) as i16;
        let body = if self.codec == Codec::H264 { length_prefixed(data) } else { data.to_vec() };
        let mut sb = vec![0x81];
        sb.extend(rel.to_be_bytes());
        sb.push(if key { 0x80 } else { 0x00 });
        sb.extend(body);
        self.f.write_all(&el(0xA3, &sb))?;
        self.frames += 1;
        if key {
            self.keyframes += 1;
        }
        self.last_ms = ms;
        Ok(())
    }

    /// 닫는다 — DateUTC 를 고쳐 쓰고 파일 시작 벽시계(유닉스 ms)를 돌려준다
    pub fn finish(mut self) -> io::Result<Option<u64>> {
        let start = self.start_wall.and_then(|w| w.duration_since(UNIX_EPOCH).ok());
        if let Some(s) = start {
            let ns = (s.as_nanos() as i128 - i128::from(MKV_EPOCH) * 1_000_000_000) as i64;
            self.f.seek(SeekFrom::Start(self.date_at))?;
            self.f.write_all(&ns.to_be_bytes())?;
        }
        self.f.flush()?;
        Ok(start.map(|s| s.as_millis() as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vint_and_ids() {
        assert_eq!(vint_size(1), vec![0x81]);
        assert_eq!(vint_size(200), vec![0x40, 200]);
        assert_eq!(id_bytes(ID_EBML), vec![0x1A, 0x45, 0xDF, 0xA3]);
        assert_eq!(uint(0xD7, 1), vec![0xD7, 0x81, 0x01]);
    }

    #[test]
    fn annexb_to_avcc_and_lengths() {
        let f = [0, 0, 0, 1, 0x67, 0x42, 0xe0, 0x1f, 0, 0, 0, 1, 0x68, 0xce, 0, 0, 1, 0x65, 0x88];
        let ns = nals(&f);
        assert_eq!(ns, vec![&[0x67, 0x42, 0xe0, 0x1f][..], &[0x68, 0xce][..], &[0x65, 0x88][..]]);
        assert_eq!(length_prefixed(&f[8..]), vec![0, 0, 0, 2, 0x68, 0xce, 0, 0, 0, 2, 0x65, 0x88]);
        let c = avcc(Some(ns[0]), Some(ns[1]));
        assert_eq!(&c[..6], &[1, 0x42, 0xe0, 0x1f, 0xff, 0xe1]);
        assert_eq!(avcc(None, None), vec![1, 0x42, 0xe0, 0x1f, 0xff, 0xe0, 0]);
    }

    #[test]
    fn dims() {
        // 640x480 VP8 키프레임 머리
        let k = [0x10, 0x02, 0x00, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0xe0, 0x01];
        assert_eq!(vp8_dims(&k), Some((640, 480)));
        // 1280x720 baseline SPS(지수 골롬으로 지은 것 — 80 x 45 매크로블록 · 자르기 없음)
        let sps = [0x67, 0x42, 0xc0, 0x1f, 0xda, 0x01, 0x40, 0x16, 0xe4];
        assert_eq!(h264_dims(&sps), Some((1280, 720)));
    }

    #[test]
    fn writes_blocks_with_rtp_time() {
        let dir = std::env::temp_dir().join(format!("oxpeer-mkv-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("a.mkv");
        let mut m = Mkv::create(&p, Codec::Opus, &[0xf8], 7).unwrap();
        let t0 = UNIX_EPOCH + std::time::Duration::from_secs(1_800_000_000);
        m.push(1000, true, &[0xf8, 1], t0, false).unwrap();
        m.push(1000 + 960, true, &[0xf8, 2], t0, true).unwrap();
        assert_eq!(m.last_ms, 20);
        let start = m.finish().unwrap().unwrap();
        // 동기 프레임(20ms 지점)의 벽시계에서 거꾸로 — 시작 = t0 − 20ms
        assert_eq!(start, 1_800_000_000_000 - 20);
        let b = std::fs::read(&p).unwrap();
        assert_eq!(&b[..4], &[0x1A, 0x45, 0xDF, 0xA3]);
        assert!(b.windows(4).any(|w| w == [0x81, 0x00, 0x14, 0x80]));
        std::fs::remove_dir_all(&dir).ok();
    }
}
