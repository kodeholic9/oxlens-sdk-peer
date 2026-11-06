// author: kodeholic (powered by Claude)
//! Ogg Opus 파일 하나(RFC 7845 · RFC 3533) — RTP 로 받은 Opus 패킷을 그대로 담는다(다시 인코딩하지 않는다).
//! 그래뉼 위치는 패킷의 TOC 가 말하는 길이(RFC 6716 §3.1)로 센다 — 한 쪽에 한 패킷.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

pub struct OggOpus {
    f: File,
    serial: u32,
    seq: u32,
    granule: u64,
    pending: Option<Vec<u8>>,
    pub packets: u64,
}

impl OggOpus {
    pub fn create(path: &Path, serial: u32) -> io::Result<OggOpus> {
        let mut w = OggOpus { f: File::create(path)?, serial, seq: 0, granule: 0, pending: None, packets: 0 };
        let mut head = b"OpusHead".to_vec();
        head.push(1); // version
        head.push(1); // channels
        head.extend_from_slice(&0u16.to_le_bytes()); // pre-skip — RTP 로 받은 것이라 인코더 지연을 모른다
        head.extend_from_slice(&48_000u32.to_le_bytes());
        head.extend_from_slice(&0i16.to_le_bytes()); // output gain
        head.push(0); // mapping family
        w.page(&head, 0x02, 0)?;
        let mut tags = b"OpusTags".to_vec();
        let vendor = b"oxpeer recorder";
        tags.extend_from_slice(&(vendor.len() as u32).to_le_bytes());
        tags.extend_from_slice(vendor);
        tags.extend_from_slice(&0u32.to_le_bytes());
        w.page(&tags, 0x00, 0)?;
        Ok(w)
    }

    /// Opus 패킷 하나 — 바로 앞 것을 쪽으로 내고 이것은 들고 있는다(마지막 쪽에 EOS 를 달기 위해)
    pub fn push(&mut self, packet: &[u8]) -> io::Result<()> {
        if let Some(p) = self.pending.take() {
            self.flush(&p, 0x00)?;
        }
        self.pending = Some(packet.to_vec());
        self.packets += 1;
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<u64> {
        match self.pending.take() {
            Some(p) => self.flush(&p, 0x04)?,
            None => self.page(&[], 0x04, self.granule)?,
        }
        self.f.flush()?;
        Ok(self.granule)
    }

    fn flush(&mut self, p: &[u8], flags: u8) -> io::Result<()> {
        self.granule += samples(p);
        let g = self.granule;
        self.page(p, flags, g)
    }

    fn page(&mut self, packet: &[u8], flags: u8, granule: u64) -> io::Result<()> {
        let mut lacing = vec![255u8; packet.len() / 255];
        lacing.push((packet.len() % 255) as u8);
        let mut h = b"OggS".to_vec();
        h.push(0);
        h.push(flags);
        h.extend_from_slice(&granule.to_le_bytes());
        h.extend_from_slice(&self.serial.to_le_bytes());
        h.extend_from_slice(&self.seq.to_le_bytes());
        h.extend_from_slice(&0u32.to_le_bytes());
        h.push(lacing.len() as u8);
        h.extend_from_slice(&lacing);
        h.extend_from_slice(packet);
        let crc = crc(&h);
        h[22..26].copy_from_slice(&crc.to_le_bytes());
        self.seq += 1;
        self.f.write_all(&h)
    }
}

/// 패킷 하나의 길이(48kHz 표본) — TOC 의 config · 프레임 수(RFC 6716 §3.1)
pub fn samples(p: &[u8]) -> u64 {
    let Some(&toc) = p.first() else { return 0 };
    let config = toc >> 3;
    let per = match config {
        0..=11 => [480, 960, 1920, 2880][usize::from(config % 4)],
        12..=15 => [480, 960][usize::from(config % 2)],
        _ => [120, 240, 480, 960][usize::from(config % 4)],
    };
    let frames = match toc & 0x03 {
        0 => 1,
        1 | 2 => 2,
        _ => p.get(1).map(|b| u64::from(b & 0x3f)).unwrap_or(0),
    };
    per * frames
}

/// Ogg 의 CRC-32(다항 0x04C11DB7 · 반사 없음 · 초기값 0)
fn crc(b: &[u8]) -> u32 {
    let mut c = 0u32;
    for &x in b {
        c ^= u32::from(x) << 24;
        for _ in 0..8 {
            c = if c & 0x8000_0000 != 0 { (c << 1) ^ 0x04C1_1DB7 } else { c << 1 };
        }
    }
    c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn toc_lengths() {
        assert_eq!(samples(&[0x78]), 960); // CELT FB 20ms · 1 프레임(config 15 = hybrid FB 20ms)
        assert_eq!(samples(&[0xF8]), 960); // CELT 20ms
        assert_eq!(samples(&[0xF9]), 1920); // 두 프레임
        assert_eq!(samples(&[0xFB, 0x03]), 2880); // 프레임 셋
        assert_eq!(samples(&[0x08]), 960); // SILK NB 20ms
    }

    #[test]
    fn crc_known() {
        // "OggS" 머리만 든 쪽 — 같은 바이트는 같은 CRC(결정성) · 0 이 아니다
        assert_ne!(crc(b"OggS\0\x02"), 0);
        assert_eq!(crc(b""), 0);
    }
}
