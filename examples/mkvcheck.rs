// author: kodeholic (powered by Claude)
//! 재생기 호환 실측 도구(context 202611/20261107a 미결) — 수신 정리층이 다시 이은 진짜 코덱 프레임(`frames_<코덱>.bin` · lib 시험 `media_fixture_*`)을
//! 녹화기의 Matroska 쓰개(`examples/recorder/mkv.rs` 그대로)로 파일에 쓴다. 판정은 `scripts/media_check.sh` 가 ffprobe · ffmpeg 로 한다.
//!
//!   cargo run --example mkvcheck -- <표본 폴더>

#[allow(dead_code)]
#[path = "recorder/mkv.rs"]
mod mkv;

use std::path::PathBuf;
use std::time::{Duration, UNIX_EPOCH};

fn main() {
    let dir = PathBuf::from(std::env::args().nth(1).expect("표본 폴더"));
    for (name, codec) in [("vp8", mkv::Codec::Vp8), ("h264", mkv::Codec::H264), ("opus", mkv::Codec::Opus)] {
        let b = std::fs::read(dir.join(format!("frames_{name}.bin"))).expect("frames");
        let mut frames = Vec::new();
        let mut i = 0;
        while i + 9 <= b.len() {
            let key = b[i] == 1;
            let ts = u32::from_be_bytes([b[i + 1], b[i + 2], b[i + 3], b[i + 4]]);
            let n = u32::from_be_bytes([b[i + 5], b[i + 6], b[i + 7], b[i + 8]]) as usize;
            frames.push((ts, key, b[i + 9..i + 9 + n].to_vec()));
            i += 9 + n;
        }
        let out = dir.join(format!("{name}.mkv"));
        let mut m = mkv::Mkv::create(&out, codec, &frames[0].2, 1).expect("mkv");
        let t0 = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        for (ts, key, data) in &frames {
            m.push(*ts, *key, data, t0, false).expect("push");
        }
        let n = m.frames;
        m.finish().expect("finish");
        println!("{} {n} 프레임", out.display());
    }
}
