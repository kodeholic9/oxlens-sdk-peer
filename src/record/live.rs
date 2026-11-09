// author: kodeholic (powered by Claude)
//! 라이브 레코드 통(지점 ① — SDK:RULE-PLAT-REC-API) · ★넘치면 꼬리 읽기로 강등(역압).
//!
//! 기록기는 통이 차면 그 통에 더 보내지 않고(`degraded = (넘친 첫 순번, 그때 열린 파트)`) 보낼 손잡이를 놓는다 — 기록기 · 다른 통은 막히지 않는다.
//! 통 쪽은 버퍼에 남은 것을 다 꺼낸 뒤 손잡이가 닫힌 것을 보고, 넘친 첫 순번부터 그 파트를 처음부터 다시 읽는다(닫힌 청크까지 —
//! 쓰는 중인 청크는 길이 자리가 아직 비어 있어 거기서 멈추고 tick 마다 다시 본다). 순번으로 앞 것을 거른다. 기록기가 닫히고 마지막 파트에 Footer 가 있으면 끝.

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;

use super::{Channel, Record};

const MAGIC: &[u8; 8] = b"\x89MCAP0\r\n";
const OP_FOOTER: u8 = 0x02;
const OP_CHANNEL: u8 = 0x04;
const OP_MESSAGE: u8 = 0x05;
const OP_CHUNK: u8 = 0x06;

/// 기록기와 통이 같이 보는 상태
#[derive(Default)]
pub(crate) struct Shared {
    pub(crate) state: Mutex<SubState>,
}

#[derive(Default)]
pub(crate) struct SubState {
    /// 원시 파트(디렉터리 · 녹음 ID) — 없으면 강등할 곳이 없다
    pub(crate) raw: Option<(PathBuf, String)>,
    pub(crate) tick: Duration,
    /// 넘쳤다 — (넘친 첫 순번, 그때 열린 파트 번호)
    pub(crate) degraded: Option<(u32, u32)>,
    /// 원시 기록을 끈 구성에서 넘쳐 멈췄다
    pub(crate) stopped: bool,
    /// 기록기가 닫혔다
    pub(crate) closed: bool,
}

/// 라이브 레코드 통 — `Client::records` 가 연다
pub struct Records {
    rx: mpsc::Receiver<Record>,
    shared: Arc<Shared>,
    tail: Option<Tail>,
}

impl Records {
    pub(crate) fn new(rx: mpsc::Receiver<Record>, shared: Arc<Shared>) -> Records {
        Records { rx, shared, tail: None }
    }

    /// 다음 레코드 — 넘쳤으면 원시 파트 꼬리에서 이어 준다 · 끝나면 None
    pub async fn next(&mut self) -> Option<Record> {
        loop {
            if let Some(t) = self.tail.as_mut() {
                return t.next(&self.shared).await;
            }
            if let Some(r) = self.rx.recv().await {
                return Some(r);
            }
            let st = self.shared.state.lock().ok()?;
            let (Some((from, part)), Some((dir, id))) = (st.degraded, st.raw.clone()) else { return None };
            let tick = st.tick.max(Duration::from_millis(20));
            drop(st);
            self.tail = Some(Tail { dir, id, from, part, cur: None, buf: VecDeque::new(), tick });
        }
    }

    /// 넘쳐 꼬리 읽기로 갈아탔나
    pub fn degraded(&self) -> bool {
        self.tail.is_some()
    }

    /// 원시 기록을 끈 구성에서 넘쳐 멈췄나(그 뒤 레코드는 이 통에 없다)
    pub fn stopped(&self) -> bool {
        self.shared.state.lock().map(|s| s.stopped).unwrap_or(false)
    }
}

struct Tail {
    dir: PathBuf,
    id: String,
    from: u32,
    part: u32,
    cur: Option<TailPart>,
    buf: VecDeque<Record>,
    tick: Duration,
}

impl Tail {
    async fn next(&mut self, shared: &Shared) -> Option<Record> {
        loop {
            if let Some(r) = self.buf.pop_front() {
                return Some(r);
            }
            let path = self.dir.join(format!("{}-{:04}.mcap", self.id, self.part));
            if self.cur.is_none() && path.exists() {
                self.cur = Some(TailPart { path: path.clone(), off: 0, topics: HashMap::new(), done: false });
            }
            if let Some(c) = self.cur.as_mut() {
                if c.poll(self.from, &mut self.buf).is_err() {
                    return None;
                }
                if !self.buf.is_empty() {
                    continue;
                }
                if c.done {
                    self.part += 1;
                    self.cur = None;
                    let next = self.dir.join(format!("{}-{:04}.mcap", self.id, self.part));
                    if next.exists() {
                        continue;
                    }
                }
            }
            if shared.state.lock().map(|s| s.closed).unwrap_or(true) && self.cur.as_ref().is_none_or(|c| c.done) {
                // 닫힌 뒤 — 마지막으로 한 번 더 보고 끝
                let next = self.dir.join(format!("{}-{:04}.mcap", self.id, self.part));
                if !next.exists() {
                    return None;
                }
                continue;
            }
            tokio::time::sleep(self.tick).await;
        }
    }
}

/// 쓰는 중일 수 있는 파트 하나 — 다 읽힌 맨 위 레코드 끝(`off`)까지만 나아간다
struct TailPart {
    path: PathBuf,
    off: u64,
    topics: HashMap<u16, Channel>,
    done: bool,
}

impl TailPart {
    fn poll(&mut self, from: u32, out: &mut VecDeque<Record>) -> std::io::Result<()> {
        let mut f = File::open(&self.path)?;
        f.seek(SeekFrom::Start(self.off))?;
        let mut b = Vec::new();
        f.read_to_end(&mut b)?;
        let mut at = 0usize;
        if self.off == 0 {
            if b.len() < MAGIC.len() {
                return Ok(());
            }
            if &b[..8] != MAGIC {
                return Err(std::io::Error::other("MCAP 매직이 아니다"));
            }
            at = 8;
        }
        while let Some((op, body, next)) = record_at(&b, at) {
            match op {
                OP_CHUNK => self.chunk(body, from, out)?,
                OP_FOOTER => {
                    self.done = true;
                    at = next;
                    break;
                }
                _ => self.one(op, body, from, out),
            }
            at = next;
        }
        self.off += at as u64;
        Ok(())
    }

    fn chunk(&mut self, body: &[u8], from: u32, out: &mut VecDeque<Record>) -> std::io::Result<()> {
        // 청크 = 시작 · 끝 시각 u64 · 풀린 크기 u64 · crc u32 · 압축 이름 · 레코드 길이 u64 · 레코드
        let bad = || std::io::Error::other("청크 꼴");
        let n = u32::from_le_bytes(body.get(28..32).ok_or_else(bad)?.try_into().map_err(|_| bad())?) as usize;
        if n != 0 {
            return Err(std::io::Error::other("압축 청크는 꼬리 읽기 밖"));
        }
        let at = 32 + n;
        let len = u64::from_le_bytes(body.get(at..at + 8).ok_or_else(bad)?.try_into().map_err(|_| bad())?) as usize;
        let recs = body.get(at + 8..at + 8 + len).ok_or_else(bad)?;
        let mut i = 0;
        while let Some((op, b, next)) = record_at(recs, i) {
            self.one(op, b, from, out);
            i = next;
        }
        Ok(())
    }

    fn one(&mut self, op: u8, body: &[u8], from: u32, out: &mut VecDeque<Record>) {
        match op {
            OP_CHANNEL if body.len() >= 8 => {
                let id = u16::from_le_bytes([body[0], body[1]]);
                let n = u32::from_le_bytes([body[4], body[5], body[6], body[7]]) as usize;
                if let Some(t) = body.get(8..8 + n).and_then(|t| std::str::from_utf8(t).ok()).and_then(Channel::parse) {
                    self.topics.insert(id, t);
                }
            }
            OP_MESSAGE if body.len() >= 22 => {
                let id = u16::from_le_bytes([body[0], body[1]]);
                let seq = u32::from_le_bytes([body[2], body[3], body[4], body[5]]);
                let t = u64::from_le_bytes(body[6..14].try_into().unwrap_or([0; 8]));
                if (seq.wrapping_sub(from) as i32) < 0 {
                    return;
                }
                if let Some(ch) = self.topics.get(&id) {
                    out.push_back(Record { sequence: seq, log_time: t, channel: ch.clone(), data: body[22..].to_vec() });
                }
            }
            _ => {}
        }
    }
}

/// `at` 의 맨 위 레코드(op · 본문 · 다음 자리) — 다 안 적혔거나(쓰는 중인 청크의 길이 자리) 모자라면 None
fn record_at(b: &[u8], at: usize) -> Option<(u8, &[u8], usize)> {
    let op = *b.get(at)?;
    let len = u64::from_le_bytes(b.get(at + 1..at + 9)?.try_into().ok()?);
    let end = (at as u64 + 9).checked_add(len)?;
    if end > b.len() as u64 {
        return None;
    }
    Some((op, &b[at + 9..end as usize], end as usize))
}
