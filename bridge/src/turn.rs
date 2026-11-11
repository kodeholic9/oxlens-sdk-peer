// author: kodeholic (powered by Claude)
//! 턴(SDK:RULE-PLAT-AGENT-TURN) — 녹음 파이프라인 발언 정책(SDK:RULE-PLAT-REC-SPLIT)의 ★발언 번호마다 턴 하나. 경계 · 경쟁 처리는 그 절 그대로이고 여기서 다시 짓지 않는다.
//! 무전 슬롯 음성만(개인 트랙 · 영상은 1판 밖) · 자기 발언은 열지 않는다 · 어느 발언에도 담기지 않은 프레임(`Orphan`)은 버그 계수.

use std::collections::{BTreeMap, BTreeSet};

use oxpeer::record::{Output, SegmentKind};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnOut {
    Start { turn: u64, speaker: String },
    End { turn: u64, reason: &'static str },
    /// 그 턴의 음성 프레임(Opus) — 메운 프레임이면 `filled`
    Audio { data: Vec<u8>, filled: bool, rtp_ts: u32 },
    /// 어느 발언에도 담기지 않은 슬롯 음성(유예 창 밖) — 버그 계수
    Orphan { source: Option<u32>, user: Option<String> },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub turns: u64,
    /// 자기 발언(열지 않음) · 그 프레임
    pub self_utterances: u64,
    pub self_frames: u64,
    /// 버그 계수 — 출처 없는 · 출처 있는(지금 화자 아님 · 들기를 넘김) 버린 프레임
    pub orphan_sourceless: u64,
    pub orphan_sourced: u64,
}

struct Cur {
    turn: u64,
    utt: u64,
}

pub struct Turns {
    me: String,
    cur: Option<Cur>,
    next: u64,
    /// 세그먼트 → 발언 번호(이 턴 셈에 드는 것만)
    seg: BTreeMap<u64, u64>,
    mine: BTreeSet<u64>,
    pub counts: Counts,
}

fn slot_audio(kind: &str, owner: &Option<String>) -> bool {
    kind == "audio" && owner.is_none()
}

impl Turns {
    pub fn new(me: impl Into<String>) -> Turns {
        Turns { me: me.into(), cur: None, next: 1, seg: BTreeMap::new(), mine: BTreeSet::new(), counts: Counts::default() }
    }

    pub fn open(&self) -> Option<u64> {
        self.cur.as_ref().map(|c| c.turn)
    }

    pub fn push(&mut self, o: &Output) -> Vec<TurnOut> {
        match o {
            Output::Segment(s) if slot_audio(&s.kind, &s.owner) => {
                let Some(utt) = s.utterance else { return vec![] };
                match s.event {
                    SegmentKind::Opened => {
                        if s.user_id.as_deref() == Some(self.me.as_str()) {
                            if self.mine.insert(utt) {
                                self.counts.self_utterances += 1;
                            }
                            return vec![];
                        }
                        self.seg.insert(s.id, utt);
                        if self.cur.as_ref().is_some_and(|c| c.utt == utt) {
                            return vec![];
                        }
                        let mut out = self.end("taken");
                        let turn = self.next;
                        self.next += 1;
                        self.counts.turns += 1;
                        self.cur = Some(Cur { turn, utt });
                        out.push(TurnOut::Start { turn, speaker: s.user_id.clone().unwrap_or_default() });
                        out
                    }
                    SegmentKind::Closed => {
                        self.seg.remove(&s.id);
                        if self.cur.as_ref().is_none_or(|c| c.utt != utt) {
                            return vec![];
                        }
                        match s.reason {
                            Some("taken") => self.end("taken"),
                            Some("idle") => self.end("idle"),
                            Some("track") | Some("end") => self.end("left"),
                            _ => vec![],
                        }
                    }
                }
            }
            Output::Frame(f) if slot_audio(&f.kind, &f.owner) => {
                let Some(utt) = f.segment.and_then(|id| self.seg.get(&id)).copied() else {
                    if f.segment.is_some() {
                        self.counts.self_frames += 1;
                    }
                    return vec![];
                };
                if self.cur.as_ref().is_some_and(|c| c.utt == utt) {
                    vec![TurnOut::Audio { data: f.data.clone(), filled: f.filled, rtp_ts: f.rtp_ts }]
                } else {
                    vec![]
                }
            }
            Output::Orphan(f) if slot_audio(&f.kind, &f.owner) => {
                if f.source.is_some() {
                    self.counts.orphan_sourced += 1;
                } else {
                    self.counts.orphan_sourceless += 1;
                }
                vec![TurnOut::Orphan { source: f.source, user: f.user_id.clone() }]
            }
            _ => vec![],
        }
    }

    fn end(&mut self, reason: &'static str) -> Vec<TurnOut> {
        match self.cur.take() {
            Some(c) => vec![TurnOut::End { turn: c.turn, reason }],
            None => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxpeer::record::{Frame, Segment};

    fn seg(event: SegmentKind, id: u64, utt: u64, user: &str, reason: Option<&'static str>) -> Output {
        Output::Segment(Segment { event, id, room: "r".into(), track_id: "slot".into(), owner: None, kind: "audio".into(), codec: "opus".into(), source: Some(1), user_id: Some(user.into()), first_utc_ns: None, last_utc_ns: None, first_log_time: 0, last_log_time: 0, frames: 0, bytes: 0, filled: 0, reason, utterance: Some(utt) })
    }

    fn frame(segment: Option<u64>, ts: u32, source: Option<u32>) -> Frame {
        Frame { segment, room: "r".into(), track_id: "slot".into(), owner: None, kind: "audio".into(), codec: "opus".into(), ssrc: 9, source, user_id: source.map(|_| "u1".into()), keyframe: true, rtp_ts: ts, utc_ns: None, log_time: 0, seq_first: 0, seq_last: 0, rec_first: 0, rec_last: 0, data: vec![0x78, 1, 2], filled: false }
    }

    fn run(t: &mut Turns, os: &[Output]) -> Vec<TurnOut> {
        os.iter().flat_map(|o| t.push(o)).collect()
    }

    #[test]
    fn one_utterance_is_one_turn_even_across_gap_split() {
        let mut t = Turns::new("agent");
        let out = run(&mut t, &[
            seg(SegmentKind::Opened, 1, 7, "u1", None),
            Output::Frame(frame(Some(1), 0, Some(1))),
            seg(SegmentKind::Closed, 1, 7, "u1", Some("gap")),
            seg(SegmentKind::Opened, 2, 7, "u1", None),
            Output::Frame(frame(Some(2), 48_000, Some(1))),
            seg(SegmentKind::Closed, 2, 7, "u1", Some("idle")),
        ]);
        assert_eq!(out.iter().filter(|o| matches!(o, TurnOut::Start { .. })).count(), 1, "구멍으로 갈린 세그먼트는 한 턴");
        assert_eq!(out.iter().filter(|o| matches!(o, TurnOut::Audio { .. })).count(), 2);
        assert_eq!(out.last(), Some(&TurnOut::End { turn: 1, reason: "idle" }));
    }

    #[test]
    fn same_speaker_pressing_again_is_a_new_turn() {
        let mut t = Turns::new("agent");
        let out = run(&mut t, &[seg(SegmentKind::Opened, 1, 1, "u1", None), seg(SegmentKind::Closed, 1, 1, "u1", Some("idle")), seg(SegmentKind::Opened, 2, 2, "u1", None), seg(SegmentKind::Closed, 2, 2, "u1", Some("taken"))]);
        assert_eq!(out, vec![TurnOut::Start { turn: 1, speaker: "u1".into() }, TurnOut::End { turn: 1, reason: "idle" }, TurnOut::Start { turn: 2, speaker: "u1".into() }, TurnOut::End { turn: 2, reason: "taken" }]);
    }

    #[test]
    fn own_utterance_is_not_a_turn() {
        let mut t = Turns::new("agent");
        let out = run(&mut t, &[seg(SegmentKind::Opened, 1, 3, "agent", None), Output::Frame(frame(Some(1), 0, Some(5))), seg(SegmentKind::Closed, 1, 3, "agent", Some("idle"))]);
        assert!(out.is_empty());
        assert_eq!((t.counts.turns, t.counts.self_utterances, t.counts.self_frames), (0, 1, 1));
    }

    #[test]
    fn orphans_are_counted_as_bugs() {
        let mut t = Turns::new("agent");
        let out = run(&mut t, &[Output::Orphan(frame(None, 0, None)), Output::Orphan(frame(None, 960, Some(4)))]);
        assert_eq!(out.len(), 2);
        assert_eq!((t.counts.orphan_sourceless, t.counts.orphan_sourced), (1, 1));
    }

    #[test]
    fn personal_and_video_tracks_are_outside_v1() {
        let mut t = Turns::new("agent");
        let Output::Segment(mut s) = seg(SegmentKind::Opened, 1, 1, "u1", None) else { unreachable!() };
        s.kind = "video".into();
        assert!(t.push(&Output::Segment(s.clone())).is_empty());
        s.kind = "audio".into();
        s.owner = Some("u1".into());
        assert!(t.push(&Output::Segment(s)).is_empty());
    }

    #[test]
    fn leaving_closes_the_turn_as_left() {
        let mut t = Turns::new("agent");
        let out = run(&mut t, &[seg(SegmentKind::Opened, 1, 1, "u1", None), seg(SegmentKind::Closed, 1, 1, "u1", Some("track"))]);
        assert_eq!(out.last(), Some(&TurnOut::End { turn: 1, reason: "left" }));
    }
}
