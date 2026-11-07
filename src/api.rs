// author: kodeholic (powered by Claude)
//! 앱 표면 — 핸들(`Client`) · 사건 통(`Events`) · 미디어 통 둘(`Media` 패킷 · `Frames` 프레임 — 트랙마다 고른 단위대로). 콜백을 받지 않는다 — 앱이 통에서 꺼내 간다.
//! 사건 이름은 SDK 정의서 효과 표의 것이다(SDK:IMPL-API-EVENT-MAP — 플랫폼 공유 계약) · 서버사이드에 해당하지 않는 것(장치 · 재생 · 발행)은 없다(SDK:RULE-PLAT-SERVER).

use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::core::{self, Cmd};
use crate::error::Error;
pub use crate::generated::ws_wire::{Duplex, Kind, MemberInfo, Permission, TokenReq, TrackEntry, TrackEntryType};
pub use crate::link::recv::{MediaFrame, Unit};
pub use crate::link::send::OutFrame;
pub use crate::link::transport::{MediaPacket, Source};

/// 접속 설정 — `base` 는 앱이 준다(연:RULE-TRN-BASE-GIVEN) · `token` 은 앱이 발급받은 것(연:SES.http.TOKEN)
#[derive(Clone, Debug)]
pub struct Config {
    /// `http://host:port/경로` — WS 는 `ws://host:port/경로/ws`
    pub base: String,
    pub token: String,
    /// 미디어 통 크기(패킷) — 앱이 늦게 꺼내 가득 차면 버리고 센다(`Stats::media_dropped`)
    pub media_capacity: usize,
    /// 프레임 통 크기 — 가득 차면 버리고 센다(`Stats::frames_dropped`)
    pub frames_capacity: usize,
    /// 받는 단위 기본(SDK:RULE-PLAT-SERVER 수신 단위 · 시각) — 트랙마다는 `Client::set_unit`
    pub unit: Unit,
}

impl Config {
    pub fn new(base: impl Into<String>, token: impl Into<String>) -> Config {
        Config { base: base.into(), token: token.into(), media_capacity: 4_096, frames_capacity: 1_024, unit: Unit::Packet }
    }
}

/// 받을 층(SDK:IMPL-TRK-RECEIVE `RemoteTrack.setLayer`) — ★값은 상한이고 실제 선택은 서버다 · 생략한 칸은 안 바꾼다(연:RULE-SUB-LAYER-PROC)
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Layer {
    pub spatial: Option<u8>,
    pub temporal: Option<u8>,
    pub paused: Option<bool>,
    pub priority: Option<u8>,
}

/// 세션 연결 상태(SDK:IMPL-SES-API `session.state`) — 위 상태 넷 그대로
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionState {
    Disconnected,
    Connecting,
    Active,
    Resuming,
}

/// 파생 요약(SDK:IMPL-SES-API `client.health`)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Health {
    Ok,
    Degraded,
    Recovering,
    Offline,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionInfo {
    pub state: SessionState,
    pub recovering: bool,
    /// 마지막으로 끊긴 사유 — 서버가 준 사유면 그 code · name, 사유 없는 끊김은 code 0 + `TRANSPORT` · `BIND_TIMEOUT` · `BIND_FAILED`
    pub reason: Option<(u16, String)>,
    pub user_id: Option<String>,
    /// 접속 응답의 확정값 `1pc` · `2pc`
    pub pc_mode: Option<String>,
    pub health: Health,
}

/// 서버마다 연결 상태(SDK:IMPL-LINK-PROJECTION)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkState {
    Connecting,
    Up,
    Wobbly,
    Restarting,
    Rebuilding,
    Closed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LinkInfo {
    pub server: String,
    pub state: LinkState,
    pub rung: u32,
    pub attempt: u32,
    /// 재구축 원인 · `path_unreachable`
    pub cause: Option<String>,
}

/// 사건 하나 — 같은 방의 사건은 서버 도착 순서대로(SDK:RULE-API-ASYNC)
#[derive(Clone, Debug)]
pub enum Event {
    /// 상태 · recovering · reason · health 가 바뀌었다
    Session(SessionInfo),
    /// 재접속 중 위가 토큰을 요구했다 — `Client::set_token` 으로 답한다(SDK:IMPL-SES-EVENTS)
    TokenRequired,
    /// 앱이 끊지 않았는데 세션이 끝났다 — retryable = 다시 `connect` 할 보람이 있다(백오프 소진)
    Closed { code: u16, name: String, retryable: bool },
    /// 새 세션으로 다시 붙어 방을 다시 세운다(이어받기 실패 · 회수 · 토큰 만료 뒤)
    Rebuilt,
    Link(LinkInfo),
    Joined { room: String },
    JoinFailed { room: String, error: Error },
    Left { room: String },
    /// 서버가 방을 닫았다(`kick` · `room_closed` · 이어받기 스냅샷에서 빠짐 `missing`)
    Forced { room: String, cause: String },
    /// 명단이 바뀌었다(투명 참가자는 없다)
    Participants { room: String, participants: Vec<MemberInfo> },
    /// 받는 트랙 목록이 바뀌었다 — 이 방의 받을 트랙 전부(내 개인 트랙 제외)
    Tracks { room: String, tracks: Vec<TrackEntry> },
    Muted { room: String, track_id: String, muted: bool },
    /// 그 트랙의 첫 복호 RTP 패킷(SDK:RULE-PLAT-SERVER `receiving`)
    Receiving { room: String, track_id: String },
    /// 그 방의 화자 — None = 아무도 말하지 않는다(SDK:IMPL-FLR-EXPOSE `speaker`)
    Speaker { room: String, user_id: Option<String> },
    /// 발언권 표시를 믿을 수 있나(DC 가 살아 있나 — `ptt.state.trusted`)
    FloorTrusted { room: String, trusted: bool },
    /// 재동기를 반영했다
    Resync { room: String },
    /// 방 · 구독의 요청이 실패했다(내 요청이 아닌 것)
    Error { room: Option<String>, error: Error },
    // ── 말하는 쪽(20261107c) ──
    /// 내 발행이 섰다(SDK 효과 표 `published`) · 실패했다 · 끝났다(서버가 지웠다 — `ended`)
    Published { room: String, track_id: String },
    PublishFailed { room: String },
    TrackEnded { room: String, track_id: String },
    /// 서버가 내 발행에 키프레임을 청했다(PLI) — 앱이 인코더에 키프레임을 시킨다(SDK 는 인코딩하지 않는다)
    KeyframeRequested { room: String, track_id: String },
    /// 발언권 누름 쪽(SDK:IMPL-FLR-EXPOSE) — event = granted · denied · queued · revoked · state · state = 위 클라 상태
    Ptt { room: String, event: String, state: String },
    /// 발언 방이 바뀌었다(소속 응답 — None = 없다)
    SpeakingRoom { room: Option<String> },
    /// 방 문자(연:AUX.op.MESSAGE)
    Message { room: String, user_id: String, content: String },
    /// 내 권한이 바뀌었다(PARTICIPANT_STATE — 투명 참가자도 본인에겐 온다)
    Permission { room: String, permission: Option<Permission> },
}

/// 발행할 트랙 — 코덱은 그 방의 미디어 코덱이다(연:RULE-PUB-PUBLISH-PROC — 앱이 고르는 칸이 없다)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrackSpec {
    pub kind: Kind,
    /// `Full` = 전이중(늘 보낸다) · `Half` = 반이중(발언권 허가 사이에만 — 무전)
    pub duplex: Duplex,
}

/// 지금 값(SDK:IMPL-SES-API 상태 읽기 — 사건을 놓친 앱이 다시 맞춘다)
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    pub session: Option<SessionInfo>,
    pub speaking_room: Option<String>,
    pub rooms: Vec<RoomView>,
    /// 게이트가 닫혀 버린 프레임 · 보낸 프레임(발행 전부)
    pub gated: u64,
    pub sent: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RoomView {
    pub room: String,
    pub joined: bool,
    pub server: Option<String>,
    pub speaker: Option<String>,
    pub ptt: PttInfo,
    pub permission: Option<Permission>,
    /// 이 방의 내 발행(track_id)
    pub published: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PttInfo {
    /// 위 클라 상태 이름(NOPERM · PREQ · PERM · QUEUED …)
    pub state: String,
    pub trusted: bool,
}

/// 수치 — 미디어 경로와 수신 정리층의 계수
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub media_dropped: u64,
    pub media_unrouted: u64,
    pub decrypt_fail: u64,
    pub frames_dropped: u64,
    /// 보낸 NACK 묶음 수 · RTX 로 메운 패킷 · 다 묻고도 못 받아 넘긴 패킷
    pub nack_sent: u64,
    pub rtx_recovered: u64,
    pub lost: u64,
    pub pli_sent: u64,
    pub rr_sent: u64,
    /// 조각이 빠져 버린 프레임 · 키프레임을 기다리느라 버린 프레임
    pub frames_broken: u64,
    pub frames_skipped: u64,
    /// QA 스위치가 버린 영상 패킷(`OXPEER_QA_DROP_VIDEO_EVERY` — 시험 수단 · 평소 0)
    pub qa_dropped: u64,
    /// 보내는 쪽 — RTX 로 다시 보낸 패킷 · 서버가 준 대역 상한(REMB) · 보낸 프레임 · 게이트가 닫혀 버린 프레임
    pub rtx_sent: u64,
    pub remb_bps: u64,
    pub frames_sent: u64,
    pub frames_gated: u64,
    /// QA 스위치가 보내지 않은 영상 패킷(`OXPEER_QA_DROP_SEND_EVERY` — 시험 수단 · 평소 0)
    pub qa_send_dropped: u64,
}

/// 내 발행 하나 — 앱이 인코딩한 프레임을 넣는다. 송신 시계가 프레임 길이대로 내보내고 · 통이 차면 `write` 가 기다린다(SDK:RULE-PLAT-SERVER 송신 시계)
pub struct LocalTrack {
    id: u64,
    pub room: String,
    pub track_id: String,
    frames: mpsc::Sender<OutFrame>,
    cmd: mpsc::UnboundedSender<Cmd>,
}

impl LocalTrack {
    /// 프레임 하나 — 통이 차면 기다린다(역압)
    pub async fn write(&self, f: OutFrame) -> Result<(), Error> {
        self.frames.send(f).await.map_err(|_| Error::closed("CLOSED"))
    }

    /// 넣은 프레임을 송신 시계가 다 내보낼 때까지 기다린다 — 발언을 놓기 전에 부른다(놓으면 게이트가 닫혀 통에 남은 것은 버려진다)
    pub async fn drain(&self) {
        while self.frames.capacity() < self.frames.max_capacity() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // 통에서 꺼낸 마지막 프레임이 제 때를 기다리고 있을 수 있다 — 한 프레임 길이(넉넉히)
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    }

    /// 반이중 ↔ 전이중(SDK:IMPL-PUB-API `setDuplex` · TRACK_SET) — 응답이 오면 돌아온다
    pub async fn set_duplex(&self, duplex: Duplex) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.cmd.send(Cmd::SetDuplex(self.id, duplex == Duplex::Half, tx)).map_err(|_| Error::closed("CLOSED"))?;
        rx.await.map_err(|_| Error::closed("CLOSED"))?
    }

    /// 발행을 내린다(`LocalTrack.stop()`)
    pub fn unpublish(self) {
        let _ = self.cmd.send(Cmd::Unpublish(self.id));
    }
}

/// 클라이언트 핸들 — 복제해도 같은 SDK 루프다
#[derive(Clone)]
pub struct Client {
    cmd: mpsc::UnboundedSender<Cmd>,
}

/// 사건 통
pub struct Events {
    rx: mpsc::UnboundedReceiver<Event>,
}

impl Events {
    pub async fn next(&mut self) -> Option<Event> {
        self.rx.recv().await
    }
}

/// 미디어 통 — 받은 RTP(복호한 것) · 단위가 `Packet` 인 트랙
pub struct Media {
    rx: mpsc::Receiver<MediaPacket>,
}

impl Media {
    pub async fn next(&mut self) -> Option<MediaPacket> {
        self.rx.recv().await
    }
}

/// 프레임 통 — 단위가 `Frame` 인 트랙
pub struct Frames {
    rx: mpsc::Receiver<MediaFrame>,
}

impl Frames {
    pub async fn next(&mut self) -> Option<MediaFrame> {
        self.rx.recv().await
    }
}

impl Client {
    /// 접속 — 세션이 ACTIVE 에 닿으면 돌아온다(SDK:IMPL-SES-API `connect`). 최초 접속의 토큰 요구 · 거절은 `Err`(auth)
    pub async fn connect(cfg: Config) -> Result<(Client, Events, Media, Frames), Error> {
        let (ev_tx, ev_rx) = mpsc::unbounded_channel();
        let (media_tx, media_rx) = mpsc::channel(cfg.media_capacity.max(16));
        let (frames_tx, frames_rx) = mpsc::channel(cfg.frames_capacity.max(16));
        let cmd = core::spawn(cfg, ev_tx, media_tx, frames_tx)?;
        let c = Client { cmd };
        let (tx, rx) = oneshot::channel();
        c.send(Cmd::Connect(tx))?;
        rx.await.map_err(|_| Error::closed("CLOSED"))??;
        Ok((c, Events { rx: ev_rx }, Media { rx: media_rx }, Frames { rx: frames_rx }))
    }

    fn send(&self, c: Cmd) -> Result<(), Error> {
        self.cmd.send(c).map_err(|_| Error::closed("CLOSED"))
    }

    /// 방 입장 — 듣기(`select:false`)로 들어간다(SDK:IMPL-ROOM-API 기본 `listen`). 들어가 첫 협상이 끝나면 돌아온다
    pub async fn join(&self, room: &str) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Join(room.to_string(), tx))?;
        rx.await.map_err(|_| Error::closed("CLOSED"))?
    }

    /// 그 방만 나간다
    pub async fn leave(&self, room: &str) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Leave(room.to_string(), tx))?;
        rx.await.map_err(|_| Error::closed("CLOSED"))?
    }

    /// 새 토큰 — 재접속 중 토큰 요구로 멈췄으면 이어서 접속한다
    pub fn set_token(&self, token: impl Into<String>) -> Result<(), Error> {
        self.send(Cmd::SetToken(token.into()))
    }

    /// 끊기 — 위 입구 disconnect(방마다 따로 나가지 않는다) · 기다리지 않는다
    pub async fn close(&self) {
        let (tx, rx) = oneshot::channel();
        if self.send(Cmd::Close(tx)).is_ok() {
            let _ = tokio::time::timeout(Duration::from_secs(2), rx).await;
        }
    }

    /// 이 방에서 말하려고 들어간다(발언 방 — select:true 입장 · 이미 발언 방이 있으면 들어간 뒤 옮긴다)
    pub async fn join_speaking(&self, room: &str) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::JoinSpeaking(room.to_string(), tx))?;
        rx.await.map_err(|_| Error::closed("CLOSED"))?
    }

    /// 발언 방을 들어가 있는 이 방으로(소속 전환 — 연:AFF)
    pub async fn set_speaking_room(&self, room: &str) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::SetSpeakingRoom(room.to_string(), tx))?;
        rx.await.map_err(|_| Error::closed("CLOSED"))?
    }

    /// 발행 — 서버가 받으면(`published`) 돌아온다. 전이중은 곧 보내고 · 반이중은 발언권 허가 사이에만 보낸다
    pub async fn publish(&self, room: &str, spec: TrackSpec) -> Result<LocalTrack, Error> {
        let (ftx, frx) = mpsc::channel(crate::core::FRAME_QUEUE);
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Publish(room.to_string(), spec, frx, tx))?;
        let (id, track_id) = rx.await.map_err(|_| Error::closed("CLOSED"))??;
        Ok(LocalTrack { id, room: room.to_string(), track_id, frames: ftx, cmd: self.cmd.clone() })
    }

    /// 발언 누름(SDK:IMPL-FLR-BUTTON `ptt.press`) — 위 입구에 닿으면 돌아온다 · 허가는 사건 `Ptt{granted}` 로
    pub async fn ptt_press(&self, room: &str, priority: u8) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::PttPress(room.to_string(), priority, tx))?;
        rx.await.map_err(|_| Error::closed("CLOSED"))?
    }

    /// 발언 놓음(`ptt.release`)
    pub async fn ptt_release(&self, room: &str) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::PttRelease(room.to_string(), tx))?;
        rx.await.map_err(|_| Error::closed("CLOSED"))?
    }

    /// 방 문자 보내기(연:AUX.op.MESSAGE) — 서버가 매긴 msg_id
    pub async fn send_message(&self, room: &str, content: &str) -> Result<String, Error> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Message(room.to_string(), content.to_string(), tx))?;
        rx.await.map_err(|_| Error::closed("CLOSED"))?
    }

    /// 지금 값
    pub async fn snapshot(&self) -> Result<Snapshot, Error> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Snapshot(tx))?;
        rx.await.map_err(|_| Error::closed("CLOSED"))
    }

    /// 이 트랙을 받는 단위 — 다음 패킷부터
    pub fn set_unit(&self, track_id: impl Into<String>, unit: Unit) -> Result<(), Error> {
        self.send(Cmd::SetUnit(track_id.into(), unit))
    }

    /// 받을 층 — 서버가 받으면 돌아온다. 입장한 방의 받는 트랙만(`NO_TRACK`) · 다시 들어가면 SDK 가 다시 보낸다
    pub async fn set_layer(&self, track_id: &str, layer: Layer) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::SetLayer(track_id.to_string(), layer, tx))?;
        rx.await.map_err(|_| Error::closed("CLOSED"))?
    }

    pub async fn stats(&self) -> Stats {
        let (tx, rx) = oneshot::channel();
        if self.send(Cmd::Stats(tx)).is_err() {
            return Stats::default();
        }
        rx.await.unwrap_or_default()
    }
}
