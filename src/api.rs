// author: kodeholic (powered by Claude)
//! 앱 표면 — 핸들(`Client`) · 사건 통(`Events`) · 미디어 통(`Media`). 콜백을 받지 않는다 — 앱이 통에서 꺼내 간다.
//! 사건 이름은 SDK 정의서 효과 표의 것이다(SDK:IMPL-API-EVENT-MAP — 플랫폼 공유 계약) · 서버사이드에 해당하지 않는 것(장치 · 재생 · 발행)은 없다(SDK:RULE-PLAT-SERVER).

use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::core::{self, Cmd};
use crate::error::Error;
pub use crate::generated::ws_wire::{MemberInfo, Permission, TokenReq, TrackEntry};
pub use crate::link::transport::MediaPacket;

/// 접속 설정 — `base` 는 앱이 준다(연:RULE-TRN-BASE-GIVEN) · `token` 은 앱이 발급받은 것(연:SES.http.TOKEN)
#[derive(Clone, Debug)]
pub struct Config {
    /// `http://host:port/경로` — WS 는 `ws://host:port/경로/ws`
    pub base: String,
    pub token: String,
    /// 미디어 통 크기(패킷) — 앱이 늦게 꺼내 가득 차면 버리고 센다(`Stats::media_dropped`)
    pub media_capacity: usize,
}

impl Config {
    pub fn new(base: impl Into<String>, token: impl Into<String>) -> Config {
        Config { base: base.into(), token: token.into(), media_capacity: 4_096 }
    }
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
}

/// 수치 — 미디어 경로의 계수
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub media_dropped: u64,
    pub media_unrouted: u64,
    pub decrypt_fail: u64,
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

/// 미디어 통 — 받은 RTP(복호한 것)
pub struct Media {
    rx: mpsc::Receiver<MediaPacket>,
}

impl Media {
    pub async fn next(&mut self) -> Option<MediaPacket> {
        self.rx.recv().await
    }
}

impl Client {
    /// 접속 — 세션이 ACTIVE 에 닿으면 돌아온다(SDK:IMPL-SES-API `connect`). 최초 접속의 토큰 요구 · 거절은 `Err`(auth)
    pub async fn connect(cfg: Config) -> Result<(Client, Events, Media), Error> {
        let (ev_tx, ev_rx) = mpsc::unbounded_channel();
        let (media_tx, media_rx) = mpsc::channel(cfg.media_capacity.max(16));
        let cmd = core::spawn(cfg, ev_tx, media_tx)?;
        let c = Client { cmd };
        let (tx, rx) = oneshot::channel();
        c.send(Cmd::Connect(tx))?;
        rx.await.map_err(|_| Error::closed("CLOSED"))??;
        Ok((c, Events { rx: ev_rx }, Media { rx: media_rx }))
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

    pub async fn stats(&self) -> Stats {
        let (tx, rx) = oneshot::channel();
        if self.send(Cmd::Stats(tx)).is_err() {
            return Stats::default();
        }
        rx.await.unwrap_or_default()
    }
}
