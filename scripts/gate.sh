#!/bin/sh
# author: kodeholic (powered by Claude)
# oxpeer 1층 게이트 — 생성본 어긋남(규격 원천 산출과 같은가) · clippy · 시험(벡터 · 프레임 · MBCP · STUN · RTP · Ogg) · 워크스페이스 전부(crates/oxpeer · crates/oxbridge-ai · crates/oxrec).
# 규격 쪽 위치는 CONTEXT(기본 ../../context — 작업 트리 oxlens/ 옆의 context 저장소).
set -e
cd "$(dirname "$0")/.."
CONTEXT="${CONTEXT:-../../context}"
python3 "$CONTEXT/spec/tools/peer_sync.py" --check "$PWD/crates/oxpeer"
cargo clippy --workspace --all-targets --examples -- -D warnings
cargo test --workspace --all-targets
echo "== oxpeer 게이트 0"
