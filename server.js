const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const MAX_WAIT_MS = 30_000;
const DISCONNECT_GRACE_MS = 15_000;

let waiting = [];
const matches = new Map();

function uid(prefix = '') { return prefix + crypto.randomBytes(8).toString('hex'); }
function log(...args) { console.log(new Date().toISOString(), ...args); }
function safeSend(ws, data) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(data));
}
function removeFromQueue(ws) {
  const before = waiting.length;
  waiting = waiting.filter(p => p.ws !== ws);
  if (before !== waiting.length) log('Removed from queue. Queue:', waiting.length);
}
function publicPlayer(p) { return { id: p.id, deck: p.deck, name: p.name || 'Player' }; }

function pairPlayers(a, b) {
  removeFromQueue(a.ws); removeFromQueue(b.ws);
  const matchId = uid('match_');
  a.matchId = matchId; b.matchId = matchId;
  a.opponent = b.ws; b.opponent = a.ws;
  const match = { a: a.ws, b: b.ws, createdAt: Date.now(), disconnectTimer: null, closed: false };
  matches.set(matchId, match);
  log('MATCH FOUND', matchId, 'A:', a.id, 'B:', b.id, 'queue:', waiting.length);
  safeSend(a.ws, { type: 'matched', matchId, you: publicPlayer(a), opponent: publicPlayer(b), host: true });
  safeSend(b.ws, { type: 'matched', matchId, you: publicPlayer(b), opponent: publicPlayer(a), host: false });
}

function endMatch(matchId) {
  const m = matches.get(matchId);
  if (!m) return;
  if (m.disconnectTimer) clearTimeout(m.disconnectTimer);
  m.closed = true;
  matches.delete(matchId);
}

function handlePlayerDisconnect(player) {
  removeFromQueue(player.ws);
  if (!player.matchId) return;
  const m = matches.get(player.matchId);
  if (!m || m.closed) return;
  const other = m.a === player.ws ? m.b : m.a;
  if (other && other.readyState === other.OPEN) {
    safeSend(other, { type: 'opponent_left', seconds: 15 });
    log('Opponent left notice sent. Match:', player.matchId, 'leaver:', player.id);
    if (m.disconnectTimer) clearTimeout(m.disconnectTimer);
    m.disconnectTimer = setTimeout(() => {
      if (other.readyState === other.OPEN) {
        safeSend(other, { type: 'opponent_forfeit', reason: 'disconnect_timeout' });
        log('Disconnect timeout. Awarding win. Match:', player.matchId);
      }
      endMatch(player.matchId);
    }, DISCONNECT_GRACE_MS);
  } else {
    endMatch(player.matchId);
  }
}

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, waiting: waiting.length, matches: matches.size, time: new Date().toISOString() }));
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Wonder Arena matchmaking server v3 is running. Health: /health');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const player = { id: uid('p_'), ws, deck: null, name: 'Player', queuedAt: 0, matchId: null, opponent: null };
  ws.player = player;
  log('WS connected', player.id, 'from', req.headers.origin || 'unknown origin', 'path', req.url);
  safeSend(ws, { type: 'connected', id: player.id });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (e) { log('Bad JSON from', player.id, raw.toString()); return; }
    log('MSG', player.id, msg.type, msg.type === 'queue' ? JSON.stringify(msg.deck) : '');

    if (msg.type === 'queue') {
      player.deck = msg.deck || null;
      player.name = msg.name || 'Player';
      player.queuedAt = Date.now();
      player.matchId = null;
      player.opponent = null;
      removeFromQueue(ws);
      const opponent = waiting.find(p => p.ws.readyState === p.ws.OPEN && p.ws !== ws);
      if (opponent) pairPlayers(player, opponent);
      else { waiting.push(player); log('Queued', player.id, 'Queue:', waiting.length); safeSend(ws, { type: 'queued', waiting: waiting.length }); }
      return;
    }

    if (msg.type === 'cancel') { removeFromQueue(ws); safeSend(ws, { type: 'cancelled' }); return; }

    if (msg.type === 'surrender') {
      if (player.opponent && player.opponent.readyState === player.opponent.OPEN) {
        safeSend(player.opponent, { type: 'opponent_surrendered' });
        log('Surrender:', player.id, 'match', player.matchId);
      }
      if (player.matchId) endMatch(player.matchId);
      return;
    }

    if (msg.type === 'relay') {
      if (player.opponent && player.opponent.readyState === player.opponent.OPEN) {
        safeSend(player.opponent, { type: 'relay', matchId: player.matchId, from: player.id, payload: msg.payload });
        log('Relayed action from', player.id, 'match', player.matchId);
      } else log('Relay failed, no opponent for', player.id);
      return;
    }
  });

  ws.on('close', (code, reason) => {
    log('WS closed', player.id, 'code', code, 'reason', reason.toString());
    handlePlayerDisconnect(player);
  });
  ws.on('error', (err) => log('WS error', player.id, err.message));
});

setInterval(() => {
  const now = Date.now();
  const expired = waiting.filter(p => now - p.queuedAt > MAX_WAIT_MS || p.ws.readyState !== p.ws.OPEN);
  waiting = waiting.filter(p => now - p.queuedAt <= MAX_WAIT_MS && p.ws.readyState === p.ws.OPEN);
  expired.forEach(p => { log('Queue timeout', p.id); safeSend(p.ws, { type: 'timeout' }); });
}, 1000);

server.listen(PORT, () => log(`Wonder Arena matchmaking server v3 listening on port ${PORT}`));
