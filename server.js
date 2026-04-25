const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const MAX_WAIT_MS = 30_000;

let waiting = [];
const matches = new Map();

function uid(prefix = '') {
  return prefix + crypto.randomBytes(8).toString('hex');
}

function safeSend(ws, data) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(data));
}

function removeFromQueue(ws) {
  waiting = waiting.filter(p => p.ws !== ws);
}

function publicPlayer(p) {
  return {
    id: p.id,
    deck: p.deck,
    name: p.name || 'Player'
  };
}

function pairPlayers(a, b) {
  removeFromQueue(a.ws);
  removeFromQueue(b.ws);

  const matchId = uid('match_');
  a.matchId = matchId;
  b.matchId = matchId;
  a.opponent = b.ws;
  b.opponent = a.ws;

  matches.set(matchId, { a: a.ws, b: b.ws, createdAt: Date.now() });

  safeSend(a.ws, {
    type: 'matched',
    matchId,
    you: publicPlayer(a),
    opponent: publicPlayer(b),
    host: true
  });

  safeSend(b.ws, {
    type: 'matched',
    matchId,
    you: publicPlayer(b),
    opponent: publicPlayer(a),
    host: false
  });
}

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, waiting: waiting.length, matches: matches.size }));
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Wonder Arena matchmaking server is running. Use WebSocket to connect.');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  const player = {
    id: uid('p_'),
    ws,
    deck: null,
    name: 'Player',
    queuedAt: 0,
    matchId: null,
    opponent: null
  };

  ws.player = player;
  safeSend(ws, { type: 'connected', id: player.id });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.type === 'queue') {
      player.deck = msg.deck || null;
      player.name = msg.name || 'Player';
      player.queuedAt = Date.now();
      player.matchId = null;
      player.opponent = null;
      removeFromQueue(ws);

      const opponent = waiting.find(p => p.ws.readyState === p.ws.OPEN && p.ws !== ws);
      if (opponent) {
        pairPlayers(player, opponent);
      } else {
        waiting.push(player);
        safeSend(ws, { type: 'queued' });
      }
      return;
    }

    if (msg.type === 'cancel') {
      removeFromQueue(ws);
      safeSend(ws, { type: 'cancelled' });
      return;
    }

    if (msg.type === 'relay' && player.opponent && player.opponent.readyState === player.opponent.OPEN) {
      safeSend(player.opponent, {
        type: 'relay',
        matchId: player.matchId,
        from: player.id,
        payload: msg.payload
      });
      return;
    }
  });

  ws.on('close', () => {
    removeFromQueue(ws);
    if (player.opponent && player.opponent.readyState === player.opponent.OPEN) {
      safeSend(player.opponent, { type: 'opponent_left' });
    }
    if (player.matchId) matches.delete(player.matchId);
  });
});

setInterval(() => {
  const now = Date.now();
  const expired = waiting.filter(p => now - p.queuedAt > MAX_WAIT_MS);
  waiting = waiting.filter(p => now - p.queuedAt <= MAX_WAIT_MS && p.ws.readyState === p.ws.OPEN);
  expired.forEach(p => safeSend(p.ws, { type: 'timeout' }));

  for (const [id, m] of matches) {
    if (m.a.readyState !== m.a.OPEN || m.b.readyState !== m.b.OPEN) matches.delete(id);
  }
}, 1000);

server.listen(PORT, () => {
  console.log(`Wonder Arena matchmaking server listening on port ${PORT}`);
});
