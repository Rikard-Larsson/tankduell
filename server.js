'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const INDEX = path.join(__dirname, 'public', 'index.html');
const MAX_ROOMS = 250;
const CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const rooms = new Map();

const server = http.createServer((req, res) => {
  const pathname = new URL(req.url || '/', 'http://localhost').pathname;
  if (req.method === 'GET' && (pathname === '/' || pathname === '/index.html')) {
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store',
      'X-Content-Type-Options': 'nosniff',
      'Referrer-Policy': 'strict-origin-when-cross-origin'
    });
    fs.createReadStream(INDEX).pipe(res);
  } else if (req.method === 'GET' && pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Sidan finns inte');
  }
});
const wss = new WebSocketServer({ noServer: true, maxPayload: 96 * 1024, perMessageDeflate: false });
server.on('upgrade', (req, socket, head) => {
  let pathname;
  try { pathname = new URL(req.url || '/', 'http://localhost').pathname; } catch (_) { pathname = ''; }
  if (pathname !== '/ws') { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws));
});

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}
function broadcast(room, obj) {
  send(room.host, obj);
  send(room.guest, obj);
}
function createCode() {
  for (let i = 0; i < 25; i++) {
    let code = '';
    for (let j = 0; j < 6; j++) code += CODE_ALPHABET[crypto.randomInt(CODE_ALPHABET.length)];
    if (!rooms.has(code)) return code;
  }
  throw new Error('Kunde inte skapa rumskod');
}
function clearMembership(ws) {
  const code = ws.roomCode;
  if (!code) return;
  delete ws.roomCode;
  delete ws.role;
  const room = rooms.get(code);
  if (!room) return;
  if (room.host === ws) {
    rooms.delete(code);
    send(room.guest, { type: 'closed', reason: 'Värden lämnade matchen.' });
    if (room.guest) { delete room.guest.roomCode; delete room.guest.role; }
  } else if (room.guest === ws) {
    room.guest = null;
    room.phase = 'waiting';
    send(room.host, { type: 'peer', connected: false });
    send(room.host, { type: 'input', u: { f: false, b: false, l: false, r: false, shoot: false } });
  }
}
const isInput = u => ({ f: !!u?.f, b: !!u?.b, l: !!u?.l, r: !!u?.r, shoot: !!u?.shoot });

wss.on('connection', ws => {
  ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', (raw, isBinary) => {
    if (isBinary) return;
    const now = Date.now();
    if (now - (ws.rateStart || 0) > 1000) { ws.rateStart = now; ws.rateCount = 0; }
    if (++ws.rateCount > 80) { ws.close(1008, 'För många meddelanden'); return; }
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (_) { return; }
    if (!msg || typeof msg !== 'object') return;
    if (msg.type === 'create') {
      if (ws.roomCode) return;
      if (rooms.size >= MAX_ROOMS) { send(ws, { type: 'error', message: 'Alla rum är upptagna. Försök igen.' }); return; }
      const code = createCode();
      rooms.set(code, { host: ws, guest: null, phase: 'waiting', lastActive: now });
      ws.roomCode = code; ws.role = 'host';
      send(ws, { type: 'joined', code, role: 'host', peer: false });
      return;
    }
    if (msg.type === 'join') {
      if (ws.roomCode) return;
      const code = typeof msg.code === 'string' ? msg.code.trim().toUpperCase() : '';
      const room = rooms.get(code);
      if (!room) { send(ws, { type: 'error', message: 'Rumskoden finns inte. Be värden skapa ett nytt rum.' }); return; }
      if (room.guest) { send(ws, { type: 'error', message: 'Rummet är redan fullt.' }); return; }
      if (room.phase !== 'waiting') { send(ws, { type: 'error', message: 'Matchen pågår redan. Vänta på nästa omgång.' }); return; }
      room.guest = ws;
      ws.roomCode = code; ws.role = 'guest';
      room.lastActive = now;
      send(ws, { type: 'joined', code, role: 'guest', peer: true });
      send(room.host, { type: 'peer', connected: true });
      return;
    }
    const room = rooms.get(ws.roomCode);
    if (!room) return;
    room.lastActive = now;
    if (msg.type === 'leave') { clearMembership(ws); return; }
    if (msg.type === 'start' && ws.role === 'host' && room.guest) {
      const map = Number.isInteger(msg.map) && msg.map >= 0 && msg.map <= 2 ? msg.map : 0;
      room.phase = 'playing';
      broadcast(room, { type: 'start', map });
    } else if (msg.type === 'input' && ws.role === 'guest' && room.phase === 'playing') {
      send(room.host, { type: 'input', u: isInput(msg.u) });
    } else if (msg.type === 'state' && ws.role === 'host' && room.phase === 'playing') {
      // Simuleringen körs hos värden. Servern skickar enbart vidare spelets bild av världen.
      if (msg.state && typeof msg.state === 'object' && room.guest && room.guest.bufferedAmount < 500000) {
        send(room.guest, { type: 'state', state: msg.state, map: msg.map });
      }
    } else if (msg.type === 'complete' && ws.role === 'host' && room.phase === 'playing') {
      room.phase = 'done';
      send(room.guest, { type: 'complete', winner: msg.winner === 2 ? 2 : 1 });
    }
  });
  ws.on('error', () => {});
  ws.on('close', () => clearMembership(ws));
});
const heartbeat = setInterval(() => {
  const now = Date.now();
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    ws.ping();
  }
  for (const [code, room] of rooms) {
    if (now - room.lastActive > 2 * 60 * 60 * 1000) {
      send(room.host, { type: 'closed', reason: 'Rummet har gått ut.' });
      send(room.guest, { type: 'closed', reason: 'Rummet har gått ut.' });
      if (room.host) { delete room.host.roomCode; delete room.host.role; }
      if (room.guest) { delete room.guest.roomCode; delete room.guest.role; }
      rooms.delete(code);
    }
  }
}, 30000);
heartbeat.unref();
server.listen(PORT, '0.0.0.0', () => console.log(`TANKDUELL ONLINE kör på http://localhost:${PORT}`));
