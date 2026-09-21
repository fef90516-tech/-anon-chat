// ============================================================
//  Анонимный чат — сервер (Node.js + WebSocket)
//  Без регистрации, без ботов, бесплатно.
// ============================================================

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

// ---- HTTP-сервер: отдаёт страницу index.html любому пути ----
const server = http.createServer((req, res) => {
  // Отдаём один и тот же файл на "/" и на "/r/КОД" — комната читается уже в браузере
  const indexPath = path.join(__dirname, 'index.html');
  fs.readFile(indexPath, (err, data) => {
    if (err) {
      res.writeHead(500);
      res.end('Ошибка сервера');
      return;
    }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

// ---- WebSocket-сервер ----
const wss = new WebSocketServer({ server });

// Комнаты: код -> Set из соединений
const rooms = new Map();

function roomClients(code) {
  if (!rooms.has(code)) rooms.set(code, new Set());
  return rooms.get(code);
}

function broadcast(code, obj, exclude) {
  const clients = rooms.get(code);
  if (!clients) return;
  const msg = JSON.stringify(obj);
  for (const c of clients) {
    if (c !== exclude && c.readyState === c.OPEN) {
      c.send(msg);
    }
  }
}

function sendCount(code) {
  const clients = rooms.get(code);
  if (!clients) return;
  const count = clients.size;
  broadcast(code, { type: 'count', count });
}

wss.on('connection', (ws) => {
  ws.roomCode = null;
  ws.nick = null;

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    // Присоединение к комнате
    if (data.type === 'join') {
      const code = String(data.room || '').trim();
      if (!code) return;
      ws.roomCode = code;
      ws.nick = data.nick || 'Гость';
      const clients = roomClients(code);
      clients.add(ws);

      // Себе — подтверждение
      ws.send(JSON.stringify({ type: 'joined', room: code, count: clients.size }));
      // Остальным — уведомление, что кто-то зашёл
      broadcast(code, { type: 'system', text: 'Собеседник присоединился к чату' }, ws);
      sendCount(code);
      return;
    }

    // Сообщение чата
    if (data.type === 'chat') {
      const code = ws.roomCode;
      if (!code) return;
      const text = String(data.text || '').slice(0, 4000);
      if (!text.trim()) return;
      broadcast(code, {
        type: 'chat',
        text,
        nick: ws.nick || 'Гость',
        ts: Date.now()
      }, ws); // отправляем всем, кроме отправителя (у него сообщение уже показано)
      return;
    }

    // Индикатор "печатает..."
    if (data.type === 'typing') {
      const code = ws.roomCode;
      if (!code) return;
      broadcast(code, { type: 'typing', nick: ws.nick || 'Гость' }, ws);
      return;
    }
  });

  ws.on('close', () => {
    const code = ws.roomCode;
    if (!code) return;
    const clients = rooms.get(code);
    if (!clients) return;
    clients.delete(ws);
    if (clients.size === 0) {
      rooms.delete(code);
    } else {
      broadcast(code, { type: 'system', text: 'Собеседник покинул чат' });
      sendCount(code);
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('====================================================');
  console.log('  Анонимный чат запущен!');
  console.log('  Откройте в браузере:  http://localhost:' + PORT);
  console.log('====================================================');
});
