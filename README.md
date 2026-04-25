# Wonder Arena Matchmaking Server

Servidor simple de matchmaking + relay WebSocket para Wonder Arena.

## Local

```bash
npm install
npm start
```

Abre:

```txt
http://localhost:3000/health
```

## Render

1. Sube esta carpeta a GitHub en un repositorio.
2. En Render, crea un nuevo Web Service desde ese repo.
3. Build Command: `npm install`
4. Start Command: `npm start`
5. Copia la URL pública, por ejemplo:
   `https://wonder-arena-matchmaking.onrender.com`
6. En el HTML del juego, cambia:
   `const MATCHMAKING_WS_URL = "wss://TU-SERVIDOR.onrender.com";`

Si tu URL empieza por `https://`, para WebSocket debe empezar por `wss://`.
