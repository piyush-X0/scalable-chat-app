import { WebSocketServer, WebSocket } from 'ws';
import { ClientMsgSchema } from './validation';
import Redis from 'ioredis';
import { randomUUID } from 'crypto';

const PORT = Number(process.env.PORT) || 8080;
const wss = new WebSocketServer({ port: PORT, maxPayload: 4 * 1024 });
console.log(`ws server is listening on PORT ${PORT}`);

const REDIS_HOST = process.env.REDIS_HOST || "127.0.0.1";
const REDIS_PORT = Number(process.env.REDIS_PORT) || 6379;

const pub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
const sub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });

pub.on("error", (e) => console.error("pub error:", e.message));
sub.on("error", (e) => console.error("sub error:", e.message));

interface Client { id: string, name: string, roomId: string };
const rooms = new Map<string, Set<WebSocket>>();
const clients = new Map<WebSocket, Client>();
const alive = new Set<WebSocket>();

type ServerMsg = | { type: "chat", payload: { from: string, message: string } }
    | { type: "system", payload: { message: string } }
    | { type: "users", payload: { users: string[] } }
    | { type: "error", payload: { message: string } };

function leaveRoom(ws: WebSocket) {
    const client = clients.get(ws);
    if (!client) return;
    pub.hdel(`users:${client.roomId}`, client.id)
        .catch((e) => console.error("hdel failed:", e.message))

    const members = rooms.get(client.roomId);
    if (members) {
        members.delete(ws);
        broadCast(client.roomId, {
            type: "system",
            payload: { message: `${client.name} left` }
        });
        if (members.size === 0) {
            rooms.delete(client.roomId);
            sub.unsubscribe(`room:${client.roomId}`)
                .catch((e) => console.error("unsubscribe failed:", e.message))
        }
    }
    sendUsers(client.roomId);
    clients.delete(ws);
}
function saveChat(roomId: string, msg: ServerMsg) {
    const key = `history:${roomId}`
    pub.lpush(key, JSON.stringify(msg))
        .then(() => pub.ltrim(key, 0, 49))
        .catch((e) => console.error("saveChat failed:", e.message))
}
function broadCast(roomId: string, msg: ServerMsg) {
    pub.publish(`room:${roomId}`, JSON.stringify(msg))
        .catch((e) => console.error("publish failed :", e.message));
}
async function sendUsers(roomId: string) {
    try {
        const users = await pub.hvals(`users:${roomId}`);
        broadCast(roomId, { type: "users", payload: { users } });
    }
    catch (e) {
        console.error("sendUsers failed :", e);
    }
}

sub.on("message", (channel, message) => {
    const roomId = channel.slice("room:".length);
    const members = rooms.get(roomId);
    if (!members) return;
    for (const c of members) {
        if (c.readyState === WebSocket.OPEN) c.send(message);
    }
});

const LIMIT = 5;
const WINDOW_MS = 3000;
const rate = new Map<WebSocket, { count: number, resetAt: number }>();
function allow(ws: WebSocket): boolean {
    const now = Date.now();
    const r = rate.get(ws);
    if (!r || now >= r.resetAt) {
        rate.set(ws, { count: 1, resetAt: now + WINDOW_MS });
        return true;
    }
    r.count++;
    return r.count <= LIMIT;
}

wss.on('connection', (ws) => {
    console.log(`[${PORT}] new connection`);
    const id = randomUUID();
    ws.on('error', console.error)

    alive.add(ws);
    ws.on("pong", () => alive.add(ws));

    ws.on("message", async (data) => {
        try {

            let raw: unknown;

            try {
                if (!allow(ws)) {
                    ws.send(JSON.stringify({ type: "error", payload: { message: "slow down" } }));
                    console.log(`slow down`);
                    return;
                }
                raw = JSON.parse(data.toString());
            } catch (e) {
                ws.send(JSON.stringify({ type: "error", payload: { message: "invalid JSON" } }));
                return;
            }
            const parsed = ClientMsgSchema.safeParse(raw);

            if (!parsed.success) {
                const issue = parsed.error.issues[0];
                const field = issue?.path.join(".") ?? "message";
                ws.send(JSON.stringify({
                    type: "error",
                    payload: { message: `${field}: ${issue?.message ?? "invalid"}` },
                }));
                return;
            }
            const msg = parsed.data;

            if (msg.type === "join") {
                const { roomId, name } = msg.payload;
                if (clients.get(ws)?.roomId === roomId) return;

                leaveRoom(ws);
                if (!rooms.has(roomId)) {
                    await sub.subscribe(`room:${roomId}`);
                    rooms.set(roomId, new Set())
                }
                rooms.get(roomId)?.add(ws);
                clients.set(ws, { id, name, roomId });
                await pub.hset(`users:${roomId}`, id, name)
                await pub.hexpire(`users:${roomId}`, 30, "FIELDS", 1, id);

                const past = await pub.lrange(`history:${roomId}`, 0, -1);
                for (const m of past.reverse()) {
                    if (ws.readyState === WebSocket.OPEN) ws.send(m);
                }

                broadCast(roomId, {
                    type: "system",
                    payload: { message: `${name} joined` }
                });
                sendUsers(roomId);
            }
            if (msg.type === "chat") {
                const client = clients.get(ws);
                if (!client) {
                    ws.send(JSON.stringify({ type: "error", payload: { message: "join a room first" } }));
                    return;
                };
                const out: ServerMsg = {
                    type: "chat",
                    payload: { from: client.name, message: msg.payload.message }
                }
                broadCast(client.roomId, out);
                saveChat(client.roomId, out);
            }
        } catch (e) {
            console.error("message handler failed :", e);
            ws.send(JSON.stringify({ type: "error", payload: { message: "server error" } }));
        }
    })
    ws.on("close", () => {
        alive.delete(ws);
        rate.delete(ws);
        leaveRoom(ws);
    });
});

const sweeper = setInterval(() => {
    console.log("sweep , clients : ", wss.clients.size, "alive: ", alive.size);
    for (const ws of wss.clients) {
        if (!alive.has(ws)) {
            ws.terminate();
            continue;
        }
        alive.delete(ws);
        ws.ping();
    }
}, 30_000);
const renewer = setInterval(() => {
    for (const c of clients.values()) {
        pub.hexpire(`users:${c.roomId}`, 30, "FIELDS", 1, c.id)
            .catch((e) => console.error("renew failed :", e.message));
    }
}, 10_000);
wss.on("close", () => {
    clearInterval(sweeper);
    clearInterval(renewer);
});