import { WebSocketServer, WebSocket } from 'ws';
import { ClientMsgSchema } from './validation';
import Redis from 'ioredis';
import { randomUUID } from 'crypto';

const PORT = Number(process.env.PORT) || 8080;
const wss = new WebSocketServer({ port: PORT, maxPayload: 4 * 1024 });
console.log(`ws server is listening on PORT ${PORT}`);

const pub = new Redis({ host: "127.0.0.1", port: 6379 });
const sub = new Redis({ host: "127.0.0.1", port: 6379 });
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

    const members = rooms.get(client.roomId);
    if (members) {
        members.delete(ws);
        broadCast(client.roomId, {
            type: "system",
            payload: { message: `${client.name} left` }
        });
        if (members.size === 0) {
            rooms.delete(client.roomId);
            sub.unsubscribe(`room:${client.roomId}`);
        }
        else {
            sendUsers(client.roomId);
        }
    }
    clients.delete(ws);
}
function broadCast(roomId: string, msg: ServerMsg) {
    pub.publish(`room:${roomId}`, JSON.stringify(msg));
}
function sendUsers(roomId: string) {
    const members = rooms.get(roomId);
    if (!members) return;
    const users = Array.from(members, (s) => clients.get(s)?.name ?? "?");
    broadCast(roomId, { type: "users", payload: { users } });
}

sub.on("message", (channel, message) => {
    const roomId = channel.slice("room:".length);
    const members = rooms.get(roomId);
    if (!members) return;
    for (const c of members) {
        if (c.readyState === WebSocket.OPEN) c.send(message);
    }
});

wss.on('connection', (ws) => {
    const id = randomUUID();
    ws.on('error', console.error)

    alive.add(ws);
    ws.on("pong", () => alive.add(ws));

    ws.on("message", async (data) => {
        let raw: unknown;
        try {
            raw = JSON.parse(data.toString());
        } catch {
            ws.send(JSON.stringify({ type: "error", payload: { message: "invaild JSON" } }));
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
                rooms.set(roomId, new Set())
                await sub.subscribe(`room:${roomId}`);
            }
            rooms.get(roomId)?.add(ws);
            clients.set(ws, { id, name, roomId });

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
            broadCast(client.roomId, {
                type: "chat",
                payload: { from: client.name, message: msg.payload.message }
            })
        }
    })
    ws.on("close", () => {
        alive.delete(ws);
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
wss.on("close", () => clearInterval(sweeper));