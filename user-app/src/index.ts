import { WebSocketServer, WebSocket } from 'ws';
import { ClientMsgSchema } from './validation';

const wss = new WebSocketServer({ port: 8080, maxPayload: 4 * 1024 });

interface Client { name: string, roomId: string };

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
        if (members.size === 0) {
            rooms.delete(client.roomId);
        }
        else {
            broadCast(client.roomId, {
                type: "system",
                payload: { message: `${client.name} left` }
            });
            sendUsers(client.roomId);
        }
    }
    clients.delete(ws);
}
function broadCast(roomId: string, msg: ServerMsg) {
    const members = rooms.get(roomId);
    if (!members) return;
    const out = JSON.stringify(msg);
    for (const c of members) {
        if (c.readyState === WebSocket.OPEN) c.send(out);
    }
}
function sendUsers(roomId: string) {
    const members = rooms.get(roomId);
    if (!members) return;
    const users = Array.from(members, (s) => clients.get(s)?.name ?? "?");
    broadCast(roomId, { type: "users", payload: { users } });
}

wss.on('connection', (ws) => {
    ws.on('error', console.error)

    alive.add(ws);
    ws.on("pong", () => alive.add(ws));

    ws.on("message", (data) => {
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
            if (!rooms.has(roomId)) rooms.set(roomId, new Set());
            rooms.get(roomId)?.add(ws);
            clients.set(ws, { name, roomId });

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