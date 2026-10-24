import { WebSocketServer, WebSocket } from 'ws';

const wss = new WebSocketServer({ port: 8080 });

interface Client { name: string, roomId: string };

const rooms = new Map<string, Set<WebSocket>>();
const clients = new Map<WebSocket, Client>();
const alive = new Set<WebSocket>();

type ClientMsg = | { type: "join", payload: { roomId: string, name: string } }
    | { type: "chat", payload: { message: string } };
type ServerMsg = | { type: "chat", payload: { from: string, message: string } }
    | { type: "system", payload: { message: string } }
    | { type: "users", payload: { users: string[] } };

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
        let msg: ClientMsg;
        try {
            msg = JSON.parse(data.toString());
        } catch {
            return;
        }
        if (typeof msg !== "object" || msg === null) return;

        if (msg.type === "join") {
            const roomId = msg.payload?.roomId;
            const name = msg.payload?.name;
            if (typeof roomId !== "string" || typeof name !== "string") return;
            const cleanName = name.trim();
            if (!roomId.trim() || !cleanName || cleanName.length > 20) return;
            if (clients.get(ws)?.roomId === roomId) return;

            leaveRoom(ws);
            if (!rooms.has(roomId)) rooms.set(roomId, new Set());
            rooms.get(roomId)?.add(ws);
            clients.set(ws, { name: cleanName, roomId });

            broadCast(roomId, {
                type: "system",
                payload: { message: `${cleanName} joined` }
            });
            sendUsers(roomId);
        }
        if (msg.type === "chat") {
            const client = clients.get(ws);
            if (!client) return;
            const text = msg.payload?.message;
            if (typeof text !== "string" || !text.trim() || text.length > 500) return;
            broadCast(client.roomId, {
                type: "chat",
                payload: { from: client.name, message: text }
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