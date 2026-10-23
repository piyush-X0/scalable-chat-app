import { WebSocketServer, WebSocket } from 'ws';

const wss = new WebSocketServer({ port: 8080 });

interface Client { name: string, roomId: string };

const rooms = new Map<string, Set<WebSocket>>();
const clients = new Map<WebSocket, Client>();

type ClientMsg = | { type: "join", payload: { roomId: string, name: string } }
    | { type: "chat", payload: { message: string } };
type ServerMsg = | { type: "chat", payload: { from: string, message: string } }
    | { type: "system", payload: { message: string } };

function leaveRoom(ws: WebSocket) {
    const client = clients.get(ws);
    if (!client) return;

    const members = rooms.get(client.roomId);
    if (members) {
        members.delete(ws);
        if (members.size === 0) rooms.delete(client.roomId);
    } else {
        broadCast(client.roomId, {
            type: "system",
            payload: { message: `${client.name} left` }
        });
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

wss.on('connection', (ws) => {
    ws.on('error', console.error)

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

            leaveRoom(ws);
            if (!rooms.has(roomId)) rooms.set(roomId, new Set());
            rooms.get(roomId)?.add(ws);
            clients.set(ws, { name: cleanName, roomId });

            broadCast(roomId, {
                type: "system",
                payload: { message: `${cleanName} joined` }
            });
        }
        if (msg.type === "chat") {
            const client = clients.get(ws);
            if (!client) return;
            const text = msg.payload?.message;
            if (text !== "string" || !text.trim() || text.length > 500) return;
            broadCast(client.roomId, {
                type: "chat",
                payload: { from: client.name, message: text }
            })
        }
        ws.on("close", () => {
            leaveRoom(ws);
        })
    })
});