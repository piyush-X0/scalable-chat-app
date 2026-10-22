import { WebSocketServer, WebSocket } from 'ws';

const wss = new WebSocketServer({ port: 8080 });

const rooms = new Map<string, Set<WebSocket>>();
const socketRoom = new Map<WebSocket, string>();

type ClientMsg = | { type: "join", payload: { roomId: string } }
    | { type: "chat", payload: { message: string } }

function leaveRoom(ws: WebSocket) {
    const roomId = socketRoom.get(ws);
    if (!roomId) return;

    const members = rooms.get(roomId);
    if (members) {
        members.delete(ws);
        if (members.size === 0) rooms.delete(roomId);
    }
    socketRoom.delete(ws);
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

        if (msg.type === "join") {
            const { roomId } = msg.payload;
            leaveRoom(ws);
            if (!rooms.has(roomId)) rooms.set(roomId, new Set());
            rooms.get(roomId)!.add(ws);
            socketRoom.set(ws, roomId);
        }
        if (msg.type === "chat") {
            const roomId = socketRoom.get(ws);
            if (!roomId) return;
            const members = rooms.get(roomId);
            if (!members) return;

            const out = JSON.stringify({
                type: "chat",
                payload: {
                    message: msg.payload.message
                }
            });
            for (const client of members) {
                if (client.readyState === WebSocket.OPEN) client.send(out);
            }
        }
        ws.on("close", () => {
            leaveRoom(ws);
        })
    })
});