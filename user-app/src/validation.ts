
import { z } from "zod";

export const ClientMsgSchema = z.discriminatedUnion("type", [
    z.object({
        type: z.literal("join"),
        payload: z.object({
            roomId: z.string().trim().min(1).max(30),
            name: z.string().trim().min(1).max(20)
        }),
    }),
    z.object({
        type: z.literal("chat"),
        payload: z.object({
            message: z.string().trim().min(1).max(500)
        })
    }),
]);