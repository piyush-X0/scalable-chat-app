
import { z } from "zod";

export const ClientMsgSchema = z.discriminatedUnion("type", [
    z.object({
        type: z.literal("join"),
        payload: z.object({
            name: z.string().trim()
                .min(1, "name is required")
                .max(20, "name must be 20 characters or fewer"),
            roomId: z.string().trim()
                .min(1, "roomId is required")
                .max(30, "roomId must be 30 characters or fewer"),
        }),
    }),
    z.object({
        type: z.literal("chat"),
        payload: z.object({
            message: z.string().trim()
                .min(1, "message can't be empty")
                .max(500, "message must be 500 characters or fewer")
        })
    }),
]);