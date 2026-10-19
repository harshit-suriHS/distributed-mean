import type { Response } from "express";
import type { z } from "zod";

/**
 * Sends a response through its DTO schema: unknown fields are stripped and a shape
 * mismatch is a 500 (our bug), so internal columns can never leak to clients.
 */
export function send<S extends z.ZodType>(
  res: Response,
  status: number,
  schema: S,
  body: z.input<S>,
) {
  res.status(status).json(schema.parse(body));
}
