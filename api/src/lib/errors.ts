import type { ErrorRequestHandler } from "express";
import { ZodError } from "zod";
import type { Logger } from "./logger.js";

/** An error the client caused or can act on. Anything else becomes a 500. */
export class AppError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

export const notFound = (what: string) => new AppError(404, "not_found", `${what} not found`);
export const conflict = (message: string) => new AppError(409, "conflict", message);

export function errorHandler(logger: Logger): ErrorRequestHandler {
  return (err, _req, res, _next) => {
    if (err instanceof ZodError) {
      res.status(400).json({
        error: { code: "validation_error", message: "invalid request", issues: err.issues },
      });
      return;
    }
    if (err instanceof AppError) {
      res.status(err.status).json({ error: { code: err.code, message: err.message } });
      return;
    }
    // express.json() parse failures carry a 4xx status
    if (typeof err?.status === "number" && err.status >= 400 && err.status < 500) {
      res.status(err.status).json({ error: { code: "bad_request", message: err.message } });
      return;
    }
    logger.error({ err }, "unhandled error");
    res.status(500).json({ error: { code: "internal", message: "internal server error" } });
  };
}
