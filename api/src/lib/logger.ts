import { pino } from "pino";

export function createLogger(level: string) {
  return pino({ level, base: { service: "api" } });
}

export type Logger = ReturnType<typeof createLogger>;
