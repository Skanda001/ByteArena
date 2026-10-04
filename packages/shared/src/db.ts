import { Pool, PoolClient, QueryResult, QueryResultRow } from "pg";
import { config } from "./config";
import { logger } from "./logger";

export const pool = new Pool({
  connectionString: config.DATABASE_URL,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

pool.on("error", (err) => {
  logger.error({ err }, "Unexpected error on idle PostgreSQL client");
});

export async function query<R extends QueryResultRow = QueryResultRow, I extends unknown[] = unknown[]>(
  text: string,
  params?: I
): Promise<QueryResult<R>> {
  const start = Date.now();
  const res = await pool.query<R>(text, params);
  const duration = Date.now() - start;
  logger.trace({ text, duration, rows: res.rowCount }, "Executed SQL query");
  return res;
}

export async function getClient(): Promise<PoolClient> {
  return pool.connect();
}

let isClosed = false;

export async function closeDb(): Promise<void> {
  if (isClosed) return;
  isClosed = true;
  await pool.end();
  logger.info("PostgreSQL pool closed");
}
