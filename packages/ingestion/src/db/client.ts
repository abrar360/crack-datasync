import { Pool } from 'pg';
import { NormalizedEvent, IngestionState } from '../types';

export class DatabaseClient {
  private pool: Pool;

  constructor(connectionString: string) {
    this.pool = new Pool({
      connectionString,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
    });
  }

  getPool(): Pool {
    return this.pool;
  }

  async insertEvents(events: NormalizedEvent[]): Promise<number> {
    if (events.length === 0) return 0;

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      let inserted = 0;
      // Batch insert with ON CONFLICT DO NOTHING for idempotency
      const batchSize = 1000;
      for (let i = 0; i < events.length; i += batchSize) {
        const batch = events.slice(i, i + batchSize);
        const values: unknown[] = [];
        const placeholders: string[] = [];

        batch.forEach((event, idx) => {
          const offset = idx * 9;
          placeholders.push(
            `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9})`
          );
          values.push(
            event.id,
            event.session_id,
            event.user_id,
            event.type,
            event.name,
            JSON.stringify(event.properties),
            event.timestamp,
            event.device_type,
            event.browser
          );
        });

        const result = await client.query(
          `INSERT INTO ingested_events (id, session_id, user_id, type, name, properties, timestamp, device_type, browser)
           VALUES ${placeholders.join(', ')}
           ON CONFLICT (id) DO NOTHING`,
          values
        );
        inserted += result.rowCount || 0;
      }

      await client.query('COMMIT');
      return inserted;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async getIngestionState(): Promise<IngestionState> {
    const result = await this.pool.query(
      'SELECT id, cursor, events_ingested, last_updated, completed FROM ingestion_state WHERE id = 1'
    );
    const row = result.rows[0];
    return {
      id: row.id,
      cursor: row.cursor,
      // events_ingested is BIGINT which pg returns as string
      events_ingested: typeof row.events_ingested === 'string'
        ? parseInt(row.events_ingested, 10)
        : row.events_ingested,
      last_updated: row.last_updated,
      completed: row.completed,
    };
  }

  async updateIngestionState(cursor: string | null, eventsIngested: number, completed: boolean = false): Promise<void> {
    await this.pool.query(
      `UPDATE ingestion_state
       SET cursor = $1, events_ingested = $2, last_updated = NOW(), completed = $3
       WHERE id = 1`,
      [cursor, eventsIngested, completed]
    );
  }

  async getEventCount(): Promise<number> {
    const result = await this.pool.query<{ count: string }>('SELECT COUNT(*) as count FROM ingested_events');
    return parseInt(result.rows[0].count, 10);
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}
