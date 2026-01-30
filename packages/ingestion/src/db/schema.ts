import { Pool } from 'pg';

export async function initializeSchema(pool: Pool): Promise<void> {
  console.log('[DB] Initializing database schema...');

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ingested_events (
      id UUID PRIMARY KEY,
      session_id UUID NOT NULL,
      user_id UUID NOT NULL,
      type VARCHAR(50) NOT NULL,
      name VARCHAR(100) NOT NULL,
      properties JSONB NOT NULL DEFAULT '{}',
      timestamp TIMESTAMPTZ NOT NULL,
      device_type VARCHAR(20) NOT NULL,
      browser VARCHAR(50) NOT NULL,
      ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_ingested_events_timestamp ON ingested_events(timestamp);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_ingested_events_session_id ON ingested_events(session_id);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_ingested_events_user_id ON ingested_events(user_id);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ingestion_state (
      id INTEGER PRIMARY KEY DEFAULT 1,
      cursor TEXT,
      events_ingested BIGINT NOT NULL DEFAULT 0,
      last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      completed BOOLEAN NOT NULL DEFAULT FALSE,
      CONSTRAINT single_row CHECK (id = 1)
    );
  `);

  // Insert initial state if not exists
  await pool.query(`
    INSERT INTO ingestion_state (id, cursor, events_ingested, completed)
    VALUES (1, NULL, 0, FALSE)
    ON CONFLICT (id) DO NOTHING;
  `);

  console.log('[DB] Schema initialized successfully');
}
