import { DatabaseClient } from './db/client';
import { initializeSchema } from './db/schema';
import { ApiClient } from './api/client';
import { NormalizedEvent } from './types';

const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/ingestion';
const API_BASE_URL = process.env.API_BASE_URL || 'http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com';
const API_KEY = process.env.TARGET_API_KEY || 'ds_02af351e5f871d09a5410dcfef149f2b';

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function runIngestion(): Promise<void> {
  console.log('==============================================');
  console.log('DataSync Ingestion - Starting');
  console.log('==============================================');
  console.log(`[CONFIG] Database: ${DATABASE_URL.replace(/:[^:@]+@/, ':***@')}`);
  console.log(`[CONFIG] API Base URL: ${API_BASE_URL}`);

  const db = new DatabaseClient(DATABASE_URL);
  const api = new ApiClient(API_BASE_URL, API_KEY);

  try {
    // Initialize schema
    await initializeSchema(db.getPool());

    // Get current ingestion state (for resumability)
    const state = await db.getIngestionState();

    if (state.completed) {
      const eventCount = await db.getEventCount();
      console.log(`[INFO] Ingestion already completed. Total events: ${eventCount}`);
      console.log('ingestion complete');
      return;
    }

    let cursor = state.cursor;
    let totalIngested = state.events_ingested;
    let page = Math.floor(totalIngested / 100000) + 1;

    if (cursor) {
      console.log(`[INFO] Resuming ingestion from cursor (${totalIngested} events already ingested)`);
    } else {
      console.log('[INFO] Starting fresh ingestion');
    }

    while (true) {
      // Try non-streaming endpoint first
      console.log(`[NONSTREAM] Fetching page ${page} (cursor: ${cursor ? cursor.slice(-8) : 'None'})...`);

      let response;
      try {
        response = await api.fetchNonStreamPage(cursor);
      } catch (error) {
        console.error(`[ERROR] Non-stream fetch failed: ${error}`);
        throw error;
      }

      if (response.rateLimitDelay > 0) {
        // Rate limited - switch to streaming endpoint
        console.log(`[WARN] Rate limited on non-stream. Switching to streaming endpoint for ${response.rateLimitDelay}s...`);

        const rateLimitEnd = Date.now() + (response.rateLimitDelay * 1000);

        while (Date.now() < rateLimitEnd) {
          console.log(`[STREAM] Fetching page ${page} (cursor: ${cursor ? cursor.slice(-8) : 'None'})...`);

          try {
            const streamResponse = await api.fetchStreamPage(cursor);

            if (streamResponse.tokenExpired) {
              // Token expired - explicitly refresh credentials before retry (matching Python behavior)
              console.log('[WARN] Stream token expired. Refreshing credentials...');
              await api.getStreamAccess(); // Force fetch new token now
              continue; // Retry immediately with new credentials
            }

            if (streamResponse.rateLimitDelay > 0) {
              console.log(`[WARN] Stream also rate limited. Waiting ${streamResponse.rateLimitDelay}s...`);
              await sleep((streamResponse.rateLimitDelay + 1) * 1000);
              continue;
            }

            if (!streamResponse.data) {
              console.log('[WARN] Stream returned empty response. Retrying...');
              await sleep(2000);
              continue;
            }

            // Process stream response - safely access data array
            const dataArray = streamResponse.data.data || [];
            const events = dataArray
              .filter(ApiClient.isValidEvent)
              .map(ApiClient.normalizeEvent);

            const inserted = await db.insertEvents(events);
            totalIngested += inserted;

            console.log(`[STREAM] Page ${page}: Inserted ${inserted}/${events.length} events (total: ${totalIngested})`);

            // Update state after each page - safely access pagination
            const pagination = streamResponse.data.pagination || {};
            const nextCursor = pagination.nextCursor || null;
            await db.updateIngestionState(nextCursor, totalIngested);

            if (!pagination.hasMore) {
              console.log('[INFO] Stream reports no more pages. Verifying with non-stream...');
              break;
            }

            cursor = nextCursor;
            page++;

          } catch (error) {
            // Generic error during stream fetch - wait and retry within stream mode
            console.log(`[WARN] Stream fetch error: ${error}`);
            await sleep(2000);
            // Continue to retry within stream mode
          }
        }

        // Rate limit period ended, add buffer then continue with non-stream
        await sleep(1000);
        console.log('[INFO] Rate limit period ended. Switching back to non-streaming endpoint...');
        continue;
      }

      // Non-streaming request succeeded
      if (!response.data) {
        console.log('[WARN] Non-streaming returned empty response. Retrying...');
        await sleep(2000);
        continue;
      }

      // Process events - safely access data array
      const dataArray = response.data.data || [];
      const events: NormalizedEvent[] = dataArray
        .filter(ApiClient.isValidEvent)
        .map(ApiClient.normalizeEvent);

      const inserted = await db.insertEvents(events);
      totalIngested += inserted;

      console.log(`[NONSTREAM] Page ${page}: Inserted ${inserted}/${events.length} events (total: ${totalIngested})`);

      // Safely access pagination
      const pagination = response.data.pagination || {};

      // Check if we're done
      if (!pagination.hasMore) {
        // Mark as completed
        await db.updateIngestionState(null, totalIngested, true);
        break;
      }

      // Update cursor for next iteration and save state
      cursor = pagination.nextCursor || null;
      await db.updateIngestionState(cursor, totalIngested);
      page++;
    }

    const finalCount = await db.getEventCount();
    console.log('==============================================');
    console.log(`INGESTION COMPLETE!`);
    console.log(`Total events in database: ${finalCount}`);
    console.log('==============================================');
    console.log('ingestion complete');

  } catch (error) {
    console.error('[ERROR] Ingestion failed:', error);
    throw error;
  } finally {
    await db.close();
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('[INFO] Received SIGINT, shutting down gracefully...');
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('[INFO] Received SIGTERM, shutting down gracefully...');
  process.exit(0);
});

// Run the ingestion
runIngestion().catch((error) => {
  console.error('[FATAL] Unhandled error:', error);
  process.exit(1);
});
