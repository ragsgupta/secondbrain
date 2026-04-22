-- Create HNSW index on chunks.embedding for fast cosine similarity search.
-- Without this, match_chunks does a full table scan over ~72k vectors and hits
-- Supabase's default statement_timeout, returning 0 results silently.
--
-- Run once in the Supabase SQL editor (Dashboard → SQL Editor → New query).
-- Index build may take 30–120s depending on table size; run outside peak hours.

CREATE INDEX IF NOT EXISTS chunks_embedding_hnsw
  ON chunks
  USING hnsw (embedding vector_cosine_ops)
  WITH (m = 16, ef_construction = 64);

-- Verify the index was created and the match_chunks function exists.
SELECT
  indexname,
  indexdef
FROM pg_indexes
WHERE tablename = 'chunks'
  AND indexname = 'chunks_embedding_hnsw';
