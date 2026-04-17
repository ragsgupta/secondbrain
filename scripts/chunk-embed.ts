/**
 * Split every document into chunks, embed them, and store in `chunks`.
 *
 * Run with:  npm run chunk
 *
 * Default: only processes documents that have no chunks yet. Safe to re-run
 * after syncs — new docs get chunked, old ones are left alone.
 *
 * Pass --rebuild to wipe all chunks and start from scratch:
 *   npx tsx scripts/chunk-embed.ts --rebuild
 */

import { config as loadEnv } from "dotenv";
loadEnv({ path: ".env.local" });

import { createServerClient } from "../lib/supabase";
import { embed } from "../lib/voyage";

const CHUNK_TARGET_CHARS = 1800;   // ~450 tokens per chunk
const DOC_BATCH = 50;               // docs pulled per loop iteration
const EMBED_BATCH = 32;             // chunks embedded per Voyage call

type Doc = { id: number; title: string | null; content: string | null };

/** Split text into ~target-sized chunks on paragraph/sentence boundaries. */
function splitIntoChunks(text: string, target = CHUNK_TARGET_CHARS): string[] {
  const src = text.trim();
  if (src.length === 0) return [];
  if (src.length <= target) return [src];

  const chunks: string[] = [];
  const paragraphs = src.split(/\n\n+/);
  let current = "";

  const flush = () => {
    if (current.trim()) chunks.push(current.trim());
    current = "";
  };

  const addToCurrent = (piece: string) => {
    if (!current) current = piece;
    else current += "\n\n" + piece;
  };

  for (const para of paragraphs) {
    if ((current.length + para.length + 2) <= target) {
      addToCurrent(para);
      continue;
    }
    // Doesn't fit.
    flush();
    if (para.length <= target) {
      current = para;
      continue;
    }
    // Single paragraph too big — split by sentence.
    const sentences = para.split(/(?<=[.!?])\s+/);
    let sub = "";
    for (const s of sentences) {
      if ((sub.length + s.length + 1) <= target) {
        sub = sub ? sub + " " + s : s;
        continue;
      }
      if (sub) chunks.push(sub.trim());
      // If a single sentence is still too big (rare — long URL/line), hard-slice.
      if (s.length > target) {
        for (let i = 0; i < s.length; i += target) chunks.push(s.slice(i, i + target));
        sub = "";
      } else {
        sub = s;
      }
    }
    if (sub) chunks.push(sub.trim());
    current = "";
  }
  flush();
  return chunks;
}

/** Prepend title so each chunk carries context when embedded. */
function contextualize(chunk: string, title: string | null): string {
  const t = title?.trim();
  return t ? `${t}\n\n${chunk}` : chunk;
}

const PAGE = 1000; // Supabase PostgREST caps responses at 1000 rows by default.

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

type ChunkRow = {
  document_id: number;
  chunk_index: number;
  content: string;
  embedding: string;
};

/**
 * Upsert with retry. Supabase's statement timeout (~8s default) trips when the
 * HNSW vector index has a lot of concurrent writes. Exponential backoff gives
 * the index a moment to catch up.
 */
async function upsertWithRetry(
  supabase: ReturnType<typeof createServerClient>,
  rows: ChunkRow[],
  maxAttempts = 5,
): Promise<void> {
  let attempt = 0;
  while (true) {
    const { error } = await supabase
      .from("chunks")
      .upsert(rows, { onConflict: "document_id,chunk_index" });
    if (!error) return;
    const details = (error as { details?: string }).details ?? "";
    const retryable =
      error.code === "57014" || // statement_timeout
      error.code === "40001" || // serialization_failure
      error.code === "40P01" || // deadlock_detected
      /timeout/i.test(error.message ?? "") ||
      /ETIMEDOUT|ECONNRESET|ECONNREFUSED|fetch failed/i.test(error.message ?? "") ||
      /ETIMEDOUT|ECONNRESET/i.test(details);
    if (!retryable || attempt >= maxAttempts) throw error;
    const delay = 1000 * Math.pow(2, attempt) + Math.random() * 500;
    console.warn(`\n  upsert ${error.code} — retrying in ${Math.round(delay)}ms`);
    await sleep(delay);
    attempt++;
  }
}

/** Paginate every row's document_id in `chunks` to build the "already chunked" set. */
async function loadDocIdsWithChunks(
  supabase: ReturnType<typeof createServerClient>,
): Promise<Set<number>> {
  const has = new Set<number>();
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from("chunks")
      .select("document_id")
      .order("id", { ascending: true })
      .range(from, from + PAGE - 1);
    if (error) throw error;
    if (!data || data.length === 0) break;
    for (const r of data) has.add(r.document_id as number);
    if (data.length < PAGE) break;
    from += PAGE;
  }
  return has;
}

/** Pull the next page of documents not already in `has`. Returns empty when exhausted. */
async function nextDocBatch(
  supabase: ReturnType<typeof createServerClient>,
  has: Set<number>,
  cursor: { lastId: number },
  limit: number,
): Promise<Doc[]> {
  const out: Doc[] = [];
  while (out.length < limit) {
    const { data, error } = await supabase
      .from("documents")
      .select("id, title, content")
      .gt("id", cursor.lastId)
      .order("id", { ascending: true })
      .limit(PAGE);
    if (error) throw error;
    if (!data || data.length === 0) break;
    for (const d of data as Doc[]) {
      cursor.lastId = d.id;
      if (!has.has(d.id)) {
        out.push(d);
        if (out.length >= limit) break;
      }
    }
    if (data.length < PAGE) break;
  }
  return out;
}

async function main() {
  const rebuild = process.argv.includes("--rebuild");
  const supabase = createServerClient();

  if (rebuild) {
    console.log("Rebuilding: deleting all chunks…");
    // Supabase requires a filter on delete; use "id > 0" which matches all.
    const { error } = await supabase.from("chunks").delete().gt("id", 0);
    if (error) throw error;
  }

  // Preload the "already chunked" set ONCE. We add to it as we process batches,
  // so we never re-scan the chunks table. This also makes the first-iteration
  // log honest about scope: N docs already chunked means N we're skipping.
  console.log("Loading existing chunk index…");
  const has = rebuild ? new Set<number>() : await loadDocIdsWithChunks(supabase);
  console.log(`  ${has.size} docs already have chunks.`);

  let totalDocs = 0;
  let totalChunks = 0;
  const cursor = { lastId: 0 };

  while (true) {
    const docs = await nextDocBatch(supabase, has, cursor, DOC_BATCH);
    if (docs.length === 0) break;

    // 1) Chunk every doc in this batch, accumulating rows to insert.
    type Pending = { document_id: number; chunk_index: number; content: string };
    const pending: Pending[] = [];
    for (const d of docs) {
      const text = (d.content ?? "").trim();
      const pieces = splitIntoChunks(text);
      if (pieces.length === 0) {
        // Doc has no content — insert a single chunk from title so it's not re-scanned.
        const t = d.title?.trim();
        if (t) pending.push({ document_id: d.id, chunk_index: 0, content: t });
        continue;
      }
      pieces.forEach((p, i) => {
        pending.push({
          document_id: d.id,
          chunk_index: i,
          content: contextualize(p, d.title),
        });
      });
    }

    if (pending.length === 0) {
      totalDocs += docs.length;
      continue;
    }

    // 2) Embed in sub-batches.
    const embeddings = new Array<number[]>(pending.length);
    for (let i = 0; i < pending.length; i += EMBED_BATCH) {
      const slice = pending.slice(i, i + EMBED_BATCH);
      const vecs = await embed(
        slice.map((p) => p.content),
        "document",
      );
      vecs.forEach((v, k) => (embeddings[i + k] = v));
      process.stdout.write(
        `  embedded ${Math.min(i + EMBED_BATCH, pending.length)}/${pending.length} chunks\r`,
      );
    }
    process.stdout.write("\n");

    // 3) Insert chunks with their embeddings. Keep batches small — Supabase's
    //    default ~8s statement timeout bites when HNSW index updates pile up.
    const rows = pending.map((p, i) => ({
      document_id: p.document_id,
      chunk_index: p.chunk_index,
      content: p.content,
      embedding: embeddings[i] as unknown as string,
    }));
    const INSERT_CHUNK = 50;
    for (let i = 0; i < rows.length; i += INSERT_CHUNK) {
      const slice = rows.slice(i, i + INSERT_CHUNK);
      await upsertWithRetry(supabase, slice);
    }

    totalDocs += docs.length;
    totalChunks += rows.length;
    console.log(`  progress: ${totalDocs} docs, ${totalChunks} chunks`);
  }

  console.log(`\nDone. ${totalDocs} docs chunked into ${totalChunks} chunks.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
