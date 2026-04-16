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

async function loadDocsWithoutChunks(
  supabase: ReturnType<typeof createServerClient>,
  limit: number,
): Promise<Doc[]> {
  // Pull documents with no rows in chunks. Done via a LEFT JOIN emulated with two queries
  // because Supabase's JS client doesn't easily express "not exists".
  const { data: existing, error: e1 } = await supabase
    .from("chunks")
    .select("document_id")
    .limit(100_000);
  if (e1) throw e1;
  const has = new Set((existing ?? []).map((r) => r.document_id as number));

  // Now pull documents in batches until we find `limit` that aren't in `has`.
  const out: Doc[] = [];
  let page = 0;
  while (out.length < limit) {
    const from = page * 500;
    const to = from + 499;
    const { data, error } = await supabase
      .from("documents")
      .select("id, title, content")
      .order("id", { ascending: true })
      .range(from, to);
    if (error) throw error;
    if (!data || data.length === 0) break;
    for (const d of data as Doc[]) {
      if (!has.has(d.id)) {
        out.push(d);
        if (out.length >= limit) break;
      }
    }
    if (data.length < 500) break;
    page++;
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

  let totalDocs = 0;
  let totalChunks = 0;

  while (true) {
    const docs = await loadDocsWithoutChunks(supabase, DOC_BATCH);
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

    // 3) Insert chunks with their embeddings.
    const rows = pending.map((p, i) => ({
      document_id: p.document_id,
      chunk_index: p.chunk_index,
      content: p.content,
      embedding: embeddings[i] as unknown as string,
    }));
    const INSERT_CHUNK = 200;
    for (let i = 0; i < rows.length; i += INSERT_CHUNK) {
      const slice = rows.slice(i, i + INSERT_CHUNK);
      const { error } = await supabase
        .from("chunks")
        .upsert(slice, { onConflict: "document_id,chunk_index" });
      if (error) throw error;
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
