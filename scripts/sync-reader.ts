/**
 * Sync Readwise Reader documents into `documents`.
 *
 * Run with:  npm run sync:reader
 *
 * Uses Readwise Reader v3 API:
 *   GET https://readwise.io/api/v3/list/   (paginated by pageCursor)
 *
 * We skip category=highlight since those come in via sync-readwise.ts.
 * Everything else (articles, emails, PDFs, tweets, videos, RSS) gets ingested.
 */

import { config as loadEnv } from "dotenv";
loadEnv({ path: ".env.local" });

import { createServerClient } from "../lib/supabase";

const BASE = "https://readwise.io/api/v3";

type ReaderDoc = {
  id: string;
  url?: string | null;
  source_url?: string | null;
  title?: string | null;
  author?: string | null;
  source?: string | null;
  category?: string | null;           // article | email | rss | highlight | note | pdf | epub | tweet | video
  location?: string | null;           // new | later | shortlist | archive | feed
  tags?: Record<string, unknown> | string[] | null;
  site_name?: string | null;
  word_count?: number | null;
  created_at?: string | null;
  updated_at?: string | null;
  published_date?: string | number | null;
  summary?: string | null;
  image_url?: string | null;
  content?: string | null;             // sometimes present
  html_content?: string | null;
  notes?: string | null;               // user's notes on the doc
  reading_progress?: number | null;
  [k: string]: unknown;
};

type ListResponse = {
  count: number;
  nextPageCursor: string | null;
  results: ReaderDoc[];
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function fetchAll(token: string): Promise<ReaderDoc[]> {
  const docs: ReaderDoc[] = [];
  let cursor: string | null = null;
  let firstLogged = false;

  do {
    const url = new URL(`${BASE}/list/`);
    if (cursor) url.searchParams.set("pageCursor", cursor);
    const res = await fetch(url, {
      headers: { Authorization: `Token ${token}` },
    });
    // Reader API is rate-limited ~20 req/min. Respect backoff if we hit 429.
    if (res.status === 429) {
      const retry = Number(res.headers.get("retry-after")) || 5;
      console.warn(`  429, waiting ${retry}s…`);
      await sleep(retry * 1000);
      continue;
    }
    if (!res.ok) {
      throw new Error(`Reader ${res.status}: ${await res.text()}`);
    }
    const data = (await res.json()) as ListResponse;
    docs.push(...data.results);

    if (!firstLogged && data.results.length > 0) {
      const first = data.results[0];
      // Truncate any long fields to keep the log readable.
      const preview = { ...first };
      if (typeof preview.content === "string" && preview.content.length > 400)
        preview.content = preview.content.slice(0, 400) + "…";
      if (typeof preview.html_content === "string")
        preview.html_content = `<${preview.html_content.length} chars>`;
      console.log("\n--- RAW FIRST READER DOC ---");
      console.log(JSON.stringify(preview, null, 2));
      console.log("--- END ---\n");
      firstLogged = true;
    }

    cursor = data.nextPageCursor;
    process.stdout.write(`  fetched ${docs.length}…\r`);
    if (cursor) await sleep(3500); // ~17 req/min, under the 20/min cap
  } while (cursor);

  process.stdout.write("\n");
  return docs;
}

function firstString(...vals: unknown[]): string | null {
  for (const v of vals) {
    if (typeof v === "string" && v.trim()) return v;
  }
  return null;
}

function toDocumentRow(d: ReaderDoc) {
  // Compose a content blob from whatever Reader gave us.
  const parts: string[] = [];
  if (d.summary) parts.push(d.summary);
  if (d.notes) parts.push(`\n\n— my note: ${d.notes}`);
  if (d.content) parts.push(`\n\n${d.content}`);

  const content = parts.join("") || null;
  return {
    source: "reader",
    source_id: d.id,
    title: d.title ?? "(untitled)",
    content,
    url: firstString(d.source_url, d.url),
    occurred_at:
      firstString(d.published_date as string, d.created_at) || null,
    metadata: {
      author: d.author,
      category: d.category,
      location: d.location,
      site_name: d.site_name,
      word_count: d.word_count,
      tags: d.tags,
      image_url: d.image_url,
      reading_progress: d.reading_progress,
      source: d.source,
      reader_url: d.url,
      updated_at: d.updated_at,
    },
  };
}

async function main() {
  const token = process.env.READWISE_API_TOKEN;
  if (!token) throw new Error("Missing READWISE_API_TOKEN in .env.local");

  console.log("Fetching from Readwise Reader…");
  const raw = await fetchAll(token);

  // Skip highlights (covered by sync-readwise.ts) and any notes-only entries.
  const filtered = raw.filter(
    (d) => d.category !== "highlight" && d.category !== "note",
  );
  const skipped = raw.length - filtered.length;
  console.log(
    `Got ${raw.length} total, ${filtered.length} to ingest (${skipped} highlights/notes skipped).`,
  );

  if (filtered.length === 0) {
    console.log("Nothing to sync.");
    return;
  }

  const rows = filtered.map(toDocumentRow);
  const supabase = createServerClient();
  const CHUNK = 500;
  let done = 0;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const slice = rows.slice(i, i + CHUNK);
    const { error } = await supabase
      .from("documents")
      .upsert(slice, { onConflict: "source,source_id" });
    if (error) throw error;
    done += slice.length;
    process.stdout.write(`  upserted ${done}/${rows.length}\r`);
  }
  process.stdout.write("\n");
  console.log("Done.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
