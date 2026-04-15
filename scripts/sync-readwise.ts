/**
 * Sync Readwise highlights into the `documents` table.
 *
 * Run with:  npm run sync:readwise
 *
 * Each highlight becomes one row in `documents`. We use Readwise's
 * `/export` endpoint, which returns books + their highlights in one shot
 * and supports pagination via `pageCursor`.
 *
 * Safe to run repeatedly — `unique (source, source_id)` + upsert means
 * re-runs update existing rows instead of duplicating them.
 */

import { config as loadEnv } from "dotenv";
loadEnv({ path: ".env.local" });

import { createServerClient } from "../lib/supabase";

type ReadwiseHighlight = {
  id: number;
  text: string;
  note: string | null;
  location: number | null;
  location_type: string | null;
  highlighted_at: string | null;
  url: string | null;
  color: string | null;
  tags: { id: number; name: string }[];
  readwise_url: string;
};

type ReadwiseBook = {
  user_book_id: number;
  title: string;
  author: string | null;
  readable_title: string | null;
  source: string | null;
  category: string | null;
  source_url: string | null;
  cover_image_url: string | null;
  book_tags: { id: number; name: string }[];
  highlights: ReadwiseHighlight[];
};

type ExportResponse = {
  count: number;
  nextPageCursor: string | null;
  results: ReadwiseBook[];
};

async function fetchAllBooks(token: string): Promise<ReadwiseBook[]> {
  const books: ReadwiseBook[] = [];
  let cursor: string | null = null;

  do {
    const url = new URL("https://readwise.io/api/v2/export/");
    if (cursor) url.searchParams.set("pageCursor", cursor);

    const res = await fetch(url, {
      headers: { Authorization: `Token ${token}` },
    });
    if (!res.ok) {
      throw new Error(`Readwise API ${res.status}: ${await res.text()}`);
    }
    const data = (await res.json()) as ExportResponse;
    books.push(...data.results);
    cursor = data.nextPageCursor;
    process.stdout.write(`  fetched ${books.length} books…\r`);
  } while (cursor);

  process.stdout.write("\n");
  return books;
}

function toDocumentRow(book: ReadwiseBook, h: ReadwiseHighlight) {
  const contentParts = [h.text];
  if (h.note) contentParts.push(`\n\n— my note: ${h.note}`);
  return {
    source: "readwise",
    source_id: String(h.id),
    title: book.title,
    content: contentParts.join(""),
    url: h.url ?? book.source_url ?? h.readwise_url,
    occurred_at: h.highlighted_at,
    metadata: {
      author: book.author,
      category: book.category,
      book_source: book.source,
      book_id: book.user_book_id,
      tags: h.tags.map((t) => t.name),
      book_tags: book.book_tags.map((t) => t.name),
      color: h.color,
      location: h.location,
      location_type: h.location_type,
      readwise_url: h.readwise_url,
      cover_image_url: book.cover_image_url,
    },
  };
}

async function main() {
  const token = process.env.READWISE_API_TOKEN;
  if (!token) throw new Error("Missing READWISE_API_TOKEN in .env.local");

  console.log("Fetching from Readwise…");
  const books = await fetchAllBooks(token);
  const rows = books.flatMap((b) => b.highlights.map((h) => toDocumentRow(b, h)));
  console.log(`Got ${books.length} books and ${rows.length} highlights.`);

  if (rows.length === 0) {
    console.log("Nothing to sync.");
    return;
  }

  const supabase = createServerClient();
  // Upsert in chunks so we don't hit payload limits on big libraries.
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
