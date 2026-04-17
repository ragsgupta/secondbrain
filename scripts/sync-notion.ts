/**
 * Sync all Notion pages the integration can access into `documents`.
 *
 * Run with:  npm run sync:notion
 *
 * Setup (one-time):
 *   1. Go to https://www.notion.so/my-integrations → "New integration"
 *   2. Name it "Second Brain", set workspace, leave capabilities as read-only.
 *   3. Copy the "Internal Integration Secret" → add to .env.local as NOTION_TOKEN=secret_xxx
 *   4. In Notion, open each top-level page/database you want included,
 *      click "..." → "Connect to" → select "Second Brain".
 *      Child pages are included automatically.
 *
 * Each Notion page becomes one document. Content is the full rendered text of
 * all blocks (headings, bullets, paragraphs, code, etc.), recursively.
 */

import { config as loadEnv } from "dotenv";
loadEnv({ path: ".env.local" });

import { Client, isFullBlock, isFullPage } from "@notionhq/client";
import type {
  BlockObjectResponse,
  PageObjectResponse,
  RichTextItemResponse,
} from "@notionhq/client/build/src/api-endpoints";
import { createServerClient } from "../lib/supabase";

// ---- Config --------------------------------------------------------
const MAX_CONTENT_CHARS = 20_000; // cap per page so Voyage/Claude prompts stay sane
const BLOCK_DEPTH_LIMIT = 5;       // how deep to recurse into nested blocks
const CONCURRENCY = 1;             // sequential — recursive block fetches burst hard at 2+
// --------------------------------------------------------------------

function getNotion(): Client {
  const token = process.env.NOTION_TOKEN;
  if (!token) throw new Error("Missing NOTION_TOKEN in .env.local");
  return new Client({ auth: token });
}

// ---- Rate-limit aware fetch ----------------------------------------
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function withRetry<T>(fn: () => Promise<T>, label = ""): Promise<T> {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (err: unknown) {
      const e = err as { status?: number; code?: string; message?: string; cause?: { code?: string } };
      const status = e.status;
      const code = e.code ?? e.cause?.code ?? "";
      const msg = e.message ?? "";
      const retryable =
        status === 429 ||
        status === 503 ||
        status === 502 ||
        (typeof status === "number" && status >= 500) ||
        code === "rate_limited" ||
        /UND_ERR|ETIMEDOUT|ECONNRESET|ECONNREFUSED|fetch failed/i.test(code) ||
        /fetch failed|connect timeout/i.test(msg);
      if (!retryable || attempt >= 6) throw err;
      const delay = 1000 * Math.pow(2, attempt) + Math.random() * 500;
      if (attempt === 0 && label) process.stdout.write(`\n  retrying ${label}…`);
      await sleep(delay);
      attempt++;
    }
  }
}

// ---- Page listing --------------------------------------------------
async function fetchAllPages(notion: Client): Promise<PageObjectResponse[]> {
  const pages: PageObjectResponse[] = [];
  let cursor: string | undefined;
  do {
    const res = await withRetry(() =>
      notion.search({
        filter: { value: "page", property: "object" },
        page_size: 100,
        start_cursor: cursor,
      }),
    );
    for (const r of res.results) {
      if (isFullPage(r)) pages.push(r);
    }
    cursor = res.has_more && res.next_cursor ? res.next_cursor : undefined;
    process.stdout.write(`  found ${pages.length} pages…\r`);
  } while (cursor);
  process.stdout.write("\n");
  return pages;
}

// ---- Block → text --------------------------------------------------
function richText(items: RichTextItemResponse[]): string {
  return items.map((t) => t.plain_text).join("");
}

function blockToLines(block: BlockObjectResponse): string[] {
  switch (block.type) {
    case "paragraph":
      return [richText(block.paragraph.rich_text)];
    case "heading_1":
      return ["# " + richText(block.heading_1.rich_text)];
    case "heading_2":
      return ["## " + richText(block.heading_2.rich_text)];
    case "heading_3":
      return ["### " + richText(block.heading_3.rich_text)];
    case "bulleted_list_item":
      return ["• " + richText(block.bulleted_list_item.rich_text)];
    case "numbered_list_item":
      return [richText(block.numbered_list_item.rich_text)];
    case "to_do": {
      const mark = block.to_do.checked ? "[x]" : "[ ]";
      return [`${mark} ${richText(block.to_do.rich_text)}`];
    }
    case "toggle":
      return [richText(block.toggle.rich_text)];
    case "quote":
      return ["> " + richText(block.quote.rich_text)];
    case "callout":
      return [richText(block.callout.rich_text)];
    case "code":
      return [richText(block.code.rich_text)];
    case "divider":
      return ["---"];
    case "table_row":
      return [block.table_row.cells.map((c) => richText(c)).join(" | ")];
    case "image":
      return block.image.type === "external"
        ? [block.image.caption.length ? richText(block.image.caption) : ""]
        : [richText(block.image.caption)];
    case "bookmark":
      return [(block.bookmark.url ?? "")];
    default:
      return [];
  }
}

async function fetchPageContent(
  notion: Client,
  blockId: string,
  depth = 0,
): Promise<string[]> {
  if (depth > BLOCK_DEPTH_LIMIT) return [];
  const lines: string[] = [];
  let cursor: string | undefined;
  do {
    const res = await withRetry(() =>
      notion.blocks.children.list({
        block_id: blockId,
        page_size: 100,
        start_cursor: cursor,
      }),
    );
    for (const b of res.results) {
      if (!isFullBlock(b)) continue;
      const bLines = blockToLines(b as BlockObjectResponse);
      const indent = "  ".repeat(depth);
      lines.push(...bLines.filter(Boolean).map((l) => indent + l));
      if (b.has_children) {
        const childLines = await fetchPageContent(notion, b.id, depth + 1);
        lines.push(...childLines);
      }
    }
    cursor = res.has_more && res.next_cursor ? res.next_cursor : undefined;
  } while (cursor);
  return lines;
}

// ---- Page metadata helpers -----------------------------------------
function getTitle(page: PageObjectResponse): string {
  for (const prop of Object.values(page.properties)) {
    if (prop.type === "title") {
      return prop.title.map((t) => t.plain_text).join("").trim() || "Untitled";
    }
  }
  return "Untitled";
}

function getNotionUrl(page: PageObjectResponse): string {
  // page.url comes back as "https://www.notion.so/Title-<id>" — use it directly.
  return page.url;
}

// ---- Concurrency helper --------------------------------------------
async function mapLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, i: number) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, async () => {
      while (true) {
        const i = next++;
        if (i >= items.length) return;
        results[i] = await fn(items[i], i);
      }
    }),
  );
  return results;
}

// ---- Main ----------------------------------------------------------
async function main() {
  const notion = getNotion();
  const supabase = createServerClient();

  console.log("Fetching all accessible Notion pages…");
  const pages = await fetchAllPages(notion);
  if (pages.length === 0) {
    console.log("No pages found. Make sure you've shared pages with the integration.");
    return;
  }
  console.log(`${pages.length} pages found. Fetching content…`);

  // Fetch full block content for every page (rate-limited).
  type PageData = { page: PageObjectResponse; title: string; content: string | null };
  let done = 0;
  const pageData = await mapLimit<PageObjectResponse, PageData>(
    pages,
    CONCURRENCY,
    async (page) => {
      const title = getTitle(page);
      const lines = await fetchPageContent(notion, page.id);
      const content = lines.join("\n").trim().slice(0, MAX_CONTENT_CHARS) || null;
      done++;
      process.stdout.write(`  fetched ${done}/${pages.length} pages\r`);
      await sleep(350); // ~3 req/sec breathing room between pages
      return { page, title, content };
    },
  );
  process.stdout.write("\n");

  // Upsert documents
  console.log("Upserting documents…");
  const CHUNK = 100;
  for (let i = 0; i < pageData.length; i += CHUNK) {
    const slice = pageData.slice(i, i + CHUNK);
    const rows = slice.map(({ page, title, content }) => ({
      source: "notion" as const,
      source_id: page.id,
      title,
      content,
      url: getNotionUrl(page),
      occurred_at: page.last_edited_time, // use last edit as "when"
      metadata: {
        created_time: page.created_time,
        last_edited_time: page.last_edited_time,
        archived: page.archived,
      },
    }));
    const { error } = await supabase
      .from("documents")
      .upsert(rows, { onConflict: "source,source_id" });
    if (error) throw error;
    process.stdout.write(`  upserted ${Math.min(i + CHUNK, pageData.length)}/${pageData.length}\r`);
  }
  process.stdout.write("\n");

  console.log(`Done. ${pageData.length} Notion pages synced.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
