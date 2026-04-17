/**
 * Sync signal-heavy Gmail messages.
 *
 * Run with:
 *   npm run sync:gmail                  (defaults to last 3 years)
 *   npx tsx scripts/sync-gmail.ts --window 3d   (for incremental/cron runs)
 *
 * Filter strategy: at 100k+ emails, "everything" is mostly automated noise.
 * Instead we bias for signal by combining Gmail's own importance heuristic
 * with explicit sender/category filters, and include sent mail so our own
 * replies anchor relationship context.
 *
 * For each message we:
 *   - Create/update a `documents` row (title = subject, content = body + snippet)
 *   - Upsert every unique email in From/To/Cc into `contacts`
 *   - Link with `interactions` rows (kind = email_from | email_to | email_cc)
 */

import { config as loadEnv } from "dotenv";
loadEnv({ path: ".env.local" });

import { google, gmail_v1 } from "googleapis";
import { createServerClient } from "../lib/supabase";

// ---- Config --------------------------------------------------------
// CLI: --window <value>  (e.g. 3d, 7d, 90d, 3y). Default: 3y.
function parseWindow(): string {
  const i = process.argv.indexOf("--window");
  if (i >= 0 && process.argv[i + 1]) return process.argv[i + 1];
  return "3y";
}
const WINDOW = parseWindow();

// Signal filter: keep emails that Gmail flagged as important, OR that we sent.
// `is:important` uses Gmail's learned importance heuristic — strong signal that
// the thread matters. `from:me`/`in:sent` captures any conversation we engaged
// with, even if Gmail didn't flag it. Combined, this drops the 90%+ of an inbox
// that is automated notifications and newsletters slipping past category filters.
//
// Noise filter: common automated-sender patterns. `from:` matches substrings
// anywhere in the sender address, so `-from:noreply` kills noreply@*, *@noreply.*,
// etc. Bounce/postmaster addresses rarely carry useful content.
const QUERY = [
  `newer_than:${WINDOW}`,
  "(is:important OR from:me OR in:sent)",
  "-from:noreply",
  "-from:no-reply",
  "-from:notifications",
  "-from:notification",
  "-from:mailer-daemon",
  "-from:postmaster",
  "-in:chats",
  "-in:spam",
  "-in:trash",
  "-category:promotions",
  "-category:social",
  "-category:forums",
  "-category:updates",
].join(" ");
const MAX_BODY_CHARS = 10_000; // truncate long threads so prompts stay reasonable
const CONCURRENCY = 5;         // Gmail per-user quota is tight; 5 + retries stays under it
const MAX_RETRIES = 5;         // on 429/5xx, retry with exponential backoff
// --------------------------------------------------------------------

type Addr = { name?: string; email: string };

function getOAuthClient() {
  const { GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN } = process.env;
  if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET || !GOOGLE_REFRESH_TOKEN) {
    throw new Error("Missing Google OAuth env vars. Run `npm run auth:google` first.");
  }
  const oauth = new google.auth.OAuth2(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET);
  oauth.setCredentials({ refresh_token: GOOGLE_REFRESH_TOKEN });
  return oauth;
}

function header(
  headers: gmail_v1.Schema$MessagePartHeader[] | undefined,
  name: string,
): string | undefined {
  return headers?.find((h) => h.name?.toLowerCase() === name.toLowerCase())?.value ?? undefined;
}

/** Parse "Alice <a@b.com>, Bob <b@b.com>" into [{name:"Alice", email:"a@b.com"}, ...] */
function parseAddressList(raw?: string): Addr[] {
  if (!raw) return [];
  const out: Addr[] = [];
  // Simple regex — handles most real-world cases well enough.
  const re = /(?:"?([^"<]+?)"?\s*)?<([^>]+)>|([^\s,;]+@[^\s,;]+)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(raw)) !== null) {
    const email = (m[2] ?? m[3] ?? "").trim().toLowerCase();
    if (!email || !email.includes("@")) continue;
    const name = m[1]?.trim();
    out.push({ email, name: name || undefined });
  }
  return out;
}

/** Recursively walk MIME parts, return best plaintext body. */
function extractBody(payload: gmail_v1.Schema$MessagePart | undefined): string {
  if (!payload) return "";

  const walk = (part: gmail_v1.Schema$MessagePart): { plain: string; html: string } => {
    const mime = part.mimeType ?? "";
    const data = part.body?.data;
    let plain = "";
    let html = "";
    if (data) {
      const decoded = Buffer.from(data, "base64url").toString("utf-8");
      if (mime === "text/plain") plain += decoded;
      else if (mime === "text/html") html += decoded;
    }
    for (const child of part.parts ?? []) {
      const r = walk(child);
      plain += r.plain;
      html += r.html;
    }
    return { plain, html };
  };

  const { plain, html } = walk(payload);
  if (plain.trim()) return plain;
  if (html.trim()) return stripHtml(html);
  return "";
}

function stripHtml(html: string): string {
  return html
    .replace(/<style[\s\S]*?<\/style>/gi, "")
    .replace(/<script[\s\S]*?<\/script>/gi, "")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

/** Parallel map with fixed concurrency — so we don't hammer Gmail. */
async function mapLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>,
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

async function listMessageIds(gmail: gmail_v1.Gmail): Promise<string[]> {
  const ids: string[] = [];
  let pageToken: string | undefined;
  do {
    const res = await gmail.users.messages.list({
      userId: "me",
      q: QUERY,
      maxResults: 500,
      pageToken,
    });
    for (const m of res.data.messages ?? []) if (m.id) ids.push(m.id);
    pageToken = res.data.nextPageToken ?? undefined;
    process.stdout.write(`  listed ${ids.length} message ids…\r`);
  } while (pageToken);
  process.stdout.write("\n");
  return ids;
}

/**
 * Drop ids already in `documents` (source='gmail'). Keeps re-runs and cron
 * runs cheap — we only fetch bodies for truly new messages.
 *
 * Supabase .in() caps around a few thousand values per request, so we chunk
 * the lookup in batches of 500.
 */
async function filterUnsynced(
  supabase: ReturnType<typeof createServerClient>,
  ids: string[],
): Promise<string[]> {
  const have = new Set<string>();
  const CHUNK = 500;
  for (let i = 0; i < ids.length; i += CHUNK) {
    const slice = ids.slice(i, i + CHUNK);
    const { data, error } = await supabase
      .from("documents")
      .select("source_id")
      .eq("source", "gmail")
      .in("source_id", slice);
    if (error) throw error;
    for (const r of data ?? []) have.add(r.source_id as string);
  }
  return ids.filter((id) => !have.has(id));
}

type MessageDoc = {
  id: string;
  threadId: string;
  subject: string;
  date: string | null;
  snippet: string;
  body: string;
  from: Addr[];
  to: Addr[];
  cc: Addr[];
  labelIds: string[];
};

function isRateLimitError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  // googleapis surfaces 429s as "Quota exceeded" or "Rate Limit Exceeded" strings,
  // and 5xxs as "Backend Error"/"Internal error". All are worth retrying.
  return (
    /quota exceeded/i.test(msg) ||
    /rate limit/i.test(msg) ||
    /backend error/i.test(msg) ||
    /internal error/i.test(msg) ||
    / 429\b/.test(msg) ||
    / 5\d\d\b/.test(msg)
  );
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function fetchMessage(gmail: gmail_v1.Gmail, id: string): Promise<MessageDoc | null> {
  let attempt = 0;
  while (true) {
    try {
      const { data } = await gmail.users.messages.get({
        userId: "me",
        id,
        format: "full",
      });
      const headers = data.payload?.headers ?? [];
      const subject = header(headers, "Subject") ?? "(no subject)";
      const dateRaw = header(headers, "Date");
      const date = dateRaw ? new Date(dateRaw).toISOString() : null;
      const body = extractBody(data.payload).slice(0, MAX_BODY_CHARS);
      return {
        id: data.id!,
        threadId: data.threadId ?? "",
        subject,
        date,
        snippet: data.snippet ?? "",
        body,
        from: parseAddressList(header(headers, "From")),
        to: parseAddressList(header(headers, "To")),
        cc: parseAddressList(header(headers, "Cc")),
        labelIds: data.labelIds ?? [],
      };
    } catch (err) {
      if (isRateLimitError(err) && attempt < MAX_RETRIES) {
        // Exponential backoff with jitter: 1s, 2s, 4s, 8s, 16s (+ up to 1s jitter).
        const delay = 1000 * Math.pow(2, attempt) + Math.random() * 1000;
        await sleep(delay);
        attempt++;
        continue;
      }
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`\n  skip ${id}: ${msg}`);
      return null;
    }
  }
}

function toDocumentRow(m: MessageDoc) {
  const content = [m.snippet, m.body].filter(Boolean).join("\n\n").trim() || null;
  return {
    source: "gmail",
    source_id: m.id,
    title: m.subject,
    content,
    url: `https://mail.google.com/mail/u/0/#inbox/${m.id}`,
    occurred_at: m.date,
    metadata: {
      thread_id: m.threadId,
      from: m.from,
      to: m.to,
      cc: m.cc,
      label_ids: m.labelIds,
    },
  };
}

function collectPeople(msgs: MessageDoc[]): Map<string, { email: string; name?: string }> {
  const byEmail = new Map<string, { email: string; name?: string }>();
  const add = (a: Addr) => {
    const email = a.email.trim().toLowerCase();
    if (!email) return;
    const prev = byEmail.get(email);
    byEmail.set(email, { email, name: prev?.name || a.name });
  };
  for (const m of msgs) {
    m.from.forEach(add);
    m.to.forEach(add);
    m.cc.forEach(add);
  }
  return byEmail;
}

async function main() {
  const countOnly = process.argv.includes("--count");
  const gmail = google.gmail({ version: "v1", auth: getOAuthClient() });

  console.log(`Window: ${WINDOW}`);
  console.log(`Listing Gmail messages matching: "${QUERY}"…`);
  const allIds = await listMessageIds(gmail);
  if (countOnly) {
    console.log(`\nTotal matching messages: ${allIds.length}`);
    return;
  }
  const supabase = createServerClient();
  if (allIds.length === 0) {
    console.log("Nothing to sync.");
    return;
  }

  console.log(`Filtering out already-synced messages…`);
  const ids = await filterUnsynced(supabase, allIds);
  console.log(`  ${ids.length} new, ${allIds.length - ids.length} already synced.`);
  if (ids.length === 0) {
    console.log("Nothing new to fetch.");
    return;
  }
  console.log(`Fetching bodies for ${ids.length} messages…`);

  const msgs: MessageDoc[] = [];
  let progress = 0;
  await mapLimit(ids, CONCURRENCY, async (id) => {
    const m = await fetchMessage(gmail, id);
    if (m) msgs.push(m);
    progress++;
    if (progress % 25 === 0 || progress === ids.length) {
      process.stdout.write(`  fetched ${progress}/${ids.length}\r`);
    }
  });
  process.stdout.write("\n");

  if (msgs.length === 0) {
    console.log("Nothing to upsert.");
    return;
  }

  // 1) Upsert documents
  console.log("Upserting documents…");
  const docRows = msgs.map(toDocumentRow);
  const docIdBySourceId = new Map<string, number>();
  const CHUNK = 200;
  for (let i = 0; i < docRows.length; i += CHUNK) {
    const slice = docRows.slice(i, i + CHUNK);
    const { data, error } = await supabase
      .from("documents")
      .upsert(slice, { onConflict: "source,source_id" })
      .select("id, source_id");
    if (error) throw error;
    for (const r of data ?? []) docIdBySourceId.set(r.source_id as string, r.id as number);
    process.stdout.write(`  upserted ${Math.min(i + CHUNK, docRows.length)}/${docRows.length}\r`);
  }
  process.stdout.write("\n");

  // 2) Upsert contacts
  console.log("Upserting contacts…");
  const peopleMap = collectPeople(msgs);
  const contactRows = Array.from(peopleMap.values()).map((p) => ({
    email: p.email,
    name: p.name ?? null,
  }));
  const contactIdByEmail = new Map<string, number>();
  for (let i = 0; i < contactRows.length; i += CHUNK) {
    const slice = contactRows.slice(i, i + CHUNK);
    const { data, error } = await supabase
      .from("contacts")
      .upsert(slice, { onConflict: "email" })
      .select("id, email");
    if (error) throw error;
    for (const r of data ?? []) contactIdByEmail.set(r.email as string, r.id as number);
  }
  console.log(`  ${contactIdByEmail.size} contacts.`);

  // 3) Build interactions
  console.log("Building interactions…");
  const interactionRows: {
    contact_id: number;
    document_id: number;
    kind: string;
    occurred_at: string | null;
  }[] = [];
  for (const m of msgs) {
    const docId = docIdBySourceId.get(m.id);
    if (!docId) continue;
    const occurred = m.date;
    const seen = new Set<string>();
    const push = (kind: string, a: Addr) => {
      const email = a.email.trim().toLowerCase();
      const cid = contactIdByEmail.get(email);
      if (!cid) return;
      const key = `${cid}|${kind}`;
      if (seen.has(key)) return;
      seen.add(key);
      interactionRows.push({ contact_id: cid, document_id: docId, kind, occurred_at: occurred });
    };
    for (const a of m.from) push("email_from", a);
    for (const a of m.to) push("email_to", a);
    for (const a of m.cc) push("email_cc", a);
  }

  // Upsert in chunks with ignoreDuplicates
  for (let i = 0; i < interactionRows.length; i += CHUNK) {
    const slice = interactionRows.slice(i, i + CHUNK);
    const { error } = await supabase
      .from("interactions")
      .upsert(slice, {
        onConflict: "contact_id,document_id,kind",
        ignoreDuplicates: true,
      });
    if (error) throw error;
  }
  console.log(`  ${interactionRows.length} interaction rows.`);

  // 4) Update last_seen_at per contact using the latest email they appeared in
  const latestByContact = new Map<number, string>();
  for (const r of interactionRows) {
    if (!r.occurred_at) continue;
    const prev = latestByContact.get(r.contact_id);
    if (!prev || r.occurred_at > prev) latestByContact.set(r.contact_id, r.occurred_at);
  }
  for (const [cid, ts] of latestByContact) {
    const { error } = await supabase
      .from("contacts")
      .update({ last_seen_at: ts })
      .eq("id", cid)
      .or(`last_seen_at.is.null,last_seen_at.lt.${ts}`);
    if (error) console.warn(`last_seen_at update failed for ${cid}: ${error.message}`);
  }

  console.log("Done.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
