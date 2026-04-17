/**
 * Sync Google Calendar events into documents + contacts + interactions.
 *
 * Run with:  npm run sync:calendar
 *            npx tsx scripts/sync-calendar.ts --window 3d   (incremental)
 *
 * For each event we:
 *   - Create/update a `documents` row (title = event summary, content = description + attendees)
 *   - Upsert every attendee's email into `contacts`
 *   - Link attendees via `interactions` (kind = calendar_attendee)
 *
 * Filters:
 *   - Skips cancelled events
 *   - Skips events the user declined
 *   - Skips events with no title
 *   - Includes single-instance expansions of recurring events (so "weekly standup"
 *     becomes individual dated entries rather than one unanchored template)
 */

import { config as loadEnv } from "dotenv";
loadEnv({ path: ".env.local" });

import { google, calendar_v3 } from "googleapis";
import { createServerClient } from "../lib/supabase";

// ---- Config --------------------------------------------------------
function parseWindow(): string {
  const i = process.argv.indexOf("--window");
  if (i >= 0 && process.argv[i + 1]) return process.argv[i + 1];
  return "3y";
}

function windowToDate(w: string): Date {
  const now = new Date();
  const m = w.match(/^(\d+)([dwmy])$/);
  if (!m) throw new Error(`Invalid --window: "${w}". Expected format like 3d, 2w, 6m, 3y.`);
  const n = parseInt(m[1], 10);
  const unit = m[2];
  const d = new Date(now);
  if (unit === "d") d.setDate(d.getDate() - n);
  else if (unit === "w") d.setDate(d.getDate() - n * 7);
  else if (unit === "m") d.setMonth(d.getMonth() - n);
  else if (unit === "y") d.setFullYear(d.getFullYear() - n);
  return d;
}

const WINDOW = parseWindow();
const TIME_MIN = windowToDate(WINDOW);
// --------------------------------------------------------------------

type Addr = { email: string; name?: string; self: boolean; responseStatus?: string };

function getOAuthClient() {
  const { GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN } = process.env;
  if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET || !GOOGLE_REFRESH_TOKEN) {
    throw new Error("Missing Google OAuth env vars. Run `npm run auth:google` first.");
  }
  const oauth = new google.auth.OAuth2(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET);
  oauth.setCredentials({ refresh_token: GOOGLE_REFRESH_TOKEN });
  return oauth;
}

function parseAttendees(event: calendar_v3.Schema$Event): Addr[] {
  return (event.attendees ?? [])
    .filter((a) => a.email && !a.resource) // skip room/resource attendees
    .map((a) => ({
      email: a.email!.trim().toLowerCase(),
      name: a.displayName?.trim() || undefined,
      self: a.self ?? false,
      responseStatus: a.responseStatus ?? undefined,
    }));
}

function selfStatus(event: calendar_v3.Schema$Event): string | null {
  return (event.attendees ?? []).find((a) => a.self)?.responseStatus ?? null;
}

function buildContent(event: calendar_v3.Schema$Event, attendees: Addr[]): string | null {
  const parts: string[] = [];
  const desc = event.description?.trim();
  if (desc) parts.push(desc);
  const others = attendees.filter((a) => !a.self);
  if (others.length > 0) {
    const names = others
      .map((a) => (a.name ? `${a.name} <${a.email}>` : a.email))
      .join(", ");
    parts.push(`Attendees: ${names}`);
  }
  if (event.location?.trim()) parts.push(`Location: ${event.location.trim()}`);
  return parts.length ? parts.join("\n\n") : null;
}

function eventStartIso(event: calendar_v3.Schema$Event): string | null {
  const s = event.start;
  if (!s) return null;
  return s.dateTime ?? (s.date ? new Date(s.date).toISOString() : null);
}

async function listEvents(
  cal: calendar_v3.Calendar,
): Promise<calendar_v3.Schema$Event[]> {
  const all: calendar_v3.Schema$Event[] = [];
  let pageToken: string | undefined;
  do {
    const res = await cal.events.list({
      calendarId: "primary",
      timeMin: TIME_MIN.toISOString(),
      timeMax: new Date().toISOString(),
      singleEvents: true,   // expand recurrences into individual dated events
      orderBy: "startTime",
      maxResults: 2500,
      pageToken,
    });
    for (const e of res.data.items ?? []) all.push(e);
    pageToken = res.data.nextPageToken ?? undefined;
    process.stdout.write(`  listed ${all.length} events…\r`);
  } while (pageToken);
  process.stdout.write("\n");
  return all;
}

function shouldSkip(event: calendar_v3.Schema$Event): string | null {
  if (event.status === "cancelled") return "cancelled";
  if (!event.summary?.trim()) return "no title";
  if (selfStatus(event) === "declined") return "declined";
  return null;
}

async function main() {
  const cal = google.calendar({ version: "v3", auth: getOAuthClient() });
  const supabase = createServerClient();

  console.log(`Window: ${WINDOW} (from ${TIME_MIN.toISOString().slice(0, 10)})`);
  console.log("Listing calendar events…");
  const rawEvents = await listEvents(cal);
  console.log(`  ${rawEvents.length} raw events fetched.`);

  // Filter
  const events: calendar_v3.Schema$Event[] = [];
  let skipped = 0;
  for (const e of rawEvents) {
    const reason = shouldSkip(e);
    if (reason) { skipped++; continue; }
    events.push(e);
  }
  console.log(`  ${events.length} kept, ${skipped} skipped (cancelled / declined / no title).`);

  if (events.length === 0) {
    console.log("Nothing to upsert.");
    return;
  }

  // 1) Upsert documents
  console.log("Upserting documents…");
  const CHUNK = 200;
  const docIdBySourceId = new Map<string, number>();

  for (let i = 0; i < events.length; i += CHUNK) {
    const slice = events.slice(i, i + CHUNK);
    const rows = slice.map((e) => {
      const attendees = parseAttendees(e);
      return {
        source: "google_calendar" as const,
        source_id: e.id!,
        title: e.summary!,
        content: buildContent(e, attendees),
        url: e.htmlLink ?? null,
        occurred_at: eventStartIso(e),
        metadata: {
          calendar_id: "primary",
          status: e.status,
          location: e.location ?? null,
          all_day: Boolean(e.start?.date && !e.start?.dateTime),
          organizer: e.organizer?.email ?? null,
          self_response: selfStatus(e),
          attendee_count: (e.attendees ?? []).filter((a) => !a.resource).length,
        },
      };
    });
    const { data, error } = await supabase
      .from("documents")
      .upsert(rows, { onConflict: "source,source_id" })
      .select("id, source_id");
    if (error) throw error;
    for (const r of data ?? []) docIdBySourceId.set(r.source_id as string, r.id as number);
    process.stdout.write(`  upserted ${Math.min(i + CHUNK, events.length)}/${events.length}\r`);
  }
  process.stdout.write("\n");

  // 2) Upsert contacts (non-self attendees only)
  console.log("Upserting contacts…");
  const byEmail = new Map<string, { email: string; name?: string }>();
  for (const e of events) {
    for (const a of parseAttendees(e)) {
      if (a.self) continue;
      const prev = byEmail.get(a.email);
      byEmail.set(a.email, { email: a.email, name: prev?.name || a.name });
    }
  }
  const contactRows = Array.from(byEmail.values()).map((c) => ({
    email: c.email,
    name: c.name ?? null,
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

  for (const e of events) {
    const docId = docIdBySourceId.get(e.id!);
    if (!docId) continue;
    const occurred = eventStartIso(e);
    const seen = new Set<number>();
    for (const a of parseAttendees(e)) {
      if (a.self) continue;
      const cid = contactIdByEmail.get(a.email);
      if (!cid || seen.has(cid)) continue;
      seen.add(cid);
      interactionRows.push({
        contact_id: cid,
        document_id: docId,
        kind: "calendar_attendee",
        occurred_at: occurred,
      });
    }
  }

  for (let i = 0; i < interactionRows.length; i += CHUNK) {
    const slice = interactionRows.slice(i, i + CHUNK);
    const { error } = await supabase
      .from("interactions")
      .upsert(slice, { onConflict: "contact_id,document_id,kind", ignoreDuplicates: true });
    if (error) throw error;
  }
  console.log(`  ${interactionRows.length} interaction rows.`);

  // 4) Update last_seen_at per contact
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
    if (error) console.warn(`last_seen_at update failed for contact ${cid}: ${error.message}`);
  }

  console.log("Done.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
