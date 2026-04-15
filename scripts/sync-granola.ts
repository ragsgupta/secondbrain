/**
 * Sync Granola meeting notes into `documents`, and extract attendees
 * into `contacts` + `interactions`.
 *
 * Run with:  npm run sync:granola
 *
 * Idempotent: re-runs update rows in place via upsert.
 */

import { config as loadEnv } from "dotenv";
loadEnv({ path: ".env.local" });

import { createServerClient } from "../lib/supabase";

const BASE = "https://public-api.granola.ai/v1";

type Person = { name?: string; email?: string };

type TranscriptEntry = {
  speaker?: { source?: string; diarization_label?: string; name?: string };
  text?: string;
  start_time?: string;
  end_time?: string;
};

type NoteSummary = {
  id: string;
  object?: string;
  title?: string;
  owner?: Person;
  created_at?: string;
  updated_at?: string;
  calendar_event?: Record<string, unknown> | null;
  attendees?: Person[];
  folder_membership?: unknown[];
  summary_text?: string;
  summary_markdown?: string;
  [k: string]: unknown;
};

type NoteFull = NoteSummary & { transcript?: TranscriptEntry[] };

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function granolaGet<T>(path: string, token: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) {
    throw new Error(`Granola ${res.status} on ${path}: ${await res.text()}`);
  }
  return (await res.json()) as T;
}

async function listAllNotes(token: string): Promise<NoteSummary[]> {
  const notes: NoteSummary[] = [];
  let cursor: string | undefined;
  do {
    const qs = cursor ? `?cursor=${encodeURIComponent(cursor)}` : "";
    const data = await granolaGet<{
      notes: NoteSummary[];
      hasMore: boolean;
      cursor?: string;
    }>(`/notes${qs}`, token);
    notes.push(...(data.notes ?? []));
    cursor = data.hasMore ? data.cursor : undefined;
    process.stdout.write(`  listed ${notes.length} notes…\r`);
    if (cursor) await sleep(250);
  } while (cursor);
  process.stdout.write("\n");
  return notes;
}

function formatTranscript(entries: TranscriptEntry[] | undefined): string {
  if (!entries?.length) return "";
  return entries
    .map((e) => {
      const who =
        e.speaker?.name ||
        e.speaker?.diarization_label ||
        e.speaker?.source ||
        "Speaker";
      return `${who}: ${e.text ?? ""}`;
    })
    .join("\n");
}

function buildContent(note: NoteFull): string | null {
  const summary = note.summary_markdown || note.summary_text || "";
  const transcript = formatTranscript(note.transcript);
  const parts = [
    summary,
    transcript && `\n\n--- Transcript ---\n${transcript}`,
  ].filter(Boolean);
  return parts.length ? parts.join("") : null;
}

function toDocumentRow(note: NoteFull) {
  return {
    source: "granola",
    source_id: note.id,
    title: note.title ?? "(untitled meeting)",
    content: buildContent(note),
    url: null as string | null,
    occurred_at: note.created_at ?? null,
    metadata: {
      owner: note.owner,
      attendees: note.attendees ?? [],
      calendar_event: note.calendar_event ?? null,
      folder_membership: note.folder_membership ?? [],
      updated_at: note.updated_at,
    },
  };
}

function normalizeEmail(e?: string): string | null {
  if (!e) return null;
  const trimmed = e.trim().toLowerCase();
  return trimmed || null;
}

/**
 * Collect unique people across all notes so we can upsert contacts
 * in a single batch and get a stable email->id map for interactions.
 */
function collectPeople(notes: NoteFull[]): Map<string, { name?: string; email: string }> {
  const byEmail = new Map<string, { name?: string; email: string }>();
  const add = (p?: Person) => {
    const email = normalizeEmail(p?.email);
    if (!email) return;
    const existing = byEmail.get(email);
    // Keep first non-empty name we see.
    const name = existing?.name || p?.name?.trim() || undefined;
    byEmail.set(email, { email, name });
  };
  for (const n of notes) {
    add(n.owner);
    for (const a of n.attendees ?? []) add(a);
  }
  return byEmail;
}

async function main() {
  const token = process.env.GRANOLA_API_KEY;
  if (!token) throw new Error("Missing GRANOLA_API_KEY in .env.local");
  const supabase = createServerClient();

  console.log("Listing Granola notes…");
  const summaries = await listAllNotes(token);
  console.log(`Found ${summaries.length} notes. Fetching full bodies…`);

  const fulls: NoteFull[] = [];
  for (let i = 0; i < summaries.length; i++) {
    const s = summaries[i];
    try {
      const full = await granolaGet<NoteFull>(
        `/notes/${encodeURIComponent(s.id)}?include=transcript`,
        token,
      );
      fulls.push(full);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes("404")) console.warn(`skip ${s.id}: ${msg}`);
    }
    process.stdout.write(`  fetched ${i + 1}/${summaries.length}\r`);
    await sleep(220);
  }
  process.stdout.write("\n");

  if (fulls.length === 0) {
    console.log("Nothing to upsert.");
    return;
  }

  // 1) Upsert documents, get id -> source_id mapping back.
  console.log("Upserting documents…");
  const docRows = fulls.map(toDocumentRow);
  const { data: docData, error: docErr } = await supabase
    .from("documents")
    .upsert(docRows, { onConflict: "source,source_id" })
    .select("id, source_id");
  if (docErr) throw docErr;
  const docIdBySourceId = new Map(docData!.map((r) => [r.source_id, r.id as number]));
  console.log(`  ${docData!.length} document rows.`);

  // 2) Upsert contacts, get id -> email mapping back.
  console.log("Upserting contacts…");
  const peopleMap = collectPeople(fulls);
  const contactRows = Array.from(peopleMap.values()).map((p) => ({
    email: p.email,
    name: p.name ?? null,
  }));
  if (contactRows.length === 0) {
    console.log("  no contacts found, done.");
    return;
  }
  const { data: contactData, error: contactErr } = await supabase
    .from("contacts")
    .upsert(contactRows, { onConflict: "email" })
    .select("id, email");
  if (contactErr) throw contactErr;
  const contactIdByEmail = new Map(contactData!.map((r) => [r.email as string, r.id as number]));
  console.log(`  ${contactData!.length} contact rows.`);

  // 3) Build interactions: one row per (person, meeting, role).
  console.log("Building interactions…");
  const interactionRows: {
    contact_id: number;
    document_id: number;
    kind: string;
    occurred_at: string | null;
  }[] = [];

  for (const note of fulls) {
    const docId = docIdBySourceId.get(note.id);
    if (!docId) continue;
    const occurred = note.created_at ?? null;

    const seen = new Set<string>(); // dedupe per-note by email+kind
    const push = (kind: string, p?: Person) => {
      const email = normalizeEmail(p?.email);
      if (!email) return;
      const cid = contactIdByEmail.get(email);
      if (!cid) return;
      const key = `${cid}|${kind}`;
      if (seen.has(key)) return;
      seen.add(key);
      interactionRows.push({ contact_id: cid, document_id: docId, kind, occurred_at: occurred });
    };

    push("meeting_owner", note.owner);
    for (const a of note.attendees ?? []) push("meeting_attendee", a);
  }

  if (interactionRows.length > 0) {
    const { error: ixErr } = await supabase
      .from("interactions")
      .upsert(interactionRows, {
        onConflict: "contact_id,document_id,kind",
        ignoreDuplicates: true,
      });
    if (ixErr) throw ixErr;
  }
  console.log(`  ${interactionRows.length} interaction rows.`);

  // 4) Nudge contacts.last_seen_at using the max meeting time we linked them to.
  const latestByContact = new Map<number, string>();
  for (const row of interactionRows) {
    if (!row.occurred_at) continue;
    const prev = latestByContact.get(row.contact_id);
    if (!prev || row.occurred_at > prev) latestByContact.set(row.contact_id, row.occurred_at);
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
