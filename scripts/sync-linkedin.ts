/**
 * Import a LinkedIn data export into documents + contacts.
 *
 * Usage:
 *   npm run sync:linkedin -- --folder ~/Downloads/Basic_LinkedInDataExport_04-17-2026
 *
 * How to get your LinkedIn export:
 *   linkedin.com/settings → Data privacy → Get a copy of your data →
 *   select Connections + Messages → Request archive.
 *   LinkedIn emails you a link within 24h. Unzip and point --folder at the result.
 *
 * What gets imported:
 *   Connections.csv → one document per connection (title = name + company + role)
 *                     + upserts email into contacts when LinkedIn provides it
 *   messages.csv   → one document per conversation thread
 *                     + upserts sender into contacts + interactions
 */

import { config as loadEnv } from "dotenv";
loadEnv({ path: ".env.local" });

import fs from "fs";
import path from "path";
import { createServerClient } from "../lib/supabase";

// ---- Config --------------------------------------------------------
function getFolder(): string {
  const i = process.argv.indexOf("--folder");
  if (i >= 0 && process.argv[i + 1]) {
    return process.argv[i + 1].replace(/^~/, process.env.HOME ?? "");
  }
  throw new Error("Pass --folder /path/to/linkedin-export-directory");
}
// --------------------------------------------------------------------

// ---- CSV parser (handles quoted fields + embedded newlines) --------
function parseCSV(raw: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuote = false;
  const text = raw.replace(/\r\n/g, "\n").replace(/\r/g, "\n");

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (inQuote) {
      if (ch === '"' && text[i + 1] === '"') {
        field += '"';
        i++;
      } else if (ch === '"') {
        inQuote = false;
      } else {
        field += ch;
      }
    } else {
      if (ch === '"') {
        inQuote = true;
      } else if (ch === ",") {
        row.push(field);
        field = "";
      } else if (ch === "\n") {
        row.push(field);
        field = "";
        if (row.some((f) => f.trim())) rows.push(row);
        row = [];
      } else {
        field += ch;
      }
    }
  }
  if (field || row.length > 0) {
    row.push(field);
    if (row.some((f) => f.trim())) rows.push(row);
  }
  return rows;
}

function csvToObjects(rows: string[][]): Record<string, string>[] {
  if (rows.length < 2) return [];
  const headers = rows[0].map((h) => h.trim());
  return rows.slice(1).map((row) => {
    const obj: Record<string, string> = {};
    headers.forEach((h, i) => {
      obj[h] = (row[i] ?? "").trim();
    });
    return obj;
  });
}

function readCSV(folder: string, filename: string): Record<string, string>[] | null {
  // LinkedIn sometimes capitalises differently across exports
  const candidates = [filename, filename.toLowerCase(), filename.toUpperCase()];
  for (const name of candidates) {
    const p = path.join(folder, name);
    if (fs.existsSync(p)) {
      const raw = fs.readFileSync(p, "utf-8");
      // LinkedIn exports often have a "Notes:" preamble before the real header row.
      // Find the header by looking for a line that is unquoted and starts with a
      // known column name — much more reliable than "first line with a comma".
      const lines = raw.split(/\r?\n/);
      const headerIdx = lines.findIndex(
        (l) =>
          !l.startsWith('"') &&
          (l.startsWith("First Name") ||
            l.startsWith("CONVERSATION ID") ||
            l.startsWith("From,") ||
            l.startsWith("Date,")),
      );
      const cleaned = lines.slice(headerIdx >= 0 ? headerIdx : 0).join("\n");
      return csvToObjects(parseCSV(cleaned));
    }
  }
  return null;
}

// ---- Connections ---------------------------------------------------
async function syncConnections(
  folder: string,
  supabase: ReturnType<typeof createServerClient>,
) {
  const rows = readCSV(folder, "Connections.csv");
  if (!rows || rows.length === 0) {
    console.log("  Connections.csv not found or empty — skipping.");
    return;
  }
  console.log(`  ${rows.length} connections found.`);

  const CHUNK = 200;
  let docCount = 0;
  let contactCount = 0;

  for (let i = 0; i < rows.length; i += CHUNK) {
    const slice = rows.slice(i, i + CHUNK);

    // Build document rows — one per connection, title = "Name — Role at Company"
    const docRows = slice.map((r) => {
      const firstName = r["First Name"] ?? r["FirstName"] ?? "";
      const lastName = r["Last Name"] ?? r["LastName"] ?? "";
      const name = `${firstName} ${lastName}`.trim();
      const company = r["Company"] ?? r["company"] ?? "";
      const position = r["Position"] ?? r["position"] ?? "";
      const email = (r["Email Address"] ?? r["Email"] ?? "").toLowerCase() || null;
      const url = r["URL"] ?? r["LinkedIn URL"] ?? null;
      const connectedOn = r["Connected On"] ?? null;

      const titleParts = [name];
      if (position) titleParts.push(position);
      if (company) titleParts.push(`at ${company}`);
      const title = titleParts.join(" — ");

      const contentParts: string[] = [];
      if (name) contentParts.push(`Name: ${name}`);
      if (position) contentParts.push(`Role: ${position}`);
      if (company) contentParts.push(`Company: ${company}`);
      if (email) contentParts.push(`Email: ${email}`);
      if (connectedOn) contentParts.push(`Connected: ${connectedOn}`);

      return {
        source: "linkedin_connection" as const,
        source_id: url ?? `li:${name.toLowerCase().replace(/\s+/g, "-")}`,
        title,
        content: contentParts.join("\n"),
        url,
        occurred_at: connectedOn ? new Date(connectedOn).toISOString() : null,
        metadata: { name, company, position, email },
      };
    });

    // Deduplicate within the batch — two connections can share a URL or
    // generate the same synthetic source_id, which causes Postgres to
    // complain about updating the same row twice in one command.
    const uniqueDocs = Array.from(
      new Map(docRows.map((r) => [r.source_id, r])).values(),
    );
    const { error: docErr } = await supabase
      .from("documents")
      .upsert(uniqueDocs, { onConflict: "source,source_id" });
    if (docErr) throw docErr;
    docCount += uniqueDocs.length;

    // Upsert contacts for connections that have an email address
    const contactRows = slice
      .map((r) => {
        const firstName = r["First Name"] ?? r["FirstName"] ?? "";
        const lastName = r["Last Name"] ?? r["LastName"] ?? "";
        const name = `${firstName} ${lastName}`.trim();
        const email = (r["Email Address"] ?? r["Email"] ?? "").toLowerCase();
        return email ? { email, name: name || null } : null;
      })
      .filter(Boolean) as { email: string; name: string | null }[];

    if (contactRows.length > 0) {
      const { error: cErr } = await supabase
        .from("contacts")
        .upsert(contactRows, { onConflict: "email" });
      if (cErr) throw cErr;
      contactCount += contactRows.length;
    }

    process.stdout.write(`  processed ${Math.min(i + CHUNK, rows.length)}/${rows.length} connections\r`);
  }
  process.stdout.write("\n");
  console.log(`  ${docCount} connection documents, ${contactCount} contacts with emails.`);
}

// ---- Messages ------------------------------------------------------
async function syncMessages(
  folder: string,
  supabase: ReturnType<typeof createServerClient>,
) {
  const rows = readCSV(folder, "messages.csv");
  if (!rows || rows.length === 0) {
    console.log("  messages.csv not found or empty — skipping.");
    return;
  }
  console.log(`  ${rows.length} message rows found.`);

  // Group by conversation ID → one document per thread
  const threads = new Map<
    string,
    { title: string; lines: { from: string; date: string; content: string }[] }
  >();

  for (const r of rows) {
    const convId =
      r["CONVERSATION ID"] ?? r["Conversation ID"] ?? r["conversation_id"] ?? "";
    const title =
      r["CONVERSATION TITLE"] ?? r["Conversation Title"] ?? r["conversation_title"] ?? "LinkedIn Message";
    const from = r["FROM"] ?? r["From"] ?? r["from"] ?? "";
    const date = r["DATE"] ?? r["Date"] ?? r["date"] ?? "";
    const content = r["CONTENT"] ?? r["Content"] ?? r["content"] ?? "";

    if (!convId || !content.trim()) continue;

    if (!threads.has(convId)) threads.set(convId, { title, lines: [] });
    threads.get(convId)!.lines.push({ from, date, content });
  }

  console.log(`  ${threads.size} conversation threads.`);
  if (threads.size === 0) return;

  const CHUNK = 100;
  const threadEntries = Array.from(threads.entries());

  for (let i = 0; i < threadEntries.length; i += CHUNK) {
    const slice = threadEntries.slice(i, i + CHUNK);
    const docRows = slice.map(([convId, thread]) => {
      const sorted = thread.lines.sort((a, b) => a.date.localeCompare(b.date));
      const content = sorted
        .map((l) => `[${l.date}] ${l.from}: ${l.content}`)
        .join("\n\n");
      const firstDate = sorted[0]?.date ?? null;
      return {
        source: "linkedin_message" as const,
        source_id: convId,
        title: thread.title,
        content: content.slice(0, 20_000),
        url: null,
        occurred_at: firstDate ? new Date(firstDate).toISOString() : null,
        metadata: { message_count: sorted.length },
      };
    });

    const { error } = await supabase
      .from("documents")
      .upsert(docRows, { onConflict: "source,source_id" });
    if (error) throw error;
    process.stdout.write(`  upserted ${Math.min(i + CHUNK, threadEntries.length)}/${threadEntries.length} threads\r`);
  }
  process.stdout.write("\n");
  console.log(`  ${threadEntries.length} message threads synced.`);
}

// ---- Main ----------------------------------------------------------
async function main() {
  const folder = getFolder();
  if (!fs.existsSync(folder)) {
    throw new Error(`Folder not found: ${folder}`);
  }
  console.log(`LinkedIn export folder: ${folder}`);
  console.log(`Files: ${fs.readdirSync(folder).join(", ")}`);

  const supabase = createServerClient();

  console.log("\nSyncing connections…");
  await syncConnections(folder, supabase);

  console.log("\nSyncing messages…");
  await syncMessages(folder, supabase);

  console.log("\nDone.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
