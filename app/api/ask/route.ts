import { NextRequest, NextResponse } from "next/server";
import { createServerClient } from "@/lib/supabase";
import { getAnthropic, DEFAULT_MODEL } from "@/lib/anthropic";

// Cap how much we send to Claude — keeps latency + cost bounded.
const MAX_DOCS = 30;
const MAX_CHARS_PER_DOC = 2000;

type DocRow = {
  id: number;
  source: string;
  title: string | null;
  content: string | null;
  url: string | null;
  occurred_at: string | null;
  metadata: Record<string, unknown> | null;
};

type ContactRow = {
  id: number;
  name: string | null;
  email: string | null;
  last_seen_at: string | null;
};

type InteractionRow = {
  contact_id: number;
  document_id: number;
  kind: string;
  contacts: ContactRow | ContactRow[] | null;
};

function truncate(s: string | null, n: number): string {
  if (!s) return "";
  return s.length > n ? s.slice(0, n) + "…" : s;
}

function formatDocForPrompt(doc: DocRow, idx: number, people: string[]): string {
  const lines: string[] = [];
  lines.push(`[#${idx + 1}] source=${doc.source} id=${doc.id}`);
  if (doc.title) lines.push(`title: ${doc.title}`);
  if (doc.occurred_at) lines.push(`when: ${doc.occurred_at}`);
  if (doc.url) lines.push(`url: ${doc.url}`);
  if (people.length) lines.push(`people: ${people.join(", ")}`);
  if (doc.content) lines.push(`content:\n${truncate(doc.content, MAX_CHARS_PER_DOC)}`);
  return lines.join("\n");
}

export async function POST(req: NextRequest) {
  try {
    const { question } = (await req.json()) as { question?: string };
    if (!question || !question.trim()) {
      return NextResponse.json({ error: "Missing question" }, { status: 400 });
    }

    const supabase = createServerClient();

    // 1) Full-text search over documents.
    const { data: docs, error: docErr } = await supabase
      .from("documents")
      .select("id, source, title, content, url, occurred_at, metadata")
      .textSearch("fts", question, { type: "websearch", config: "english" })
      .limit(MAX_DOCS);
    if (docErr) throw docErr;

    const docRows = (docs ?? []) as DocRow[];

    // 2) For matched docs, pull linked contacts so we can show "who was in this".
    let peopleByDoc = new Map<number, string[]>();
    if (docRows.length) {
      const docIds = docRows.map((d) => d.id);
      const { data: ix, error: ixErr } = await supabase
        .from("interactions")
        .select("contact_id, document_id, kind, contacts(id, name, email, last_seen_at)")
        .in("document_id", docIds);
      if (ixErr) throw ixErr;

      for (const row of (ix ?? []) as InteractionRow[]) {
        const c = Array.isArray(row.contacts) ? row.contacts[0] : row.contacts;
        if (!c) continue;
        const label = c.name ? `${c.name}${c.email ? ` <${c.email}>` : ""}` : c.email;
        if (!label) continue;
        const arr = peopleByDoc.get(row.document_id) ?? [];
        if (!arr.includes(label)) arr.push(label);
        peopleByDoc.set(row.document_id, arr);
      }
    }

    if (docRows.length === 0) {
      return NextResponse.json({
        answer:
          "I couldn't find anything in your brain that matches that question. Try different keywords — full-text search needs overlapping terms.",
        sources: [],
      });
    }

    // 3) Build prompt and call Claude.
    const formattedDocs = docRows
      .map((d, i) => formatDocForPrompt(d, i, peopleByDoc.get(d.id) ?? []))
      .join("\n\n---\n\n");

    const system = `You are Rags's personal "second brain". You answer questions using ONLY the context provided below, which is drawn from Rags's Readwise highlights, Granola meeting notes, and other personal sources.

Rules:
- Ground every claim in the provided context. Cite sources inline as [#N] matching the document number.
- If the context is insufficient, say so plainly — do not speculate or use general knowledge.
- Be concise. Prefer bullet points for lists of people or items.
- When the question is about people ("who should I reach out to", "who have I met"), surface names with emails where available, and explain why each person is relevant.
- When synthesizing ideas from multiple docs, cluster related points and note where they reinforce or differ.
- Use Rags's own voice/words from highlights where illustrative.`;

    const user = `Question: ${question}

---

Context (${docRows.length} documents):

${formattedDocs}`;

    const anthropic = getAnthropic();
    const response = await anthropic.messages.create({
      model: DEFAULT_MODEL,
      max_tokens: 2048,
      system,
      messages: [{ role: "user", content: user }],
    });

    const text = response.content
      .filter((b) => b.type === "text")
      .map((b) => (b as { text: string }).text)
      .join("\n");

    const sources = docRows.map((d, i) => ({
      n: i + 1,
      id: d.id,
      source: d.source,
      title: d.title,
      url: d.url,
      occurred_at: d.occurred_at,
      people: peopleByDoc.get(d.id) ?? [],
    }));

    return NextResponse.json({ answer: text, sources });
  } catch (err) {
    console.error(err);
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}
