import { NextRequest, NextResponse } from "next/server";
import { createServerClient } from "@/lib/supabase";
import { getAnthropic, DEFAULT_MODEL } from "@/lib/anthropic";
import { embed } from "@/lib/voyage";

const MAX_DOCS = 60;
const FTS_PER_QUERY = 12;
const VECTOR_LIMIT = 60;
const MAX_CHARS_PER_DOC = 2000;

type DocRow = {
  id: number;
  source: string;
  title: string | null;
  content: string | null;
  url: string | null;
  occurred_at: string | null;
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

/**
 * One Haiku call that returns BOTH keyword rewrites (for FTS) AND a
 * hypothetical matching document (for HyDE-style vector search).
 */
async function expandQuery(
  question: string,
): Promise<{ keywords: string[]; hypothetical: string; linkedinTerms: string[] }> {
  const anthropic = getAnthropic();
  const res = await anthropic.messages.create({
    model: "claude-haiku-4-5-20251001",
    max_tokens: 700,
    system: `You expand a user's question into retrieval helpers for a personal knowledge base (emails, notes, meeting transcripts, article summaries, and LinkedIn connections).

Output EXACTLY this JSON and nothing else — no preamble, no code fences:
{
  "keywords": ["3-5 short search queries with synonyms and common phrasings real documents would use"],
  "hypothetical": "2-4 sentences written in the voice of someone writing a RELEVANT DOCUMENT (e.g., an email TO the user, or a meeting transcript). Not the question's words — the kind of phrasing an actual matching doc would contain. This string will be embedded for semantic search.",
  "linkedin_terms": ["5-10 role titles, job functions, or industry sectors that a RELEVANT BUYER/CONTACT would have in their LinkedIn profile — think about who would purchase or care about this, not the product domain itself. Use short fragments that would appear verbatim in a job title or company name."]
}

Example for "who should I reach out to about a drone-based warehouse security product?":
{
  "keywords": ["warehouse security automation", "drone surveillance operations", "physical security enterprise"],
  "hypothetical": "Hey Rags, we've been evaluating new security approaches for our distribution network and the autonomous surveillance angle is really interesting. Would love to see a demo. Our team handles about 40 facilities across the midwest.",
  "linkedin_terms": ["VP Operations", "Head of Security", "Director Facilities", "Chief Security Officer", "VP Supply Chain", "Director Loss Prevention", "Head of Logistics", "VP Real Estate", "SVP Operations", "warehousing", "distribution"]
}`,
    messages: [{ role: "user", content: question }],
  });
  const text = res.content
    .filter((b) => b.type === "text")
    .map((b) => (b as { text: string }).text)
    .join("");
  try {
    // Strip possible code fences just in case.
    const cleaned = text.replace(/^```(?:json)?\s*|\s*```$/g, "").trim();
    const parsed = JSON.parse(cleaned) as {
      keywords?: unknown;
      hypothetical?: unknown;
      linkedin_terms?: unknown;
    };
    const keywords = Array.isArray(parsed.keywords)
      ? (parsed.keywords as unknown[])
          .filter((k): k is string => typeof k === "string" && k.trim().length > 2)
          .slice(0, 5)
      : [];
    const hypothetical = typeof parsed.hypothetical === "string" ? parsed.hypothetical : "";
    const linkedinTerms = Array.isArray(parsed.linkedin_terms)
      ? (parsed.linkedin_terms as unknown[])
          .filter((k): k is string => typeof k === "string" && k.trim().length > 2)
          .slice(0, 10)
      : [];
    return { keywords, hypothetical, linkedinTerms };
  } catch {
    return { keywords: [], hypothetical: "", linkedinTerms: [] };
  }
}

export async function POST(req: NextRequest) {
  try {
    const { question } = (await req.json()) as { question?: string };
    if (!question || !question.trim()) {
      return NextResponse.json({ error: "Missing question" }, { status: 400 });
    }

    const supabase = createServerClient();

    // --- Retrieval: hybrid (HyDE + query embedding + FTS across rewrites) ---
    const expansion = await expandQuery(question).catch(() => ({
      keywords: [] as string[],
      hypothetical: "",
      linkedinTerms: [] as string[],
    }));
    const { keywords: rewrites, hypothetical, linkedinTerms } = expansion;

    // Embed both the raw question (as query) AND the hypothetical doc (as document).
    // Searching with the document-type embedding of a fake matching doc lands us
    // in the right semantic neighborhood — the HyDE trick.
    const embedInputs = [question];
    if (hypothetical) embedInputs.push(hypothetical);
    const embedTypes: ("query" | "document")[] = hypothetical
      ? ["query", "document"]
      : ["query"];
    // Voyage's API takes a single input_type per call — so two calls in parallel.
    const [queryVecArr, hypoVecArr] = await Promise.all([
      embed([question], "query"),
      hypothetical ? embed([hypothetical], "document") : Promise.resolve([[]]),
    ]);
    const queryVec = queryVecArr[0];
    const hypoVec = hypothetical ? hypoVecArr[0] : null;
    void embedInputs;
    void embedTypes;

    // FTS across question + keyword rewrites
    const searchQueries = [question, ...rewrites];
    const idsFromFts = new Set<number>();
    // LinkedIn-specific FTS: search connection docs by buyer-persona role/industry terms.
    // Runs in parallel with the main FTS — sparse connection docs compete poorly in
    // general semantic search, so they get their own dedicated pass here.
    const idsFromLinkedin = new Set<number>();
    const LINKEDIN_PER_TERM = 20;

    await Promise.all([
      ...searchQueries.map(async (q) => {
        const { data, error } = await supabase
          .from("documents")
          .select("id")
          .textSearch("fts", q, { type: "websearch", config: "english" })
          .limit(FTS_PER_QUERY);
        if (!error && data) for (const r of data) idsFromFts.add(r.id as number);
      }),
      ...linkedinTerms.map(async (term) => {
        const { data, error } = await supabase
          .from("documents")
          .select("id")
          .eq("source", "linkedin_connection")
          .textSearch("fts", term, { type: "websearch", config: "english" })
          .limit(LINKEDIN_PER_TERM);
        if (!error && data) for (const r of data) idsFromLinkedin.add(r.id as number);
      }),
    ]);

    // Vector search from both the query vector and the HyDE vector, in parallel.
    // match_chunks returns document_ids ranked by their BEST chunk's similarity —
    // so a doc with one strongly-matching paragraph beats a doc that's mildly
    // on-topic throughout. Fixes the dilution problem.
    let vecError: string | null = null;
    const runMatch = async (vec: number[]): Promise<number[]> => {
      const { data, error } = await supabase.rpc("match_chunks", {
        query_embedding: vec as unknown as string,
        match_limit: VECTOR_LIMIT,
      });
      if (error) {
        vecError = error.message ?? String(error);
        return [];
      }
      if (!data) return [];
      return (data as { document_id: number }[]).map((r) => r.document_id);
    };
    const [vecFromQuery, vecFromHyde] = await Promise.all([
      runMatch(queryVec).catch((e: unknown) => { vecError = e instanceof Error ? e.message : String(e); return [] as number[]; }),
      hypoVec ? runMatch(hypoVec).catch((e: unknown) => { vecError = e instanceof Error ? e.message : String(e); return [] as number[]; }) : Promise.resolve([]),
    ]);

    // Interleave vector results so top-rank hits from both sources land early.
    const idsFromVec: number[] = [];
    const seenVec = new Set<number>();
    const maxLen = Math.max(vecFromQuery.length, vecFromHyde.length);
    for (let i = 0; i < maxLen; i++) {
      for (const arr of [vecFromHyde, vecFromQuery]) {
        const id = arr[i];
        if (id !== undefined && !seenVec.has(id)) {
          seenVec.add(id);
          idsFromVec.push(id);
        }
      }
    }

    // Merge: vector hits first (semantic), then LinkedIn-specific persona matches,
    // then general FTS-only hits. LinkedIn connections get their own lane because
    // sparse profile content can't compete against rich docs in vector search.
    const merged: number[] = [];
    const seen = new Set<number>();
    for (const id of idsFromVec) {
      if (!seen.has(id)) { seen.add(id); merged.push(id); }
    }
    for (const id of idsFromLinkedin) {
      if (!seen.has(id)) { seen.add(id); merged.push(id); }
    }
    for (const id of idsFromFts) {
      if (!seen.has(id)) { seen.add(id); merged.push(id); }
    }
    const docIds = merged.slice(0, MAX_DOCS);

    if (docIds.length === 0) {
      return NextResponse.json({
        answer:
          "I couldn't find anything in your brain that matches. Try different phrasing or check if that data is ingested.",
        sources: [],
      });
    }

    const { data: docsRaw, error: docErr } = await supabase
      .from("documents")
      .select("id, source, title, content, url, occurred_at")
      .in("id", docIds);
    if (docErr) throw docErr;

    // Preserve our merged order (Supabase .in() returns in insertion order, not our rank).
    const byId = new Map((docsRaw as DocRow[]).map((d) => [d.id, d]));
    const docRows = docIds.map((id) => byId.get(id)).filter(Boolean) as DocRow[];

    // Fetch contacts on these docs
    const peopleByDoc = new Map<number, string[]>();
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

    // --- Synthesis ---
    const formattedDocs = docRows
      .map((d, i) => formatDocForPrompt(d, i, peopleByDoc.get(d.id) ?? []))
      .join("\n\n---\n\n");

    const system = `You are Rags's personal "second brain". You answer questions using ONLY the context provided below, which is drawn from Rags's Readwise highlights, Readwise Reader articles, Granola meeting notes, Gmail, and LinkedIn connections/messages.

Rules:
- Ground every claim in the provided context. Cite sources inline as [#N] matching the document number.
- If the context is insufficient, say so plainly — do not speculate or use general knowledge.
- Be concise. Prefer bullet points for lists of people or items.
- When the question is about people ("who should I reach out to", "who has reached out to me"), surface names with roles and companies where available, and explain why each person is relevant based on the text.
- Documents with source=linkedin_connection represent people in Rags's LinkedIn network. Their title shows "Name — Role at Company". When surfacing contacts, include their role and company so Rags knows why they're relevant.
- Documents with source=linkedin_message are LinkedIn conversation threads — use these to add relationship history context.
- When a document's tone, request, or context implies the answer even without using the literal words of the question, say so — e.g., someone asking to "catch up and share their thinking" is implicitly seeking advice.
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

    return NextResponse.json({
      answer: text,
      sources,
      debug: {
        rewrites,
        hypothetical,
        linkedin_terms: linkedinTerms,
        vec_hits: idsFromVec.length,
        fts_hits: idsFromFts.size,
        linkedin_hits: idsFromLinkedin.size,
        ...(vecError ? { vec_error: vecError } : {}),
      },
    });
  } catch (err) {
    console.error(err);
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}
