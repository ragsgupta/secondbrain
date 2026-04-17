/**
 * Diagnostic: report the actual state of the chunks table.
 * Run with: npx tsx scripts/diag-chunks.ts
 */
import { config as loadEnv } from "dotenv";
loadEnv({ path: ".env.local" });

import { createServerClient } from "../lib/supabase";

async function main() {
  const supabase = createServerClient();

  // Total chunk rows via exact count
  const { count: totalChunks } = await supabase
    .from("chunks")
    .select("*", { count: "exact", head: true });
  console.log(`Total chunks:            ${totalChunks}`);

  // Total document rows
  const { count: totalDocs } = await supabase
    .from("documents")
    .select("*", { count: "exact", head: true });
  console.log(`Total documents:         ${totalDocs}`);

  // Paginate chunks to count distinct document_ids (ground truth on "has" set).
  const PAGE = 1000;
  const distinct = new Set<number>();
  let from = 0;
  let pages = 0;
  while (true) {
    const { data, error } = await supabase
      .from("chunks")
      .select("document_id")
      .order("id", { ascending: true })
      .range(from, from + PAGE - 1);
    if (error) throw error;
    if (!data || data.length === 0) break;
    pages++;
    for (const r of data) distinct.add(r.document_id as number);
    if (data.length < PAGE) break;
    from += PAGE;
  }
  console.log(`Distinct document_ids:   ${distinct.size}   (via ${pages} pages)`);

  // Sample: a few doc_ids with chunk counts
  const { data: sample } = await supabase
    .from("chunks")
    .select("document_id")
    .order("id", { ascending: false })
    .limit(5);
  console.log(`Recent chunks point to doc_ids:`, sample?.map((r) => r.document_id));

  // Break down docs by source
  const { data: bySource } = await supabase
    .from("documents")
    .select("source")
    .order("id", { ascending: true });
  const counts: Record<string, number> = {};
  for (const r of bySource ?? []) counts[r.source as string] = (counts[r.source as string] ?? 0) + 1;
  console.log(`Documents by source:`, counts);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
