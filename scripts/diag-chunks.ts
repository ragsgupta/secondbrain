/**
 * Diagnostic: report the actual state of the chunks table.
 * Run with: npx tsx scripts/diag-chunks.ts
 */
import { config as loadEnv } from "dotenv";
loadEnv({ path: ".env.local" });

import { createServerClient } from "../lib/supabase";
import { embed } from "../lib/voyage";

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

  // Fix bySource: paginate to avoid 1000-row PostgREST default limit
  const sourceCounts: Record<string, number> = {};
  let srcFrom = 0;
  while (true) {
    const { data, error } = await supabase
      .from("documents")
      .select("source")
      .order("id", { ascending: true })
      .range(srcFrom, srcFrom + PAGE - 1);
    if (error) throw error;
    if (!data || data.length === 0) break;
    for (const r of data) sourceCounts[r.source as string] = (sourceCounts[r.source as string] ?? 0) + 1;
    if (data.length < PAGE) break;
    srcFrom += PAGE;
  }
  console.log(`Documents by source:`, sourceCounts);

  // Test match_chunks directly with a real embedding
  console.log(`\nTesting match_chunks RPC…`);
  try {
    const [vec] = await embed(["security operations physical infrastructure"], "query");
    console.log(`  Embedding OK — dim=${vec.length}, first3=[${vec.slice(0, 3).map(v => v.toFixed(4)).join(", ")}]`);

    const { data: rpcData, error: rpcErr } = await supabase.rpc("match_chunks", {
      query_embedding: vec as unknown as string,
      match_limit: 5,
    });
    if (rpcErr) {
      console.error(`  match_chunks ERROR:`, rpcErr);
    } else {
      console.log(`  match_chunks returned ${(rpcData as unknown[])?.length ?? 0} rows`);
      if (rpcData && (rpcData as unknown[]).length > 0) {
        console.log(`  Top hit:`, (rpcData as Record<string, unknown>[])[0]);
      }
    }
  } catch (e) {
    console.error(`  Embed or RPC threw:`, e);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
