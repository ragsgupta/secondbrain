/**
 * Fill in `embedding` for every document that doesn't have one yet.
 *
 * Run with:  npm run embed
 *
 * Safe to re-run — only processes rows where embedding is null.
 * Voyage accepts up to 128 inputs per request; we batch by that.
 */

import { config as loadEnv } from "dotenv";
loadEnv({ path: ".env.local" });

import { createServerClient } from "../lib/supabase";
import { embed } from "../lib/voyage";

const BATCH = 32;
const MAX_CHARS = 8_000; // Stay under Voyage's 120k-tokens-per-request cap comfortably

type Row = { id: number; title: string | null; content: string | null };

function buildText(r: Row): string {
  const parts = [r.title ?? "", r.content ?? ""].filter(Boolean);
  return parts.join("\n\n").slice(0, MAX_CHARS);
}

// Sentinel unit vector for docs with no usable text. Still a valid vector
// (unit length), so pgvector indexes are happy; but it won't match anything
// specific semantically, which is fine — empty docs shouldn't rank for queries.
const DIM = 1024;
const SENTINEL: number[] = Array(DIM).fill(1 / Math.sqrt(DIM));

async function main() {
  const supabase = createServerClient();

  while (true) {
    const { data, error } = await supabase
      .from("documents")
      .select("id, title, content")
      .is("embedding", null)
      .limit(BATCH);
    if (error) throw error;
    if (!data || data.length === 0) break;

    const texts = data.map(buildText);
    const nonEmptyIdxs: number[] = [];
    const emptyIdxs: number[] = [];
    texts.forEach((t, i) => (t.trim() ? nonEmptyIdxs : emptyIdxs).push(i));

    // 1) Embed the ones with usable text.
    if (nonEmptyIdxs.length > 0) {
      const embTexts = nonEmptyIdxs.map((i) => texts[i]);
      const vecs = await embed(embTexts, "document");
      await Promise.all(
        nonEmptyIdxs.map(async (idx, k) => {
          const { error: upErr } = await supabase
            .from("documents")
            .update({ embedding: vecs[k] as unknown as string })
            .eq("id", data[idx].id);
          if (upErr) throw upErr;
        }),
      );
    }

    // 2) Mark empty docs with the sentinel so they never re-enter the loop.
    if (emptyIdxs.length > 0) {
      await Promise.all(
        emptyIdxs.map(async (i) => {
          const { error: upErr } = await supabase
            .from("documents")
            .update({ embedding: SENTINEL as unknown as string })
            .eq("id", data[i].id);
          if (upErr) throw upErr;
        }),
      );
    }

    console.log(
      `  batch: embedded=${nonEmptyIdxs.length}, sentinel=${emptyIdxs.length} (of ${data.length})`,
    );
  }

  console.log("Done.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
