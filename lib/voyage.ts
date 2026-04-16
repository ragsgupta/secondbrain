/**
 * Thin client for Voyage AI embeddings.
 * https://docs.voyageai.com/docs/embeddings
 */

const MODEL = "voyage-3-large";
const DIMS = 1024;

export type EmbedInputType = "document" | "query";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function embed(texts: string[], inputType: EmbedInputType): Promise<number[][]> {
  const key = process.env.VOYAGE_API_KEY;
  if (!key) throw new Error("Missing VOYAGE_API_KEY");
  if (texts.length === 0) return [];

  // Retry on 429 with exponential backoff.
  let attempt = 0;
  while (true) {
    const res = await fetch("https://api.voyageai.com/v1/embeddings", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${key}`,
      },
      body: JSON.stringify({
        model: MODEL,
        input: texts,
        input_type: inputType,
      }),
    });
    if (res.status === 429) {
      attempt++;
      const wait = Math.min(60_000, 5_000 * 2 ** (attempt - 1));
      console.warn(`  Voyage 429, waiting ${wait / 1000}s (attempt ${attempt})…`);
      await sleep(wait);
      if (attempt >= 5) throw new Error("Voyage 429 after 5 retries");
      continue;
    }
    if (!res.ok) {
      throw new Error(`Voyage ${res.status}: ${await res.text()}`);
    }
    const data = (await res.json()) as {
      data: { embedding: number[]; index: number }[];
    };
    const out = new Array<number[]>(texts.length);
    for (const row of data.data) out[row.index] = row.embedding;
    return out;
  }
}

export { MODEL as VOYAGE_MODEL, DIMS as VOYAGE_DIMS };
