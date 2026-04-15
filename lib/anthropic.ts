import Anthropic from "@anthropic-ai/sdk";

export function getAnthropic() {
  const key = process.env.ANTHROPIC_API_KEY;
  if (!key) throw new Error("Missing ANTHROPIC_API_KEY");
  return new Anthropic({ apiKey: key });
}

// Default model for synthesis queries. Sonnet 4.6 is the cost/quality sweet spot.
export const DEFAULT_MODEL = "claude-sonnet-4-6";
