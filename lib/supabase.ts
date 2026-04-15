import { createClient } from "@supabase/supabase-js";

// Server-side Supabase client using the service role key.
// Never import this from client components — the service role key bypasses RLS.
export function createServerClient() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) {
    throw new Error("Missing SUPABASE env vars. Check .env.local.");
  }
  return createClient(url, key, {
    auth: { persistSession: false },
  });
}
