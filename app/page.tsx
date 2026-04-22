"use client";

import { useState } from "react";

type Source = {
  n: number;
  id: number;
  source: string;
  title: string | null;
  url: string | null;
  occurred_at: string | null;
  people: string[];
};

type Debug = {
  rewrites?: string[];
  hypothetical?: string;
  linkedin_terms?: string[];
  vec_hits?: number;
  fts_hits?: number;
  linkedin_hits?: number;
  vec_error?: string;
};

export default function Home() {
  const [question, setQuestion] = useState("");
  const [loading, setLoading] = useState(false);
  const [answer, setAnswer] = useState<string | null>(null);
  const [sources, setSources] = useState<Source[]>([]);
  const [debug, setDebug] = useState<Debug | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function ask(e: React.FormEvent) {
    e.preventDefault();
    if (!question.trim() || loading) return;
    setLoading(true);
    setError(null);
    setAnswer(null);
    setSources([]);
    setDebug(null);
    try {
      const res = await fetch("/api/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error ?? "Unknown error");
      setAnswer(data.answer);
      setSources(data.sources ?? []);
      setDebug(data.debug ?? null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="min-h-screen bg-zinc-50 font-sans text-zinc-900 dark:bg-zinc-950 dark:text-zinc-100">
      <main className="mx-auto max-w-3xl px-6 py-16">
        <header className="mb-10">
          <h1 className="text-3xl font-semibold tracking-tight">Second Brain</h1>
          <p className="mt-2 text-sm text-zinc-500">
            Ask questions across your Readwise highlights, Reader articles, Granola meetings, Gmail, and LinkedIn connections.
          </p>
        </header>

        <form onSubmit={ask} className="mb-8">
          <textarea
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            placeholder="Who should I reach out to about private schools? What have I saved about AI adoption at startups?"
            rows={3}
            className="w-full resize-none rounded-xl border border-zinc-200 bg-white px-4 py-3 text-base shadow-sm outline-none focus:border-zinc-400 dark:border-zinc-800 dark:bg-zinc-900"
            onKeyDown={(e) => {
              if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) ask(e);
            }}
          />
          <div className="mt-3 flex items-center justify-between">
            <span className="text-xs text-zinc-400">⌘+Enter to submit</span>
            <button
              type="submit"
              disabled={loading || !question.trim()}
              className="rounded-lg bg-zinc-900 px-4 py-2 text-sm font-medium text-white transition hover:bg-zinc-700 disabled:opacity-40 dark:bg-zinc-100 dark:text-zinc-900 dark:hover:bg-zinc-300"
            >
              {loading ? "Thinking…" : "Ask"}
            </button>
          </div>
        </form>

        {error && (
          <div className="mb-6 rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900/50 dark:bg-red-950/30 dark:text-red-300">
            {error}
          </div>
        )}

        {answer && (
          <section className="mb-10 rounded-xl border border-zinc-200 bg-white p-6 shadow-sm dark:border-zinc-800 dark:bg-zinc-900">
            <div className="prose prose-zinc max-w-none whitespace-pre-wrap text-[15px] leading-7 dark:prose-invert">
              {answer}
            </div>
          </section>
        )}

        {debug && (
          <section className="mb-6 rounded-lg border border-dashed border-zinc-300 bg-zinc-50 p-3 text-xs text-zinc-600 dark:border-zinc-700 dark:bg-zinc-900/50 dark:text-zinc-400">
            <div className="font-mono">
              vector hits: {debug.vec_hits ?? 0} · FTS hits: {debug.fts_hits ?? 0} · LinkedIn hits: {debug.linkedin_hits ?? 0}
            </div>
            {debug.vec_error && (
              <div className="mt-1 font-mono text-red-500">vec error: {debug.vec_error}</div>
            )}
            {debug.rewrites && debug.rewrites.length > 0 && (
              <div className="mt-1 font-mono">
                rewrites:
                <ul className="ml-4 list-disc">
                  {debug.rewrites.map((r, i) => (
                    <li key={i}>{r}</li>
                  ))}
                </ul>
              </div>
            )}
            {debug.linkedin_terms && debug.linkedin_terms.length > 0 && (
              <div className="mt-1 font-mono">
                linkedin terms:
                <ul className="ml-4 list-disc">
                  {debug.linkedin_terms.map((t, i) => (
                    <li key={i}>{t}</li>
                  ))}
                </ul>
              </div>
            )}
            {debug.hypothetical && (
              <div className="mt-2 font-mono">
                hypothetical:
                <div className="ml-4 italic">&ldquo;{debug.hypothetical}&rdquo;</div>
              </div>
            )}
          </section>
        )}

        {sources.length > 0 && (
          <section>
            <h2 className="mb-3 text-xs font-semibold uppercase tracking-wider text-zinc-500">
              Sources ({sources.length})
            </h2>
            <ul className="space-y-2">
              {sources.map((s) => (
                <li
                  key={s.id}
                  className="rounded-lg border border-zinc-200 bg-white p-3 text-sm dark:border-zinc-800 dark:bg-zinc-900"
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center gap-2">
                        <span className="text-xs font-mono text-zinc-400">[#{s.n}]</span>
                        <span className="rounded bg-zinc-100 px-1.5 py-0.5 text-xs text-zinc-600 dark:bg-zinc-800 dark:text-zinc-400">
                          {s.source}
                        </span>
                        {s.occurred_at && (
                          <span className="text-xs text-zinc-400">
                            {new Date(s.occurred_at).toLocaleDateString()}
                          </span>
                        )}
                      </div>
                      <div className="mt-1 truncate font-medium">
                        {s.url ? (
                          <a
                            href={s.url}
                            target="_blank"
                            rel="noreferrer"
                            className="hover:underline"
                          >
                            {s.title ?? "(untitled)"}
                          </a>
                        ) : (
                          (s.title ?? "(untitled)")
                        )}
                      </div>
                      {s.people.length > 0 && (
                        <div className="mt-1 truncate text-xs text-zinc-500">
                          {s.people.join(" · ")}
                        </div>
                      )}
                    </div>
                  </div>
                </li>
              ))}
            </ul>
          </section>
        )}
      </main>
    </div>
  );
}
