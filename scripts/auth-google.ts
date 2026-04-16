/**
 * One-time Google OAuth flow to get a refresh token.
 *
 * Run with:  npm run auth:google
 *
 * What it does:
 *   1. Spins up a temporary local web server on port 8765.
 *   2. Opens your browser to Google's consent page.
 *   3. You click "Allow"; Google redirects back to localhost:8765 with a code.
 *   4. We exchange the code for a refresh token and print it.
 *   5. You paste the refresh token into .env.local as GOOGLE_REFRESH_TOKEN.
 *
 * After this, all future Gmail/Calendar syncs are automatic — no browser needed.
 */

import { config as loadEnv } from "dotenv";
loadEnv({ path: ".env.local" });

import http from "http";
import { spawn } from "child_process";
import { google } from "googleapis";

const SCOPES = [
  "https://www.googleapis.com/auth/gmail.readonly",
  "https://www.googleapis.com/auth/calendar.readonly",
];

const PORT = 8765;
const REDIRECT = `http://localhost:${PORT}`;

async function main() {
  const clientId = process.env.GOOGLE_CLIENT_ID;
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET;
  if (!clientId || !clientSecret) {
    throw new Error("Missing GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET in .env.local");
  }

  const oauth2 = new google.auth.OAuth2(clientId, clientSecret, REDIRECT);

  const authUrl = oauth2.generateAuthUrl({
    access_type: "offline",   // request a refresh token
    prompt: "consent",        // force re-consent so we always get a refresh_token
    scope: SCOPES,
  });

  console.log("Starting local callback server on", REDIRECT);
  console.log("\nOpening browser to:\n" + authUrl + "\n");

  const code: string = await new Promise((resolve, reject) => {
    const server = http.createServer((req, res) => {
      try {
        const url = new URL(req.url ?? "/", REDIRECT);
        const code = url.searchParams.get("code");
        const err = url.searchParams.get("error");
        if (err) {
          res.writeHead(400, { "Content-Type": "text/html" });
          res.end(`<h1>Auth error: ${err}</h1>`);
          server.close();
          reject(new Error(err));
          return;
        }
        if (code) {
          res.writeHead(200, { "Content-Type": "text/html" });
          res.end(
            "<h1>Auth complete.</h1><p>You can close this tab and return to the terminal.</p>",
          );
          server.close();
          resolve(code);
        }
      } catch (e) {
        reject(e);
      }
    });
    server.listen(PORT, () => {
      // Best-effort: try to open the browser automatically (macOS).
      spawn("open", [authUrl], { detached: true, stdio: "ignore" }).on("error", () => {
        /* user can just click the URL above */
      });
    });
  });

  const { tokens } = await oauth2.getToken(code);

  if (!tokens.refresh_token) {
    console.error(
      "\nNo refresh_token returned. This usually happens if you've authorized this app before.\n" +
        "Fix: go to https://myaccount.google.com/permissions, remove 'Second Brain', then re-run.",
    );
    process.exit(1);
  }

  console.log("\n=== SUCCESS ===\n");
  console.log("Add this line to .env.local (replace any existing value):\n");
  console.log(`GOOGLE_REFRESH_TOKEN=${tokens.refresh_token}\n`);
  console.log("Then you're done — this refresh token is long-lived.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
