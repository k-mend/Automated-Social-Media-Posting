"""
linkedln.py — Automated LinkedIn AI content agent (Render Cron + Upstash Redis)
────────────────────────────────────────────────────────────────────────────────
Requires:
    pip install requests feedparser schedule groq python-dotenv

Env vars (.env locally, Render Dashboard in production):
    GROQ_API_KEY
    LI_ACCESS_TOKEN
    LI_AUTHOR_URN           — bare numeric ID or full urn:li:person:XXXX
    LI_CLIENT_ID            — only needed for --auth
    LI_CLIENT_SECRET        — only needed for --auth
    UPSTASH_REDIS_REST_URL  — from upstash.com (free, no card)
    UPSTASH_REDIS_REST_TOKEN

─── UPSTASH SETUP (one-time, free) ──────────────────────────────────────────
1. Go to https://upstash.com → Create account (no card needed)
2. Create a Redis database → copy REST URL + REST Token
3. Paste both into your .env / Render environment variables
─────────────────────────────────────────────────────────────────────────────

─── RENDER DEPLOY ───────────────────────────────────────────────────────────
1. Push this file + requirements.txt to GitHub
2. Render Dashboard → New → Cron Job
3. Build command:  pip install -r requirements.txt
4. Run command:    python linkedln.py --run-once
5. Schedule:       0 9 * * *   (09:00 UTC daily)
6. Add all env vars in Render → Environment tab
─────────────────────────────────────────────────────────────────────────────
"""

import os
import sys
import json
import time
import hashlib
import logging
import feedparser
import requests
import schedule
import webbrowser
import http.server
import urllib.parse
from pathlib import Path
from dotenv import load_dotenv
from groq import Groq

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("agent_linkedin.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
MAX_POSTS_PER_DAY  = 2
LI_POST_CHAR_LIMIT = 3000
BODY_CHAR_LIMIT    = 2800
VIRAL_EXAMPLES     = Path("viral_examples_linkedin.txt")
POSTING_QUEUE      = Path("queue_linkedin.json")
LI_API_BASE        = "https://api.linkedin.com/v2"
REDIS_KEY_TTL      = 7_776_000   # 90 days in seconds


# ── Upstash Redis — zero-dependency REST client ───────────────────────────────
class UpstashRedis:
    """
    Talks to Upstash via their HTTP REST API.
    No extra packages needed — just requests (already a dependency).
    Falls back silently if env vars are missing (local dev without Redis).
    """
    def __init__(self):
        self.url   = os.getenv("UPSTASH_REDIS_REST_URL", "").rstrip("/")
        self.token = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")
        self.ok    = bool(self.url and self.token)
        if not self.ok:
            log.warning("Upstash Redis not configured — dedup disabled. "
                        "Set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN.")

    def _call(self, *cmd_parts) -> dict:
        """Execute a Redis command via the REST pipeline endpoint."""
        url = f"{self.url}/{'/'.join(urllib.parse.quote(str(p), safe='') for p in cmd_parts)}"
        r   = requests.get(
            url,
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=5,
        )
        r.raise_for_status()
        return r.json()

    def exists(self, key: str) -> bool:
        if not self.ok:
            return False
        try:
            return self._call("EXISTS", key).get("result", 0) == 1
        except Exception as e:
            log.error("Redis EXISTS error: %s", e)
            return False

    def set_with_ttl(self, key: str, value: str = "1"):
        """SET key value EX ttl — expires after REDIS_KEY_TTL seconds."""
        if not self.ok:
            return
        try:
            self._call("SET", key, value, "EX", REDIS_KEY_TTL)
        except Exception as e:
            log.error("Redis SET error: %s", e)


# Module-level Redis client (initialised once)
redis = UpstashRedis()


# ── Dedup helpers ─────────────────────────────────────────────────────────────
def _content_hash(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()

def has_been_posted(h: str) -> bool:
    return redis.exists(f"li:posted:{h}")

def mark_as_posted(h: str):
    redis.set_with_ttl(f"li:posted:{h}")


# ── URN normaliser ─────────────────────────────────────────────────────────────
def _normalise_urn(raw: str) -> str:
    """
    Accepts any format and returns the confirmed-working urn:li:person: format.
    Confirmed working: urn:li:person:Zb9EXzHhi0 (from your successful test run).
    """
    clean = raw.strip().split(":")[-1]   # handles bare ID, urn:li:person:X, urn:li:member:X
    return f"urn:li:person:{clean}"


# ── Viral patterns (loaded once at startup) ───────────────────────────────────
def _load_viral_patterns() -> str:
    try:
        return VIRAL_EXAMPLES.read_text(encoding="utf-8")[-3000:]
    except FileNotFoundError:
        return """Example viral LinkedIn posts:

This dev open-sourced a LoRA trainer that cuts GPU costs by 70% 🔥
3 lines of code. That's it.

Stop learning PyTorch the hard way 💡
This repo has 50+ production-ready templates nobody talks about.
Here's what's inside:
1. Pre-trained adapters for common tasks
2. One-click fine-tuning scripts
3. Cost benchmarks vs full training

Anthropic just dropped a paper that changes everything 🚀
Here's what it means for builders:
1. Context windows just got smarter
2. Agents can now self-correct
3. API cost dropped 30%
"""

VIRAL_PATTERNS = _load_viral_patterns()


# ── LinkedIn OAuth2 token helper ──────────────────────────────────────────────
class LinkedInAuth:
    """Run once locally: python linkedln.py --auth"""
    AUTH_URL  = "https://www.linkedin.com/oauth/v2/authorization"
    TOKEN_URL = "https://www.linkedin.com/oauth/v2/accessToken"
    REDIRECT  = "http://localhost:8765/callback"
    SCOPE     = "w_member_social profile openid"

    def __init__(self):
        self.client_id     = os.getenv("LI_CLIENT_ID")
        self.client_secret = os.getenv("LI_CLIENT_SECRET")
        if not self.client_id or not self.client_secret:
            raise EnvironmentError("Set LI_CLIENT_ID and LI_CLIENT_SECRET in .env to run --auth.")

    def run(self):
        code_holder = {}

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                log.info("Auth callback received. Path: %s", self.path)
                parsed = urllib.parse.urlparse(self.path)
                qs     = urllib.parse.parse_qs(parsed.query)
                code_holder["code"] = qs.get("code", [None])[0]
                error = qs.get("error", [None])[0]
                if error:
                    log.error("LinkedIn error: %s — %s",
                              error, qs.get("error_description", [""])[0])
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"<h2>Auth complete. You can close this tab.</h2>")
            def log_message(self, *args): pass

        class ReusableTCPServer(http.server.HTTPServer):
            allow_reuse_address = True

        srv = ReusableTCPServer(("localhost", 8765), Handler)
        log.info("Local auth server listening on port 8765.")

        params = {
            "response_type": "code",
            "client_id":     self.client_id,
            "redirect_uri":  self.REDIRECT,
            "scope":         self.SCOPE,
            "state":         "li_auth",
        }
        auth_url = f"{self.AUTH_URL}?{urllib.parse.urlencode(params)}"
        webbrowser.open(auth_url)
        print("\n" + "=" * 60)
        print("If browser didn't open, paste this URL manually:")
        print(auth_url)
        print("=" * 60 + "\n")
        log.info("Waiting for callback on http://localhost:8765/callback ...")

        srv.timeout = 120
        srv.handle_request()

        code = code_holder.get("code")
        if not code:
            raise RuntimeError(
                "No auth code received. Checklist:\n"
                "  1. Did you click Allow on the LinkedIn consent screen?\n"
                "  2. Is http://localhost:8765/callback registered in your app?\n"
                "  3. Check port conflict: sudo lsof -i :8765\n"
                "  4. Try the URL above in a private/incognito window."
            )

        resp = requests.post(self.TOKEN_URL, data={
            "grant_type":    "authorization_code",
            "code":          code,
            "redirect_uri":  self.REDIRECT,
            "client_id":     self.client_id,
            "client_secret": self.client_secret,
        })
        resp.raise_for_status()
        token = resp.json()["access_token"]

        print("\n✅ LinkedIn Auth complete!")
        print("\nAdd this to your .env / Render environment variables:")
        print(f"LI_ACCESS_TOKEN={token}")
        print("\nLI_AUTHOR_URN stays the same. Token valid ~60 days.")


# ── Main agent ────────────────────────────────────────────────────────────────
class ViralLinkedInAgent:

    def __init__(self):
        # ── Groq ──────────────────────────────────────────────────────────────
        groq_key = os.getenv("GROQ_API_KEY")
        if not groq_key:
            raise EnvironmentError("GROQ_API_KEY not set.")
        self.groq = Groq(api_key=groq_key)

        # ── LinkedIn ───────────────────────────────────────────────────────────
        self.li_token = os.getenv("LI_ACCESS_TOKEN")
        raw_urn       = os.getenv("LI_AUTHOR_URN")
        if not self.li_token or not raw_urn:
            raise EnvironmentError(
                "LI_ACCESS_TOKEN and LI_AUTHOR_URN must be set.\n"
                "Run: python linkedln.py --auth"
            )
        self.li_author = _normalise_urn(raw_urn)
        log.info("ViralLinkedInAgent ready. Author URN: %s", self.li_author)
        log.info("Redis dedup: %s", "enabled" if redis.ok else "DISABLED (no Upstash config)")

    # ── Content fetching ───────────────────────────────────────────────────────
    def _fetch_github(self) -> list[dict]:
        try:
            r = requests.get(
                "https://api.github.com/search/repositories"
                "?q=stars:>500+language:python&sort=updated&order=desc",
                headers={"Accept": "application/vnd.github.v3+json"}, timeout=6,
            )
            r.raise_for_status()
            return [
                {"title": i["name"], "description": i.get("description") or "No description", "url": i["html_url"]}
                for i in r.json().get("items", [])[:3]
            ]
        except Exception as e:
            log.warning("GitHub fetch failed: %s", e)
            return []

    def _fetch_arxiv(self) -> list[dict]:
        try:
            feed = feedparser.parse(
                "http://export.arxiv.org/api/query"
                "?search_query=cat:cs.LG+OR+cat:cs.AI+OR+cat:cs.CL"
                "&max_results=5&sortBy=submittedDate&sortOrder=descending"
            )
            return [
                {"title": e.title, "description": (e.summary[:300] + "…"), "url": e.link}
                for e in feed.entries
            ]
        except Exception as e:
            log.warning("arXiv fetch failed: %s", e)
            return []

    def _fetch_hackernews(self) -> list[dict]:
        results = []
        keywords = {"ai", "llm", "gpt", "machine learning", "deep learning",
                    "neural", "openai", "anthropic", "deepseek"}
        try:
            ids = requests.get(
                "https://hacker-news.firebaseio.com/v0/topstories.json", timeout=5
            ).json()[:40]
            for sid in ids:
                if len(results) >= 3:
                    break
                try:
                    s = requests.get(
                        f"https://hacker-news.firebaseio.com/v0/item/{sid}.json", timeout=3
                    ).json()
                    title = s.get("title", "")
                    if any(kw in title.lower() for kw in keywords):
                        results.append({
                            "title":       title,
                            "description": "Trending on Hacker News",
                            "url":         s.get("url", f"https://news.ycombinator.com/item?id={sid}"),
                        })
                except Exception:
                    continue
        except Exception as e:
            log.warning("HN fetch failed: %s", e)
        return results

    def fetch_content(self) -> list[dict]:
        raw   = self._fetch_github() + self._fetch_arxiv() + self._fetch_hackernews()
        fresh = []
        for item in raw:
            h = _content_hash(item["title"])
            if not has_been_posted(h):
                fresh.append({**item, "hash": h})
        log.info("Fetched %d items, %d new after Redis dedup.", len(raw), len(fresh))
        return fresh

    # ── Link shortening ────────────────────────────────────────────────────────
    @staticmethod
    def shorten_link(url: str) -> str:
        if not url:
            return ""
        try:
            r = requests.get(
                f"https://tinyurl.com/api-create.php?url={urllib.parse.quote(url, safe=':/')}",
                timeout=4,
            )
            short = r.text.strip()
            return short if short.startswith("http") else url
        except Exception:
            return url

    # ── Post generation ────────────────────────────────────────────────────────
    def _build_prompt(self, item: dict) -> str:
        return f"""Write a viral LinkedIn post about this content.

TITLE: {item['title']}
DESCRIPTION: {item['description']}

VIRAL PATTERNS FROM TOP PERFORMERS:
{VIRAL_PATTERNS}

LINKEDIN-SPECIFIC RULES:
1. Hook (line 1): "This [creator/repo] just [achievement]" OR "Stop [mistake]" OR "X things about [topic]:"
2. One emoji on the hook line only: 🔥, 💡, or 🚀
3. Value prop: 1-2 punchy sentences after the hook
4. Numbered list if relevant (max 3 short items, one per line)
5. Professional but conversational tone — audience is practitioners + hiring managers
6. Blank line between sections for readability
7. 2-3 hashtags on the LAST line (e.g. #AI #MachineLearning #OpenSource)
8. Hard limit: {BODY_CHAR_LIMIT} characters (link appended separately)
9. Do NOT include any URL in your output

OUTPUT ONLY THE POST TEXT. No commentary, no quotes, no markdown fences."""

    def curate_post(self, item: dict) -> str | None:
        try:
            resp = self.groq.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": self._build_prompt(item)}],
                max_tokens=500,
            )
            body = "\n".join(
                line for line in resp.choices[0].message.content.strip().splitlines()
                if not line.strip().startswith("http")
            ).strip()

            if len(body) > BODY_CHAR_LIMIT:
                body = body[:BODY_CHAR_LIMIT].rsplit("\n", 1)[0] + "…"

            short = self.shorten_link(item["url"])
            full  = f"{body}\n\n{short}"

            if len(full) > LI_POST_CHAR_LIMIT:
                allowed = LI_POST_CHAR_LIMIT - len(short) - 4
                full    = body[:allowed].rstrip() + f"…\n\n{short}"

            return full

        except Exception as e:
            log.error("Post generation failed for '%s': %s", item["title"], e)
            return None

    # ── LinkedIn API posting ───────────────────────────────────────────────────
    def _li_post(self, text: str) -> bool:
        payload = {
            "author":      self.li_author,
            "commentary":  text,
            "visibility":  "PUBLIC",
            "distribution": {
                "feedDistribution":               "MAIN_FEED",
                "targetEntities":                 [],
                "thirdPartyDistributionChannels": [],
            },
            "lifecycleState":            "PUBLISHED",
            "isReshareDisabledByAuthor": False,
        }
        try:
            r = requests.post(
                f"{LI_API_BASE}/posts",
                headers={
                    "Authorization":             f"Bearer {self.li_token}",
                    "Content-Type":              "application/json",
                    "X-Restli-Protocol-Version": "2.0.0",
                    "LinkedIn-Version":          "202501",
                },
                json=payload, timeout=10,
            )
            if r.status_code in (200, 201):
                post_id = r.headers.get("x-restli-id", "unknown")
                log.info("Posted to LinkedIn. ID: %s", post_id)
                return True
            log.error("LinkedIn API error %d: %s", r.status_code, r.text[:300])
            return False
        except Exception as e:
            log.error("LinkedIn request failed: %s", e)
            return False

    # ── Core pipeline (used by both --run-once and scheduler) ─────────────────
    def run_pipeline(self):
        log.info("Pipeline starting.")
        items  = self.fetch_content()
        posted = 0

        for item in items[:MAX_POSTS_PER_DAY]:
            text = self.curate_post(item)
            if text and self._li_post(text):
                mark_as_posted(item["hash"])   # ← saved to Upstash Redis
                posted += 1
                time.sleep(3)

        log.info("Pipeline complete. Posted %d item(s).", posted)
        return posted


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":

    # --auth: one-time LinkedIn OAuth2 flow (run locally)
    if "--auth" in sys.argv:
        LinkedInAuth().run()
        sys.exit(0)

    # --test: run pipeline once immediately (local testing)
    if "--test" in sys.argv:
        agent = ViralLinkedInAgent()
        print("\n--- Running pipeline ---")
        agent.run_pipeline()
        sys.exit(0)

    # --run-once: used by Render cron job (runs pipeline and exits cleanly)
    if "--run-once" in sys.argv:
        agent = ViralLinkedInAgent()
        agent.run_pipeline()
        sys.exit(0)

    # Default: local scheduler mode (keeps process alive on your own machine)
    agent = ViralLinkedInAgent()
    schedule.every().day.at("09:00").do(agent.run_pipeline)
    log.info("Scheduler running. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(30)