"""
viral_agent_linkedin.py — Zero-cost automated LinkedIn AI content agent
────────────────────────────────────────────────────────────────────────
Requires:
    pip install requests feedparser schedule groq python-dotenv

Env vars in .env file:
    GROQ_API_KEY
    LI_ACCESS_TOKEN
    LI_AUTHOR_URN       — just the numeric ID OR full urn:li:member:XXXX
    LI_CLIENT_ID        — only needed for --auth
    LI_CLIENT_SECRET    — only needed for --auth

─── LINKEDIN OAUTH2 SETUP (one-time, free) ──────────────────────────────────
1. Go to https://www.linkedin.com/developers/apps → "Create app"
2. Add product: "Share on LinkedIn" (gives w_member_social scope)
3. Under Auth tab → add redirect URL: http://localhost:8765/callback
4. Run once: python viral_agent_linkedin.py --auth
   Prints your access token + author URN → paste into .env
   Tokens last 60 days; re-run --auth to refresh.
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
POSTED_CACHE       = Path("posted_hashes_linkedin.json")
VIRAL_EXAMPLES     = Path("viral_examples_linkedin.txt")
POSTING_QUEUE      = Path("queue_linkedin.json")
LI_API_BASE        = "https://api.linkedin.com/v2"


# ── URN normaliser ────────────────────────────────────────────────────────────
def _normalise_urn(raw: str) -> str:
    """Accept bare numeric ID or full URN, always return full URN."""
    raw = raw.strip()
    if raw.startswith("urn:li:person:"):
        return raw
    return f"urn:li:person:{raw}"


# ── Cache helpers ─────────────────────────────────────────────────────────────
def _load_cache() -> set:
    if POSTED_CACHE.exists():
        return set(json.loads(POSTED_CACHE.read_text()))
    return set()

def _save_cache(cache: set):
    POSTED_CACHE.write_text(json.dumps(list(cache)))

def _content_hash(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


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
    """Run once: python viral_agent_linkedin.py --auth"""
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
                qs = urllib.parse.parse_qs(parsed.query)
                code_holder["code"] = qs.get("code", [None])[0]
                error = qs.get("error", [None])[0]
                if error:
                    log.error("LinkedIn returned error: %s — %s",
                              error, qs.get("error_description", [""])[0])
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"<h2>Auth complete. You can close this tab.</h2>")
            def log_message(self, *args): pass

        # Server starts BEFORE browser to avoid race condition
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
        print("If the browser didn't open, paste this URL manually:")
        print(auth_url)
        print("=" * 60 + "\n")
        log.info("Waiting for callback on http://localhost:8765/callback ...")

        srv.timeout = 120
        srv.handle_request()

        code = code_holder.get("code")
        if not code:
            raise RuntimeError(
                "No auth code received. Checklist:\n"
                "  1. Did you click 'Allow' on the LinkedIn consent screen?\n"
                "  2. Is http://localhost:8765/callback registered in your LinkedIn\n"
                "     app under Auth tab → Authorized redirect URLs?\n"
                "  3. Check for port conflict: sudo lsof -i :8765\n"
                "     If anything shows, kill it and retry.\n"
                "  4. Try the printed URL in a private/incognito window."
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
        print("\nAdd this to your .env file:")
        print(f"LI_ACCESS_TOKEN={token}")
        print("\nYour LI_AUTHOR_URN stays the same: 1629293805")
        print("Token is valid for ~60 days. Re-run --auth to refresh.")


# ── Main agent ────────────────────────────────────────────────────────────────
class ViralLinkedInAgent:

    def __init__(self):
        # ── Groq ──────────────────────────────────────────────────────────────
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise EnvironmentError("GROQ_API_KEY not set in .env")
        self.groq = Groq(api_key=api_key)

        # ── LinkedIn credentials ───────────────────────────────────────────────
        self.li_token = os.getenv("LI_ACCESS_TOKEN")
        raw_urn       = os.getenv("LI_AUTHOR_URN")
        if not self.li_token or not raw_urn:
            raise EnvironmentError(
                "LI_ACCESS_TOKEN and LI_AUTHOR_URN must be set in .env\n"
                "Run: python viral_agent_linkedin.py --auth"
            )
        self.li_author    = _normalise_urn(raw_urn)
        self.posted_cache = _load_cache()
        log.info("ViralLinkedInAgent ready. Author URN: %s", self.li_author)

    # ── Content fetching ──────────────────────────────────────────────────────
    def _fetch_github(self) -> list[dict]:
        try:
            r = requests.get(
                "https://api.github.com/search/repositories"
                "?q=stars:>500+language:python&sort=updated&order=desc",
                headers={"Accept": "application/vnd.github.v3+json"},
                timeout=6,
            )
            r.raise_for_status()
            return [
                {
                    "type":        "github",
                    "title":       i["name"],
                    "description": i.get("description") or "No description",
                    "url":         i["html_url"],
                }
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
                {
                    "type":        "paper",
                    "title":       e.title,
                    "description": (e.summary[:300] + "…") if len(e.summary) > 300 else e.summary,
                    "url":         e.link,
                }
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
                            "type":        "hn",
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
            if h not in self.posted_cache:
                fresh.append({**item, "hash": h})
        log.info("Fetched %d items (%d new after dedup).", len(raw), len(fresh))
        return fresh

    # ── Link shortening ───────────────────────────────────────────────────────
    @staticmethod
    def shorten_link(url: str) -> str:
        if not url:
            return ""
        try:
            r = requests.get(
                f"https://tinyurl.com/api-create.php?url={urllib.parse.quote(url, safe=':/')}",
                timeout=4,
            )
            r.raise_for_status()
            short = r.text.strip()
            return short if short.startswith("http") else url
        except Exception:
            return url

    # ── Post generation ───────────────────────────────────────────────────────
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
8. Hard limit: {BODY_CHAR_LIMIT} characters (link is appended separately by the system)
9. Do NOT include any URL in your output

OUTPUT ONLY THE POST TEXT. No commentary, no quotes, no markdown fences."""

    def curate_post(self, item: dict) -> str | None:
        try:
            resp = self.groq.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": self._build_prompt(item)}],
                max_tokens=500,
            )
            body = resp.choices[0].message.content.strip()

            # Strip any URL lines the model snuck in
            cleaned_lines = [
                line for line in body.splitlines()
                if not line.strip().startswith("http")
            ]
            body = "\n".join(cleaned_lines).strip()

            # Truncate body before appending link
            if len(body) > BODY_CHAR_LIMIT:
                body = body[:BODY_CHAR_LIMIT].rsplit("\n", 1)[0] + "…"

            short = self.shorten_link(item["url"])
            full  = f"{body}\n\n{short}"

            # Final hard cap
            if len(full) > LI_POST_CHAR_LIMIT:
                allowed = LI_POST_CHAR_LIMIT - len(short) - 4
                full    = body[:allowed].rstrip() + f"…\n\n{short}"

            return full

        except Exception as e:
            log.error("Post generation failed for '%s': %s", item["title"], e)
            return None

    # ── Queue management ──────────────────────────────────────────────────────
    def create_posting_queue(self) -> list[dict]:
        items = self.fetch_content()
        posts = []
        for item in items[:MAX_POSTS_PER_DAY]:
            text = self.curate_post(item)
            if text:
                posts.append({"text": text, "hash": item["hash"]})
                log.info("Queued (%d chars): %s…", len(text), text[:60])

        POSTING_QUEUE.write_text(json.dumps(posts, indent=2, ensure_ascii=False))
        log.info("Queue written: %d post(s).", len(posts))
        return posts

    # ── LinkedIn API posting ──────────────────────────────────────────────────
    def _li_post(self, text: str) -> bool:
        # LinkedIn Posts API v2 (replaces deprecated ugcPosts)
        payload = {
            "author":          self.li_author,
            "commentary":      text,
            "visibility":      "PUBLIC",
            "distribution": {
                "feedDistribution":               "MAIN_FEED",
                "targetEntities":                 [],
                "thirdPartyDistributionChannels": [],
            },
            "lifecycleState":        "PUBLISHED",
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
                json=payload,
                timeout=10,
            )
            if r.status_code in (200, 201):
                post_id = r.headers.get("x-restli-id", r.json().get("id", "unknown")) if r.text else r.headers.get("x-restli-id", "unknown")
                log.info("Posted to LinkedIn. ID: %s", post_id)
                return True
            log.error("LinkedIn API error %d: %s", r.status_code, r.text[:300])
            return False
        except Exception as e:
            log.error("LinkedIn request failed: %s", e)
            return False

    def run_posting_queue(self):
        if not POSTING_QUEUE.exists():
            log.warning("No queue file found — generating now.")
            self.create_posting_queue()

        try:
            queue = json.loads(POSTING_QUEUE.read_text())
        except Exception as e:
            log.error("Queue read error: %s", e)
            return

        for entry in queue:
            if entry["hash"] in self.posted_cache:
                log.info("Skipping already-posted item.")
                continue
            if self._li_post(entry["text"]):
                self.posted_cache.add(entry["hash"])
                _save_cache(self.posted_cache)
                time.sleep(5)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if "--auth" in sys.argv:
        LinkedInAuth().run()
        sys.exit(0)

    if "--test" in sys.argv:
        agent = ViralLinkedInAgent()
        print("\n--- Generating queue ---")
        agent.create_posting_queue()
        print("\n--- Posting now ---")
        agent.run_posting_queue()
        sys.exit(0)

    agent = ViralLinkedInAgent()

    schedule.every().day.at("08:00").do(agent.create_posting_queue)
    schedule.every().day.at("09:00").do(agent.run_posting_queue)

    log.info("Scheduler running. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(30)