# pipeline/scrape_reddit.py
import argparse, os, time, sys
from typing import List
from datetime import datetime, timedelta
import requests
from util import ensure_dirs, save_jsonl, clean_text, now_iso

try:
    import praw
except Exception:
    praw = None

def month_ago_utc(months: int) -> int:
    now = datetime.utcnow()
    start = now - timedelta(days=30 * months)
    return int(start.timestamp())

def scrape_pushshift(subs: List[str], since_utc: int, limit: int, min_upvotes: int):
    base = "https://api.pushshift.io/reddit/search/submission/"
    size = 250
    out = []

    for sub in subs:
        params = {
            "subreddit": sub.replace("r/", ""),
            "after": since_utc,
            "size": size,
            "sort": "desc",
            "sort_type": "score",
        }
        fetched = 0
        while fetched < limit:
            try:
                r = requests.get(base, params=params, timeout=30)
                r.raise_for_status()
                data = r.json().get("data", [])
                if not data:
                    break

                for d in data:
                    if d.get("score", 0) < min_upvotes:
                        continue
                    out.append({
                        "id": d.get("id"),
                        "subreddit": sub,
                        "created_utc": d.get("created_utc"),
                        "title": clean_text(d.get("title", "")),
                        "selftext": clean_text(d.get("selftext", "")),
                        "url": d.get("full_link") or f"https://reddit.com/{d.get('id')}",
                        "upvotes": d.get("score", 0),
                        "num_comments": d.get("num_comments", 0),
                        "source": "pushshift",
                        "fetched_at": now_iso(),
                    })

                fetched += len(data)
                # stop if we’ve reached the per-subreddit cap
                if fetched >= limit:
                    break

                # try to paginate backwards by last item's created_utc
                last = data[-1].get("created_utc")
                if last:
                    params["before"] = last
                time.sleep(0.6)
            except Exception as e:
                print("pushshift error:", e, file=sys.stderr)
                time.sleep(2)
                break
    return out

def scrape_reddit_api(subs: List[str], since_utc: int, limit: int, min_upvotes: int):
    if not praw:
        print("PRAW not installed, skipping Reddit API.", file=sys.stderr)
        return []
    cid = os.getenv("REDDIT_CLIENT_ID")
    secret = os.getenv("REDDIT_CLIENT_SECRET")
    ua = os.getenv("REDDIT_USER_AGENT", "reddit_research_agent/0.1")
    if not (cid and secret):
        print("Missing Reddit API credentials; set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET.", file=sys.stderr)
        return []

    reddit = praw.Reddit(client_id=cid, client_secret=secret, user_agent=ua)
    out = []
    for sub in subs:
        count = 0
        for post in reddit.subreddit(sub.replace("r/", "")).top(time_filter="year", limit=limit):
            if post.score < min_upvotes:
                continue
            if post.created_utc < since_utc:
                continue
            out.append({
                "id": post.id,
                "subreddit": sub,
                "created_utc": int(post.created_utc),
                "title": clean_text(post.title or ""),
                "selftext": clean_text(getattr(post, "selftext", "") or ""),
                "url": f"https://www.reddit.com{post.permalink}",
                "upvotes": int(post.score or 0),
                "num_comments": int(post.num_comments or 0),
                "source": "reddit_api",
                "fetched_at": now_iso(),
            })
            count += 1
            if count >= limit:
                break
        time.sleep(0.4)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--subs", nargs="+", required=True)
    ap.add_argument("--months", type=int, default=12)
    ap.add_argument("--limit", type=int, default=2000)
    ap.add_argument("--min-upvotes", type=int, default=20)
    ap.add_argument("--use-pushshift", action="store_true")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    ensure_dirs(args.out)
    since = month_ago_utc(args.months)

    rows = []
    if not args.use_pushshift:
        rows += scrape_reddit_api(args.subs, since, args.limit, args.min_upvotes)
    if args.use_pushshift or not rows:
        rows += scrape_pushshift(args.subs, since, args.limit, args.min_upvotes)

    # dedupe by id
    seen = set()
    deduped = []
    for r in rows:
        if r["id"] in seen:
            continue
        seen.add(r["id"])
        deduped.append(r)

    save_jsonl(deduped, args.out)
    print(f"Saved {len(deduped)} items to {args.out}")

if __name__ == "__main__":
    main()
