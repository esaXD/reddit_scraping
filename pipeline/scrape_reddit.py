# pipeline/scrape_reddit.py
import argparse
import os
import time
import sys
from typing import List
from datetime import datetime, timedelta
import requests
from util import ensure_dirs, save_jsonl, clean_text, now_iso
from discover_subs import build_keywords, build_search_terms, ASCII_FALLBACK

BASE = "https://api.pullpush.io/reddit/search/submission/"  # PullPush mirror

def month_ago_utc(months: int) -> int:
    now = datetime.utcnow()
    start = now - timedelta(days=30 * months)
    return int(start.timestamp())

def _safe_permalink(subreddit: str, rid: str) -> str:
    sub = (subreddit or "").replace("r/", "")
    rid = rid or ""
    if sub:
        return f"https://www.reddit.com/r/{sub}/comments/{rid}/"
    return f"https://www.reddit.com/comments/{rid}/"

def _req_with_retry(params, max_retry: int = 3):
    last_err = None
    for i in range(max_retry):
        try:
            r = requests.get(BASE, params=params, timeout=30)
            r.raise_for_status()
            return r.json().get("data", [])
        except Exception as e:
            last_err = e
            time.sleep(0.6 * (i + 1))
    print("pullpush error:", last_err, file=sys.stderr)
    return []

def pushshift_by_subs(subs: List[str], since_utc: int, limit: int, min_upvotes: int):
    size = 100  # daha küçük batch daha stabil
    out = []
    for sub in subs:
        params = {
            "subreddit": sub.replace("r/", ""),
            "after": since_utc,
            "size": size,
            "sort": "desc",
            "sort_type": "created_utc",
        }
        fetched = 0
        while fetched < limit:
            data = _req_with_retry(params)
            if not data:
                break
            for d in data:
                if d.get("score", 0) < min_upvotes:
                    continue
                rid = d.get("id")
                subname = d.get("subreddit") or sub.replace("r/", "")
                out.append({
                    "id": rid,
                    "subreddit": "r/" + subname,
                    "created_utc": d.get("created_utc"),
                    "title": clean_text(d.get("title", "")),
                    "selftext": clean_text(d.get("selftext", "")),
                    "url": d.get("full_link") or _safe_permalink(subname, rid),
                    "upvotes": d.get("score", 0),
                    "num_comments": d.get("num_comments", 0),
                    "source": "pullpush_sub",
                    "fetched_at": now_iso(),
                })
            fetched += len(data)
            if fetched >= limit:
                break
            last_ts = data[-1].get("created_utc")
            if not last_ts:
                break
            params["before"] = int(last_ts)  # .0 floatlardan kaçın
            time.sleep(0.3)
    return out

def pushshift_by_keywords(keywords: List[str], since_utc: int, limit: int, min_upvotes: int):
    if not keywords:
        return []
    size = 100
    out = []
    q = " OR ".join(keywords)
    params = {
        "q": q,
        "after": since_utc,
        "size": size,
        "sort": "desc",
        "sort_type": "created_utc",
    }
    fetched = 0
    while fetched < limit:
        data = _req_with_retry(params)
        if not data:
            break
        for d in data:
            if d.get("score", 0) < min_upvotes:
                continue
            rid = d.get("id")
            subname = d.get("subreddit", "")
            out.append({
                "id": rid,
                "subreddit": "r/" + str(subname),
                "created_utc": d.get("created_utc"),
                "title": clean_text(d.get("title", "")),
                "selftext": clean_text(d.get("selftext", "")),
                "url": d.get("full_link") or _safe_permalink(subname, rid),
                "upvotes": d.get("score", 0),
                "num_comments": d.get("num_comments", 0),
                "source": "pullpush_kw",
                "fetched_at": now_iso(),
            })
        fetched += len(data)
        if fetched >= limit:
            break
        last_ts = data[-1].get("created_utc")
        if not last_ts:
            break
        params["before"] = int(last_ts)
        time.sleep(0.3)
    return out

def main():
    import json
    ap = argparse.ArgumentParser()
    ap.add_argument("--subs", nargs="+", required=True)
    ap.add_argument("--prompt", default="")
    ap.add_argument("--months", type=int, default=12)
    ap.add_argument("--limit", type=int, default=2000)
    ap.add_argument("--min-upvotes", type=int, default=20)
    ap.add_argument("--keywords", nargs="*", default=[])
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    ensure_dirs(a.out)
    since = month_ago_utc(a.months)

    # Subreddit sonuçları + keyword sonuçları = birleşim
    prompt_text = a.prompt or ""
    keyword_input = " ".join(a.keywords or [])
    keyword_variants = build_keywords(prompt_text, keyword_input)
    search_terms = build_search_terms(prompt_text, keyword_input) if (prompt_text or keyword_input) else []

    rows_subs = pushshift_by_subs(a.subs, since, a.limit, a.min_upvotes)
    rows_kw = pushshift_by_keywords(search_terms, since, a.limit, a.min_upvotes) if search_terms else []
    rows = rows_subs + rows_kw

    # dedupe by id
    seen = set()
    ded = []
    for r in rows:
        if r["id"] in seen:
            continue
        seen.add(r["id"])
        ded.append(r)

    if keyword_variants:
        match_terms = []
        for term in keyword_variants:
            low = term.casefold()
            match_terms.append(low)
            match_terms.append(low.translate(ASCII_FALLBACK))
        match_terms = [m for m in {t for t in match_terms if t}]

        def _matches(row):
            text = f"{row.get('title','')} {row.get('selftext','')}".casefold()
            return any(term in text for term in match_terms)

        ded = [r for r in ded if _matches(r)]

    save_jsonl(ded, a.out)
    print(f"Saved {len(ded)} items to {a.out}")

if __name__ == "__main__":
    main()
