# pipeline/scrape_reddit.py
import argparse, os, time, sys, math
from typing import List
from datetime import datetime, timedelta
import requests
from util import ensure_dirs, save_jsonl, clean_text, now_iso

BASE = "https://api.pullpush.io/reddit/search/submission/"  # pullpush mirror

def month_ago_utc(months: int) -> int:
    now = datetime.utcnow()
    start = now - timedelta(days=30*months)
    return int(start.timestamp())

def _safe_permalink(subreddit: str, rid: str) -> str:
    sub = (subreddit or "").replace("r/","")
    rid = rid or ""
    # En güvenlisi: /comments/{id}
    if sub:
        return f"https://www.reddit.com/r/{sub}/comments/{rid}/"
    return f"https://www.reddit.com/comments/{rid}/"

def _req_with_retry(params, max_retry=3):
    last_err = None
    for i in range(max_retry):
        try:
            r = requests.get(BASE, params=params, timeout=30)
            r.raise_for_status()
            return r.json().get("data", [])
        except Exception as e:
            last_err = e
            # kısa geri bekleme (500 vs durumlarında)
            time.sleep(0.6 * (i + 1))
    print("pullpush error:", last_err, file=sys.stderr)
    return []

def pushshift_by_subs(subs: List[str], since_utc: int, limit: int, min_upvotes: int):
    size = 100  # daha düşük batch daha stabil
    out = []
    for sub in subs:
        params = {
            "subreddit": sub.replace("r/",""),
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
                if d.get("score",0) < min_upvotes:
                    continue
                rid = d.get("id")
                subname = d.get("subreddit") or sub.replace("r/","")
                out.append({
                    "id": rid,
                    "subreddit": "r/"+subname,
                    "created_utc": d.get("created_utc"),
                    "title": clean_text(d.get("title","")),
                    "selftext": clean_text(d.get("selftext","")),
                    "url": d.get("full_link") or _safe_permalink(subname, rid),
                    "upvotes": d.get("score",0),
                    "num_comments": d.get("num_comments",0),
                    "source": "pullpush_sub",
                    "fetched_at": now_iso(),
                })
            fetched += len(data)
            if fetched >= limit:
                break
            last_ts = data[-1].get("created_utc")
            if not last_ts:
                break
            # float '... .0' hatalarından kaçın
            params["before"] = int(last_ts)
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
            if d.get("score",0) < min_upvotes:
                continue
            rid = d.get("id")
            subname = d.get("subreddit","")
            out.append({
                "id": rid,
                "subreddit": "r/"+str(subname),
                "created_utc": d.get("created_utc"),
                "title": clean_text(d.get("title","")),
                "selftext": clean_text(d.get("selftext","")),
                "url": d.get("full_link") or _safe_permalink(subname, rid),
                "upvotes": d.get("score",0),
                "num_comments": d.get("num_comments",0),
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
    ap.add_argument("--months", type=int, default=12)
    ap.add_argument("--limit", type=int, default=2000)
    ap.add_argument("--min-upvotes", type=int, default=20)
    ap.add_argument("--keywords", nargs="*", default=[])
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    ensure_dirs(a.out)
    since = month_ago_utc(a.months)

    rows = pushshift_by_subs(a.subs, since, a.limit, a.min_upvotes)
    # Sub’lardan hiç gelmezse keyword ile tüm reddit araması
    if not rows and a.keywords:
        rows = pushshift_by_keywords(a.keywords, since, a.limit, a.min_upvotes)

    # dedupe
    seen, ded = set(), []
    for r in rows:
        if r["id"] in seen:
            continue
        seen.add(r["id"])
        ded.append(r)

    save_jsonl(ded, a.out)
    print(f"Saved {len(ded)} items to {a.out}")

if __name__ == "__main__":
    main()
