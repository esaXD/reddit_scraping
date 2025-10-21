# pipeline/scrape_reddit.py
import argparse, os, time, sys
from typing import List
from datetime import datetime, timedelta
import requests
from util import ensure_dirs, save_jsonl, clean_text, now_iso

def month_ago_utc(months: int) -> int:
    now = datetime.utcnow()
    start = now - timedelta(days=30*months)
    return int(start.timestamp())

def pushshift_by_subs(subs: List[str], since_utc: int, limit: int, min_upvotes: int):
    base = "https://api.pushshift.io/reddit/search/submission/"
    size = 250
    out = []
    for sub in subs:
        params = {
            "subreddit": sub.replace("r/",""),
            "after": since_utc,
            "size": size,
            "sort": "desc",
            "sort_type": "created_utc",   # zaman bazlı
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
                    if d.get("score",0) < min_upvotes: 
                        continue
                    out.append({
                        "id": d.get("id"),
                        "subreddit": sub,
                        "created_utc": d.get("created_utc"),
                        "title": clean_text(d.get("title","")),
                        "selftext": clean_text(d.get("selftext","")),
                        "url": d.get("full_link") or f"https://reddit.com/{d.get('id')}",
                        "upvotes": d.get("score",0),
                        "num_comments": d.get("num_comments",0),
                        "source": "pushshift",
                        "fetched_at": now_iso(),
                    })
                fetched += len(data)
                if fetched >= limit:
                    break
                last_ts = data[-1].get("created_utc")
                if not last_ts:
                    break
                params["before"] = last_ts
                time.sleep(0.5)
            except Exception as e:
                print("pushshift error:", e, file=sys.stderr)
                time.sleep(1.0)
                break
    return out

def pushshift_by_keywords(keywords: List[str], since_utc: int, limit: int, min_upvotes: int):
    if not keywords:
        return []
    base = "https://api.pushshift.io/reddit/search/submission/"
    size = 250
    out = []
    # basit: tüm Reddit'te keyword OR ile ara
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
        try:
            r = requests.get(base, params=params, timeout=30)
            r.raise_for_status()
            data = r.json().get("data", [])
            if not data:
                break
            for d in data:
                if d.get("score",0) < min_upvotes:
                    continue
                out.append({
                    "id": d.get("id"),
                    "subreddit": "r/"+str(d.get("subreddit","")),
                    "created_utc": d.get("created_utc"),
                    "title": clean_text(d.get("title","")),
                    "selftext": clean_text(d.get("selftext","")),
                    "url": d.get("full_link") or f"https://reddit.com/{d.get('id')}",
                    "upvotes": d.get("score",0),
                    "num_comments": d.get("num_comments",0),
                    "source": "pushshift_kw",
                    "fetched_at": now_iso(),
                })
            fetched += len(data)
            if fetched >= limit:
                break
            last_ts = data[-1].get("created_utc")
            if not last_ts:
                break
            params["before"] = last_ts
            time.sleep(0.5)
        except Exception as e:
            print("pushshift kw error:", e, file=sys.stderr)
            time.sleep(1.0)
            break
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
    args = ap.parse_args()

    ensure_dirs(args.out)
    since = month_ago_utc(args.months)

    # 1) Subreddit tabanlı dene
    rows = pushshift_by_subs(args.subs, since, args.limit, args.min_upvotes)

    # 2) Hiçbir şey gelmediyse: keyword tabanlı geniş arama
    if not rows and args.keywords:
        rows = pushshift_by_keywords(args.keywords, since, args.limit, args.min_upvotes)

    # dedupe
    seen, ded = set(), []
    for r in rows:
        if r["id"] in seen: 
            continue
        seen.add(r["id"]); ded.append(r)

    save_jsonl(ded, args.out)
    print(f"Saved {len(ded)} items to {args.out}")

if __name__ == "__main__":
    main()
