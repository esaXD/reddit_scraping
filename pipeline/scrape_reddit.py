# pipeline/scrape_reddit.py
import argparse
import json
import os
import time
import sys
from typing import List
from datetime import datetime, timedelta
import requests
from util import ensure_dirs, save_jsonl, clean_text, now_iso
from discover_subs import english_keywords

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
    print("pullpush error:", last_err, file=sys.stderr, flush=True)
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
            print(f"[subs:{sub}] batch={len(data)} fetched={fetched + len(data)}", flush=True)
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
        print(f"[subs:{sub}] total_kept={len(out)}", flush=True)
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
        print(f"[kw] query='{q[:80]}' batch={len(data)} fetched={fetched + len(data)}", flush=True)
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
    print(f"[kw] query='{q[:80]}' total_kept={len(out)}", flush=True)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--subs", nargs="+", required=True)
    ap.add_argument("--prompt", default="")
    ap.add_argument("--months", type=int, default=12)
    ap.add_argument("--limit", type=int, default=2000)
    ap.add_argument("--min-upvotes", type=int, default=20)
    ap.add_argument("--keywords", nargs="*", default=[])
    ap.add_argument("--keywords-json", default="")
    ap.add_argument("--exclude-keywords-json", default="")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    ensure_dirs(a.out)

    prompt_text = a.prompt or ""
    raw_keywords = list(a.keywords or [])
    if a.keywords_json:
        try:
            extra = json.loads(a.keywords_json)
            if isinstance(extra, list):
                raw_keywords.extend(str(x) for x in extra if str(x).strip())
        except Exception:
            pass
    keywords_clean = []
    seen_kw = set()
    for kw in raw_keywords:
        if kw is None:
            continue
        text_kw = str(kw).strip()
        if not text_kw:
            continue
        key_kw = text_kw.lower()
        if key_kw in seen_kw:
            continue
        seen_kw.add(key_kw)
        keywords_clean.append(text_kw)
    
    keyword_input = " ".join(keywords_clean)
    
    exclude_terms = []
    if a.exclude_keywords_json:
        try:
            extra_ex = json.loads(a.exclude_keywords_json)
            if isinstance(extra_ex, list):
                exclude_terms = [str(x).strip().casefold() for x in extra_ex if str(x).strip()]
        except Exception:
            pass
    
    if keywords_clean:
        print("Seed keywords:", ", ".join(keywords_clean), flush=True)
    
    base_months = max(1, int(a.months))
    base_min_upvotes = max(0, int(a.min_upvotes))

    # use user-provided time window and filters directly
    since = month_ago_utc(base_months)

    subs_rows = pushshift_by_subs(a.subs, since, a.limit, base_min_upvotes)
    print(f"[scrape] subreddit pass collected {len(subs_rows)} rows", flush=True)

    rows_kw = []
    if keywords_clean:
        search_terms = english_keywords("", keyword_input)[:16] if keyword_input else []
        if not search_terms:
            search_terms = keywords_clean
        search_terms = [term for term in search_terms if term]
        if search_terms:
            result = pushshift_by_keywords(search_terms, since, a.limit, base_min_upvotes)
            rows_kw.extend(result)
            print(f"[scrape] keyword search collected {len(result)} rows", flush=True)

    rows = subs_rows + rows_kw
    print(f"[scrape] total raw rows before dedupe: {len(rows)}", flush=True)

    ded = []
    seen = set()
    for r in rows:
        rid = r.get("id")
        if rid in seen:
            continue
        seen.add(rid)
        ded.append(r)

    if exclude_terms:
        before = len(ded)
        filtered_ex = []
        for row in ded:
            text_row = f"{row.get('title','')} {row.get('selftext','')}".casefold()
            if any(term in text_row for term in exclude_terms if term):
                continue
            filtered_ex.append(row)
        removed = before - len(filtered_ex)
        ded = filtered_ex
        if removed:
            print(f"[filter] removed {removed} posts based on exclude keywords.", flush=True)

    if keywords_clean:
        lowers = [k.casefold() for k in keywords_clean]
        filtered_kw = []
        for row in ded:
            text_row = f"{row.get('title','')} {row.get('selftext','')}".casefold()
            if any(term in text_row for term in lowers):
                filtered_kw.append(row)
        print(f"[filter] keyword pass kept {len(filtered_kw)} posts", flush=True)
        ded = filtered_kw

    original_count = len(ded)
    save_jsonl(ded, a.out)
    print(f"Saved {len(ded)} items to {a.out} (from {original_count} collected)", flush=True)

if __name__ == "__main__":
    main()
