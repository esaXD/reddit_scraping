# pipeline/scrape_reddit.py
import argparse
import os
import time
import sys
from typing import List
from datetime import datetime, timedelta
import requests
from util import ensure_dirs, save_jsonl, clean_text, now_iso
from discover_subs import english_keywords, build_search_queries, ASCII_FALLBACK

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
    base_months = max(1, int(a.months))
    base_min_upvotes = max(0, int(a.min_upvotes))
    target_posts = max(50, min(a.limit, 200))

    # Subreddit sonuçları + keyword sonuçları = birleşim
    prompt_text = a.prompt or ""
    keyword_input = " ".join(a.keywords or [])
    keyword_variants = english_keywords(prompt_text, keyword_input)
    search_strategies = build_search_queries(prompt_text, keyword_input) if (prompt_text or keyword_input) else []

    attempts = []
    attempts.append(
        {"months": base_months, "min_upvotes": base_min_upvotes, "label": "base window"}
    )
    if base_min_upvotes > 5:
        attempts.append(
            {"months": base_months, "min_upvotes": max(base_min_upvotes // 2, 3), "label": "lower upvotes"}
        )
    if base_months < 24:
        attempts.append(
            {"months": max(24, base_months * 2), "min_upvotes": max(base_min_upvotes // 2, 0), "label": "older window"}
        )
    attempts.append(
        {"months": max(36, base_months * 3), "min_upvotes": 0, "label": "broad fallback"}
    )

    ded = []
    seen = set()
    for attempt in attempts:
        months_cur = attempt["months"]
        min_upvotes_cur = attempt["min_upvotes"]
        label = attempt["label"]
        since = month_ago_utc(months_cur)

        print(f"--- Attempt '{label}' months={months_cur} min_upvotes={min_upvotes_cur} ---", flush=True)
        subs_rows = pushshift_by_subs(a.subs, since, a.limit, min_upvotes_cur)
        print(f"[attempt:{label}] subs_rows={len(subs_rows)}", flush=True)

        kw_rows = []
        if search_strategies:
            for idx, terms in enumerate(search_strategies, 1):
                results = pushshift_by_keywords(terms, since, a.limit, min_upvotes_cur)
                kw_rows.extend(results)
                print(f"[attempt:{label}] keyword_strategy_{idx} added={len(results)} cumulative={len(kw_rows)}", flush=True)
                if len(kw_rows) >= max(40, a.limit // 5):
                    break
        else:
            print(f"[attempt:{label}] no keyword strategies available", flush=True)

        rows = subs_rows + kw_rows
        added = 0
        for r in rows:
            if r["id"] in seen:
                continue
            seen.add(r["id"])
            ded.append(r)
            added += 1
        print(f"[attempt:{label}] added={added} total={len(ded)}", flush=True)

        if len(ded) >= target_posts:
            print(f"[attempt:{label}] reached target {target_posts}; stopping attempts.", flush=True)
            break

    original_count = len(ded)

    if keyword_variants:
        match_terms = []
        for term in keyword_variants:
            low = term.casefold()
            match_terms.append(low)
            match_terms.append(low.translate(ASCII_FALLBACK))
        match_terms = [m for m in {t for t in match_terms if t}]

        broad_terms = {part.strip().casefold() for term in keyword_variants for part in term.split()}
        broad_terms |= {bt.translate(ASCII_FALLBACK) for bt in broad_terms}
        broad_terms = {t for t in broad_terms if t}

        def _matches(row, terms):
            text = f"{row.get('title','')} {row.get('selftext','')}".casefold()
            return any(term in text for term in terms)

        filtered = [r for r in ded if _matches(r, match_terms)]
        if len(filtered) < 15 and broad_terms:
            print(f"[filter] strict kept {len(filtered)} posts; retrying with broader tokens.", flush=True)
            filtered = [r for r in ded if _matches(r, broad_terms)]

        if not filtered and ded:
            print("[filter] all posts removed by keyword filter; returning unfiltered data.", flush=True)
        else:
            ded = filtered

    save_jsonl(ded, a.out)
    print(f"Saved {len(ded)} items to {a.out} (from {original_count} collected)", flush=True)

if __name__ == "__main__":
    main()
