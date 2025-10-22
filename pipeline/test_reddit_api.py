# pipeline/test_reddit_api.py
import argparse
import json
import os
import shlex
import time
from typing import List, Dict

import requests

from discover_subs import (
    BASE as SEARCH_BASE,
    build_search_queries,
    after_ts,
)


def _check(url: str, params: Dict[str, str], label: str, timeout: int = 20):
    started = time.time()
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        elapsed = time.time() - started
        status = resp.status_code
        ok = resp.ok
        body = ""
        items = None
        if ok:
            try:
                payload = resp.json()
                items = len(payload.get("data", [])) if isinstance(payload, dict) else None
            except Exception:
                items = None
        else:
            body = resp.text[:200].replace("\n", " ").strip()
        return {
            "label": label,
            "url": resp.url,
            "status": status,
            "ok": ok,
            "elapsed_sec": round(elapsed, 3),
            "items": items,
            "body_preview": body,
        }
    except Exception as exc:
        elapsed = time.time() - started
        return {
            "label": label,
            "url": requests.Request("GET", url, params=params).prepare().url,
            "status": None,
            "ok": False,
            "elapsed_sec": round(elapsed, 3),
            "items": None,
            "body_preview": str(exc),
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--prompt", required=True)
    parser.add_argument("--keywords", default="")
    parser.add_argument("--keywords-json", default="")
    parser.add_argument("--subs", default="")
    parser.add_argument("--months", type=int, default=12)
    parser.add_argument("--max-subs", type=int, default=8)
    parser.add_argument("--out", default="data/api_diagnostics.json")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    keyword_tokens = []
    if args.keywords:
        try:
            keyword_tokens.extend(shlex.split(args.keywords))
        except Exception:
            keyword_tokens.extend(args.keywords.replace(",", " ").split())
    if args.keywords_json:
        try:
            data = json.loads(args.keywords_json)
            if isinstance(data, list):
                keyword_tokens.extend(str(x).strip() for x in data if str(x).strip())
        except Exception:
            pass
    seen = set()
    keywords_clean = []
    for tok in keyword_tokens:
        key = tok.lower()
        if key in seen:
            continue
        seen.add(key)
        keywords_clean.append(tok)
    keyword_blob = " ".join(keywords_clean)

    queries = build_search_queries(args.prompt, keyword_blob, max_terms=args.max_subs * 2)
    checks = []
    if not queries:
        checks.append({
            "label": "search_query",
            "url": "",
            "status": None,
            "ok": False,
            "elapsed_sec": 0.0,
            "items": None,
            "body_preview": "No keywords generated from prompt/keywords; cannot test PullPush search.",
        })
    else:
        for idx, terms in enumerate(queries, 1):
            query = " OR ".join(terms)
            params = {
                "q": query,
                "after": after_ts(args.months),
                "size": 25,
                "sort": "desc",
                "sort_type": "created_utc",
            }
            label = "search_query" if len(queries) == 1 else f"search_query_{idx}"
            result = _check(SEARCH_BASE, params, label)
            checks.append(result)
            if result.get("ok") and result.get("items"):
                break

    subs = [s.strip() for s in args.subs.split() if s.strip()]
    for sub in subs[: max(args.max_subs, 5)]:
        name = sub.split("/")[-1]
        params = {
            "subreddit": name,
            "after": after_ts(args.months),
            "size": 25,
            "sort": "desc",
            "sort_type": "created_utc",
        }
        checks.append(_check(SEARCH_BASE, params, f"subreddit:{name}"))

    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump({"checks": checks}, fh, ensure_ascii=False, indent=2)

    bad = [c for c in checks if not c.get("ok")]
    for c in checks:
        if c["ok"]:
            print(f"[OK] {c['label']} status={c['status']} items={c.get('items')}")
        else:
            print(f"[FAIL] {c['label']} status={c.get('status')} detail={c.get('body_preview')}")

    status = "healthy" if not bad else "degraded"
    print(f"Reddit API health: {status}")
    env_file = os.getenv("GITHUB_ENV")
    if env_file:
        with open(env_file, "a", encoding="utf-8") as env:
            env.write(f"REDDIT_API_STATUS={status}\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
