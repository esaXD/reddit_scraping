# pipeline/llm_seed.py
import argparse
import json
import os
import shlex

MODEL = "gpt-4o-mini-search-preview"

SYSTEM = (
    "You are an expert research planner who can browse the web and Reddit indices. "
    "Given a user prompt, propose the most relevant subreddits to investigate and the search "
    "keywords/filters that will retrieve high-signal posts. Return strict JSON only."
)

USER_TMPL = """User prompt: {prompt}
Max subreddits requested: {max_subs}
Required JSON schema:
{{
  "subreddits": [{{"name": "r/...", "why": "..."}}, ... up to {max_subs}],
  "keywords": ["keyword or phrase", "..."],
  "filters": {{"must_include": ["..."], "should_include": ["..."], "exclude": ["..."]}},
  "search_queries": ["search query for PullPush/Reddit", "..."],
  "timeframe_months": integer (>=1),
  "min_upvotes": integer (>=0),
  "confidence": "high|medium|low",
  "notes": "Optional short guidance."
}}
- Subreddit names must include the r/ prefix and be unique.
- Keywords should focus on the user prompt context; include multi-word phrases where helpful.
- Filters.must_include are terms that absolutely must match; should_include are nice-to-have boosters; exclude removes noisy topics.
- If you're uncertain, still provide your best guess but reduce confidence.
- Prefer specific, niche subreddits over generic ones when relevant.
"""


def call_openai(prompt: str, max_subs: int):
    try:
        from openai import OpenAI
    except ImportError:
        return None

    if not os.getenv("OPENAI_API_KEY"):
        return None

    client = OpenAI()
    user = USER_TMPL.format(prompt=prompt, max_subs=max_subs)
    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": user},
            ],
            temperature=0.2,
            max_tokens=800,
            response_format={"type": "json_object"},
        )
        return resp.choices[0].message.content
    except Exception:
        return None


def normalize_subreddits(items):
    raw = items or []
    out, seen = [], set()
    for entry in raw:
        if isinstance(entry, dict):
            name = entry.get("name") or entry.get("subreddit") or ""
        else:
            name = str(entry)
        name = name.strip()
        if not name:
            continue
        if not name.lower().startswith("r/"):
            name = "r/" + name.split("/")[-1]
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def normalize_keywords(raw):
    if not raw:
        return []
    if isinstance(raw, list):
        items = raw
    else:
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                items = data
            else:
                items = [raw]
        except Exception:
            try:
                items = shlex.split(str(raw))
            except Exception:
                items = str(raw).replace(",", " ").split()
    cleaned = []
    seen = set()
    for item in items:
        if item is None:
            continue
        text = str(item).strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(text)
    return cleaned


def fallback_seed(prompt: str, max_subs: int, default_months: int, default_min_upvotes: int):
    # Basic fallback: use noun-like tokens as keywords, no subreddit suggestions.
    raw_tokens = []
    try:
        raw_tokens = shlex.split(prompt)
    except Exception:
        raw_tokens = prompt.split()
    raw_tokens = [tok.strip(" ,.!?\"'").lower() for tok in raw_tokens if tok.strip()]
    keywords = sorted(set(tok for tok in raw_tokens if len(tok) > 3))[:max_subs]
    return {
        "subreddits": [],
        "keywords": keywords,
        "filters": {"must_include": keywords[:3], "should_include": [], "exclude": []},
        "search_queries": [],
        "timeframe_months": default_months,
        "min_upvotes": default_min_upvotes,
        "confidence": "low",
        "notes": "Fallback seed (LLM unavailable). Please refine manually.",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prompt", required=True)
    ap.add_argument("--max-subs", type=int, default=8)
    ap.add_argument("--default-months", type=int, default=12)
    ap.add_argument("--default-min-upvotes", type=int, default=20)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-subs", required=True)
    ap.add_argument("--out-keywords", required=True)
    args = ap.parse_args()

    raw = call_openai(args.prompt, args.max_subs)
    data = None
    if raw:
        try:
            data = json.loads(raw)
        except Exception:
            data = None

    if not data:
        data = fallback_seed(args.prompt, args.max_subs, args.default_months, args.default_min_upvotes)

    subs = normalize_subreddits(data.get("subreddits"))
    keywords = normalize_keywords(data.get("keywords"))

    filters = data.get("filters") or {}
    must_include = normalize_keywords(filters.get("must_include"))
    should_include = normalize_keywords(filters.get("should_include"))
    exclude = normalize_keywords(filters.get("exclude"))

    all_keywords = []
    for lst in (keywords, must_include, should_include):
        for item in lst:
            if item not in all_keywords:
                all_keywords.append(item)

    seed = {
        "prompt": args.prompt,
        "subreddits": subs[: args.max_subs],
        "keywords": all_keywords,
        "filters": {
            "must_include": must_include,
            "should_include": should_include,
            "exclude": exclude,
        },
        "search_queries": data.get("search_queries", []),
        "timeframe_months": int(data.get("timeframe_months") or args.default_months),
        "min_upvotes": int(data.get("min_upvotes") or args.default_min_upvotes),
        "confidence": data.get("confidence", "unknown"),
        "notes": data.get("notes", ""),
    }

    os.makedirs(os.path.dirname(args.out_json), exist_ok=True)
    with open(args.out_json, "w", encoding="utf-8") as fh:
        json.dump(seed, fh, ensure_ascii=False, indent=2)

    with open(args.out_subs, "w", encoding="utf-8") as fh:
        fh.write("\n".join(seed["subreddits"]))

    with open(args.out_keywords, "w", encoding="utf-8") as fh:
        fh.write("\n".join(all_keywords))

    print("LLM seed discovery:")
    print("  Subreddits:", " ".join(seed["subreddits"]) or "(none)")
    print("  Keywords:", ", ".join(all_keywords) or "(none)")
    print("  Must include:", ", ".join(must_include) or "(none)")
    print("  Should include:", ", ".join(should_include) or "(none)")
    print("  Exclude:", ", ".join(exclude) or "(none)")
    print("  Timeframe (months):", seed["timeframe_months"])
    print("  Min upvotes:", seed["min_upvotes"])
    print("  Confidence:", seed["confidence"])
    if seed["notes"]:
        print("  Notes:", seed["notes"])


if __name__ == "__main__":
    main()
