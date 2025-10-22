# pipeline/llm_seed.py
import argparse
import json
import os
import shlex
import sys
from typing import Dict, List

MODEL = "gpt-4o-mini"

SYSTEM = """You are Reddit Research Planner, an assistant for crafting Reddit research seeds.
Using the given user prompt:
- infer the core topic(s), target audience, and user intent
- consult your internal knowledge of Reddit (subreddit focus, activity, audience) to recommend sources
- produce a structured JSON plan that maximizes relevance while preserving breadth for later filtering
- avoid overly generic subreddits (r/all, r/popular, r/technology) unless you justify them

Return JSON matching this schema exactly:
{
  "subreddits": [
    {"name": "r/...", "confidence": 0.0-1.0, "why": "...", "signal_score": 0-10,
     "volume_hint": "high|medium|low", "flags": ["nsfw?"...], "source": "llm|catalog"},
    ...
  ],
  "topic_themes": [
    {"name": "...", "audience": "...", "pain_points": ["..."], "desired_outcomes": ["..."],
     "seed_keywords": ["...","..."]}
  ],
  "keyword_plan": {
    "core": [{"phrase": "...", "explanation": "...", "intent_score": 0-1, "language": "en|tr|..."}],
    "long_tail": [...],
    "exploratory": [...],
    "negative": ["...", "..."]
  },
  "search_queries": [{"query": "...", "focus": "..."}],
  "filters": {
    "must_include": ["..."],
    "should_include": ["..."],
    "exclude": ["..."],
    "languages": ["en","tr",...]
  },
  "timeframe_months": integer >= 1,
  "min_upvotes": integer >= 0,
  "min_comments": integer >= 0,
  "min_word_count": integer >= 0,
  "validation_hints": ["..."],
  "confidence": "high|medium|low",
  "warnings": ["..."]
}

Guidelines:
- Provide 5-10 subreddits; include at least 3 niche/high-signal ones (confidence >=0.6) when possible.
- Mention why each subreddit is relevant (link to prompt intent, user base, typical discussions).
- Keywords: include multi-word phrases, both English and original-language variants if applicable.
- must_include should contain only precise, low-noise phrases (avoid generic words like "market", "technology").
- Should_include can capture broader synonyms and adjacent topics.
- Negative should list obvious off-topic concepts that might dominate results.
- search_queries should be OR-based strings we can pass to PullPush/Reddit.
- Provide validation hints (e.g., "Check subscriber count", "Verify recent posts contain {keyword}").
- If you're unsure, keep confidence low and explain gaps.
"""

GENERIC_STOP = {
    "market",
    "markets",
    "marketplace",
    "analysis",
    "insight",
    "insights",
    "business",
    "industry",
    "industries",
    "technology",
    "tech",
    "innovation",
    "trend",
    "trends",
    "strategy",
    "product",
    "products",
    "news",
    "general",
    "discussion",
    "info",
}

def call_openai(prompt: str, max_subs: int):
    try:
        from openai import OpenAI
    except ImportError:
        return None

    if not os.getenv("OPENAI_API_KEY"):
        return None

    client = OpenAI()
    user_prompt = f"User prompt: {prompt}\nMaximum subreddits: {max_subs}"
    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
            max_tokens=1400,
            response_format={"type": "json_object"},
        )
        return resp.choices[0].message.content
    except Exception:
        return None


def dedupe(seq: List[str]) -> List[str]:
    seen = set()
    out = []
    for item in seq:
        if not item:
            continue
        txt = str(item).strip()
        if not txt:
            continue
        key = txt.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(txt)
    return out


def normalize_subreddits(items):
    raw = items or []
    out = []
    seen = set()
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
        out.append({"name": name, "raw": entry})
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
    cleaned = dedupe(items)
    return cleaned


def refine_filters(filters: Dict[str, List[str]], keywords: List[str]):
    filters = filters or {}
    must = normalize_keywords(filters.get("must_include"))
    should = normalize_keywords(filters.get("should_include"))
    exclude = normalize_keywords(filters.get("exclude"))

    refined_must = []
    for term in must:
        low = term.lower()
        if low in GENERIC_STOP or len(term) <= 3:
            should.append(term)
        else:
            refined_must.append(term)

    if not refined_must:
        for term in keywords:
            low = term.lower()
            if low in GENERIC_STOP or len(term) <= 3:
                continue
            refined_must.append(term)
            break

    return {
        "must_include": dedupe(refined_must),
        "should_include": dedupe(should),
        "exclude": exclude,
        "languages": filters.get("languages") or [],
    }


def reduce_subreddits(subs_normalized, max_subs):
    scored = []
    for entry in subs_normalized:
        raw = entry["raw"]
        if isinstance(raw, dict):
            confidence = float(raw.get("confidence", 0.0))
            score = float(raw.get("signal_score", confidence * 10))
            scored.append((score, entry["name"], raw))
        else:
            scored.append((0.0, entry["name"], {"why": "LLM suggestion", "confidence": 0.0}))

    scored.sort(reverse=True, key=lambda x: x[0])
    top = []
    seen = set()
    for _, name, raw in scored:
        if name.lower() in seen:
            continue
        seen.add(name.lower())
        top.append({"name": name, "meta": raw})
        if len(top) >= max_subs:
            break
    return top


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
    if not raw:
        print("LLM seed discovery failed (no response). Aborting.", file=sys.stderr, flush=True)
        sys.exit(1)
    try:
        data = json.loads(raw)
    except Exception:
        print("LLM seed discovery returned invalid JSON. Aborting.", file=sys.stderr, flush=True)
        sys.exit(1)

    subs_norm = normalize_subreddits(data.get("subreddits"))
    subs_top = reduce_subreddits(subs_norm, args.max_subs)
    keywords = normalize_keywords(data.get("keyword_plan", {}).get("core") or data.get("keywords"))

    filters = refine_filters(data.get("filters"), keywords)

    seed = {
        "prompt": args.prompt,
        "subreddits": subs_top,
        "topic_themes": data.get("topic_themes", []),
        "keywords": keywords,
        "keyword_plan": data.get("keyword_plan", {}),
        "filters": filters,
        "search_queries": data.get("search_queries", []),
        "timeframe_months": int(data.get("timeframe_months") or args.default_months),
        "min_upvotes": int(data.get("min_upvotes") or args.default_min_upvotes),
        "min_comments": int(data.get("min_comments") or 0),
        "min_word_count": int(data.get("min_word_count") or 0),
        "validation_hints": data.get("validation_hints", []),
        "confidence": data.get("confidence", "unknown"),
        "warnings": data.get("warnings", []),
    }

    os.makedirs(os.path.dirname(args.out_json), exist_ok=True)
    with open(args.out_json, "w", encoding="utf-8") as fh:
        json.dump(seed, fh, ensure_ascii=False, indent=2)

    subs_flat = [s["name"] for s in subs_top]
    with open(args.out_subs, "w", encoding="utf-8") as fh:
        fh.write("\n".join(subs_flat))

    with open(args.out_keywords, "w", encoding="utf-8") as fh:
        fh.write("\n".join(seed["keywords"]))

    print("LLM seed discovery:")
    print("  Subreddits:", " ".join(subs_flat) or "(none)")
    print("  Keywords:", ", ".join(seed["keywords"]) or "(none)")
    print("  Must include:", ", ".join(filters["must_include"]) or "(none)")
    print("  Should include:", ", ".join(filters["should_include"]) or "(none)")
    print("  Exclude:", ", ".join(filters["exclude"]) or "(none)")
    print("  Timeframe (months):", seed["timeframe_months"])
    print("  Min upvotes:", seed["min_upvotes"])
    print("  Confidence:", seed["confidence"])
    if seed["warnings"]:
        print("  Warnings:", "; ".join(seed["warnings"]))


if __name__ == "__main__":
    main()
