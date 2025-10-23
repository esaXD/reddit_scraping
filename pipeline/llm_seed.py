# pipeline/llm_seed.py
import argparse
import json
import os
import sys
from typing import List

MODEL = "gpt-4o"

SYSTEM = """You are a Reddit research planner. Given a user prompt, suggest up to __MAX_SUBS__ highly relevant subreddits.
Return strict JSON:
{
  "subreddits": [
    {"name": "r/...", "confidence": 0-1, "why": "...", "flags": ["nsfw?", ...]}
  ],
  "warnings": ["..."],
  "validation_hints": ["..."],
  "confidence": "high|medium|low"
}
- Prefer niche, on-topic subreddits over generic feeds.
- Exclude r/all, r/popular, r/technology unless absolutely necessary (and flag them).
- If unsure, keep confidence low and add a warning.
"""

GENERIC_SUBS = {
    "r/technology",
    "r/science",
    "r/worldnews",
    "r/news",
    "r/all",
    "r/popular",
    "r/askreddit",
}


def call_openai(prompt: str, max_subs: int):
    try:
        from openai import OpenAI
        from openai import APIConnectionError, RateLimitError, AuthenticationError, BadRequestError
    except ImportError as e:
        print(f"[seed] openai import hatası: {e}", file=sys.stderr, flush=True)
        return None

    if not os.getenv("OPENAI_API_KEY"):
        print("[seed] OPENAI_API_KEY tanımlı değil.", file=sys.stderr, flush=True)
        return None

    client = OpenAI()
    user_prompt = f"User prompt: {prompt}\nMaximum subreddits: {{max_subs}}"
    try:
        system_prompt = SYSTEM.replace("__MAX_SUBS__", str(max_subs))
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
            max_tokens=1000,
            response_format={"type": "json_object"},
        )
        return resp.choices[0].message.content

    except AuthenticationError as e:
        print(f"[seed] Auth hatası: {e}", file=sys.stderr, flush=True)
    except RateLimitError as e:
        print(f"[seed] Rate limit: {e}", file=sys.stderr, flush=True)
    except APIConnectionError as e:
        print(f"[seed] Bağlantı hatası: {e}", file=sys.stderr, flush=True)
    except BadRequestError as e:
        print(f"[seed] Geçersiz istek: {e}", file=sys.stderr, flush=True)
    except Exception as e:
        # Son çare: mümkünse response gövdesi
        msg = getattr(e, "message", None) or str(e)
        print(f"[seed] Genel hata: {msg}", file=sys.stderr, flush=True)
    return None



def normalize_subreddits(items):
    out = []
    seen = set()
    for entry in items or []:
        if isinstance(entry, dict):
            name = entry.get("name") or entry.get("subreddit")
            meta = entry
        else:
            name = str(entry)
            meta = {"why": "seed suggestion", "confidence": 0.0, "flags": []}
        if not name:
            continue
        name = name.strip()
        if not name.lower().startswith("r/"):
            name = "r/" + name.split("/")[-1]
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append({"name": name, "meta": meta})
    return out


def prune_generic(subs: List[dict], limit: int) -> List[dict]:
    filtered = [entry for entry in subs if entry["name"].lower() not in GENERIC_SUBS]
    if filtered:
        subs = filtered
    return subs[:limit]


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
    subs_top = prune_generic(subs_norm, args.max_subs)

    seed = {
        "prompt": args.prompt,
        "subreddits": subs_top,
        "topic_themes": data.get("topic_themes", []),
        "warnings": data.get("warnings", []),
        "validation_hints": data.get("validation_hints", []),
        "confidence": data.get("confidence", "unknown"),
        "timeframe_months": int(args.default_months),
        "min_upvotes": int(args.default_min_upvotes),
    }

    os.makedirs(os.path.dirname(args.out_json), exist_ok=True)
    with open(args.out_json, "w", encoding="utf-8") as fh:
        json.dump(seed, fh, ensure_ascii=False, indent=2)

    subs_flat = [s["name"] for s in subs_top]
    with open(args.out_subs, "w", encoding="utf-8") as fh:
        fh.write("\n".join(subs_flat))

    with open(args.out_keywords, "w", encoding="utf-8") as fh:
        fh.write("")

    print("LLM seed discovery:")
    print("  Subreddits:", " ".join(subs_flat) or "(none)")
    print("  Keywords: (none)")
    print("  Timeframe (months):", seed["timeframe_months"])
    print("  Min upvotes:", seed["min_upvotes"])
    print("  Confidence:", seed["confidence"])
    if seed["warnings"]:
        print("  Warnings:", "; ".join(seed["warnings"]))


if __name__ == "__main__":
    main()
