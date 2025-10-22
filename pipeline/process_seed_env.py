import argparse
import json
import os
import shlex
from pathlib import Path


def dedupe(sequence):
    out = []
    seen = set()
    for item in sequence:
        if not item:
            continue
        text = str(item).strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def normalize_user_keywords(raw: str):
    if not raw:
        return []
    try:
        parts = shlex.split(raw)
    except Exception:
        parts = raw.replace(",", " ").split()
    return dedupe(parts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed-json", required=True)
    ap.add_argument("--user-keywords", default="")
    ap.add_argument("--env-json-out", required=True)
    ap.add_argument("--env-file", required=True)
    args = ap.parse_args()

    seed = json.loads(Path(args.seed_json).read_text(encoding="utf-8"))
    subs_entries = seed.get("subreddits", [])
    subs_names = []
    for entry in subs_entries:
        if isinstance(entry, dict):
            name = entry.get("name") or entry.get("subreddit")
        else:
            name = entry
        if not name:
            continue
        name = str(name).strip()
        if not name.lower().startswith("r/"):
            name = "r/" + name.split("/")[-1]
        if name.lower() not in {s.lower() for s in subs_names}:
            subs_names.append(name)
    subs = " ".join(subs_names)
    keywords = seed.get("keywords", [])
    filters = seed.get("filters", {}) or {}
    must = filters.get("must_include", [])
    should = filters.get("should_include", [])
    exclude = filters.get("exclude", [])

    combined = dedupe(list(keywords) + list(must) + list(should))
    user_kw = normalize_user_keywords(args.user_keywords)
    combined = dedupe(combined + user_kw)

    months = seed.get("timeframe_months")
    min_upvotes = seed.get("min_upvotes")

    print(f"Seed subreddits: {subs or '(none)'}")
    print("Seed keywords:", ", ".join(combined) or "(none)")
    print("Seed exclude keywords:", ", ".join(dedupe(exclude)) or "(none)")

    env_payload = {
        "subs": subs,
        "keywords": combined,
        "months": months,
        "min_upvotes": min_upvotes,
        "exclude_keywords": exclude,
    }
    Path(args.env_json_out).write_text(json.dumps(env_payload, ensure_ascii=False), encoding="utf-8")

    with open(args.env_file, "a", encoding="utf-8") as fh:
        fh.write(f"SEED_SUBS={subs}\n")
        fh.write(f"SEED_KEYWORDS_JSON={json.dumps(combined, ensure_ascii=False)}\n")
        fh.write(f"SEED_EXCLUDE_KEYWORDS_JSON={json.dumps(dedupe(exclude), ensure_ascii=False)}\n")
        if months:
            fh.write(f"SEED_MONTHS={months}\n")
        if min_upvotes is not None:
            fh.write(f"SEED_MIN_UPVOTES={min_upvotes}\n")


if __name__ == "__main__":
    main()
