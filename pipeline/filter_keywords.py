import argparse, json, shlex
from discover_subs import english_keywords

def parse_keywords(raw: str):
    # tırnaklı ifadeleri tek anahtar olarak almak için shlex
    if not raw:
        return []
    try:
        toks = shlex.split(raw)
    except Exception:
        toks = raw.split()
    return [t.strip() for t in toks if t.strip()]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    ap.add_argument("--keywords", default="")
    ap.add_argument("--keywords-json", default="")
    ap.add_argument("--mode", choices=["any","all"], default="any")
    args = ap.parse_args()

    kws_raw = parse_keywords(args.keywords)
    if args.keywords_json:
        try:
            data = json.loads(args.keywords_json)
            if isinstance(data, list):
                kws_raw.extend(str(x).strip() for x in data if str(x).strip())
        except Exception:
            pass
    # dedupe preserving order
    seen = set()
    deduped = []
    for item in kws_raw:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    kws_raw = deduped

    english = english_keywords("", " ".join(kws_raw))[:24]
    kws = [k.casefold() for k in english]
    if not kws:
        # no-op copy
        with open(args.in_path, "r", encoding="utf-8") as f, open(args.out_path, "w", encoding="utf-8") as g:
            for line in f: g.write(line)
        print("No keywords provided; skipping filter.")
        return

    kept = 0
    with open(args.in_path, "r", encoding="utf-8") as f, open(args.out_path, "w", encoding="utf-8") as g:
        for line in f:
            d = json.loads(line)
            txt = (d.get("title","") + " " + d.get("selftext","")).casefold()
            ok = all(k in txt for k in kws) if args.mode=="all" else any(k in txt for k in kws)
            if ok:
                g.write(json.dumps(d, ensure_ascii=False) + "\n")
                kept += 1
    print(f"Filter {args.mode} {kws} -> kept {kept}")

if __name__ == "__main__":
    main()
