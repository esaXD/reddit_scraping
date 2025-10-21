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
    ap.add_argument("--mode", choices=["any","all"], default="any")
    args = ap.parse_args()

    kws_raw = parse_keywords(args.keywords)
    kws = [k.casefold() for k in english_keywords("", " ".join(kws_raw))]
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
