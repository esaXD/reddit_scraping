import argparse, json

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    ap.add_argument("--keywords", nargs="+", default=[])
    args = ap.parse_args()

    kws = [k.lower() for k in args.keywords]
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
            txt = (d.get("title","") + " " + d.get("selftext","")).lower()
            if all(k in txt for k in kws):
                g.write(json.dumps(d, ensure_ascii=False) + "\n")
                kept += 1
    print(f"Applied keyword filter {kws} -> kept {kept}")

if __name__ == "__main__":
    main()
