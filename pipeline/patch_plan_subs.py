# pipeline/patch_plan_subs.py
import json, argparse

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True)
    ap.add_argument("--subs-file", required=True)  # contains: r/a r/b r/c ...
    a = ap.parse_args()

    plan = json.load(open(a.plan, "r", encoding="utf-8"))
    subs_txt = open(a.subs_file, "r", encoding="utf-8").read().strip()
    new_subs = subs_txt.split() if subs_txt else []
    if new_subs:
        plan["subreddits"] = new_subs
        with open(a.plan, "w", encoding="utf-8") as f:
            json.dump(plan, f, ensure_ascii=False, indent=2)
        print("Patched plan.json with validated subreddits:", " ".join(new_subs))
    else:
        print("No subs found in", a.subs_file)

if __name__ == "__main__":
    main()
