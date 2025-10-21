# pipeline/validate_subs.py
import os, argparse, json, sys, time

def via_praw(candidates):
    try:
        import praw
    except Exception:
        return []
    cid = os.getenv("REDDIT_CLIENT_ID")
    secret = os.getenv("REDDIT_CLIENT_SECRET")
    ua = os.getenv("REDDIT_USER_AGENT", "reddit_research/0.1")
    if not (cid and secret):
        return []
    reddit = praw.Reddit(client_id=cid, client_secret=secret, user_agent=ua)
    good = []
    for sr in candidates:
        name = sr.replace("r/","")
        try:
            s = reddit.subreddit(name)
            if getattr(s, "over18", False):
                continue
            subs = getattr(s, "subscribers", 0) or 0
            if subs < 10000:
                continue
            good.append("r/" + s.display_name)
            time.sleep(0.2)
        except Exception:
            continue
    return good

NEG = { "r/Apps", "r/technology", "r/health", "r/fitness", "r/all", "r/popular", "r/AskReddit" }

def heuristic_clean(candidates, limit=8):
    """
    Deduplicate and cap candidate subreddits without pulling in unrelated defaults.
    """
    out, seen = [], set()
    for s in candidates:
        s = "r/" + s.split("/")[-1]
        if s in NEG:
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
        if len(out) >= limit:
            break
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)   # <-- dest ile rezerve kelimeyi aş
    ap.add_argument("--out", dest="out_path", required=True)
    ap.add_argument("--limit", type=int, default=8)
    a = ap.parse_args()

    plan = json.load(open(a.in_path, "r", encoding="utf-8"))
    cand = plan.get("subreddits", [])[: a.limit]

    good = via_praw(cand)
    if not good:
        good = heuristic_clean(cand, a.limit)

    with open(a.out_path, "w", encoding="utf-8") as f:
        f.write(" ".join(good))
    print("Validated subs:", " ".join(good))

if __name__ == "__main__":
    main()
