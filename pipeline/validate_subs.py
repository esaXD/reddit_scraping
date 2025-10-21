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
            good.append("r/"+s.display_name)
            time.sleep(0.2)
        except Exception:
            continue
    return good

# Çok temel bir kara liste ve pozitif liste
NEG = { "r/Apps", "r/technology", "r/health", "r/fitness", "r/all", "r/popular", "r/AskReddit" }
POS = [ "r/meditation","r/Meditation","r/mindfulness","r/digitalminimalism","r/NoSurf",
        "r/selfimprovement","r/productivity","r/GetDisciplined","r/Anxiety","r/DecidingToBeBetter" ]

def heuristic_clean(candidates, limit=8):
    out = []
    seen = set()
    for s in candidates + POS:
        s = "r/"+s.split("/")[-1]
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
    ap.add_argument("--in", required=True)   # plan.json
    ap.add_argument("--out", required=True)  # cleaned_subs.txt
    ap.add_argument("--limit", type=int, default=8)
    a = ap.parse_args()

    plan = json.load(open(a.in, "r", encoding="utf-8"))
    cand = plan.get("subreddits", [])[: a.limit]
    # 1) PRAW ile doğrula (varsa)
    good = via_praw(cand)
    if not good:
        # 2) Heuristik temizle
        good = heuristic_clean(cand, a.limit)
    with open(a.out, "w", encoding="utf-8") as f:
        f.write(" ".join(good))
    print("Validated subs:", " ".join(good))

if __name__ == "__main__":
    main()
