import argparse, os, time, sys, json
from typing import List
from datetime import datetime, timedelta
import requests
from util import ensure_dirs, save_jsonl, clean_text, now_iso
try:
    import praw
except Exception:
    praw = None
def month_ago_utc(months: int) -> int:
    now = datetime.utcnow(); start = now - timedelta(days=30*months); return int(start.timestamp())
def scrape_pushshift(subs: List[str], since_utc: int, limit: int, min_upvotes: int):
    base="https://api.pushshift.io/reddit/search/submission/"; size=250; out=[]
    for sub in subs:
        params={"subreddit": sub.replace("r/",""), "after": since_utc, "size": size, "sort":"desc","sort_type":"score"}
        fetched=0
        while fetched<limit:
            try:
                r=requests.get(base, params=params, timeout=30); r.raise_for_status()
                data=r.json().get("data",[]); if not data: break
                for d in data:
                    if d.get("score",0)<min_upvotes: continue
                    out.append({"id":d.get("id"),"subreddit":sub,"created_utc":d.get("created_utc"),"title":clean_text(d.get("title","")),"selftext":clean_text(d.get("selftext","")),"url":d.get("full_link") or f"https://reddit.com/{d.get('id')}","upvotes":d.get("score",0),"num_comments":d.get("num_comments",0),"source":"pushshift","fetched_at":now_iso()})
                fetched+=len(data); 
                if data: params["before"]=data[-1].get("created_utc")
                time.sleep(0.6)
            except Exception as e:
                print("pushshift error:", e, file=sys.stderr); time.sleep(2); break
    return out
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--subs", nargs="+", required=True); ap.add_argument("--months", type=int, default=12)
    ap.add_argument("--limit", type=int, default=2000); ap.add_argument("--min-upvotes", type=int, default=20)
    ap.add_argument("--use-pushshift", action="store_true"); ap.add_argument("--out", required=True)
    a=ap.parse_args()
    ensure_dirs(a.out); since=month_ago_utc(a.months)
    rows=scrape_pushshift(a.subs, since, a.limit, a.min_upvotes)
    seen=set(); ded=[]; 
    for r in rows:
        if r["id"] in seen: continue
        seen.add(r["id"]); ded.append(r)
    save_jsonl(ded, a.out); print(f"Saved {len(ded)} items to {a.out}")
if __name__=="__main__": main()
