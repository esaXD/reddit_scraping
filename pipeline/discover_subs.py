# pipeline/discover_subs.py
import argparse, time, sys, re, shlex, math
import requests
from collections import Counter
from datetime import datetime, timedelta

BASE = "https://api.pullpush.io/reddit/search/submission/"

TR_STOP = {"ve","ile","de","da","mi","mı","mu","mü","bir","bu","şu","o","çok","gibi","için","ile","ya","ama","fakat","ancak","ki","ise"}
EN_STOP = {"the","a","an","and","or","of","in","on","to","for","by","is","are","with","from","about","how"}
def tokens(s: str):
    # tırnak içi ifadeleri koru, diğerlerini kelimele
    if not s: return []
    try:
        parts = shlex.split(s)
    except Exception:
        parts = s.split()
    out = []
    for p in parts:
        if " " in p: out.append(p)  # "kullanıcı deneyimi" gibi
        else:
            p2 = re.sub(r"[^0-9a-zA-ZçğıöşüÇĞİÖŞÜ+#\-_/\.]", " ", p).strip()
            if p2: out += p2.split()
    return out

def after_ts(months: int) -> int:
    return int((datetime.utcnow() - timedelta(days=30*months)).timestamp())

def _req(params, retry=3, sleep=0.5):
    last = None
    for i in range(retry):
        try:
            r = requests.get(BASE, params=params, timeout=30)
            r.raise_for_status()
            return r.json().get("data", [])
        except Exception as e:
            last = e; time.sleep(sleep*(i+1))
    print("discover_subs error:", last, file=sys.stderr)
    return []

def discover(prompt, keywords, months=12, max_subs=8, pages=8, page_size=100):
    # anahtarlar: prompt+keywords birleşimi
    toks = tokens(prompt) + tokens(keywords)
    # stopword temizliği, kısa kelimeleri at
    kws = [t for t in toks if t.lower() not in TR_STOP|EN_STOP and len(t) >= 3]
    # çok kelimeli ifadeleri OR ile birleştir
    q = " OR ".join(kws) if kws else ""
    if not q:
        return []  # hiçbir şey yoksa boş bırak

    params = {
        "q": q,
        "after": after_ts(months),
        "size": page_size,
        "sort": "desc",
        "sort_type": "created_utc",
    }
    subs = Counter()
    before = None
    for _ in range(pages):
        if before: params["before"] = before
        data = _req(params)
        if not data: break
        for d in data:
            sr = d.get("subreddit")
            if not sr: continue
            subs.update([str(sr)])
        before = int(data[-1].get("created_utc", 0)) or None
        time.sleep(0.25)

    # r/… formatla, en çok geçenlerden başla
    top = []
    for sr, _cnt in subs.most_common(64):
        s = "r/"+sr.split("/")[-1]
        if s.lower() in {"r/all","r/popular","r/askreddit"}:
            continue
        if s not in top:
            top.append(s)
        if len(top) >= max_subs:
            break
    return top

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prompt", required=True)
    ap.add_argument("--keywords", default="")
    ap.add_argument("--months", type=int, default=12)
    ap.add_argument("--max-subs", type=int, default=8)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    subs = discover(a.prompt, a.keywords, a.months, a.max_subs)
    with open(a.out, "w", encoding="utf-8") as f:
        f.write(" ".join(subs))
    print("Discovered subs:", " ".join(subs))

if __name__ == "__main__":
    main()
