import re, os, math, json, datetime as dt
def now_iso(): return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
def clean_text(s: str) -> str:
    if s is None: return ""
    s = s.replace("\r"," ").replace("\n"," ").strip()
    s = re.sub(r"\s+"," ", s); return s
def heuristic_english(text: str) -> bool:
    en_chars = sum(ch.isalpha() and ch.lower() in "abcdefghijklmnopqrstuvwxyz" for ch in text)
    total = sum(ch.isalpha() for ch in text); 
    return True if total==0 else (en_chars/total)>0.7
def ensure_dirs(*paths):
    for p in paths:
        d = os.path.dirname(p)
        if d: os.makedirs(d, exist_ok=True)
def save_jsonl(rows, path):
    ensure_dirs(path)
    with open(path,"w",encoding="utf-8") as f:
        for r in rows: f.write(json.dumps(r, ensure_ascii=False)+"\n")
def read_jsonl(path):
    with open(path,"r",encoding="utf-8") as f:
        for line in f: yield json.loads(line)
def pain_score(sentiment: float, upvotes: int, comments: int) -> float:
    neg = max(0.0, -sentiment); import math
    return round((neg * (1 + math.log1p(upvotes + comments))), 3)
def extract_gaps(text: str):
    text = text.lower(); import re
    pats=[r"i (?:tried|used) ([^.,;:]+?) but ([^.;]+)", r"i wish (?:there was|there were) ([^.;]+)", r"it doesn't work because ([^.;]+)", r"i can't ([^.;]+)"]
    out=[]; 
    for pat in pats:
        for m in re.finditer(pat, text): out.append(m.group(0).strip())
    return out[:5]
