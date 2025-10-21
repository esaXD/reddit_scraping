# pipeline/discover_subs.py
import argparse, time, sys, re, shlex
import requests
from collections import Counter
from datetime import datetime, timedelta

BASE = "https://api.pullpush.io/reddit/search/submission/"

TR_STOP = {
    "ve", "ile", "de", "da", "mi", "mı", "mu", "mü", "bir", "bu", "şu", "o",
    "çok", "cok", "gibi", "icin", "için", "ya", "ama", "fakat", "ancak", "ki",
    "ise", "neden", "niye", "nasıl", "nasil", "ne", "var", "yok", "olan",
    "olmak", "olması", "olmasi", "kadar", "hakkında", "hakkinda", "üzerine",
    "uzerine", "üzerinde", "uzerinde"
}
EN_STOP = {
    "the", "a", "an", "and", "or", "of", "in", "on", "to", "for", "by", "is",
    "are", "with", "from", "about", "how", "what", "why", "when", "where",
    "who", "does", "should", "could"
}
STOP_WORDS = {w.casefold() for w in TR_STOP.union(EN_STOP)}

ASCII_FALLBACK = str.maketrans({
    "ç": "c", "ğ": "g", "ı": "i", "ş": "s", "ö": "o", "ü": "u",
    "Ç": "C", "Ğ": "G", "İ": "I", "Ö": "O", "Ş": "S", "Ü": "U"
})
TOKEN_CLEAN_RE = re.compile(r"[^0-9A-Za-zçğıöşüÇĞİÖŞÜıİ+#\-/_.]")


def tokens(text: str):
    if not text:
        return []
    try:
        parts = shlex.split(text)
    except Exception:
        parts = text.split()
    out = []
    for part in parts:
        if not part:
            continue
        cleaned = TOKEN_CLEAN_RE.sub(" ", part)
        if " " in part:
            phrase = " ".join(chunk.casefold() for chunk in cleaned.split())
            if phrase:
                out.append(phrase)
            continue
        for chunk in cleaned.split():
            chunk = chunk.casefold()
            if chunk:
                out.append(chunk)
    return out


def _expand_keywords(keywords):
    expanded, seen = [], set()
    for kw in keywords:
        for cand in (kw, kw.translate(ASCII_FALLBACK)):
            cand = cand.strip()
            if len(cand) < 3:
                continue
            if cand in STOP_WORDS:
                continue
            if cand in seen:
                continue
            seen.add(cand)
            expanded.append(cand)
    return expanded


def after_ts(months: int) -> int:
    return int((datetime.utcnow() - timedelta(days=30 * months)).timestamp())


def _req(params, retry=3, sleep=0.5):
    last = None
    for i in range(retry):
        try:
            resp = requests.get(BASE, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json().get("data", [])
        except Exception as exc:
            last = exc
            time.sleep(sleep * (i + 1))
    print("discover_subs error:", last, file=sys.stderr)
    return []


def discover(prompt, keywords, months=12, max_subs=8, pages=8, page_size=100):
    toks = tokens(prompt) + tokens(keywords)
    base_keywords, seen = [], set()
    for tok in toks:
        if tok in STOP_WORDS or len(tok) < 3:
            continue
        if tok in seen:
            continue
        seen.add(tok)
        base_keywords.append(tok)

    keywords = _expand_keywords(base_keywords)
    if not keywords:
        return []

    terms = ['"{kw}"' if " " in kw else kw for kw in keywords[:16]]
    query = " OR ".join(terms)
    if not query:
        return []

    params = {
        "q": query,
        "after": after_ts(months),
        "size": page_size,
        "sort": "desc",
        "sort_type": "created_utc",
    }
    subs = Counter()
    before = None
    for _ in range(pages):
        if before:
            params["before"] = before
        data = _req(params)
        if not data:
            break
        for item in data:
            sr = item.get("subreddit")
            if not sr:
                continue
            subs.update([str(sr)])
        before = int(data[-1].get("created_utc", 0)) or None
        time.sleep(0.25)

    top = []
    for sr, _cnt in subs.most_common(64):
        candidate = "r/" + sr.split("/")[-1]
        if candidate.lower() in {"r/all", "r/popular", "r/askreddit"}:
            continue
        if candidate not in top:
            top.append(candidate)
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
    args = ap.parse_args()

    subs = discover(args.prompt, args.keywords, args.months, args.max_subs)
    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write(" ".join(subs))
    print("Discovered subs:", " ".join(subs))


if __name__ == "__main__":
    main()
