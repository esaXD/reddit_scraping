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


def _normalize_lookup(term: str) -> str:
    cleaned = TOKEN_CLEAN_RE.sub(" ", term or "").strip().casefold()
    cleaned = cleaned.translate(ASCII_FALLBACK)
    return cleaned


SYNONYM_MAP = {
    _normalize_lookup("ilmi"): ["physiognomy", "face reading", "face analysis"],
    _normalize_lookup("sima"): ["physiognomy", "face reading"],
    _normalize_lookup("ilmi sima"): ["physiognomy", "face reading", "face mapping"],
    _normalize_lookup("yüz okuma"): ["face reading", "physiognomy"],
    _normalize_lookup("mobil"): ["mobile", "mobile app", "mobile application"],
    _normalize_lookup("uygulama"): ["app", "application", "mobile app"],
    _normalize_lookup("uygulama özellikler"): ["app features", "feature set", "product requirements"],
    _normalize_lookup("özellikler"): ["features", "feature set"],
    _normalize_lookup("kullanıcı deneyimi"): ["user experience", "ux", "ux research", "ux design"],
    _normalize_lookup("tasarım"): ["design", "app design", "ui design", "ux design"],
    _normalize_lookup("performans"): ["performance", "app performance", "performance optimization"],
    _normalize_lookup("güvenlik"): ["security", "app security", "mobile security", "data privacy"],
    _normalize_lookup("yenilikçilik"): ["innovation", "innovative features", "innovation strategy"],
    _normalize_lookup("yapay zeka"): ["artificial intelligence", "ai"],
    _normalize_lookup("sesli komut"): ["voice control", "voice commands", "speech recognition"],
}


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
        variants = [kw, kw.translate(ASCII_FALLBACK)]
        lookup_keys = {_normalize_lookup(kw), _normalize_lookup(kw.translate(ASCII_FALLBACK))}
        for lk in lookup_keys:
            if lk and lk in SYNONYM_MAP:
                variants.extend(SYNONYM_MAP[lk])
        for cand in variants:
            cand = cand.strip()
            if len(cand) < 3:
                continue
            cand_key = cand.casefold()
            if cand_key in STOP_WORDS:
                continue
            if cand_key in seen:
                continue
            seen.add(cand_key)
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


def build_keywords(prompt: str, keywords: str):
    toks = tokens(prompt) + tokens(keywords)
    base_keywords, seen = [], set()
    for tok in toks:
        if tok in STOP_WORDS or len(tok) < 3:
            continue
        if tok in seen:
            continue
        seen.add(tok)
        base_keywords.append(tok)
    return _expand_keywords(base_keywords)


def build_search_terms(prompt: str, keywords: str, max_terms: int = 16):
    expanded = build_keywords(prompt, keywords)
    if not expanded:
        return []
    terms = [f'"{kw}"' if " " in kw else kw for kw in expanded[:max_terms]]
    return terms


def discover(prompt, keywords, months=12, max_subs=8, pages=8, page_size=100):
    terms = build_search_terms(prompt, keywords)
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
