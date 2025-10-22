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
    _normalize_lookup("kullanıcı"): ["user", "users"],
    _normalize_lookup("deneyimi"): ["experience", "user experience"],
    _normalize_lookup("kullanıcı deneyimi"): ["user experience", "ux", "ux research", "ux design"],
    _normalize_lookup("tasarım"): ["design", "app design", "ui design", "ux design"],
    _normalize_lookup("performans"): ["performance", "app performance", "performance optimization"],
    _normalize_lookup("güvenlik"): ["security", "app security", "mobile security", "data privacy"],
    _normalize_lookup("yenilikçilik"): ["innovation", "innovative features", "innovation strategy"],
    _normalize_lookup("uygulama geliştirme"): ["application development", "product development", "mobile development"],
    _normalize_lookup("uygulama gelistirme"): ["application development", "product development", "mobile development"],
    _normalize_lookup("geliştirme"): ["development", "application development"],
    _normalize_lookup("gelistirme"): ["development", "application development"],
    _normalize_lookup("yapay zeka"): ["artificial intelligence", "ai"],
    _normalize_lookup("sesli komut"): ["voice control", "voice commands", "speech recognition"],
    _normalize_lookup("haptik"): ["haptic", "haptic technology", "haptic glove"],
    _normalize_lookup("haptic"): ["haptic", "haptic technology", "haptic glove"],
    _normalize_lookup("haptik eldiven"): ["haptic glove", "vr glove", "haptic wearable"],
    _normalize_lookup("haptic glove"): ["haptic glove", "vr glove", "haptic wearable"],
    _normalize_lookup("eldiven"): ["glove", "wearable glove", "smart glove"],
    _normalize_lookup("eldivenin"): ["glove", "wearable glove", "smart glove"],
    _normalize_lookup("pazar"): ["market", "market landscape"],
    _normalize_lookup("pazarı"): ["market", "market landscape"],
    _normalize_lookup("market"): ["market", "market positioning"],
    _normalize_lookup("markette"): ["market", "market positioning"],
    _normalize_lookup("marketteki"): ["market", "market positioning"],
    _normalize_lookup("yeri"): ["positioning", "market position"],
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
        base_forms = [kw, kw.translate(ASCII_FALLBACK)]
        variants = []
        lookup_keys = {_normalize_lookup(kw), _normalize_lookup(kw.translate(ASCII_FALLBACK))}
        suffixes = ["lari", "leri", "lar", "ler", "nin", "nın", "nun", "nün", "in", "ın", "un", "ün",
                    "si", "sı", "su", "sü", "da", "de", "ta", "te", "dan", "den", "tan", "ten",
                    "ya", "ye", "yla", "yle", "yla", "yle", "li", "lı", "lu", "lü"]
        extended = set()
        for lk in list(lookup_keys):
            if not lk:
                continue
            for suf in suffixes:
                if lk.endswith(suf) and len(lk) - len(suf) >= 3:
                    extended.add(lk[:-len(suf)])
        lookup_keys |= extended
        for lk in lookup_keys:
            if lk and lk in SYNONYM_MAP:
                variants.extend(SYNONYM_MAP[lk])
        if not variants:
            variants = base_forms
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


def english_keywords(prompt: str, keywords: str):
    expanded = build_keywords(prompt, keywords)
    english, seen = [], set()
    for kw in expanded:
        ascii_kw = kw.encode("ascii", "ignore").decode().strip()
        if not ascii_kw:
            continue
        if ascii_kw.lower() in seen:
            continue
        seen.add(ascii_kw.lower())
        english.append(ascii_kw)
    if english:
        return english

    for tok in tokens(prompt) + tokens(keywords):
        ascii_tok = tok.translate(ASCII_FALLBACK).encode("ascii", "ignore").decode().strip()
        if not ascii_tok:
            continue
        key = ascii_tok.lower()
        if key in seen:
            continue
        seen.add(key)
        english.append(ascii_tok)
    return english


def _basic_terms(prompt: str, keywords: str):
    base = []
    seen = set()
    suffixes = ["lari", "leri", "lar", "ler", "nin", "nın", "nun", "nün", "in", "ın", "un", "ün",
                "si", "sı", "su", "sü", "da", "de", "ta", "te", "dan", "den", "tan", "ten",
                "ya", "ye", "yla", "yle", "li", "lı", "lu", "lü"]
    for tok in tokens(prompt) + tokens(keywords):
        if not tok:
            continue
        ascii_tok = tok.translate(ASCII_FALLBACK)
        ascii_tok = TOKEN_CLEAN_RE.sub(" ", ascii_tok).strip()
        ascii_tok = ascii_tok.encode("ascii", "ignore").decode().strip()
        if not ascii_tok or len(ascii_tok) < 3:
            continue
        key = ascii_tok.lower()
        lookup_keys = {_normalize_lookup(tok), _normalize_lookup(ascii_tok)}
        for lk in list(lookup_keys):
            if not lk:
                continue
            for suf in suffixes:
                if lk.endswith(suf) and len(lk) - len(suf) >= 3:
                    lookup_keys.add(lk[:-len(suf)])
        variants = []
        for lk in lookup_keys:
            if lk and lk in SYNONYM_MAP:
                variants.extend(SYNONYM_MAP[lk])
        if variants:
            for v in variants:
                vk = v.casefold()
                if vk in seen or vk in STOP_WORDS:
                    continue
                seen.add(vk)
                base.append(v)
            continue
        if key in STOP_WORDS or key in seen:
            continue
        seen.add(key)
        base.append(ascii_tok)
    return base


def build_search_queries(prompt: str, keywords: str, max_terms: int = 16):
    queries = []
    primary = english_keywords(prompt, keywords)
    if primary:
        terms = []
        for kw in primary:
            if len(terms) >= max_terms:
                break
            terms.append(f'"{kw}"' if " " in kw else kw)
        if terms:
            queries.append(terms)

    basics = _basic_terms(prompt, keywords)
    if basics:
        queries.append(basics[:max_terms])

    if not queries:
        fallback = _basic_terms(prompt, "") or english_keywords(prompt, keywords)
        fallback = fallback[:max_terms] if fallback else ["technology", "innovation", "product"]
        queries.append(fallback)

    uniq = []
    seen = set()
    for terms in queries:
        key = tuple(terms)
        if key in seen or not terms:
            continue
        seen.add(key)
        uniq.append(terms)
    return uniq


FALLBACK_SUBS = {
    "physiognomy": ["r/physiognomy", "r/faceanalysis", "r/bodylanguage"],
    "face": ["r/physiognomy", "r/faceanalysis", "r/AskPsychology"],
    "mobile": ["r/MobileApps", "r/AppIdeas", "r/androiddev", "r/iOSProgramming"],
    "ux": ["r/userexperience", "r/UXResearch", "r/UXDesign", "r/userexperience_design"],
    "product": ["r/ProductManagement", "r/ProductDesign", "r/ProductMarketing"],
    "security": ["r/cybersecurity", "r/techsupport", "r/netsecstudents"],
    "innovation": ["r/Futurology", "r/startups", "r/Entrepreneur"],
    "haptic": ["r/virtualreality", "r/VRGaming", "r/HapticTech", "r/AR_MR_XR", "r/wearables"],
    "glove": ["r/virtualreality", "r/oculus", "r/HapticTech", "r/engineering"],
    "vr": ["r/virtualreality", "r/VRGaming", "r/SteamVR", "r/oculus"],
}


def fallback_subreddits(prompt: str, keywords: str, limit: int = 8):
    terms = english_keywords(prompt, keywords)
    tokens = set()
    for term in terms:
        tokens.add(term.casefold())
        tokens.update(term.casefold().split())
    tokens = {t for t in tokens if t}

    curated = []
    seen = set()
    for token in tokens:
        for key, subs in FALLBACK_SUBS.items():
            if key in token:
                for sub in subs:
                    norm = "r/" + sub.split("/")[-1]
                    if norm.lower() in seen:
                        continue
                    seen.add(norm.lower())
                    curated.append(norm)
                    if len(curated) >= limit:
                        return curated
    return curated[:limit]


def build_search_terms(prompt: str, keywords: str, max_terms: int = 16):
    queries = build_search_queries(prompt, keywords, max_terms)
    return queries[0] if queries else []


def discover(prompt, keywords, months=12, max_subs=8, pages=8, page_size=100):
    strategies = build_search_queries(prompt, keywords, max_terms=max_subs * 2 or 16)
    subs = Counter()
    for idx, terms in enumerate(strategies, 1):
        query = " OR ".join(terms)
        if not query:
            continue
        print(f"[discover] Strategy {idx}: {query}")
        params = {
            "q": query,
            "after": after_ts(months),
            "size": page_size,
            "sort": "desc",
            "sort_type": "created_utc",
        }
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
        if subs:
            break

    if not subs:
        fallback = fallback_subreddits(prompt, keywords, max_subs)
        if fallback:
            print("[discover] Fallback curated subs:", " ".join(fallback))
            return fallback
        return []

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
