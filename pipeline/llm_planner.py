# pipeline/llm_planner.py
import os
import json
import re
import argparse
import shlex
from discover_subs import english_keywords

SYSTEM = "You plan Reddit research. Return strict JSON only."
USER_TMPL = """Prompt: {prompt}
Report type hint: {report_type}
Max subreddits: {max_subs}
Defaults: months={def_months}, min_upvotes={def_min}, limit={def_limit}
Candidate subreddits from search: {seed_subs}
Candidate keywords from search: {seed_keywords}
Schema:
{{
  "subreddits": ["r/...", "..."],
  "params": {{"months": 12, "min_upvotes": 20, "limit": 1000}},
  "filters": {{"keywords": ["..."]}},
  "report_type": "market|competitor|trend|sentiment|ideation|faq"
}}"""

CURATED = {
    "ai": ["r/MachineLearning", "r/artificial", "r/LocalLLaMA", "r/datascience"],
    "wellness": ["r/wellness", "r/selfimprovement", "r/productivity", "r/Meditation", "r/Anxiety"],
    "meditation": ["r/Meditation", "r/mindfulness", "r/selfimprovement"],
    "anxiety": ["r/Anxiety", "r/mentalhealth", "r/DecidingToBeBetter"],
    "productivity": ["r/productivity", "r/selfimprovement", "r/GetDisciplined", "r/lifehacks"],
}

def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9\s]", " ", (s or "").lower()).strip()

def is_non_english(s: str) -> bool:
    if not s:
        return False
    tr = set("çğıöşüÇĞİÖŞÜ")
    return any((ord(ch) > 127) or (ch in tr) for ch in s)

def _prepare_keywords(prompt_text: str, raw_keywords) -> list:
    if raw_keywords is None or raw_keywords == "":
        kws = english_keywords(prompt_text, "")
        return kws[:24] if kws else []
    if isinstance(raw_keywords, (list, tuple)):
        parts = []
        for item in raw_keywords:
            if item is None:
                continue
            s = str(item).strip()
            if not s:
                continue
            if " " in s:
                parts.append(f'"{s}"')
            else:
                parts.append(s)
        joined = " ".join(parts)
    else:
        joined = str(raw_keywords)
    kws = english_keywords(prompt_text, joined)
    return kws[:24] if kws else []


def parse_seed_subs(seed_str: str) -> list:
    if not seed_str:
        return []
    parts = seed_str.replace(",", " ").split()
    return normalize_sub_list(parts)


def parse_seed_keywords(seed_json: str):
    if not seed_json:
        return []
    try:
        data = json.loads(seed_json)
        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
    except Exception:
        pass
    cleaned = str(seed_json).strip()
    if cleaned.startswith("[") and cleaned.endswith("]"):
        inner = cleaned[1:-1]
        rough = inner.split(",")
        extracted = []
        for item in rough:
            txt = item.strip().strip("\"' ")
            if txt:
                extracted.append(txt)
        if extracted:
            return extracted
    try:
        parts = shlex.split(seed_json)
    except Exception:
        parts = seed_json.replace(",", " ").split()
    return [p.strip() for p in parts if p.strip()]


def dedupe_merge(primary, extra):
    out = []
    seen = set()
    for seq in (primary or [], extra or []):
        for item in seq:
            if item is None:
                continue
            text = str(item).strip()
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(text)
    return out

def normalize_sub_list(subs_iterable) -> list:
    out, seen = [], set()
    for raw in subs_iterable or []:
        s = str(raw).strip()
        if not s:
            continue
        tag = "r/" + s.split("/")[-1]
        key = tag.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(tag)
    return out


def heuristic(prompt, report_type, max_subs, m, u, lim, keywords, seed_subs, seed_keywords, seed_exclude):
    words = norm(prompt).split()
    subs = normalize_sub_list(seed_subs)
    seen = {s.lower() for s in subs}

    # Basit curated eşleşmeler (nötr; meditasyona zorlamıyoruz)
    for w in words:
        if w in CURATED:
            for s in CURATED[w]:
                tag = "r/" + s.split("/")[-1]
                if tag.lower() in seen:
                    continue
                subs.append(tag)
                seen.add(tag.lower())

    # Hiçbir şey bulunamazsa boş bırak; discovery dolduracak
    if not subs:
        subs = []

    # Report type otomatik seçimi
    if report_type == "auto":
        if any(w in words for w in ["market", "pricing", "monetization"]):
            report_type = "market"
        elif any(w in words for w in ["competitor", "vs", "alternative"]):
            report_type = "competitor"
        elif any(w in words for w in ["trend", "spike", "decline"]):
            report_type = "trend"
        elif any(w in words for w in ["sentiment", "opinion", "review"]):
            report_type = "sentiment"
        elif any(w in words for w in ["idea", "ideation", "pg"]):
            report_type = "ideation"
        else:
            report_type = "faq"

    subs = subs[:max_subs]

    seed_kw_text = " ".join(seed_keywords)
    keyword_payload = " ".join(filter(None, [seed_kw_text, keywords]))
    filters = english_keywords(prompt, keyword_payload)
    if not filters:
        filters = english_keywords(prompt, "")
    filters = filters[:24] if filters else []

    plan = {
        "subreddits": subs,
        "params": {"months": int(m), "min_upvotes": int(u), "limit": int(lim)},
        "report_type": report_type,
    }
    if filters:
        plan["filters"] = {"keywords": filters}
    return plan

def call_openai(system, user):
    try:
        from openai import OpenAI
        if not os.getenv("OPENAI_API_KEY"):
            return None
        client = OpenAI()
        r = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": system},
                      {"role": "user", "content": user}],
            temperature=0.2,
            max_tokens=500,
            response_format={"type": "json_object"},
        )
        return r.choices[0].message.content
    except Exception:
        return None

def call_anthropic(system, user):
    try:
        import anthropic
        if not os.getenv("ANTHROPIC_API_KEY"):
            return None
        client = anthropic.Anthropic()
        m = client.messages.create(
            model="claude-3-5-sonnet-latest",
            max_tokens=500,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        return "".join([c.text for c in m.content])
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prompt", required=True)
    ap.add_argument("--report-type", default="auto")
    ap.add_argument("--max-subs", type=int, default=8)
    ap.add_argument("--default-months", type=int, default=12)
    ap.add_argument("--default-min-upvotes", type=int, default=20)
    ap.add_argument("--default-limit", type=int, default=1000)
    ap.add_argument("--keywords", default="")
    ap.add_argument("--seed-subs", default="")
    ap.add_argument("--seed-keywords-json", default="")
    ap.add_argument("--seed-exclude-json", default="")
    ap.add_argument("--seed-plan-json", default="")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    seed_list = parse_seed_subs(a.seed_subs)
    seed_keywords = parse_seed_keywords(a.seed_keywords_json)
    seed_exclude = parse_seed_keywords(a.seed_exclude_json)

    seed_plan = {}
    if a.seed_plan_json:
        try:
            seed_plan = json.load(open(a.seed_plan_json, "r", encoding="utf-8"))
        except Exception:
            seed_plan = {}

    user = USER_TMPL.format(
        prompt=a.prompt,
        report_type=a.report_type,
        max_subs=a.max_subs,
        def_months=a.default_months,
        def_min=a.default_min_upvotes,
        def_limit=a.default_limit,
        seed_subs=", ".join(seed_list) if seed_list else "(none)",
        seed_keywords=", ".join(seed_keywords) if seed_keywords else "(none)",
    )

    txt = call_anthropic(SYSTEM, user) or call_openai(SYSTEM, user)
    plan = None
    if txt:
        try:
            plan = json.loads(txt)
        except Exception:
            plan = None

    if not plan or not plan.get("subreddits"):
        plan = heuristic(
            a.prompt,
            a.report_type,
            a.max_subs,
            a.default_months,
            a.default_min_upvotes,
            a.default_limit,
            a.keywords,
            seed_list,
            seed_keywords,
            seed_exclude,
        )
        plan["rationale"] = "heuristic"
    else:
        plan["rationale"] = "llm"
        if not plan.get("subreddits") and seed_list:
            plan["subreddits"] = seed_list[: a.max_subs]

    existing_kw = plan.get("filters", {}).get("keywords")
    seed_kw_json = json.dumps(seed_keywords, ensure_ascii=False)
    seed_ex_json = json.dumps(seed_exclude, ensure_ascii=False)
    canonical_kw = (
        _prepare_keywords(a.prompt, existing_kw)
        or _prepare_keywords(a.prompt, seed_kw_json)
        or _prepare_keywords(a.prompt, seed_ex_json)
        or _prepare_keywords(a.prompt, a.keywords)
    )
    if canonical_kw:
        plan.setdefault("filters", {})["keywords"] = canonical_kw
    else:
        plan.pop("filters", None)

    if seed_exclude:
        current_ex = plan.get("filters", {}).get("exclude", [])
        merged_ex = dedupe_merge(current_ex, seed_exclude)
        if merged_ex:
            plan.setdefault("filters", {})["exclude"] = merged_ex


    if seed_plan:
        plan.setdefault("topic_themes", seed_plan.get("topic_themes", plan.get("topic_themes", [])))
        plan.setdefault("keyword_plan", seed_plan.get("keyword_plan", plan.get("keyword_plan", {})))
        plan["seed_metadata"] = seed_plan
        plan.setdefault("warnings", dedupe_merge(plan.get("warnings", []), seed_plan.get("warnings", [])))
        seed_filters = seed_plan.get("filters", {}) or {}
        if seed_filters.get("languages"):
            plan.setdefault("filters", {})["languages"] = dedupe_merge(plan.get("filters", {}).get("languages", []), seed_filters.get("languages"))
        plan.setdefault("validation_hints", dedupe_merge(plan.get("validation_hints", []), seed_plan.get("validation_hints", [])))
    # normalize
    uniq = []
    seen = set()
    for s in plan["subreddits"]:
        s = "r/" + s.split("/")[-1]
        if s.lower() not in seen:
            seen.add(s.lower())
            uniq.append(s)
    plan["subreddits"] = uniq[: a.max_subs]
    plan["original_prompt"] = a.prompt
    if seed_list or seed_keywords:
        meta_subs = []
        if seed_plan:
            for entry in seed_plan.get("subreddits", []):
                if isinstance(entry, dict):
                    meta_subs.append({
                        "name": entry.get("name"),
                        "confidence": entry.get("confidence"),
                        "why": entry.get("why"),
                        "signal_score": entry.get("signal_score"),
                        "volume_hint": entry.get("volume_hint"),
                        "flags": entry.get("flags"),
                    })
        plan["seed_context"] = {
            "subreddits": seed_list,
            "keywords": seed_keywords,
            "exclude": seed_exclude,
            "metadata": meta_subs,
        }

    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(plan, f, ensure_ascii=False, indent=2)
    print(json.dumps(plan, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
