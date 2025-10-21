import os
import json
import re
import argparse

SYSTEM = "You plan Reddit research. Return strict JSON only."
USER_TMPL = """Prompt: {prompt}
Report type hint: {report_type}
Max subreddits: {max_subs}
Defaults: months={def_months}, min_upvotes={def_min}, limit={def_limit}
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

def heuristic(prompt, report_type, max_subs, m, u, lim, keywords):
    words = norm(prompt).split()
    subs = []

    for w in words:
        if w in CURATED:
            subs += CURATED[w]

        if not subs:
        subs = []  # boş kalsın; discovery dolduracak


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

    uniq = []
    seen = set()
    for s in subs:
        s = "r/" + s.split("/")[-1]
        if s.lower() in seen:
            continue
        seen.add(s.lower())
        uniq.append(s)
    subs = uniq[:max_subs]

    return {
        "subreddits": subs,
        "params": {"months": int(m), "min_upvotes": int(u), "limit": int(lim)},
        "filters": {"keywords": (keywords.split() if keywords else [])},
        "report_type": report_type,
    }

def call_openai(system, user):
    try:
        from openai import OpenAI
        if not os.getenv("OPENAI_API_KEY"):
            return None
        client = OpenAI()
        r = client.chat.completions.create(
            model="gpt-4o-mini",
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
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    user = USER_TMPL.format(
        prompt=a.prompt,
        report_type=a.report_type,
        max_subs=a.max_subs,
        def_months=a.default_months,
        def_min=a.default_min_upvotes,
        def_limit=a.default_limit,
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
        )
        plan["rationale"] = "heuristic"
    else:
        plan["rationale"] = "llm"

    uniq = []
    seen = set()
    for s in plan["subreddits"]:
        s = "r/" + s.split("/")[-1]
        if s.lower() not in seen:
            seen.add(s.lower())
            uniq.append(s)
    plan["subreddits"] = uniq[: a.max_subs]

    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(plan, f, ensure_ascii=False, indent=2)
    print(json.dumps(plan, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
