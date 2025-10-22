# pipeline/llm_summary.py
import os
import argparse
import json
import pandas as pd
from discover_subs import english_keywords

SYS = (
    "You summarize Reddit-derived analysis for product/market insight. "
    "Be concise, reference user language, and output Markdown only."
)

PROMPT_TMPL = """Context:
- User prompt: {prompt_text}
- Report type: {rtype}
- Subreddits: {subs}
- Params: months={months}, min_upvotes={minup}, limit={limit}
- Seed filters -> must: {must_terms}; should: {should_terms}; exclude: {exclude_terms}; languages: {filter_languages}
- Keyword focus terms: {focus_terms}
- Topic themes: {theme_notes}
- Warnings: {warnings}
- Matched posts: {matched}/{total_rows}
- Data coverage: {coverage_note}
- Sample posts:
{samples}

Task:
Create an executive summary in Markdown that stays faithful to the user prompt and the matched Reddit posts.
- If matched posts == 0, clearly state that no direct evidence was found and offer next-step research suggestions (specific subreddits, keywords, or data needs).
- If matched posts < 5, ground every insight in the available posts, flag the limited evidence, and include a short “Next research steps” bullet list.
- If matched posts ≥ 5, list 3-5 pain points with brief user-language snippets, highlight opportunities tied to the prompt, and propose up to 3 product ideas (name + one-liner + why-now) based on evidence.
- Never drift into generic commentary; stay anchored to the prompt and evidence. If evidence veers off-topic, say so explicitly.
Keep the answer under 250-300 words."""

def call_openai(md: str):
    try:
        from openai import OpenAI
        if not os.getenv("OPENAI_API_KEY"):
            return None
        client = OpenAI()
        rsp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": SYS}, {"role": "user", "content": md}],
            temperature=0.2,
            max_tokens=500,
        )
        return rsp.choices[0].message.content
    except Exception:
        return None

def call_anthropic(md: str):
    try:
        import anthropic
        if not os.getenv("ANTHROPIC_API_KEY"):
            return None
        client = anthropic.Anthropic()
        msg = client.messages.create(
            model="claude-3-5-sonnet-latest",
            max_tokens=500,
            system=SYS,
            messages=[{"role": "user", "content": md}]
        )
        return "".join(c.text for c in msg.content)
    except Exception:
        return None

def _dedupe(seq):
    out = []
    seen = set()
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

def build_focus_terms(plan: dict) -> list:
    prompt_text = plan.get("original_prompt", "")
    filters = plan.get("filters", {}) or {}
    base_terms = []
    base_terms.extend(filters.get("must_include", []))
    base_terms.extend(filters.get("should_include", []))
    base_terms.extend(plan.get("keywords", []))

    keyword_plan = plan.get("keyword_plan", {}) or {}
    for bucket in ("core", "long_tail", "exploratory"):
        for item in keyword_plan.get(bucket, []):
            if isinstance(item, dict):
                phrase = item.get("phrase") or item.get("keyword")
            else:
                phrase = item
            if phrase:
                base_terms.append(str(phrase))

    base_terms = _dedupe(base_terms)[:48]
    joined = " ".join(base_terms)
    terms = english_keywords(prompt_text, joined)
    return _dedupe(terms)[:32]

def extract_filter_strings(plan: dict):
    filters = plan.get("filters", {}) or {}
    must = _dedupe(filters.get("must_include", []))
    should = _dedupe(filters.get("should_include", []))
    exclude = _dedupe(filters.get("exclude", []))
    languages = _dedupe(filters.get("languages", []))
    return must, should, exclude, languages

def format_theme_notes(plan: dict) -> str:
    themes = plan.get("topic_themes", []) or []
    if not themes:
        return "(none)"
    notes = []
    for theme in themes[:5]:
        name = theme.get("name") or "theme"
        audience = theme.get("audience") or "-"
        pains = ", ".join(theme.get("pain_points", [])[:3]) or "-"
        outcomes = ", ".join(theme.get("desired_outcomes", [])[:2]) or "-"
        notes.append(f"{name} (audience: {audience}; pains: {pains}; outcomes: {outcomes})")
    return " | ".join(notes)

def format_warnings(plan: dict) -> str:
    warnings = plan.get("warnings", []) or []
    seed_ctx = plan.get("seed_context", {}) or {}
    warnings = warnings or seed_ctx.get("warnings", [])
    if not warnings:
        return "(none)"
    return "; ".join(_dedupe(warnings))

def match_posts(df: pd.DataFrame, terms: list) -> pd.DataFrame:
    if df.empty or not terms:
        return pd.DataFrame(columns=df.columns)
    lowers = [t.casefold() for t in terms]

    def matches(text: str) -> bool:
        text = (text or "").casefold()
        return any(term in text for term in lowers)

    mask = df["text"].apply(matches)
    return df[mask]

def render_samples(df: pd.DataFrame, limit: int = 8) -> str:
    if df.empty:
        return "- No directly matched posts. Consider broader data collection."
    rows = df.head(limit)[["title", "url", "pain_score"]].fillna("")
    lines = []
    for title, url, pain in rows.itertuples(index=False):
        link = url if isinstance(url, str) and url else ""
        base = f"- [{title}]({link})" if link else f"- {title}"
        if pain != "":
            base += f" (pain {pain:.2f})" if isinstance(pain, (int, float)) else f" (pain {pain})"
        lines.append(base)
    return "
".join(lines)

def coverage_note(match_count: int, total: int) -> str:
    if total == 0:
        return "No data points available after scraping."
    if match_count == 0:
        return "No posts mention the targeted keywords; insights must come from adjacent evidence or next steps."
    if match_count < 5:
        return "Very limited evidence; treat findings as directional only."
    return "Sufficient evidence from matched posts to ground insights."

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True)
    ap.add_argument("--analysis", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    plan = json.load(open(args.plan, "r", encoding="utf-8"))
    df = pd.read_parquet(args.analysis)
    if "text" not in df.columns:
        df["text"] = (df["title"].fillna("") + " " + df["selftext"].fillna("")).astype(str)

    focus_terms = build_focus_terms(plan)
    matched_df = match_posts(df, focus_terms)
    total_rows = len(df)
    matched = len(matched_df)

    if {"title", "url", "pain_score"}.issubset(matched_df.columns) and matched > 0:
        samples_src = matched_df.sort_values("pain_score", ascending=False)
    elif {"title", "url", "pain_score"}.issubset(df.columns):
        samples_src = df.sort_values("pain_score", ascending=False)
    else:
        samples_src = pd.DataFrame(columns=["title", "url", "pain_score"])

    samples = render_samples(samples_src, limit=8)
    cov_note = coverage_note(matched, total_rows)
    must_terms, should_terms, exclude_terms, filter_languages = extract_filter_strings(plan)
    theme_notes = format_theme_notes(plan)
    warnings = format_warnings(plan)

    md = PROMPT_TMPL.format(
        prompt_text=plan.get("original_prompt", "<unknown prompt>"),
        rtype=plan.get("report_type", "auto"),
        subs=", ".join(plan.get("subreddits", [])),
        months=plan.get("params", {}).get("months", "-"),
        minup=plan.get("params", {}).get("min_upvotes", "-"),
        limit=plan.get("params", {}).get("limit", "-"),
        must_terms=", ".join(must_terms) or "(none)",
        should_terms=", ".join(should_terms) or "(none)",
        exclude_terms=", ".join(exclude_terms) or "(none)",
        filter_languages=", ".join(filter_languages) or "(none)",
        focus_terms=", ".join(focus_terms) if focus_terms else "(none)",
        theme_notes=theme_notes,
        warnings=warnings,
        matched=matched,
        total_rows=total_rows,
        coverage_note=cov_note,
        samples=samples,
    )

    txt = call_anthropic(md) or call_openai(md)
    if not txt:
        txt = "LLM summary skipped (no API key)."

    with open(args.out, "w", encoding="utf-8") as f:
        f.write(txt)

if __name__ == "__main__":
    main()
