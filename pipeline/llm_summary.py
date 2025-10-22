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
- Keyword focus terms: {focus_terms}
- Matched posts: {matched}/{total_rows}
- Data coverage: {coverage_note}
- Sample posts:
{samples}

Task:
Create an executive summary in Markdown that stays faithful to the user prompt and the matched Reddit posts.
- If matched posts == 0, clearly state that no direct evidence was found and offer next-step research suggestions (specific subreddits, keywords, or data needs).
- If matched posts < 5, ground every insight in the available posts, flag the limited evidence, and include a short “Next research steps” bullet list.
- If matched posts ≥ 5, list 3-5 pain points with brief user-language snippets, highlight opportunities tied to the prompt, and propose up to 3 product ideas (name + one-liner + why-now) based on evidence.
- Never drift into generic mobile development commentary; stay anchored to the prompt and evidence. If evidence veers off-topic, say so explicitly.
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
            messages=[{"role": "user", "content": md}],
        )
        return "".join(c.text for c in msg.content)
    except Exception:
        return None


def build_focus_terms(plan: dict) -> list:
    keywords = plan.get("filters", {}).get("keywords") or []
    prompt_text = plan.get("original_prompt", "")
    joined = " ".join(keywords)
    terms = english_keywords(prompt_text, joined)
    # Deduplicate while preserving order
    seen = set()
    out = []
    for term in terms:
        key = term.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(term)
        if len(out) >= 32:
            break
    return out


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
    return "\n".join(lines)


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

    md = PROMPT_TMPL.format(
        prompt_text=plan.get("original_prompt", "<unknown prompt>"),
        rtype=plan.get("report_type", "auto"),
        subs=", ".join(plan.get("subreddits", [])),
        months=plan.get("params", {}).get("months", "-"),
        minup=plan.get("params", {}).get("min_upvotes", "-"),
        limit=plan.get("params", {}).get("limit", "-"),
        focus_terms=", ".join(focus_terms) if focus_terms else "(none)",
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
