# pipeline/llm_summary.py
import os, argparse, json, pandas as pd

SYS = "You summarize Reddit-derived analysis for product/market insight. Be concise, bullet the key pains, opportunities, and user language. Output Markdown only."

PROMPT = """Context:
- Report type: {rtype}
- Subreddits: {subs}
- Params: months={months}, min_upvotes={minup}, limit={limit}
- Example top clusters and posts (truncated): 
{samples}

Task:
Write an executive summary (Markdown). Include:
- Top 3-5 pain points (with short user-language quotes or paraphrases)
- Opportunities (where a product could help)
- If market: segments & JTBD hints
- If ideation: 3 concrete product ideas (name + one-liner + why-now)

Keep it under 250-300 words.
"""

def call_openai(md):
    try:
        from openai import OpenAI
        if not os.getenv("OPENAI_API_KEY"):
            return None
        client = OpenAI()
        rsp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"system","content":SYS},{"role":"user","content":md}],
            temperature=0.2,
            max_tokens=500,
        )
        return rsp.choices[0].message.content
    except Exception:
        return None

def call_anthropic(md):
    try:
        import anthropic
        if not os.getenv("ANTHROPIC_API_KEY"):
            return None
        client = anthropic.Anthropic()
        msg = client.messages.create(
            model="claude-3-5-sonnet-latest",
            max_tokens=500,
            system=SYS,
            messages=[{"role":"user","content":md}],
        )
        return "".join([c.text for c in msg.content])
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True)
    ap.add_argument("--analysis", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    plan = json.load(open(args.plan, "r", encoding="utf-8"))
    df = pd.read_parquet(args.analysis)

    # Örnekler: en yüksek pain_score’lu ilk 10 post
    if {"title","url","pain_score"}.issubset(df.columns):
        df2 = df.sort_values("pain_score", ascending=False).head(10)[["title","url","pain_score"]]
        samples = "\n".join(f"- {t} (pain {p:.2f})" for t,p in zip(df2["title"], df2["pain_score"]))
    else:
        samples = "- No samples (data sparse)"

    md = PROMPT.format(
        rtype = plan.get("report_type","auto"),
        subs = ", ".join(plan.get("subreddits", [])),
        months = plan.get("params",{}).get("months","-"),
        minup = plan.get("params",{}).get("min_upvotes","-"),
        limit = plan.get("params",{}).get("limit","-"),
        samples = samples
    )

    txt = call_anthropic(md) or call_openai(md)
    if not txt:
        txt = "LLM summary skipped (no API key)."

    with open(args.out, "w", encoding="utf-8") as f:
        f.write(txt)

if __name__ == "__main__":
    main()
