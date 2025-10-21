import argparse, json, os
from util import ensure_dirs
from pathlib import Path

PROMPT = """You are a YC partner channeling Paul Graham.
Given clusters of Reddit pains, propose 5 startup ideas (Problem, Insight, Solution, ICP, Why Now, GTM, Business Model, Moat, 2-Week MVP). Keep it crisp.
"""

def load_clusters(parquet_path: str):
    import pyarrow.parquet as pq
    df = pq.read_table(parquet_path).to_pandas()
    out = []
    for cid, cdf in df.groupby("cluster"):
        out.append({"cluster_id": int(cid), "size": int(len(cdf)), "avg_pain": float(cdf["pain_score"].mean()), "sample_titles": cdf["title"].head(10).tolist()})
    return out

def call_llm(prompt: str):
    if os.getenv("ANTHROPIC_API_KEY"):
        try:
            import anthropic
            client = anthropic.Anthropic()
            msg = client.messages.create(model="claude-3-5-sonnet-latest", max_tokens=1500, system="You are a concise YC partner.", messages=[{"role":"user","content":prompt}])
            return "".join([c.text for c in msg.content])
        except Exception as e:
            return f"[Anthropic error] {e}\\nStub Ideas..."
    if os.getenv("OPENAI_API_KEY"):
        try:
            from openai import OpenAI
            client = OpenAI()
            rsp = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"system","content":"You are a concise YC partner."},{"role":"user","content":prompt}], temperature=0.7, max_tokens=1500)
            return rsp.choices[0].message.content
        except Exception as e:
            return f"[OpenAI error] {e}\\nStub Ideas..."
    return "[LLM not configured]\\n1) Adaptive Meditation Coach...\\n2) CalmFeed..."

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--analysis", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    ensure_dirs(args.out)
    clusters = load_clusters(args.analysis)
    ideas = call_llm(PROMPT + "\\n\\n" + json.dumps({"clusters": clusters}, ensure_ascii=False, indent=2))
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(ideas)
    print(f"Wrote product ideas to {args.out}")

if __name__ == "__main__":
    main()
