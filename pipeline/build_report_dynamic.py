import argparse, json, markdown, pandas as pd
from util import ensure_dirs
TPL = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Reddit Research Report</title>
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:40px;background:#f8fafc;color:#1e293b;line-height:1.7;max-width:1200px}
h1,h2,h3{color:#0f172a;margin-top:1.5em}.box{background:#fff;padding:24px 30px;border-radius:16px;border:1px solid #e2e8f0;box-shadow:0 2px 6px rgba(0,0,0,.05);margin-bottom:30px}
a{color:#2563eb;text-decoration:none}a:hover{text-decoration:underline}
.small{color:#475569;font-size:.95em}
</style></head><body>
<h1>Reddit Research Report</h1>
<div class="box"><div class="small">
<b>Subreddits:</b> {subs}<br/><b>Params:</b> months={months}, min_upvotes={minup}, limit={limit}<br/><b>Keywords:</b> {keywords}<br/>
<b>Report type:</b> {rtype}
</div></div>
<h2>Pain Map</h2><div class="box">{pain}</div>
{extra}
</body></html>"""
def md_to_html(p):
    with open(p,"r",encoding="utf-8") as f: md=f.read()
    return markdown.markdown(md, extensions=["extra","tables","sane_lists","nl2br"])
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--plan", required=True); ap.add_argument("--pain-map", required=True)
    ap.add_argument("--analysis", required=True); ap.add_argument("--out", required=True); a=ap.parse_args()
    plan=json.load(open(a.plan,"r",encoding="utf-8"))
    pain=md_to_html(a.pain_map); df=pd.read_parquet(a.analysis)
    extra=""
    if plan["report_type"]=="sentiment":
        extra=f"<h2>Overall Sentiment</h2><div class='box'>Average compound sentiment: <b>{df['sentiment'].mean():.3f}</b></div>"
    html=TPL.format(subs=", ".join(plan["subreddits"]), months=plan["params"]["months"], minup=plan["params"]["min_upvotes"], limit=plan["params"]["limit"], keywords=" ".join(plan['filters'].get('keywords',[])) or "—", rtype=plan["report_type"], pain=pain, extra=extra)
    ensure_dirs(a.out); 
    with open(a.out,"w",encoding="utf-8") as f: f.write(html)
    print(f"Wrote {a.out}")
if __name__=="__main__": main()
