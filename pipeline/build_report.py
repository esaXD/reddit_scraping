import argparse, markdown
from util import ensure_dirs

HTML_TMPL = """<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Reddit Insights</title>
<style>
body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 40px; background:#f8fafc; color:#1e293b; line-height:1.7; max-width:1200px; }
h1,h2,h3 { color:#0f172a; margin-top:1.5em; }
.box { background:#fff; padding:24px 30px; border-radius:16px; border:1px solid #e2e8f0; box-shadow:0 2px 6px rgba(0,0,0,.05); margin-bottom:30px; }
a { color:#2563eb; text-decoration:none; } a:hover{ text-decoration:underline; }
</style></head>
<body>
  <h1>Reddit Market Insights</h1>
  <div class="box"><p>This report summarizes Reddit posts from selected subreddits, clustered into key pain areas with gap hints and product ideas.</p></div>
  <h2>Pain Map</h2>
  <div class="box">{{PAIN_MAP}}</div>
  <h2>Product Ideas</h2>
  <div class="box">{{IDEAS}}</div>
</body></html>"""

def md_to_html(md_path: str) -> str:
    with open(md_path, "r", encoding="utf-8") as f:
        md = f.read()
    return markdown.markdown(md, extensions=["extra", "tables", "sane_lists", "nl2br"])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pain-map", required=True)
    ap.add_argument("--ideas", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    ensure_dirs(args.out)
    pain_html = md_to_html(args.pain_map)
    ideas_html = md_to_html(args.ideas)
    html = HTML_TMPL.replace("{{PAIN_MAP}}", pain_html).replace("{{IDEAS}}", ideas_html)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote HTML to {args.out}")

if __name__ == "__main__":
    main()
