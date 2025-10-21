# pipeline/analyze.py
import argparse, os, json
import pandas as pd
import numpy as np
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
nltk.download("vader_lexicon", quiet=True)
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from util import read_jsonl, heuristic_english, pain_score, extract_gaps, ensure_dirs

REQ_COLS = ["title", "selftext", "upvotes", "num_comments"]

def best_k(X, kmin=6, kmax=12):
    best, best_s = kmin, -1
    for k in range(kmin, kmax+1):
        km = KMeans(n_clusters=k, n_init=10, random_state=42)
        labels = km.fit_predict(X)
        if len(set(labels)) < 2:
            continue
        s = silhouette_score(X, labels)
        if s > best_s:
            best, best_s = k, s
    return best

def summarize_cluster(texts):
    vectorizer = TfidfVectorizer(stop_words="english", max_features=2000)
    X = vectorizer.fit_transform(texts)
    means = np.asarray(X.mean(axis=0)).ravel()
    idx = means.argsort()[::-1][:10]
    vocab = np.array(vectorizer.get_feature_names_out())[idx]
    return vocab.tolist()

def write_empty_outputs(out_parquet, out_report, reason="No data"):
    ensure_dirs(out_parquet, out_report)
    # boş bir tablo üret (şema korunsun diye gerekli kolonları ekle)
    df_empty = pd.DataFrame(columns=REQ_COLS + ["text", "sentiment", "pain_score", "cluster"])
    df_empty.to_parquet(out_parquet, index=False)
    with open(out_report, "w", encoding="utf-8") as f:
        f.write(f"# Pain Map (Reddit)\n- Items: **0**\n- Clusters: **0**\n\n> {reason}\n")
    print(f"Wrote empty outputs: {out_parquet} & {out_report} ({reason})")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    ap.add_argument("--report", dest="report_path", required=True)
    ap.add_argument("--kmin", type=int, default=6)
    ap.add_argument("--kmax", type=int, default=12)
    args = ap.parse_args()

    # dosya yoksa/boşsa güvenli çık
    if not os.path.exists(args.in_path) or os.path.getsize(args.in_path) == 0:
        write_empty_outputs(args.out_path, args.report_path, reason="raw.jsonl missing or empty")
        return

    rows = list(read_jsonl(args.in_path))
    if not rows:
        write_empty_outputs(args.out_path, args.report_path, reason="No rows after scraping/filter")
        return

    df = pd.DataFrame(rows)

    # Eksik kolonları tamamla
    for c in REQ_COLS:
        if c not in df.columns:
            df[c] = "" if c in ("title", "selftext") else 0

    # metin alanını oluştur
    df["text"] = (df["title"].fillna("") + ". " + df["selftext"].fillna("")).astype(str)
    df = df[df["text"].str.len() > 20]
    df = df[df["text"].apply(heuristic_english)].copy()
    if df.empty:
        write_empty_outputs(args.out_path, args.report_path, reason="No english/long enough posts")
        return

    df.reset_index(drop=True, inplace=True)

    # sentiment & pain
    sia = SentimentIntensityAnalyzer()
    df["sentiment"] = df["text"].apply(lambda t: sia.polarity_scores(t)["compound"])
    # upvotes/num_comments numerik olsun
    df["upvotes"] = pd.to_numeric(df["upvotes"], errors="coerce").fillna(0).astype(int)
    df["num_comments"] = pd.to_numeric(df["num_comments"], errors="coerce").fillna(0).astype(int)
    df["pain_score"] = [pain_score(s, int(u), int(c)) for s,u,c in zip(df["sentiment"], df["upvotes"], df["num_comments"])]

    # vektörleştir & kümele
    try:
        vec = TfidfVectorizer(stop_words="english", max_df=0.7, min_df=5, max_features=20000, ngram_range=(1,2))
        X = vec.fit_transform(df["text"])
        if X.shape[0] < 2:
            raise ValueError("Not enough docs to cluster")
        k = best_k(X, args.kmin, args.kmax)
        km = KMeans(n_clusters=k, n_init=10, random_state=42)
        df["cluster"] = km.fit_predict(X)
    except Exception as e:
        # az veri durumunda clustersız devam
        df["cluster"] = 0
        k = 1

    # cluster özetleri
    clusters = []
    for cid in sorted(df["cluster"].unique()):
        cdf = df[df["cluster"] == cid]
        try:
            top_terms = summarize_cluster(cdf["text"].tolist())
        except Exception:
            top_terms = []
        top_posts = cdf.sort_values("pain_score", ascending=False).head(5)[
            ["title","url","upvotes","num_comments","pain_score"]
        ].to_dict(orient="records")
        gaps = []
        for t in cdf["text"].head(500):
            gaps.extend(extract_gaps(t))
        clusters.append({
            "cluster_id": int(cid),
            "size": int(len(cdf)),
            "avg_pain": float(cdf["pain_score"].mean()) if len(cdf) else 0.0,
            "top_terms": top_terms,
            "sample_posts": top_posts,
            "gap_examples": list(dict.fromkeys(gaps))[:10],
        })

    ensure_dirs(args.out_path, args.report_path)
    df.to_parquet(args.out_path, index=False)

    # rapor
    lines = [
        "# Pain Map (Reddit)",
        f"- Items: **{len(df)}**",
        f"- Clusters: **{k}**",
        ""
    ]
    for c in clusters:
        lines.append(f"## Cluster {c['cluster_id']} — size {c['size']} — avg pain {c['avg_pain']:.2f}")
        if c["top_terms"]:
            lines.append(f"**Top terms:** {', '.join(c['top_terms'])}")
        if c["gap_examples"]:
            lines.append("**Gap hints:**")
            for g in c["gap_examples"]:
                lines.append(f"- {g}")
        if c["sample_posts"]:
            lines.append("**Painful posts:**")
            for p in c["sample_posts"]:
                lines.append(f"- [{p['title']}]({p.get('url','')})  (↑{p['upvotes']}, 💬{p['num_comments']}, pain {p['pain_score']})")
        lines.append("")
    with open(args.report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Wrote analysis to {args.out_path} and report to {args.report_path}")

if __name__ == "__main__":
    main()
