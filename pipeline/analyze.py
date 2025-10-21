import argparse, pandas as pd, numpy as np
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk; nltk.download("vader_lexicon", quiet=True)
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from util import read_jsonl, heuristic_english, pain_score, extract_gaps, ensure_dirs
def best_k(X, kmin=6, kmax=12):
    best, best_s = kmin, -1
    for k in range(kmin, kmax+1):
        km=KMeans(n_clusters=k, n_init=10, random_state=42); labels=km.fit_predict(X)
        if len(set(labels))<2: continue
        s=silhouette_score(X, labels); 
        if s>best_s: best, best_s = k, s
    return best
def summarize_cluster(texts):
    vec=TfidfVectorizer(stop_words="english", max_features=2000); X=vec.fit_transform(texts)
    means=np.asarray(X.mean(axis=0)).ravel(); idx=means.argsort()[::-1][:10]
    vocab=np.array(vec.get_feature_names_out())[idx]; return vocab.tolist()
def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True); ap.add_argument("--report", dest="report_path", required=True)
    ap.add_argument("--kmin", type=int, default=6); ap.add_argument("--kmax", type=int, default=12); a=ap.parse_args()
    rows=list(read_jsonl(a.in_path)); df=pd.DataFrame(rows)
    df["text"]=(df["title"].fillna("")+". "+df["selftext"].fillna("")).astype(str)
    df=df[df["text"].str.len()>20]; df=df[df["text"].apply(heuristic_english)].copy(); df.reset_index(drop=True, inplace=True)
    sia=SentimentIntensityAnalyzer(); df["sentiment"]=df["text"].apply(lambda t: sia.polarity_scores(t)["compound"])
    df["pain_score"]=[pain_score(s,int(u),int(c)) for s,u,c in zip(df["sentiment"], df["upvotes"], df["num_comments"])]
    vec=TfidfVectorizer(stop_words="english", max_df=0.7, min_df=5, max_features=20000, ngram_range=(1,2)); X=vec.fit_transform(df["text"])
    k=best_k(X, a.kmin, a.kmax); km=KMeans(n_clusters=k, n_init=10, random_state=42); df["cluster"]=km.fit_predict(X)
    clusters=[]
    for cid in sorted(df["cluster"].unique()):
        cdf=df[df["cluster"]==cid]; top_terms=summarize_cluster(cdf["text"].tolist())
        top_posts=cdf.sort_values("pain_score", ascending=False).head(5)[["title","url","upvotes","num_comments","pain_score"]].to_dict(orient="records")
        gaps=[]; 
        for t in cdf["text"].head(500): gaps.extend(extract_gaps(t))
        clusters.append({"cluster_id":int(cid),"size":int(len(cdf)),"avg_pain":float(cdf["pain_score"].mean()),"top_terms":top_terms,"sample_posts":top_posts,"gap_examples":list(dict.fromkeys(gaps))[:10]})
    ensure_dirs(a.out_path, a.report_path); df.to_parquet(a.out_path, index=False)
    lines=["# Pain Map (Reddit)", f"- Items: **{len(df)}**", f"- Clusters: **{k}**",""]
    for c in clusters:
        lines.append(f"## Cluster {c['cluster_id']} — size {c['size']} — avg pain {c['avg_pain']:.2f}")
        lines.append(f"**Top terms:** {', '.join(c['top_terms'])}")
        if c["gap_examples"]: 
            lines.append("**Gap hints:**"); 
            for g in c["gap_examples"]: lines.append(f"- {g}")
        lines.append("**Painful posts:**")
        for p in c["sample_posts"]: lines.append(f"- [{p['title']}]({p['url']})  (↑{p['upvotes']}, 💬{p['num_comments']}, pain {p['pain_score']})")
        lines.append("")
    with open(a.report_path,"w",encoding="utf-8") as f: f.write("\n".join(lines))
    print(f"Wrote analysis to {a.out_path} and report to {a.report_path}")
if __name__=="__main__": main()
