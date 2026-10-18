"""BuyBox Analytics insights dashboard.

Reads only the small result files the Spark / MapReduce / Cassandra pipeline wrote to outputs/,
so it runs anywhere (Windows included) without Spark:

    streamlit run dashboard/app.py

A page can be opened directly with ?page=<key>, e.g. http://localhost:8501/?page=stream
"""
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs"
TABLES, FIGS = OUT / "tables", OUT / "figures"

BLUE, ORANGE, GREEN, RED, PURPLE, GREY = "#2E6FBA", "#E08A2B", "#3A9A5B", "#C8423B", "#7B5EA7", "#8A8F98"
RATING_COLORS = {1: "#C8423B", 2: "#E07B53", 3: "#C9B458", 4: "#7FB069", 5: "#3A9A5B"}
SEQ = [BLUE, ORANGE, GREEN, PURPLE, RED, "#2A9D8F", "#B5838D", GREY]

st.set_page_config(page_title="BuyBox Analytics · Amazon review insights", page_icon="📊", layout="wide")

# ---------- one consistent look for every chart ----------
pio.templates["reviewlens"] = go.layout.Template(layout=dict(
    font=dict(family="Inter, Segoe UI, sans-serif", size=13, color="#1C2433"),
    paper_bgcolor="white", plot_bgcolor="white", colorway=SEQ,
    margin=dict(l=10, r=10, t=40, b=10), title=dict(font=dict(size=15), x=0, xanchor="left"),
    xaxis=dict(gridcolor="#EEF1F6", zeroline=False, linecolor="#D5DAE3"),
    yaxis=dict(gridcolor="#EEF1F6", zeroline=False, linecolor="#D5DAE3"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, title=None),
    hoverlabel=dict(bgcolor="white"),
))
pio.templates.default = "reviewlens"

st.markdown("""
<style>
  .block-container {padding-top: 3.2rem; padding-bottom: 2rem; max-width: 1400px;}
  h1, h2, h3 {letter-spacing: -0.01em;}
  .page-title {font-size: 1.9rem; font-weight: 700; margin: 0 0 .1rem 0; color: #1C2433;}
  .page-sub {color: #5B6475; font-size: 1rem; margin-bottom: 1.2rem;}
  .kpi {background: white; border: 1px solid #E3E8F0; border-radius: 12px; padding: 14px 16px;
        box-shadow: 0 1px 2px rgba(16,24,40,.04); min-height: 128px;}
  .kpi .label {color: #5B6475; font-size: .78rem; text-transform: uppercase; letter-spacing: .05em; font-weight: 600;}
  .kpi .value {font-size: 1.65rem; font-weight: 700; color: #1C2433; line-height: 1.25; margin-top: 2px;}
  .kpi .note {color: #5B6475; font-size: .8rem; margin-top: 2px;}
  .kpi.accent {border-left: 4px solid #2E6FBA;}
  .insight {background: #EEF4FB; border: 1px solid #D3E2F4; border-radius: 10px; padding: 12px 16px;
            color: #1C2433; font-size: .95rem; margin-bottom: .6rem;}
  .insight b {color: #1F4E8C;}
  [data-testid="stSidebar"] {background: #1F2A44;}
  [data-testid="stSidebar"] * {color: #E8ECF4 !important;}
  [data-testid="stSidebar"] .brand {font-size: 1.45rem; font-weight: 800; margin-bottom: 0;}
  [data-testid="stSidebar"] .tag {font-size: .82rem; opacity: .75; margin-bottom: 1rem;}
  [data-testid="stSidebar"] hr {border-color: rgba(255,255,255,.15);}
  [data-testid="stVerticalBlockBorderWrapper"] {background: white; border-radius: 12px;}
  .section {font-size: 1.05rem; font-weight: 700; color: #1C2433; margin: 0 0 .3rem 0;}
  .caption {color: #5B6475; font-size: .85rem; margin-top: -.2rem; margin-bottom: .4rem;}
</style>
""", unsafe_allow_html=True)


# ---------- data helpers ----------
@st.cache_data
def table(name, sep=","):
    path = TABLES / name
    return pd.read_csv(path, sep=sep) if path.exists() else pd.DataFrame()


@st.cache_data
def summary():
    f = OUT / "summary.json"
    return json.loads(f.read_text()) if f.exists() else {}


def kpis(items, accent=False):
    cols = st.columns(len(items))
    for col, (label, value, note) in zip(cols, items):
        col.markdown(f"""<div class="kpi{' accent' if accent else ''}"><div class="label">{label}</div>
            <div class="value">{value}</div><div class="note">{note or '&nbsp;'}</div></div>""",
                     unsafe_allow_html=True)
    st.write("")


def header(title, sub):
    st.markdown(f'<div class="page-title">{title}</div><div class="page-sub">{sub}</div>', unsafe_allow_html=True)


def insight(html):
    st.markdown(f'<div class="insight">{html}</div>', unsafe_allow_html=True)


def card(title, caption=None):
    box = st.container(border=True)
    box.markdown(f'<div class="section">{title}</div>' + (f'<div class="caption">{caption}</div>' if caption else ""),
                 unsafe_allow_html=True)
    return box


def chart(box, fig, height=360):
    fig.update_layout(height=height)
    if getattr(fig.layout, "yaxis2", None) is not None and fig.layout.yaxis2.overlaying:
        fig.update_layout(yaxis2=dict(tickformat=".2f", nticks=6))
    box.plotly_chart(fig, width="stretch", config={"displayModeBar": False})


def image(box, name):
    p = FIGS / f"{name}.png"
    if p.exists():
        box.image(str(p), width="stretch")


def short(s, n=55):
    s = str(s)
    return s if len(s) <= n else s[: n - 1] + "…"


S = summary()
if not S:
    st.error("No pipeline output found. Run the pipeline first (see README).")
    st.stop()
ing, ana, sen = S.get("ingest", {}), S.get("analytics", {}), S.get("sentiment", {})
gs, strm, nosql, perf = S.get("graph_segments", {}), S.get("streaming", {}), S.get("nosql", {}), S.get("performance", {})

# ---------- navigation (sidebar + ?page= deep links) ----------
PAGES = {
    "overview": "🏠  Overview",
    "trends": "📈  Ratings & trends",
    "products": "🛍️  Products & brands",
    "sentiment": "💬  Text & sentiment",
    "network": "🕸️  Network & segments",
    "stream": "⚡  Real-time stream",
    "storage": "🗄️  Hadoop & Cassandra",
    "performance": "🚀  Performance",
}
keys = list(PAGES)
current = st.query_params.get("page", "overview")
current = current if current in PAGES else "overview"
with st.sidebar:
    st.markdown('<div class="brand">BuyBox Analytics</div><div class="tag">Big Data Analytics on Amazon reviews</div>',
                unsafe_allow_html=True)
    page = st.radio("Navigate", keys, index=keys.index(current), format_func=PAGES.get, label_visibility="collapsed")
    st.query_params["page"] = page
    st.markdown("---")
    st.markdown(f"""**Dataset** · Amazon Reviews 2023<br>
Category: {ing.get('category', '?').replace('_', ' ')}<br>
{ing.get('first_year')}–{ing.get('last_year')} · {ing.get('raw_mb', 0):.0f} MB JSON

**Stack** · HDFS · MapReduce · Spark 3.5 · MLlib · Structured Streaming · Cassandra 5.0

Last run: {S.get('run', {}).get('finished', '?')}""", unsafe_allow_html=True)

# =====================================================================================
if page == "overview":
    header("BuyBox Analytics: Amazon review analytics at scale",
           "540 MB of semi-structured JSON reviews processed end to end on Hadoop, Spark and Cassandra.")
    kpis([("Reviews (clean)", f"{ing.get('clean_reviews', 0):,}", f"{ing.get('removed_rows', 0):,} rows removed"),
          ("Products", f"{ing.get('reviewed_products', 0):,}", f"{ing.get('products_in_metadata', 0):,} in metadata"),
          ("Customers", f"{ing.get('users', 0):,}", "unique reviewers"),
          ("Average rating", f"{ana.get('avg_rating', 0):.2f} ★", f"{ana.get('pct_5_star', 0):.0f}% are 5★"),
          ("Negative reviews", f"{ana.get('pct_negative', 0):.1f}%", "rated 1–2★"),
          ("Storage", f"{ing.get('parquet_mb', 0):.0f} MB",
           f"Parquet (−{100 * (1 - ing.get('parquet_mb', 1) / max(ing.get('raw_mb', 1), 1)):.0f}% vs JSON)")],
         accent=True)

    left, right = st.columns([1.55, 1])
    with left:
        box = card("System architecture", "Raw JSON lands in HDFS, is processed by MapReduce and Spark, "
                                          "served from Cassandra and visualised here.")
        image(box, "00_architecture")
    with right:
        box = card("Key insights")
        m = sen.get("models", [{}])[0]
        with box:
            insight(f"<b>J-shaped ratings:</b> {ana.get('pct_5_star', 0):.0f}% of reviews are 5★, yet "
                    f"{ana.get('pct_negative', 0):.0f}% are 1–2★.")
            insight(f"<b>Peak activity:</b> {ana.get('peak_month')} with {ana.get('peak_month_reviews', 0):,} reviews; "
                    "satisfaction fell to its lowest in 2022.")
            insight(f"<b>Top complaints:</b> {', '.join(ana.get('top_complaint_bigrams', [])[:4])}.")
            insight(f"<b>Sentiment model:</b> {m.get('accuracy', 0):.1%} accuracy; "
                    f"{sen.get('hidden_complaints', 0):,} five-star reviews hide complaints.")
            insight(f"<b>Real-time:</b> {strm.get('products_flagged', 0)} products flagged for negative spikes "
                    "while the stream was replayed.")
            insight(f"<b>Optimisation:</b> Parquet {perf.get('parquet_vs_json_speedup')}× and partition pruning "
                    f"{perf.get('partition_pruning_speedup')}× faster than raw JSON.")

    q = ing.get("quality", {})
    c1, c2 = st.columns([1, 1.55])
    with c1:
        box = card("Data quality (veracity)")
        box.dataframe(pd.DataFrame([
            ("Raw reviews", f"{ing.get('raw_reviews', 0):,}"), ("Empty review text", f"{q.get('empty_text', 0):,}"),
            ("Duplicates + empty removed", f"{ing.get('removed_rows', 0):,}"),
            ("Malformed metadata rows", f"{ing.get('corrupt_metadata_rows', 0):,}"),
            ("Reviews with photos", f"{q.get('with_images', 0):,}")], columns=["Check", "Count"]),
            hide_index=True, width="stretch")
    with c2:
        steps = S.get("run", {}).get("step_seconds", {})
        if steps:
            box = card("Pipeline run time per Spark step", "Seconds on a 16-thread laptop (WSL2, single node)")
            d = pd.DataFrame(steps.items(), columns=["step", "seconds"])
            chart(box, px.bar(d, x="seconds", y="step", orientation="h", text="seconds",
                              color_discrete_sequence=[BLUE]).update_yaxes(title=None, autorange="reversed")
                  .update_xaxes(title=None), 280)

# =====================================================================================
elif page == "trends":
    header("Ratings & trends", "How review volume and satisfaction evolve over time, and when customers write.")
    m = table("monthly_trend.csv")
    m["month"] = pd.to_datetime(m["month"])
    box = card("Review volume and average rating per month",
               f"Volume peaked in {ana.get('peak_month')} ({ana.get('peak_month_reviews', 0):,} reviews); "
               "the 6-month rolling rating fell to its lowest point in 2022.")
    fig = go.Figure()
    fig.add_bar(x=m["month"], y=m["reviews"], name="Reviews per month", marker_color=BLUE, opacity=0.75)
    fig.add_scatter(x=m["month"], y=m["avg_rating"].rolling(6, min_periods=1).mean(), name="Avg rating (6-month)",
                    yaxis="y2", line=dict(color=ORANGE, width=3))
    fig.update_layout(yaxis=dict(title="Reviews"), yaxis2=dict(overlaying="y", side="right", range=[3.4, 4.4],
                                                               title="Avg rating", showgrid=False))
    chart(box, fig, 380)

    c1, c2 = st.columns(2)
    rd = table("rating_distribution.csv")
    with c1:
        box = card("Rating distribution", "A J-shaped curve: customers review when delighted or angry.")
        rd["stars"] = rd["rating"].astype(int).astype(str) + "★"
        chart(box, px.bar(rd, x="stars", y="count", text=rd["pct"].map("{:.1f}%".format), color="stars",
                          color_discrete_sequence=[RATING_COLORS[i] for i in range(1, 6)])
              .update_layout(showlegend=False).update_xaxes(title=None).update_yaxes(title="Reviews"), 340)
    with c2:
        hy = table("heatmap_year_rating.csv").set_index("year")
        hy.columns = [f"{int(float(c))}★" for c in hy.columns]
        box = card("Rating mix by year", "% of each year's reviews; the 1★ share rises after 2020.")
        chart(box, px.imshow(hy.round(1), aspect="auto", color_continuous_scale="YlOrRd", text_auto=".0f",
                             labels=dict(x="", y="", color="%")), 340)

    hw = table("heatmap_weekday_hour.csv").set_index("weekday")
    box = card("When are reviews written?", "Reviews per weekday and hour (UTC): most arrive 17:00–20:00 UTC "
                                            "on weekdays, i.e. US daytime.")
    chart(box, px.imshow(hw, aspect="auto", color_continuous_scale="Blues",
                         labels=dict(x="Hour of day (UTC)", y="", color="Reviews")), 300)

# =====================================================================================
elif page == "products":
    header("Products & brands", "Which products and brands customers love, and which ones disappoint.")
    vu = table("verified_vs_unverified.csv").set_index("verified_purchase")
    kpis([("Verified purchases", f"{vu.loc[True, 'n_reviews']:,}", f"avg {vu.loc[True, 'avg_rating']:.2f}★"),
          ("Unverified", f"{vu.loc[False, 'n_reviews']:,}", f"avg {vu.loc[False, 'avg_rating']:.2f}★ (higher)"),
          ("Review length", f"{vu.loc[False, 'avg_length'] / vu.loc[True, 'avg_length']:.1f}×",
           "unverified vs verified"),
          ("Lowest-rated popular", f"{table('worst_popular_products.csv')['avg_rating'].min():.2f} ★",
           "magnetic eyelashes, 315 reviews")])

    def products(name):
        d = table(name).drop(columns="parent_asin").head(10)
        d["product"] = d["product"].map(lambda s: short(s, 36))
        return d

    cols = {"product": st.column_config.TextColumn("Product", width="medium"),
            "brand": "Brand", "n_reviews": st.column_config.NumberColumn("Reviews", format="%d"),
            "avg_rating": st.column_config.ProgressColumn("Avg ★", min_value=1, max_value=5, format="%.2f"),
            "pct_negative": st.column_config.NumberColumn("% negative", format="%.1f%%")}
    c1, c2 = st.columns(2)
    with c1:
        box = card("Most-reviewed products")
        box.dataframe(products("top_products.csv"), column_config=cols,
                      hide_index=True, width="stretch", height=390)
    with c2:
        box = card("Popular but poorly rated", "Products with ≥ 200 reviews and the lowest averages: quality alarms.")
        box.dataframe(products("worst_popular_products.csv"), column_config=cols,
                      hide_index=True, width="stretch", height=390)

    tb = table("top_brands.csv")
    c1, c2 = st.columns([1.5, 1])
    with c1:
        box = card("Brands: volume vs satisfaction", "Bubble size = number of products; colour = average rating.")
        tb["label"] = [b if i < 8 or r < 3.7 or r > 4.5 else "" for i, (b, r) in enumerate(zip(tb["brand"], tb["avg_rating"]))]
        fig = px.scatter(tb, x="n_reviews", y="avg_rating", size="products", color="avg_rating", text="label",
                         hover_name="brand",
                         color_continuous_scale="RdYlGn", range_color=[3.3, 4.7], size_max=40,
                         labels=dict(n_reviews="Reviews", avg_rating="Avg rating"))
        fig.update_traces(textposition="top center", textfont_size=10)
        chart(box, fig, 430)
    with c2:
        box = card("Rating behaviour by stars", "Helpful votes are U-shaped; mixed 2–4★ reviews are the longest.")
        rh = table("rating_helpfulness.csv")
        rh["stars"] = rh["rating"].astype(int).astype(str) + "★"
        fig = go.Figure()
        fig.add_bar(x=rh["stars"], y=rh["avg_length"], name="Avg length (chars)",
                    marker_color=[RATING_COLORS[i] for i in range(1, 6)])
        fig.add_scatter(x=rh["stars"], y=rh["avg_helpful"], name="Avg helpful votes", yaxis="y2",
                        line=dict(color="#1C2433", width=3), mode="lines+markers")
        fig.update_layout(yaxis=dict(title="Characters"),
                          yaxis2=dict(overlaying="y", side="right", title="Helpful votes", showgrid=False))
        chart(box, fig, 430)

# =====================================================================================
elif page == "sentiment":
    header("Text & sentiment", "What customers say, a Spark MLlib sentiment model, and ratings that contradict text.")
    res = table("model_comparison.csv")
    lr = res.iloc[0]
    kpis([("Accuracy", f"{lr['accuracy']:.1%}", "logistic regression"),
          ("ROC-AUC", f"{lr['roc_auc']:.3f}", "positive vs negative"),
          ("Recall (negative)", f"{lr['recall_negative']:.1%}", "unhappy reviews caught"),
          ("Training set", f"{sen.get('training_reviews', 0):,}", "reviews, 3★ excluded"),
          ("Hidden complaints", f"{sen.get('hidden_complaints', 0):,}",
           f"{sen.get('hidden_complaint_pct', 0)}% of 5★ test reviews")], accent=True)

    c1, c2 = st.columns([1, 1.4])
    with c1:
        cm = table("confusion_matrix.csv").set_index("label")
        cm.index, cm.columns = ["Actual negative", "Actual positive"], ["Predicted negative", "Predicted positive"]
        box = card("Confusion matrix (test set)")
        chart(box, px.imshow(cm, text_auto=",d", color_continuous_scale="Blues", aspect="auto")
              .update_coloraxes(showscale=False), 330)
        res_view = res[["model", "accuracy", "f1_weighted", "roc_auc"]].copy()
        res_view["model"] = res_view["model"].str.replace(r" \(.*\)", "", regex=True)
        box.dataframe(res_view.rename(columns={"model": "Model", "accuracy": "Accuracy", "f1_weighted": "F1",
                                               "roc_auc": "ROC-AUC"}), hide_index=True, width="stretch")
    with c2:
        terms = table("sentiment_terms.csv")
        t = pd.concat([terms[terms.side == "negative"].head(12), terms[terms.side == "positive"].head(12)])
        box = card("Most influential terms", "Logistic-regression weights for words/bigrams in ≥ 1,000 reviews. "
                                             "Negations like “not good” are learned as phrases.")
        chart(box, px.bar(t.sort_values("weight"), x="weight", y="term", orientation="h", color="side",
                          color_discrete_map={"negative": RED, "positive": GREEN})
              .update_yaxes(title=None).update_xaxes(title="Weight"), 560)

    c1, c2 = st.columns(2)
    with c1:
        wb = table("complaint_bigrams.csv").head(15)
        box = card("Top complaint phrases (1–2★)", "Bigram count with Spark RDDs: flatMap → map → reduceByKey")
        chart(box, px.bar(wb.iloc[::-1], x="count", y="bigram", orientation="h", color_discrete_sequence=[RED])
              .update_yaxes(title=None), 420)
    with c2:
        pw = table("praise_words.csv").head(15)
        box = card("Top praise words (4–5★)")
        chart(box, px.bar(pw.iloc[::-1], x="count", y="word", orientation="h", color_discrete_sequence=[GREEN])
              .update_yaxes(title=None), 420)

    box = card("Ratings that contradict the text",
               "5★ reviews the model is ≥ 80% sure are negative, and 1★ reviews it reads as positive.")
    ex = table("mismatch_examples.csv")
    ex["text"] = ex["text"].str.replace(r"<br\s*/?>", " ", regex=True)
    box.dataframe(ex, hide_index=True, width="stretch", height=300,
                  column_config={"type": st.column_config.TextColumn("Type", width="small"),
                                 "rating": st.column_config.NumberColumn("Stars", format="%d★", width="small"),
                                 "p_negative": st.column_config.NumberColumn("P(negative)", format="%.2f", width="small"),
                                 "text": st.column_config.TextColumn("Review text", width="large")})

# =====================================================================================
elif page == "network":
    header("Network & segments", "Products linked by shared customers, and products grouped by behaviour.")
    kpis([("Products in graph", f"{gs.get('graph_nodes', 0):,}", "reviewed by ≥ 2 shared customers"),
          ("Links", f"{gs.get('graph_edges', 0):,}", "co-review edges"),
          ("Largest component", f"{gs.get('giant_component', 0):,}", "connected products"),
          ("Segments", f"{gs.get('k', 0)}", f"K-Means, silhouette {gs.get('silhouette', 0):.2f}"),
          ("Products segmented", f"{gs.get('segmented_products', 0):,}", "with ≥ 20 reviews")])
    c1, c2 = st.columns([1.3, 1])
    with c1:
        box = card("Co-review network", "Core of the largest component · colour = Louvain community · "
                                        "size = PageRank (computed on Spark RDDs)")
        image(box, "09_coreview_network")
    with c2:
        box = card("Communities (shopping baskets)")
        cm = table("network_communities.csv")
        cm["example_products"] = cm["example_products"].map(lambda s: short(str(s).split(" | ")[0], 38))
        cm["top_brands"] = cm["top_brands"].map(lambda s: short(s, 28))
        box.dataframe(cm, hide_index=True, width="stretch",
                      column_config={"community": st.column_config.NumberColumn("#", width="small"),
                                     "size": "Products", "avg_rating": st.column_config.NumberColumn("Avg ★", format="%.2f"),
                                     "top_brands": "Top brands", "example_products": st.column_config.TextColumn(
                                         "Example product", width="medium")})
        pr = table("network_top_pagerank.csv").head(8)
        pr["product_title"] = pr["product_title"].map(short)
        box = card("Hub products (highest PageRank)")
        chart(box, px.bar(pr.iloc[::-1], x="pagerank", y="product_title", orientation="h",
                          color_discrete_sequence=[PURPLE]).update_yaxes(title=None)
              .update_xaxes(title="PageRank", tickformat=".3f", nticks=4, tickangle=0), 330)

    pts, seg = table("product_pca_points.csv"), table("product_segments.csv")
    names = dict(zip(seg["cluster"], seg["segment"]))
    pts["segment"] = pts["prediction"].map(names)
    c1, c2 = st.columns([1.3, 1])
    with c1:
        box = card("Product segments in 2-D", f"10 behavioural features → K-Means → PCA "
                                              f"({gs.get('pca_variance_2d', 0):.0%} of variance shown). Hover for products.")
        chart(box, px.scatter(pts, x="pc1", y="pc2", color="segment", hover_name="product_title", opacity=0.55,
                              hover_data={"n_reviews": True, "avg_rating": ":.2f", "pc1": False, "pc2": False,
                                          "segment": False}, labels=dict(pc1="PC1", pc2="PC2"))
              .update_traces(marker_size=5), 460)
    with c2:
        box = card("Segment profiles")
        prof = seg[["segment", "products", "avg_rating", "pct_negative", "pct_verified", "n_reviews"]].copy()
        prof["pct_negative"] *= 100
        prof["pct_verified"] *= 100
        box.dataframe(prof, hide_index=True, width="stretch", column_config={
            "segment": st.column_config.TextColumn("Segment", width="medium"), "products": "Products",
            "avg_rating": st.column_config.NumberColumn("Avg ★", format="%.2f"),
            "pct_negative": st.column_config.NumberColumn("% neg", format="%.0f%%"),
            "pct_verified": st.column_config.NumberColumn("% verified", format="%.0f%%"),
            "n_reviews": st.column_config.NumberColumn("Reviews/product", format="%.0f")})
        insight("<b>Low-verified, long reviews</b> (only 45% verified, 2.5× longer) look like incentivised or "
                "early-review-programme products. <b>Problem products</b> have 38% negative reviews.")

# =====================================================================================
elif page == "stream":
    header("Real-time stream", "Spark Structured Streaming replay of Jan–Mar 2021 with event-time windows "
                               "and product alerts.")
    d = table("stream_daily_kpis.csv")
    d["day"] = pd.to_datetime(d["day"])
    al = table("stream_alerts.csv")
    al["window_start"] = pd.to_datetime(al["window_start"])
    kpis([("Events replayed", f"{strm.get('replayed_events', 0):,}", f"{strm.get('files', 0)} files in landing zone"),
          ("Micro-batches", f"{strm.get('micro_batches', 0)}", "trigger every 2 s"),
          ("Throughput", f"{strm.get('avg_rows_per_sec', 0):,.0f}/s", "rows per second (avg)"),
          ("Products flagged", f"{strm.get('products_flagged', 0)}", "≥ 5 negatives and ≥ 40% in 7 days")], accent=True)

    upto = st.select_slider("Replay the stream up to", options=list(d["day"].dt.date), value=d["day"].dt.date.iloc[-1])
    v = d[d["day"].dt.date <= upto]
    shown = al[al["window_start"].dt.date <= upto]
    c1, c2 = st.columns([1.6, 1])
    with c1:
        box = card("Live daily KPIs", "Daily event-time windows with a 10-day watermark; red markers are alerts.")
        fig = go.Figure()
        fig.add_bar(x=v["day"], y=v["reviews"], name="Reviews/day", marker_color=BLUE, opacity=0.8)
        fig.add_scatter(x=v["day"], y=v["avg_rating"].rolling(7, min_periods=1).mean(), yaxis="y2",
                        name="Avg rating (7-day)", line=dict(color=ORANGE, width=3))
        per_day = shown.groupby(shown["window_start"].dt.floor("D")).size()
        fig.add_scatter(x=per_day.index, y=[v["reviews"].max() * 1.08] * len(per_day), mode="markers",
                        name="Alert raised", marker=dict(color=RED, size=11, symbol="triangle-down"))
        fig.update_layout(xaxis=dict(range=[d["day"].min(), d["day"].max()]), yaxis=dict(title="Reviews"),
                          yaxis2=dict(overlaying="y", side="right", range=[3.5, 4.3], title="Avg rating",
                                      showgrid=False))
        chart(box, fig, 420)
    with c2:
        box = card(f"Alerts raised: {len(shown)}", "Products with a negative spike in a 7-day sliding window")
        a = shown.sort_values("window_start", ascending=False)[["window_start", "product", "negatives", "reviews",
                                                                  "avg_rating"]]
        box.dataframe(a, hide_index=True, width="stretch", height=400, column_config={
            "window_start": st.column_config.DateColumn("Week of", format="MMM D"),
            "product": st.column_config.TextColumn("Product", width="large"),
            "negatives": "Neg", "reviews": "Total", "avg_rating": st.column_config.NumberColumn("Avg ★", format="%.1f")})
    b = table("stream_batches.csv")
    b = b[b["input_rows"] > 0].reset_index(drop=True)
    b["batch"] = range(1, len(b) + 1)
    box = card("Micro-batch processing", "Rows per micro-batch and processing time (two files per trigger)")
    fig = go.Figure()
    fig.add_bar(x=b["batch"], y=b["input_rows"], name="Rows in batch", marker_color=BLUE)
    fig.add_scatter(x=b["batch"], y=b["duration_ms"] / 1000, name="Batch duration (s)", yaxis="y2",
                    line=dict(color=ORANGE, width=2), mode="lines+markers")
    fig.update_layout(xaxis_title="Micro-batch", yaxis=dict(title="Rows"),
                      yaxis2=dict(overlaying="y", side="right", title="Seconds", showgrid=False))
    chart(box, fig, 280)

# =====================================================================================
elif page == "storage":
    header("Hadoop & Cassandra", "Distributed storage in HDFS, MapReduce jobs on the raw JSON, and the Cassandra "
                                 "serving layer.")
    loads = table("cassandra_loads.csv")
    q = table("cassandra_queries.csv")
    kpis([("HDFS block size", "64 MB", "review file = 5 blocks"),
          ("MapReduce jobs", "2", "Hadoop Streaming (Python)"),
          ("Rows in Cassandra", f"{loads['rows'].sum():,}" if len(loads) else "–", "3 query-first tables"),
          ("Partition read", f"{q.iloc[1:4]['ms'].median():.1f} ms" if len(q) else "–", "single-partition query"),
          ("Full scan", f"{q.iloc[0]['ms'] / 1000:.1f} s" if len(q) else "–", "anti-pattern, for contrast")])
    c1, c2 = st.columns(2)
    with c1:
        wc = table("mr_complaint_wordcount.tsv", sep="\t")
        if len(wc):
            wc.columns = ["word", "count"]
            box = card("MapReduce job 1: complaint word count", "mapper → combiner → reducer over 1–2★ reviews in HDFS")
            chart(box, px.bar(wc.head(15).iloc[::-1], x="count", y="word", orientation="h",
                              color_discrete_sequence=[RED]).update_yaxes(title=None), 420)
    with c2:
        ps = TABLES / "mr_product_stats.tsv"
        if ps.exists():
            box = card("MapReduce job 2: per-product rating statistics", "Rating histogram computed by the reducer")
            mr = pd.read_csv(ps, sep="\t").head(30)
            mr["pct_negative"] *= 100
            box.dataframe(mr[["parent_asin", "n_reviews", "avg_rating", "pct_negative", "r1", "r2", "r3", "r4", "r5"]],
                          hide_index=True, width="stretch", height=420, column_config={
                              "parent_asin": "Product (ASIN)", "n_reviews": "Reviews",
                              "avg_rating": st.column_config.NumberColumn("Avg ★", format="%.2f"),
                              "pct_negative": st.column_config.NumberColumn("% neg", format="%.0f%%"),
                              "r1": "1★", "r2": "2★", "r3": "3★", "r4": "4★", "r5": "5★"})
    c1, c2 = st.columns([1, 1.2])
    with c1:
        box = card("Cassandra data model", "One table per application query (query-first design)")
        box.code("""-- Q1 product card
product_summary:    PRIMARY KEY (parent_asin)
-- Q2 latest reviews of a product
reviews_by_product: PRIMARY KEY ((parent_asin), ts, user_id)
                    ORDER BY ts DESC
-- Q3 monthly trend of a brand
brand_monthly:      PRIMARY KEY ((brand), month)
                    ORDER BY month DESC""", language="sql")
        box.dataframe(loads.rename(columns={"table": "Table", "rows": "Rows", "seconds": "Load s",
                                            "rows_per_sec": "Rows/s"}), hide_index=True, width="stretch")
    with c2:
        box = card("Query latency (log scale)", "Single-partition reads vs a full-table scan")
        q2 = q.copy()
        q2["query"] = q2["query"].str.replace(r" \(.*\)", "", regex=True)
        q2["kind"] = ["Full scan"] + ["Partition query"] * 4 + ["CRUD"] * (len(q2) - 5)
        chart(box, px.bar(q2.iloc[::-1], x="ms", y="query", orientation="h", color="kind", log_x=True, text="ms",
                          color_discrete_map={"Full scan": RED, "Partition query": GREEN, "CRUD": BLUE})
              .update_yaxes(title=None).update_xaxes(title="milliseconds"), 400)
    rep = OUT / "hdfs_report.txt"
    if rep.exists():
        with st.expander("HDFS commands and report (hdfs dfs, fsck, dfsadmin)"):
            st.code(rep.read_text(), language="bash")

# =====================================================================================
elif page == "performance":
    header("Performance", "Each optimisation measured on the same query with and without it.")
    kpis([("Parquet vs JSON", f"{perf.get('parquet_vs_json_speedup')}×", "columnar storage"),
          ("Partition pruning", f"{perf.get('partition_pruning_speedup')}×", "partitionBy(year)"),
          ("Column pruning", f"{perf.get('column_pruning_speedup')}×", "read only needed columns"),
          ("Caching", f"{perf.get('cache_speedup')}×", "iterative workload"),
          ("Broadcast join", f"{perf.get('broadcast_join_speedup')}×", "vs sort-merge join"),
          ("Parallelism", f"{perf.get('single_partition_seconds', 0) / max(perf.get('best_partition_seconds', 1), 1e-3):.1f}×",
           f"1 → {perf.get('best_partitions')} partitions")], accent=True)
    p = table("performance_experiments.csv")
    p["variant"] = ["Baseline", "Optimised"] * (len(p) // 2)
    box = card("Baseline vs optimised", "Best of 3 runs, seconds (lower is better)")
    chart(box, px.bar(p, x="experiment", y="seconds", color="variant", barmode="group", hover_data=["setup"],
                      text="seconds", color_discrete_map={"Baseline": RED, "Optimised": GREEN})
          .update_xaxes(title=None), 380)
    c1, c2 = st.columns(2)
    with c1:
        pp = table("performance_partitions.csv")
        box = card("Parallelism", "RDD word count over all reviews with 1–32 partitions")
        chart(box, px.line(pp, x="partitions", y="seconds", markers=True, log_x=True,
                           color_discrete_sequence=[PURPLE])
              .update_xaxes(title="Partitions", tickvals=list(pp["partitions"]),
                            ticktext=list(map(str, pp["partitions"]))), 320)
    with c2:
        sc = table("performance_scalability.csv")
        box = card("Scalability", "Runtime as the data grows from 12.5% to 100%")
        chart(box, px.line(sc, x="rows", y="seconds", markers=True, color_discrete_sequence=[BLUE])
              .update_xaxes(title="Rows"), 320)
