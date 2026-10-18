"""Step 4 - Network graph and high-dimensional analysis.

(a) Product co-review network: two products are linked when the same customers reviewed both.
    PageRank is implemented with Spark RDDs (iterative Spark algorithm), communities with NetworkX.
(b) Product segmentation: 10 behavioural features per product -> StandardScaler -> K-Means (MLlib)
    -> PCA to 2-D so the high-dimensional clusters can be visualised.
"""
import math

import networkx as nx
import numpy as np
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import PCA, StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from . import config as C
from .viz import plt

MIN_SHARED_REVIEWERS = 2


def co_review_edges(df):
    """Self-join on user_id -> (product_a, product_b, shared_reviewers)."""
    ur = df.select("user_id", "parent_asin").distinct()
    active = ur.groupBy("user_id").count().filter("count BETWEEN 2 AND 50").select("user_id")
    ur = ur.join(active, "user_id")
    a, b = ur.alias("a"), ur.alias("b")
    return (a.join(b, (F.col("a.user_id") == F.col("b.user_id")) & (F.col("a.parent_asin") < F.col("b.parent_asin")))
             .groupBy(F.col("a.parent_asin").alias("src"), F.col("b.parent_asin").alias("dst"))
             .agg(F.count("*").alias("weight"))
             .filter(F.col("weight") >= MIN_SHARED_REVIEWERS))


def rdd_pagerank(edges, iterations=15, damping=0.85):
    """Textbook PageRank on Spark RDDs (undirected graph -> each edge in both directions)."""
    pairs = edges.select("src", "dst").rdd.flatMap(lambda r: [(r.src, r.dst), (r.dst, r.src)])
    links = pairs.groupByKey().mapValues(list).cache()
    n = links.count()
    ranks = links.mapValues(lambda _: 1.0)
    for _ in range(iterations):
        contribs = links.join(ranks).flatMap(
            lambda kv: [(dst, kv[1][1] / len(kv[1][0])) for dst in kv[1][0]])
        ranks = contribs.reduceByKey(lambda x, y: x + y).mapValues(lambda r: (1 - damping) + damping * r)
    out = ranks.map(lambda kv: (kv[0], kv[1] / n)).toDF(["parent_asin", "pagerank"])
    links.unpersist()
    return out, n


def network(spark, df, products):
    edges = co_review_edges(df).cache()
    n_edges = edges.count()
    pr, n_nodes = rdd_pagerank(edges)
    stats = df.groupBy("parent_asin").agg(F.count("*").alias("n_reviews"), F.avg("rating").alias("avg_rating"))
    nodes = (pr.join(stats, "parent_asin")
               .join(products.select("parent_asin", "product_title", "brand"), "parent_asin", "left")
               .orderBy(F.desc("pagerank")))
    top_nodes = nodes.limit(30).toPandas()
    top_nodes["product_title"] = top_nodes["product_title"].fillna("unknown").str[:70]
    C.save_table(top_nodes, "network_top_pagerank")
    print(f"Co-review graph: {n_nodes:,} products, {n_edges:,} edges (>= {MIN_SHARED_REVIEWERS} shared reviewers)")

    # The aggregated edge list is small, so the visual layer works on it with NetworkX:
    # keep the largest connected components (up to ~250 products) - the connected "core" of the market
    all_edges = edges.toPandas()
    full = nx.Graph()
    full.add_weighted_edges_from(all_edges[["src", "dst", "weight"]].itertuples(index=False))
    giant = max(nx.connected_components(full), key=len)
    pr_map = dict(zip(*pr.select("parent_asin", "pagerank").toPandas().T.values))
    keep = sorted(giant, key=lambda n: -pr_map.get(n, 0))[:250]          # core of the giant component
    keep = max(nx.connected_components(full.subgraph(keep)), key=len)
    G = full.subgraph(keep).copy()
    sub_nodes = nodes.filter(F.col("parent_asin").isin(list(keep))).toPandas().set_index("parent_asin")
    communities = nx.community.louvain_communities(G, weight="weight", seed=7)
    communities = sorted(communities, key=len, reverse=True)
    comm_of = {node: i for i, c in enumerate(communities) for node in c}

    comm_rows = []
    for i, c in enumerate(communities[:8]):
        members = sub_nodes.loc[list(c)].sort_values("pagerank", ascending=False)
        comm_rows.append({"community": i, "size": len(c),
                          "avg_rating": round(members["avg_rating"].mean(), 2),
                          "top_brands": ", ".join(members["brand"].dropna().value_counts().head(3).index),
                          "example_products": " | ".join(members["product_title"].fillna("?").str[:40].head(3))})
    C.save_table(pd.DataFrame(comm_rows), "network_communities")

    pos = nx.spring_layout(G, k=0.6 / math.sqrt(max(G.number_of_nodes(), 1)) * 3, seed=7, weight="weight",
                           iterations=100)
    fig, ax = plt.subplots(figsize=(11, 9))
    cmap = plt.cm.tab10
    nx.draw_networkx_edges(G, pos, ax=ax, alpha=0.15,
                           width=[0.3 + math.log1p(d["weight"]) * 0.5 for _, _, d in G.edges(data=True)])
    nodelist = list(G.nodes())
    nx.draw_networkx_nodes(G, pos, ax=ax, nodelist=nodelist,
                           node_size=[20 + 40 * sub_nodes.loc[n, "pagerank"] * n_nodes for n in nodelist],
                           node_color=[cmap(comm_of[n] % 10) for n in nodelist], alpha=0.85, linewidths=0)
    for n in sorted(nodelist, key=lambda x: -sub_nodes.loc[x, "pagerank"])[:6]:
        title = sub_nodes.loc[n, "product_title"]
        ax.annotate(str(title)[:30] if isinstance(title, str) else n, pos[n], fontsize=8, ha="center",
                    xytext=(0, 9), textcoords="offset points",
                    bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.8))
    ax.set_title(f"Product co-review network: core of the largest component\n{G.number_of_nodes()} products, "
                 f"{G.number_of_edges()} links · colour = Louvain community · size = PageRank")
    ax.axis("off")
    C.save_fig(fig, "09_coreview_network")

    edges.unpersist()
    return {"graph_nodes": n_nodes, "graph_edges": n_edges, "giant_component": len(giant),
            "connected_components": nx.number_connected_components(full), "visualised_nodes": G.number_of_nodes(),
            "communities_in_view": len(communities),
            "top_pagerank_product": top_nodes.iloc[0]["product_title"] if len(top_nodes) else None}


FEATURES = ["log_reviews", "avg_rating", "std_rating", "pct_negative", "pct_verified", "avg_len",
            "avg_helpful", "pct_with_photo", "active_months", "pct_recent"]


def segmentation(spark, df):
    last_year = df.agg(F.max("year")).first()[0]
    prod = (df.groupBy("parent_asin")
              .agg(F.log1p(F.count("*")).alias("log_reviews"), F.avg("rating").alias("avg_rating"),
                   F.coalesce(F.stddev("rating"), F.lit(0.0)).alias("std_rating"),
                   F.avg((F.col("rating") <= 2).cast("int")).alias("pct_negative"),
                   F.avg(F.col("verified_purchase").cast("int")).alias("pct_verified"),
                   F.avg("text_len").alias("avg_len"), F.avg("helpful_vote").alias("avg_helpful"),
                   F.avg((F.col("n_images") > 0).cast("int")).alias("pct_with_photo"),
                   F.countDistinct("month").alias("active_months"),
                   F.avg((F.col("year") >= last_year - 1).cast("int")).alias("pct_recent"),
                   F.count("*").alias("n_reviews"), F.first("product_title").alias("product_title"))
              .filter("n_reviews >= 20").cache())
    n_prod = prod.count()

    prep = Pipeline(stages=[VectorAssembler(inputCols=FEATURES, outputCol="raw"),
                            StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True)]
                    ).fit(prod)
    X = prep.transform(prod).cache()

    sil = []
    for k in range(3, 9):
        m = KMeans(k=k, seed=42, featuresCol="features").fit(X)
        sil.append((k, ClusteringEvaluator(featuresCol="features").evaluate(m.transform(X))))
    sil_df = pd.DataFrame(sil, columns=["k", "silhouette"])
    C.save_table(sil_df, "kmeans_silhouette")
    best_k = int(sil_df.loc[sil_df["silhouette"].idxmax(), "k"])
    best_k = max(best_k, 4)          # at least 4 segments so the result is useful to describe
    km = KMeans(k=best_k, seed=42, featuresCol="features").fit(X)
    pca = PCA(k=2, inputCol="features", outputCol="pc").fit(X)
    out = pca.transform(km.transform(X)).withColumn("pc", vector_to_array("pc"))
    pdf = out.select("parent_asin", "product_title", "prediction", "n_reviews", *FEATURES,
                     F.col("pc")[0].alias("pc1"), F.col("pc")[1].alias("pc2")).toPandas()

    profile = pdf.groupby("prediction")[FEATURES + ["n_reviews"]].mean()
    profile["products"] = pdf.groupby("prediction").size()
    z = (profile[FEATURES] - pdf[FEATURES].mean()) / pdf[FEATURES].std()
    profile["segment"] = [describe_segment(r) for _, r in z.iterrows()]
    C.save_table(profile.reset_index().rename(columns={"prediction": "cluster"}).round(3), "product_segments")
    C.save_table(pdf.sample(min(len(pdf), 5000), random_state=1).round(4), "product_pca_points")

    fig, axes = plt.subplots(1, 2, figsize=(15, 6), gridspec_kw={"width_ratios": [1.2, 1]})
    cmap = plt.cm.tab10
    for c in sorted(pdf["prediction"].unique()):
        s = pdf[pdf["prediction"] == c]
        axes[0].scatter(s["pc1"], s["pc2"], s=6, alpha=0.5, color=cmap(c),
                        label=f"{c}: {profile.loc[c, 'segment']} ({len(s):,})")
    axes[0].set(xlabel=f"PC1 ({pca.explainedVariance[0]:.0%} var)",
                ylabel=f"PC2 ({pca.explainedVariance[1]:.0%} var)",
                title=f"{n_prod:,} products, 10-D features projected to 2-D (PCA)")
    axes[0].legend(markerscale=3, fontsize=8, loc="best")
    z = (profile[FEATURES] - pdf[FEATURES].mean()) / pdf[FEATURES].std()
    im = axes[1].imshow(z.values, cmap="RdBu_r", vmin=-2, vmax=2, aspect="auto")
    axes[1].set_xticks(range(len(FEATURES)), FEATURES, rotation=45, ha="right")
    axes[1].set_yticks(range(len(z)), [f"{i}: {s}" for i, s in zip(z.index, profile["segment"])])
    axes[1].set_title("Segment profiles (z-score of cluster mean)")
    axes[1].grid(False)
    fig.colorbar(im, ax=axes[1])
    C.save_fig(fig, "10_product_segments")

    loadings = pd.DataFrame(pca.pc.toArray(), index=FEATURES, columns=["pc1", "pc2"]).round(3)
    C.save_table(loadings.reset_index().rename(columns={"index": "feature"}), "pca_loadings")
    X.unpersist(); prod.unpersist()
    return {"segmented_products": n_prod, "k": best_k,
            "silhouette": round(float(sil_df["silhouette"].max()), 3),
            "pca_variance_2d": round(float(sum(pca.explainedVariance)), 3),
            "segments": {int(i): f"{r['segment']} ({int(r['products'])} products)" for i, r in profile.iterrows()}}


def describe_segment(z):
    """Name a cluster after its most distinctive features (z-scores of the cluster mean)."""
    if z["log_reviews"] > 1 or z["active_months"] > 1:
        return "Established best-sellers"
    if z["pct_verified"] < -1:
        return "Low-verified, long reviews" if z["avg_len"] > 0.5 else "Low-verified"
    if z["pct_recent"] > 1:
        return "New & trending"
    if z["pct_negative"] > 0.5:
        return "Problem products"
    if z["avg_rating"] > 0.3:
        return "Loved niche products"
    if z["pct_with_photo"] > 1:
        return "Photo-heavy"
    return "Average performers"


def run(spark):
    df = C.load_reviews(spark).cache()
    products = C.load_products(spark)
    net = network(spark, df, products)
    seg = segmentation(spark, df)
    C.update_summary("graph_segments", {**net, **seg})
    df.unpersist()
