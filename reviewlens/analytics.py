"""Step 2 - Descriptive analytics with the three Spark APIs: DataFrame, Spark SQL and RDD (MapReduce style)."""
import re

import pandas as pd
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import functions as F

from . import config as C
from .viz import BLUE, GREEN, ORANGE, RATING_COLORS, RED, plt

EXTRA_STOP = {"product", "one", "would", "get", "got", "also", "use", "used", "using", "even", "really",
              "just", "like", "it's", "i'm", "don't", "didn't", "doesn't", "can't", "will", "bought",
              "item", "amazon", "buy", "could", "much", "well", "make", "made", "way", "first", "two"}


def rating_distribution(df):
    pdf = df.groupBy("rating").count().orderBy("rating").toPandas()
    pdf["pct"] = (100 * pdf["count"] / pdf["count"].sum()).round(2)
    C.save_table(pdf, "rating_distribution")
    fig, ax = plt.subplots(figsize=(6, 3.8))
    bars = ax.bar(pdf["rating"].astype(int).astype(str), pdf["count"],
                  color=[RATING_COLORS[int(r)] for r in pdf["rating"]])
    for b, p in zip(bars, pdf["pct"]):
        ax.annotate(f"{p:.1f}%", (b.get_x() + b.get_width() / 2, b.get_height()), ha="center", va="bottom")
    ax.set(xlabel="Star rating", ylabel="Reviews", title="Rating distribution (J-shaped)")
    C.save_fig(fig, "01_rating_distribution")
    return pdf


def monthly_trend(df):
    pdf = (df.filter("year >= 2008").groupBy("month")
             .agg(F.count("*").alias("reviews"), F.avg("rating").alias("avg_rating"),
                  F.avg(F.col("verified_purchase").cast("int")).alias("verified_share"))
             .orderBy("month").toPandas())
    C.save_table(pdf, "monthly_trend")
    pdf["month"] = pd.to_datetime(pdf["month"])
    fig, ax1 = plt.subplots(figsize=(10, 4))
    ax1.fill_between(pdf["month"], pdf["reviews"], color=BLUE, alpha=0.25)
    ax1.plot(pdf["month"], pdf["reviews"], color=BLUE)
    ax1.set_ylabel("Reviews per month", color=BLUE)
    ax2 = ax1.twinx()
    ax2.plot(pdf["month"], pdf["avg_rating"].rolling(6, min_periods=1).mean(), color=ORANGE, lw=2)
    ax2.set_ylabel("Avg rating (6-month rolling)", color=ORANGE)
    ax2.grid(False)
    ax1.set_title("Time series: review volume and average rating")
    C.save_fig(fig, "02_monthly_trend")
    return pdf


def heatmaps(df):
    year_rating = (df.filter("year >= 2010").groupBy("year", "rating").count().toPandas()
                     .pivot(index="year", columns="rating", values="count").fillna(0))
    share = year_rating.div(year_rating.sum(axis=1), axis=0) * 100
    C.save_table(share.reset_index(), "heatmap_year_rating")

    order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    hour_day = (df.groupBy("weekday", "hour").count().toPandas()
                  .pivot(index="weekday", columns="hour", values="count").reindex(order).fillna(0))
    C.save_table(hour_day.reset_index(), "heatmap_weekday_hour")

    fig, axes = plt.subplots(1, 2, figsize=(14, 5), gridspec_kw={"width_ratios": [1, 1.6]})
    im = axes[0].imshow(share.values, aspect="auto", cmap="YlOrRd")
    axes[0].set_xticks(range(5), [int(c) for c in share.columns])
    axes[0].set_yticks(range(len(share.index)), share.index)
    axes[0].set(xlabel="Star rating", ylabel="Year", title="Rating mix by year (% of year)")
    axes[0].grid(False)
    fig.colorbar(im, ax=axes[0])
    im2 = axes[1].imshow(hour_day.values, aspect="auto", cmap="Blues")
    axes[1].set_xticks(range(0, 24, 2), range(0, 24, 2))
    axes[1].set_yticks(range(7), order)
    axes[1].set(xlabel="Hour of day (UTC)", title="When are reviews posted? (count)")
    axes[1].grid(False)
    fig.colorbar(im2, ax=axes[1])
    C.save_fig(fig, "03_heatmaps")
    return share


def sql_insights(spark, df):
    df.createOrReplaceTempView("reviews")
    out = {}
    out["top_products"] = spark.sql("""
        SELECT parent_asin, SUBSTRING(COALESCE(product_title,'unknown'),1,70) AS product, brand,
               COUNT(*) AS n_reviews, ROUND(AVG(rating),2) AS avg_rating,
               ROUND(100*AVG(CASE WHEN rating <= 2 THEN 1 ELSE 0 END),1) AS pct_negative
        FROM reviews GROUP BY parent_asin, product_title, brand
        ORDER BY n_reviews DESC LIMIT 15""").toPandas()

    # Popular products that customers dislike: candidates for quality investigation
    out["worst_popular_products"] = spark.sql("""
        SELECT parent_asin, SUBSTRING(COALESCE(product_title,'unknown'),1,70) AS product, brand,
               COUNT(*) AS n_reviews, ROUND(AVG(rating),2) AS avg_rating,
               ROUND(100*AVG(CASE WHEN rating <= 2 THEN 1 ELSE 0 END),1) AS pct_negative
        FROM reviews GROUP BY parent_asin, product_title, brand
        HAVING COUNT(*) >= 200 ORDER BY avg_rating ASC LIMIT 15""").toPandas()

    out["top_brands"] = spark.sql("""
        SELECT brand, COUNT(DISTINCT parent_asin) AS products, COUNT(*) AS n_reviews,
               ROUND(AVG(rating),2) AS avg_rating
        FROM reviews WHERE brand IS NOT NULL AND brand <> 'Unknown'
        GROUP BY brand HAVING COUNT(*) >= 500 ORDER BY n_reviews DESC LIMIT 20""").toPandas()

    out["verified_vs_unverified"] = spark.sql("""
        SELECT verified_purchase, COUNT(*) AS n_reviews, ROUND(AVG(rating),3) AS avg_rating,
               ROUND(AVG(helpful_vote),3) AS avg_helpful, ROUND(AVG(text_len),0) AS avg_length,
               ROUND(100*AVG(CASE WHEN rating = 5 THEN 1 ELSE 0 END),1) AS pct_5_star
        FROM reviews GROUP BY verified_purchase""").toPandas()

    out["rating_helpfulness"] = spark.sql("""
        SELECT rating, ROUND(AVG(helpful_vote),3) AS avg_helpful, ROUND(AVG(text_len),0) AS avg_length,
               ROUND(100*AVG(CASE WHEN n_images > 0 THEN 1 ELSE 0 END),2) AS pct_with_photo
        FROM reviews GROUP BY rating ORDER BY rating""").toPandas()

    # Window function: rank each brand's products by review count
    out["brand_best_seller"] = spark.sql("""
        SELECT brand, product, n_reviews, avg_rating FROM (
          SELECT brand, SUBSTRING(product_title,1,60) AS product, COUNT(*) AS n_reviews,
                 ROUND(AVG(rating),2) AS avg_rating,
                 ROW_NUMBER() OVER (PARTITION BY brand ORDER BY COUNT(*) DESC) AS rk
          FROM reviews WHERE brand IS NOT NULL AND brand <> 'Unknown'
          GROUP BY brand, product_title) t
        WHERE rk = 1 ORDER BY n_reviews DESC LIMIT 15""").toPandas()

    for name, pdf in out.items():
        C.save_table(pdf, name)

    tb = out["top_brands"].head(12).iloc[::-1]
    fig, ax = plt.subplots(figsize=(8, 5))
    colors = plt.cm.RdYlGn((tb["avg_rating"] - 3.5) / 1.3)
    ax.barh(tb["brand"].str[:28], tb["n_reviews"], color=colors)
    for y, (n, r) in enumerate(zip(tb["n_reviews"], tb["avg_rating"])):
        ax.text(n, y, f" {r:.2f}★", va="center", fontsize=8)
    ax.set(xlabel="Reviews", title="Most-reviewed brands (colour = avg rating)")
    C.save_fig(fig, "04_top_brands")

    rh = out["rating_helpfulness"]
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8))
    axes[0].bar(rh["rating"].astype(int).astype(str), rh["avg_helpful"],
                color=[RATING_COLORS[int(r)] for r in rh["rating"]])
    axes[0].set(xlabel="Star rating", ylabel="Avg helpful votes", title="Helpfulness is U-shaped: extremes get votes")
    axes[1].bar(rh["rating"].astype(int).astype(str), rh["avg_length"],
                color=[RATING_COLORS[int(r)] for r in rh["rating"]])
    axes[1].set(xlabel="Star rating", ylabel="Avg characters", title="Mixed (2-4★) reviews are the longest")
    C.save_fig(fig, "05_helpfulness_length")
    return out


def rdd_word_counts(spark, df):
    """Classic MapReduce word count on Spark RDDs: flatMap -> map -> reduceByKey."""
    stop = set(StopWordsRemover.loadDefaultStopWords("english")) | EXTRA_STOP
    stop_b = spark.sparkContext.broadcast(stop)
    token = re.compile(r"[a-z']{3,}")

    def words(text):
        return [w for w in token.findall((text or "").lower()) if w not in stop_b.value]

    def bigrams(text):
        ws = words(text)
        return [f"{a} {b}" for a, b in zip(ws, ws[1:])]

    neg = df.filter("rating <= 2").select("text").rdd.map(lambda r: r.text)
    pos = df.filter("rating >= 4").select("text").rdd.map(lambda r: r.text)

    def top(rdd, fn, n=25):
        return (rdd.flatMap(fn).map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
                   .takeOrdered(n, key=lambda kv: -kv[1]))

    neg_words = pd.DataFrame(top(neg, words), columns=["word", "count"])
    pos_words = pd.DataFrame(top(pos, words), columns=["word", "count"])
    neg_bigrams = pd.DataFrame(top(neg, bigrams), columns=["bigram", "count"])
    C.save_table(neg_words, "complaint_words")
    C.save_table(pos_words, "praise_words")
    C.save_table(neg_bigrams, "complaint_bigrams")

    fig, axes = plt.subplots(1, 3, figsize=(15, 6))
    for ax, data, col, color, title in (
            (axes[0], neg_words, "word", RED, "Top words in 1-2★ reviews"),
            (axes[1], neg_bigrams, "bigram", ORANGE, "Top bigrams in 1-2★ reviews"),
            (axes[2], pos_words, "word", GREEN, "Top words in 4-5★ reviews")):
        d = data.head(20).iloc[::-1]
        ax.barh(d[col], d["count"], color=color)
        ax.set(title=title, xlabel="Frequency")
    C.save_fig(fig, "06_word_counts_rdd")
    return neg_words, neg_bigrams


def run(spark):
    df = C.load_reviews(spark).cache()
    total = df.count()
    rd = rating_distribution(df)
    monthly = monthly_trend(df)
    heatmaps(df)
    sql = sql_insights(spark, df)
    neg_words, neg_bigrams = rdd_word_counts(spark, df)

    vu = sql["verified_vs_unverified"].set_index("verified_purchase")
    peak = monthly.loc[monthly["reviews"].idxmax()]
    rh = sql["rating_helpfulness"].set_index("rating")
    C.update_summary("analytics", {
        "total_reviews": total,
        "pct_5_star": float(rd.loc[rd.rating == 5, "pct"].sum()),
        "pct_positive": float(rd.loc[rd.rating >= 4, "pct"].sum()),
        "pct_negative": float(rd.loc[rd.rating <= 2, "pct"].sum()),
        "avg_rating": round(float((rd.rating * rd["count"]).sum() / rd["count"].sum()), 3),
        "peak_month": str(peak["month"])[:7], "peak_month_reviews": int(peak["reviews"]),
        "verified_avg_rating": float(vu.loc[True, "avg_rating"]) if True in vu.index else None,
        "unverified_avg_rating": float(vu.loc[False, "avg_rating"]) if False in vu.index else None,
        "helpful_votes_1_star": float(rh.loc[1.0, "avg_helpful"]),
        "helpful_votes_5_star": float(rh.loc[5.0, "avg_helpful"]),
        "top_complaint_words": neg_words["word"].head(10).tolist(),
        "top_complaint_bigrams": neg_bigrams["bigram"].head(10).tolist(),
        "most_reviewed_product": sql["top_products"].iloc[0]["product"],
        "worst_popular_product": sql["worst_popular_products"].iloc[0]["product"]
            if len(sql["worst_popular_products"]) else None,
    })
    df.unpersist()
