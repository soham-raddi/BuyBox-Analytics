"""Step 3 - Sentiment classification with Spark MLlib (the 'Twitter sentiment analysis' case study, on reviews)
and detection of reviews whose star rating contradicts their text."""
import os
import time

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import CountVectorizer, IDF, NGram, RegexTokenizer, StopWordsRemover, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from . import config as C
from .viz import BLUE, GREEN, ORANGE, RED, plt

NEGATIONS = {"not", "no", "nor", "don", "didn", "doesn", "isn", "wasn", "won", "wouldn", "couldn",
             "never", "but", "very", "too"}


def features_stages():
    stop = [w for w in StopWordsRemover.loadDefaultStopWords("english") if w not in NEGATIONS]
    return [
        RegexTokenizer(inputCol="full_text", outputCol="tokens", pattern=r"[^a-z']+", minTokenLength=2),
        StopWordsRemover(inputCol="tokens", outputCol="words", stopWords=stop),
        NGram(n=2, inputCol="words", outputCol="bigrams"),          # keeps "not good", "broke after"
        CountVectorizer(inputCol="words", outputCol="tf_uni", vocabSize=30000, minDF=5),
        CountVectorizer(inputCol="bigrams", outputCol="tf_bi", vocabSize=20000, minDF=5),
        VectorAssembler(inputCols=["tf_uni", "tf_bi"], outputCol="tf"),
    ]


def evaluate(preds):
    m = MulticlassClassificationEvaluator
    # AUC on class probabilities: Naive Bayes' rawPrediction is an unnormalised log-likelihood
    preds = preds.withColumn("p_pos", vector_to_array("probability")[1])
    return {
        "accuracy": round(m(metricName="accuracy").evaluate(preds), 4),
        "f1_weighted": round(m(metricName="f1").evaluate(preds), 4),
        "precision_negative": round(m(metricName="precisionByLabel", metricLabel=0.0).evaluate(preds), 4),
        "recall_negative": round(m(metricName="recallByLabel", metricLabel=0.0).evaluate(preds), 4),
        "roc_auc": round(BinaryClassificationEvaluator(rawPredictionCol="p_pos",
                                                       metricName="areaUnderROC").evaluate(preds), 4),
    }


def run(spark):
    df = C.load_reviews(spark)
    sample = float(os.environ.get("RL_ML_SAMPLE", "1.0"))
    data = (df.filter("rating != 3")
              .withColumn("label", F.when(F.col("rating") >= 4, 1.0).otherwise(0.0))
              .withColumn("full_text", F.lower(F.concat_ws(" ", F.coalesce("review_title", F.lit("")), "text"))))
    if sample < 1.0:
        data = data.sample(fraction=sample, seed=42)

    counts = {r["label"]: r["count"] for r in data.groupBy("label").count().collect()}
    n = sum(counts.values())
    w_pos, w_neg = n / (2 * counts[1.0]), n / (2 * counts[0.0])
    data = data.withColumn("weight", F.when(F.col("label") == 1.0, w_pos).otherwise(w_neg))
    print(f"Training data: {n:,} reviews  positive={counts[1.0]:,} negative={counts[0.0]:,}")

    train, test = data.randomSplit([0.8, 0.2], seed=42)
    train.cache()

    # Shared feature pipeline (fit once), then two classifiers on top of it
    t0 = time.time()
    feat_model = Pipeline(stages=features_stages()).fit(train)
    train_f = feat_model.transform(train).cache()
    test_f = feat_model.transform(test).cache()
    idf = IDF(inputCol="tf", outputCol="features", minDocFreq=5).fit(train_f)
    train_f, test_f = idf.transform(train_f), idf.transform(test_f)
    feat_time = time.time() - t0

    results, models = [], {}
    t0 = time.time()
    lr = LogisticRegression(featuresCol="features", weightCol="weight", maxIter=30,
                            regParam=0.02, elasticNetParam=0.0).fit(train_f)
    lr_preds = lr.transform(test_f).cache()
    results.append({"model": "Logistic Regression (TF-IDF uni+bigrams)", **evaluate(lr_preds),
                    "train_seconds": round(time.time() - t0 + feat_time, 1)})
    models["lr"] = lr

    t0 = time.time()
    nb = NaiveBayes(featuresCol="tf", weightCol="weight", smoothing=1.0, modelType="multinomial").fit(train_f)
    nb_preds = nb.transform(test_f)
    results.append({"model": "Multinomial Naive Bayes (term counts)", **evaluate(nb_preds),
                    "train_seconds": round(time.time() - t0 + feat_time, 1)})
    res = pd.DataFrame(results)
    print(res.to_string(index=False))
    C.save_table(res, "model_comparison")

    # Explainability: words with the strongest weights in the LR model. Only common terms (>= 1000 reviews)
    # are shown - rare words get large IDF-scaled weights but say little about customers in general.
    vocab = feat_model.stages[3].vocabulary + feat_model.stages[4].vocabulary
    coefs = lr.coefficients.toArray()
    coef_df = pd.DataFrame({"term": vocab, "weight": coefs[:len(vocab)], "doc_freq": idf.docFreq[:len(vocab)]})
    coef_df = coef_df[coef_df["doc_freq"] >= 1000]
    top_neg = coef_df.nsmallest(20, "weight")
    top_pos = coef_df.nlargest(20, "weight")
    C.save_table(pd.concat([top_neg.assign(side="negative"), top_pos.assign(side="positive")]), "sentiment_terms")

    cm = (lr_preds.groupBy("label", "prediction").count().toPandas()
                 .pivot(index="label", columns="prediction", values="count").fillna(0).astype(int))
    C.save_table(cm.reset_index(), "confusion_matrix")

    fig, axes = plt.subplots(1, 2, figsize=(13, 5), gridspec_kw={"width_ratios": [1, 1.6]})
    axes[0].imshow(cm.values, cmap="Blues")
    for i in range(cm.shape[0]):
        for j in range(cm.shape[1]):
            axes[0].text(j, i, f"{cm.values[i, j]:,}", ha="center", va="center",
                         color="white" if cm.values[i, j] > cm.values.max() / 2 else "black")
    axes[0].set_xticks([0, 1], ["Pred negative", "Pred positive"])
    axes[0].set_yticks([0, 1], ["Actual negative", "Actual positive"])
    axes[0].set_title(f"Logistic regression - accuracy {results[0]['accuracy']:.3f}")
    axes[0].grid(False)
    terms = pd.concat([top_neg.head(12), top_pos.head(12).iloc[::-1]])
    axes[1].barh(terms["term"], terms["weight"], color=[RED if w < 0 else GREEN for w in terms["weight"]])
    axes[1].set(title="Most influential terms (LR coefficients)", xlabel="Weight  (← negative | positive →)")
    C.save_fig(fig, "07_sentiment_model")

    # Rating / text mismatch: 5-star reviews that read negative and 1-star reviews that read positive
    scored = lr_preds.withColumn("p_negative", vector_to_array("probability")[0])
    hidden = scored.filter((F.col("rating") == 5) & (F.col("p_negative") > 0.8)).cache()
    misclick = scored.filter((F.col("rating") == 1) & (F.col("p_negative") < 0.2))
    n5, n1 = scored.filter("rating = 5").count(), scored.filter("rating = 1").count()
    n_hidden, n_misclick = hidden.count(), misclick.count()
    print(f"5-star reviews that read negative: {n_hidden:,}/{n5:,}; 1-star that read positive: {n_misclick:,}/{n1:,}")

    examples = pd.concat([
        hidden.orderBy(F.desc("p_negative")).limit(10)
              .select(F.lit("5★ but negative text").alias("type"), "rating",
                      F.round("p_negative", 3).alias("p_negative"),
                      F.substring("text", 1, 220).alias("text")).toPandas(),
        misclick.orderBy("p_negative").limit(10)
                .select(F.lit("1★ but positive text").alias("type"), "rating",
                        F.round("p_negative", 3).alias("p_negative"),
                        F.substring("text", 1, 220).alias("text")).toPandas()])
    C.save_table(examples, "mismatch_examples")

    hidden_products = (hidden.groupBy("parent_asin", "product_title").count()
                       .orderBy(F.desc("count")).limit(15)
                       .select("parent_asin", F.substring(F.coalesce("product_title", F.lit("unknown")), 1, 70)
                               .alias("product"), F.col("count").alias("hidden_complaints")).toPandas())
    C.save_table(hidden_products, "hidden_complaint_products")

    sent_year = (scored.filter("year >= 2012").groupBy("year")
                 .agg(F.avg(1 - F.col("p_negative")).alias("text_positive"),
                      F.avg(F.col("label")).alias("stars_positive"), F.count("*").alias("n"))
                 .orderBy("year").toPandas())
    C.save_table(sent_year, "sentiment_vs_stars_by_year")
    fig, ax = plt.subplots(figsize=(8, 3.8))
    ax.plot(sent_year["year"], sent_year["stars_positive"], marker="o", color=BLUE, label="Share of 4-5★ ratings")
    ax.plot(sent_year["year"], sent_year["text_positive"], marker="s", color=ORANGE,
            label="Avg positive probability from text")
    ax.set(xlabel="Year", ylabel="Proportion", title="Star ratings vs what the text says")
    ax.legend()
    C.save_fig(fig, "08_sentiment_vs_stars")

    feat_model.write().overwrite().save(f"{C.MODEL_URI}/sentiment_features")
    idf.write().overwrite().save(f"{C.MODEL_URI}/sentiment_idf")
    lr.write().overwrite().save(f"{C.MODEL_URI}/sentiment_lr")
    C.update_summary("sentiment", {
        "training_reviews": n, "models": results,
        "hidden_complaints": n_hidden, "five_star_test_reviews": n5,
        "hidden_complaint_pct": round(100 * n_hidden / max(n5, 1), 2),
        "positive_text_one_star": n_misclick, "one_star_test_reviews": n1,
        "top_negative_terms": top_neg["term"].head(10).tolist(),
        "top_positive_terms": top_pos["term"].head(10).tolist(),
    })
    for d in (train, train_f, test_f, lr_preds, hidden):
        d.unpersist()
