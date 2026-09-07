# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Wallet Features
# MAGIC Per-address behavioral features from ETH transactions, both as sender and receiver, over the
# MAGIC configured window. Scoped to Ethereum only — Bitcoin's UTXO model doesn't give a clean per-wallet
# MAGIC "sent" side without address-clustering heuristics (a different, harder problem), so BTC wallet-level
# MAGIC features are a known gap here, not silently faked.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

WINDOW_DATES = date_range(BACKFILL_START_DATE, BACKFILL_END_DATE)
eth_tx = spark.table(silver_table("eth", "transactions")).filter(F.col("date").isin(WINDOW_DATES))

# COMMAND ----------

sent = (
    eth_tx.groupBy(F.col("from_address").alias("address"))
    .agg(
        F.count("*").alias("sent_tx_count"),
        F.sum("value_eth").alias("sent_total_value_eth"),
        F.avg("value_eth").alias("sent_avg_value_eth"),
        F.stddev("value_eth").alias("sent_std_value_eth"),
        F.max("value_eth").alias("sent_max_value_eth"),
        F.countDistinct("to_address").alias("sent_distinct_recipients"),
        F.avg("gas_price").alias("avg_gas_price_wei"),
        F.countDistinct("date").alias("sent_active_days"),
    )
)

received = (
    eth_tx.groupBy(F.col("to_address").alias("address"))
    .agg(
        F.count("*").alias("recv_tx_count"),
        F.sum("value_eth").alias("recv_total_value_eth"),
        F.avg("value_eth").alias("recv_avg_value_eth"),
        F.stddev("value_eth").alias("recv_std_value_eth"),
        F.max("value_eth").alias("recv_max_value_eth"),
        F.countDistinct("from_address").alias("recv_distinct_senders"),
        F.countDistinct("date").alias("recv_active_days"),
    )
)

# COMMAND ----------

fill_cols = [c for c in sent.columns if c != "address"] + [c for c in received.columns if c != "address"]

wallet_features = (
    sent.join(received, on="address", how="full_outer")
    .fillna(0, subset=fill_cols)
    .withColumn("total_tx_count", F.col("sent_tx_count") + F.col("recv_tx_count"))
    .withColumn("net_flow_eth", F.col("recv_total_value_eth") - F.col("sent_total_value_eth"))
    .withColumn("active_days", F.greatest("sent_active_days", "recv_active_days"))
)

wallet_features.write.format("delta").mode("overwrite").saveAsTable(gold_table("eth_wallet_features"))

display(wallet_features.orderBy(F.desc("total_tx_count")).limit(20))

# COMMAND ----------

wallet_features = spark.table(gold_table("eth_wallet_features"))

display(
    wallet_features
    .groupBy("total_tx_count")
    .count()
    .orderBy("total_tx_count")
    .limit(20)
)

# COMMAND ----------

for threshold in [3, 5, 10, 20, 50]:
    n = wallet_features.filter(F.col("total_tx_count") >= threshold).count()
    print(f"total_tx_count >= {threshold}: {n:,} addresses")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's go with total_tx_count >= 10 (809,722 addresses) — enough real recurring activity to make behavioral features meaningful, still comfortably sized for Pandas/scikit-learn, and a cutoff I can justify plainly ("needed enough transactions for avg/std/counterparty features to mean anything").

# COMMAND ----------

FEATURE_COLS = [
    "sent_tx_count", "sent_total_value_eth", "sent_avg_value_eth", "sent_std_value_eth",
    "sent_max_value_eth", "sent_distinct_recipients", "avg_gas_price_wei", "sent_active_days",
    "recv_tx_count", "recv_total_value_eth", "recv_avg_value_eth", "recv_std_value_eth",
    "recv_max_value_eth", "recv_distinct_senders", "recv_active_days",
    "total_tx_count", "net_flow_eth", "active_days",
]

active_wallets = wallet_features.filter(F.col("total_tx_count") >= 10)
pdf = active_wallets.select("address", *FEATURE_COLS).toPandas()
print(pdf.shape)
pdf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC Preprocess — the value/count columns are heavily right-skewed (typical for on-chain data), so log-transform them before modeling. `net_flow_eth` can be negative (received − sent), so it needs a signed log rather than plain `log1p`.

# COMMAND ----------

import numpy as np

LOG_COLS = [
    "sent_tx_count", "sent_total_value_eth", "sent_avg_value_eth", "sent_std_value_eth",
    "sent_max_value_eth", "sent_distinct_recipients", "avg_gas_price_wei", "sent_active_days",
    "recv_tx_count", "recv_total_value_eth", "recv_avg_value_eth", "recv_std_value_eth",
    "recv_max_value_eth", "recv_distinct_senders", "recv_active_days",
    "total_tx_count", "active_days",
]

features = pdf.copy()
for col in LOG_COLS:
    features[col] = np.log1p(features[col])

features["net_flow_eth"] = np.sign(pdf["net_flow_eth"]) * np.log1p(np.abs(pdf["net_flow_eth"]))

X = features[FEATURE_COLS]

# COMMAND ----------

# MAGIC %md
# MAGIC No `StandardScaler` needed here — Isolation Forest splits on random thresholds within each feature's own range, so it's scale-invariant per feature (unlike, say, k-NN or PCA-based approaches, which would need scaling). The log-transform matters because it reshapes the distribution, not just the scale.

# COMMAND ----------

import mlflow
from sklearn.ensemble import IsolationForest

CONTAMINATION = 0.01  # flag the most anomalous 1% -> ~8,100 wallets to review

with mlflow.start_run(run_name="eth_wallet_anomaly_isolation_forest"):
    mlflow.log_param("contamination", CONTAMINATION)
    mlflow.log_param("n_wallets", len(X))
    mlflow.log_param("min_tx_count_cutoff", 10)
    mlflow.log_param("feature_cols", FEATURE_COLS)

    model = IsolationForest(n_estimators=200, contamination=CONTAMINATION, random_state=42, n_jobs=-1)
    model.fit(X)

    pdf["anomaly_score"] = -model.score_samples(X)  # higher = more anomalous
    pdf["is_anomaly"] = model.predict(X) == -1

    n_flagged = int(pdf["is_anomaly"].sum())
    mlflow.log_metric("n_flagged_anomalies", n_flagged)
    mlflow.log_metric("pct_flagged", n_flagged / len(pdf) * 100)
    mlflow.sklearn.log_model(model, "isolation_forest_model")

print(f"Flagged {pdf['is_anomaly'].sum():,} of {len(pdf):,} wallets")

# COMMAND ----------

# Sanity check: what does a flagged wallet actually look like?
display(pdf[pdf["is_anomaly"]].sort_values("anomaly_score", ascending=False).head(20))

# COMMAND ----------

anomaly_results = spark.createDataFrame(pdf[["address", "anomaly_score", "is_anomaly"]])
anomaly_results.write.format("delta").mode("overwrite").saveAsTable(gold_table("eth_wallet_anomalies"))

# COMMAND ----------

whale_addresses = (
    spark.table(gold_table("whale_transactions"))
    .filter(F.col("chain") == "ETH")
    .select(F.col("to_address").alias("address"))
    .union(
        spark.table(gold_table("whale_transactions"))
        .filter((F.col("chain") == "ETH") & F.col("from_address").isNotNull())
        .select(F.col("from_address").alias("address"))
    )
    .distinct()
)

flagged = spark.table(gold_table("eth_wallet_anomalies")).filter(F.col("is_anomaly"))
overlap = flagged.join(whale_addresses, on="address", how="inner").count()
total_flagged = flagged.count()

print(f"{overlap:,} of {total_flagged:,} anomalous wallets ({overlap/total_flagged*100:.1f}%) also appear as a whale-transaction counterparty")

# COMMAND ----------

anomalies_with_overlap = (
    flagged
    .join(whale_addresses.withColumn("is_whale_ctpty", F.lit(True)), on="address", how="left")
    .fillna(False, subset=["is_whale_ctpty"])
    .join(spark.table(gold_table("eth_wallet_features")), on="address", how="left")
)

display(
    anomalies_with_overlap.groupBy("is_whale_ctpty").agg(
        F.count("*").alias("n"),
        F.avg("total_tx_count").alias("avg_total_tx_count"),
        F.avg(F.col("recv_total_value_eth") + F.col("sent_total_value_eth")).alias("avg_total_value_eth"),
        F.avg("sent_distinct_recipients").alias("avg_sent_distinct_recipients"),
        F.avg("recv_distinct_senders").alias("avg_recv_distinct_senders"),
    )
)