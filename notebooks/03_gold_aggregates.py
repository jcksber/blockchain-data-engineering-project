# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 - Gold Aggregates
# MAGIC Business-level tables: daily network health per chain, and a unified whale-transaction table
# MAGIC across both chains. The whale table is deliberately built now -- it's the feature source for the
# MAGIC Month 2 anomaly-detection project, so getting its grain and columns right here saves rework later.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

WINDOW_DATES = date_range(BACKFILL_START_DATE, BACKFILL_END_DATE)

def read_silver(chain: str, table: str):
    return spark.table(silver_table(chain, table)).filter(F.col("date").isin(WINDOW_DATES))

# COMMAND ----------

# MAGIC %md ### eth_daily_network_stats

# COMMAND ----------

eth_tx = read_silver("eth", "transactions")
eth_blocks = read_silver("eth", "blocks")

eth_daily = (
    eth_tx.groupBy("date").agg(
        F.count("*").alias("tx_count"),
        F.sum("value_eth").alias("total_value_eth"),
        F.sum("fee_eth").alias("total_fees_eth"),
        F.avg("gas_price").alias("avg_gas_price_wei"),
        F.countDistinct("from_address").alias("active_senders"),
        F.countDistinct("to_address").alias("active_receivers"),
        (F.sum(F.col("tx_success").cast("int")) / F.count("*") * 100).alias("success_rate_pct"),
    )
    .join(
        eth_blocks.groupBy("date").agg(
            F.count("*").alias("block_count"),
            F.avg("gas_used_pct").alias("avg_block_utilization_pct"),
        ),
        on="date", how="left",
    )
    .orderBy("date")
)
eth_daily.write.format("delta").mode("overwrite").option(
    "replaceWhere", f"date IN ({','.join(repr(d) for d in WINDOW_DATES)})"
).partitionBy("date").saveAsTable(gold_table("eth_daily_network_stats"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM blockchain_project.gold.eth_daily_network_stats LIMIT 20;

# COMMAND ----------

# MAGIC %md ### btc_daily_network_stats

# COMMAND ----------

btc_tx = read_silver("btc", "transactions")

btc_daily = (
    btc_tx.groupBy("date").agg(
        F.count("*").alias("tx_count"),
        F.sum("value_btc").alias("total_value_btc"),
        F.sum("fee_btc").alias("total_fees_btc"),
        F.avg("fee_btc").alias("avg_fee_btc"),
        F.sum(F.col("is_coinbase").cast("int")).alias("coinbase_tx_count"),
    )
    .orderBy("date")
)
btc_daily.write.format("delta").mode("overwrite").option(
    "replaceWhere", f"date IN ({','.join(repr(d) for d in WINDOW_DATES)})"
).partitionBy("date").saveAsTable(gold_table("btc_daily_network_stats"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### whale_transactions
# MAGIC Unified schema across chains so Month 2's anomaly-detection notebook can featurize both together.
# MAGIC Note the ETH side has a real `from_address`; the BTC side doesn't at this grain (UTXO outputs
# MAGIC don't carry a single clean sender the way an ETH tx does) -- `from_address` is left null for BTC
# MAGIC rather than faked, and that's worth calling out explicitly in the writeup later.

# COMMAND ----------

eth_whales = (
    eth_tx.filter(F.col("value_eth") > F.lit(WHALE_ETH_THRESHOLD))
    .select(
        F.lit("ETH").alias("chain"),
        F.col("hash").alias("transaction_hash"),
        F.col("block_timestamp").alias("timestamp"),
        F.col("value_eth").alias("value"),
        "from_address",
        F.col("to_address").alias("to_address"),
        "date",
    )
)

btc_tx_outputs = read_silver("btc", "transaction_outputs")
btc_whales = (
    btc_tx_outputs.filter(F.col("value_btc") > F.lit(WHALE_BTC_THRESHOLD))
    .select(
        F.lit("BTC").alias("chain"),
        "transaction_hash",
        F.col("block_timestamp").alias("timestamp"),
        F.col("value_btc").alias("value"),
        F.lit(None).cast("string").alias("from_address"),
        F.col("address").alias("to_address"),
        "date",
    )
)

whale_transactions = eth_whales.unionByName(btc_whales).orderBy(F.desc("value"))
whale_transactions.write.format("delta").mode("overwrite").option(
    "replaceWhere", f"date IN ({','.join(repr(d) for d in WINDOW_DATES)})"
).partitionBy("date").saveAsTable(gold_table("whale_transactions"))

# COMMAND ----------

display(spark.table(gold_table("whale_transactions")).limit(20))