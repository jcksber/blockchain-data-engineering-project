# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02 - Silver Transform
# MAGIC Cleans, deduplicates, and enriches each Bronze table into Silver: drop exact duplicates on the
# MAGIC natural key, filter out rows that are structurally broken, and add human-usable derived columns
# MAGIC (native-unit values, since the source stores Ethereum amounts in Wei and gas price in Wei-per-gas).
# MAGIC
# MAGIC **Flagged assumption -- verify before trusting downstream numbers:** the Bitcoin schema docs list
# MAGIC `input_value` / `output_value` / `fee` as `double` without stating the unit. Standard blockchain-etl
# MAGIC convention is native BTC (not satoshis), and that's assumed below -- but confirm with a sanity check
# MAGIC once you have real data (e.g. `avg(output_value)` per transaction should look like single-digit-to-low
# MAGIC hundreds of BTC, not billions) and adjust `BTC_VALUE_DIVISOR` if it turns out to be satoshis.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

WEI_PER_ETH = 10**18
BTC_VALUE_DIVISOR = 1  # see flagged assumption above; set to 1e8 if values turn out to be satoshis

# COMMAND ----------

WINDOW_DATES = date_range(BACKFILL_START_DATE, BACKFILL_END_DATE)

def read_bronze(chain: str, table: str):
    return spark.table(bronze_table(chain, table)).filter(F.col("date").isin(WINDOW_DATES))

def write_silver(df, target_table: str):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"date IN ({','.join(repr(d) for d in WINDOW_DATES)})")
        .partitionBy("date")
        .saveAsTable(target_table)
    )

# COMMAND ----------

# MAGIC %md ### eth.blocks -> silver

# COMMAND ----------

eth_blocks_bronze = read_bronze("eth", "blocks")

eth_blocks_silver = (
    eth_blocks_bronze
    .dropDuplicates(["hash"])
    .filter(F.col("gas_used") >= 0)
    .filter(F.col("gas_limit") > 0)
    .withColumn("gas_used_pct", F.round(F.col("gas_used") / F.col("gas_limit") * 100, 2))
)
write_silver(eth_blocks_silver, silver_table("eth", "blocks"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM blockchain_project.silver.eth_blocks LIMIT 20;

# COMMAND ----------

# MAGIC %md ### eth.transactions -> silver

# COMMAND ----------

# DBTITLE 1,Cell 8
eth_tx_bronze = read_bronze("eth", "transactions")

eth_tx_silver = (
    eth_tx_bronze
    .dropDuplicates(["hash"])
    .filter(F.col("value") >= 0)
    .filter(F.col("receipt_gas_used") >= 0)
    .filter(F.col("from_address").isNotNull())
    .withColumn("value_eth", F.col("value") / F.lit(WEI_PER_ETH))
    .withColumn(
        "fee_eth",
        (F.coalesce(F.col("receipt_gas_used"), F.col("gas")).cast("decimal(38,0)") *
         F.coalesce(F.col("receipt_effective_gas_price"), F.col("gas_price")).cast("decimal(38,0)")) / F.lit(WEI_PER_ETH),
    )
    .withColumn("tx_success", F.col("receipt_status") == 1)
)
write_silver(eth_tx_silver, silver_table("eth", "transactions"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM blockchain_project.silver.eth_transactions LIMIT 20;

# COMMAND ----------

# MAGIC %md ### eth.token_transfers -> silver

# COMMAND ----------

eth_tt_bronze = read_bronze("eth", "token_transfers")

eth_tt_silver = (
    eth_tt_bronze
    .dropDuplicates(["transaction_hash", "log_index"])
    .filter(F.col("value") >= 0)
    .filter(F.col("token_address").isNotNull())
)
write_silver(eth_tt_silver, silver_table("eth", "token_transfers"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM blockchain_project.silver.eth_token_transfers LIMIT 20;
# MAGIC SELECT COUNT(*) FROM blockchain_project.silver.eth_token_transfers;

# COMMAND ----------

# MAGIC %md ### btc.blocks -> silver

# COMMAND ----------

btc_blocks_bronze = read_bronze("btc", "blocks")

btc_blocks_silver = (
    btc_blocks_bronze
    .dropDuplicates(["hash"])
    .filter(F.col("transaction_count") >= 0)
)
write_silver(btc_blocks_silver, silver_table("btc", "blocks"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM blockchain_project.silver.btc_blocks LIMIT 20;

# COMMAND ----------

# MAGIC %md ### btc.transactions -> silver
# MAGIC Also explodes `outputs` into a flat `btc_transaction_outputs` Silver table -- this is what feeds
# MAGIC whale detection and per-address analysis in Gold (Bitcoin's UTXO model doesn't give you a clean
# MAGIC single "to_address" per transaction the way Ethereum does).

# COMMAND ----------

btc_tx_bronze = read_bronze("btc", "transactions")

btc_tx_silver = (
    btc_tx_bronze
    .dropDuplicates(["hash"])
    .filter(F.col("output_value") >= 0)
    .withColumn("value_btc", F.col("output_value") / F.lit(BTC_VALUE_DIVISOR))
    .withColumn("fee_btc", F.col("fee") / F.lit(BTC_VALUE_DIVISOR))
    .drop("inputs", "outputs")  # kept in Bronze; flattened separately below
)
write_silver(btc_tx_silver, silver_table("btc", "transactions"))

btc_tx_outputs_silver = (
    btc_tx_bronze
    .select(
        "date", "hash", "block_number", "block_timestamp", "is_coinbase",
        F.explode("outputs").alias("out"),
    )
    .select(
        "date", F.col("hash").alias("transaction_hash"), "block_number", "block_timestamp", "is_coinbase",
        F.col("out.index").alias("output_index"),
        F.col("out.address").alias("address"),
        (F.col("out.value") / F.lit(BTC_VALUE_DIVISOR)).alias("value_btc"),
        F.col("out.type").alias("script_type"),
    )
    .filter(F.col("address").isNotNull())
)
write_silver(btc_tx_outputs_silver, silver_table("btc", "transaction_outputs"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM blockchain_project.silver.btc_transaction_outputs;