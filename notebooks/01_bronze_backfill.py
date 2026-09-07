# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 - Bronze Backfill
# MAGIC Reads a date range of raw Parquet from the AWS Public Blockchain S3 bucket for Bitcoin and
# MAGIC Ethereum, and lands it as Bronze Delta tables in Unity Catalog -- schema-on-write, one partition
# MAGIC (`date`) written per run so re-running a date is a clean overwrite of just that partition
# MAGIC (idempotent backfill/replay).
# MAGIC
# MAGIC Explicit schemas are declared for every source table below (rather than relying on Spark's schema
# MAGIC inference) -- inference on Parquet is normally safe, but declaring it here means a schema drift in
# MAGIC the upstream dataset fails loudly at read time instead of silently changing your Bronze table.
# MAGIC
# MAGIC **Incremental, not a full re-scan every run:** `backfill_start_date`/`backfill_end_date` only
# MAGIC define the *ceiling* -- the actual floor per table is one day past whatever `date` is already
# MAGIC the max in that table's Bronze data. First run does the full window; every run after that only
# MAGIC touches new partitions and skips everything already landed, which is what makes this safe to run  as a daily scheduled Job instead of a one-time script.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType,
    IntegerType, BooleanType, ArrayType,
)
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ### Source schemas
# MAGIC Per the [AWS digital-assets-on-aws schema reference](https://github.com/aws-solutions-library-samples/guidance-for-digital-assets-on-aws/blob/main/analytics/consumer/schema/).

# COMMAND ----------

# DBTITLE 1,Cell 5
ETH_BLOCKS_SCHEMA = StructType([
    StructField("date", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("number", LongType()),
    StructField("hash", StringType()),
    StructField("parent_hash", StringType()),
    StructField("nonce", StringType()),
    StructField("sha3_uncles", StringType()),
    StructField("logs_bloom", StringType()),
    StructField("transactions_root", StringType()),
    StructField("state_root", StringType()),
    StructField("receipts_root", StringType()),
    StructField("miner", StringType()),
    StructField("difficulty", DoubleType()),
    StructField("total_difficulty", DoubleType()),
    StructField("size", LongType()),
    StructField("extra_data", StringType()),
    StructField("gas_limit", LongType()),
    StructField("gas_used", LongType()),
    StructField("transaction_count", LongType()),
    StructField("base_fee_per_gas", LongType()),
])

ETH_TRANSACTIONS_SCHEMA = StructType([
    StructField("date", StringType()),
    StructField("hash", StringType()),
    StructField("nonce", LongType()),
    StructField("transaction_index", LongType()),
    StructField("from_address", StringType()),
    StructField("to_address", StringType()),
    StructField("value", DoubleType()),  # Wei
    StructField("gas", LongType()),
    StructField("gas_price", LongType()),
    StructField("input", StringType()),
    StructField("receipt_cumulative_gas_used", LongType()),
    StructField("receipt_gas_used", LongType()),
    StructField("receipt_contract_address", StringType()),
    StructField("receipt_status", LongType()),
    StructField("block_timestamp", TimestampType()),
    StructField("block_number", LongType()),
    StructField("block_hash", StringType()),
    StructField("max_fee_per_gas", LongType()),
    StructField("max_priority_fee_per_gas", LongType()),
    StructField("transaction_type", LongType()),
    StructField("receipt_effective_gas_price", LongType()),
])

ETH_TOKEN_TRANSFERS_SCHEMA = StructType([
    StructField("date", StringType()),
    StructField("token_address", StringType()),
    StructField("from_address", StringType()),
    StructField("to_address", StringType()),
    StructField("value", DoubleType()),
    StructField("transaction_hash", StringType()),
    StructField("log_index", LongType()),
    StructField("block_timestamp", TimestampType()),
    StructField("block_number", LongType()),
    StructField("block_hash", StringType()),
])

BTC_BLOCKS_SCHEMA = StructType([
    StructField("date", StringType()),
    StructField("hash", StringType()),
    StructField("size", LongType()),
    StructField("stripped_size", LongType()),
    StructField("weight", LongType()),
    StructField("number", LongType()),
    StructField("version", LongType()),
    StructField("merkle_root", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("nonce", LongType()),
    StructField("bits", StringType()),
    StructField("coinbase_param", StringType()),
    StructField("transaction_count", LongType()),
    StructField("mediantime", TimestampType()),
    StructField("difficulty", DoubleType()),
    StructField("chainwork", StringType()),
    StructField("previousblockhash", StringType()),
])

# btc.transactions has nested inputs/outputs arrays; we keep them as-is in Bronze (raw fidelity)
# and flatten what we need in Silver.
BTC_TX_IO_ELEMENT = StructType([
    StructField("index", LongType()),
    StructField("script_asm", StringType()),
    StructField("script_hex", StringType()),
    StructField("required_signatures", LongType()),
    StructField("type", StringType()),
    StructField("address", StringType()),
    StructField("value", DoubleType()),
])

BTC_TRANSACTIONS_SCHEMA = StructType([
    StructField("date", StringType()),
    StructField("hash", StringType()),
    StructField("size", LongType()),
    StructField("virtual_size", LongType()),
    StructField("version", LongType()),
    StructField("lock_time", LongType()),
    StructField("block_hash", StringType()),
    StructField("block_number", LongType()),
    StructField("block_timestamp", TimestampType()),
    StructField("index", LongType()),
    StructField("input_count", LongType()),
    StructField("output_count", LongType()),
    StructField("input_value", DoubleType()),
    StructField("output_value", DoubleType()),
    StructField("is_coinbase", BooleanType()),
    StructField("fee", DoubleType()),
    StructField("inputs", ArrayType(BTC_TX_IO_ELEMENT)),
    StructField("outputs", ArrayType(BTC_TX_IO_ELEMENT)),
])

# COMMAND ----------

# One entry per Bronze table we're landing in this project phase.
SOURCES = [
    # (chain, table, schema)
    ("eth", "blocks", ETH_BLOCKS_SCHEMA),
    ("eth", "transactions", ETH_TRANSACTIONS_SCHEMA),
    ("eth", "token_transfers", ETH_TOKEN_TRANSFERS_SCHEMA),
    ("btc", "blocks", BTC_BLOCKS_SCHEMA),
    ("btc", "transactions", BTC_TRANSACTIONS_SCHEMA),
]

# COMMAND ----------

def backfill_partition(chain: str, table: str, schema: StructType, day: str) -> int:
    """Read one date partition from the source bucket and overwrite that partition in Bronze.
    Returns the row count read (0 if the partition doesn't exist for that day -- some early
    Bitcoin dates, or days with no token transfers, can be legitimately absent/empty)."""
    path = source_path(chain, table, day)
    try:
        df = spark.read.schema(schema).parquet(path)
    except Exception as e:
        print(f"  [skip] {chain}.{table} {day}: no data or read error ({e})")
        return 0

    row_count = df.count()
    if row_count == 0:
        return 0

    df = df.withColumn("_ingested_at", F.current_timestamp()) \
           .withColumn("_source_path", F.lit(path))

    target = bronze_table(chain, table)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"date = '{day}'")
        .partitionBy("date")
        .saveAsTable(target)
    )
    return row_count

# COMMAND ----------

def last_ingested_date(chain: str, table: str):
    """Max `date` already landed in this chain/table's Bronze table -- None if the table
    doesn't exist yet (first run) or exists but is empty."""
    target = bronze_table(chain, table)
    if not spark.catalog.tableExists(target):
        return None
    row = spark.table(target).agg(F.max("date")).collect()[0]
    return date.fromisoformat(row[0]) if row[0] is not None else None


def dates_to_process(chain: str, table: str):
    """Every date in the configured window, minus whatever's already in Bronze -- so a
    re-run only ever ingests genuinely new dates instead of re-reading history."""
    window = date_range(BACKFILL_START_DATE, BACKFILL_END_DATE)
    last = last_ingested_date(chain, table)
    if last is None:
        return window
    return [d for d in window if date.fromisoformat(d) > last]

# COMMAND ----------

summary = []
for chain, table, schema in SOURCES:
    window = date_range(BACKFILL_START_DATE, BACKFILL_END_DATE)
    pending = dates_to_process(chain, table)
    skipped = len(window) - len(pending)

    if skipped:
        print(f"{chain}.{table}: skipping {skipped} already-ingested day(s)")
    if not pending:
        print(f"{chain}.{table}: up to date -- nothing new to ingest")
        continue

    for day in pending:
        n = backfill_partition(chain, table, schema, day)
        summary.append((chain, table, day, n))
        if n:
            print(f"{chain}.{table} {day}: {n:,} rows")

# COMMAND ----------

# MAGIC %md ### Backfill summary

# COMMAND ----------

if summary:
    summary_df = spark.createDataFrame(summary, ["chain", "table", "date", "row_count"])
    display(summary_df.groupBy("chain", "table").agg(
        F.sum("row_count").alias("total_rows"),
        F.count("*").alias("days_processed"),
    ))
else:
    print("Nothing new across any table -- all sources already up to date for this window.")

# COMMAND ----------

for chain, table, _ in SOURCES:
    print(f"--- {chain}.{table} ---")
    display(spark.table(bronze_table(chain, table)).limit(10))