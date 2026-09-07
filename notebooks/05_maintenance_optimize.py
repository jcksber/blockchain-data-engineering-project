# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Maintenance: OPTIMIZE / Z-ORDER
# MAGIC Runs on its own, less-frequent schedule (weekly, not part of the daily pipeline) — compacting
# MAGIC small files and clustering data is a cost/performance concern, not a correctness one, so it
# MAGIC doesn't need to block the daily Bronze/Silver/Gold run or its own monitoring/alerting path.
# MAGIC
# MAGIC Z-ORDER columns are picked to match how the data actually gets queried downstream (wallet/address
# MAGIC lookups for the Month 2 modeling project), not just guessed — `date` itself doesn't need
# MAGIC Z-ORDERing since it's already the partition column.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

OPTIMIZE_TARGETS = [
    (silver_table("eth", "transactions"), ["from_address", "to_address"]),
    (silver_table("eth", "token_transfers"), ["token_address"]),
    (silver_table("btc", "transaction_outputs"), ["address"]),
    (gold_table("whale_transactions"), ["chain", "to_address"]),
]

# COMMAND ----------

for table, zorder_cols in OPTIMIZE_TARGETS:
    cols = ", ".join(zorder_cols)
    print(f"OPTIMIZE {table} ZORDER BY ({cols})")
    spark.sql(f"OPTIMIZE {table} ZORDER BY ({cols})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deliberately not running VACUUM here
# MAGIC `VACUUM` permanently deletes files no longer referenced by the Delta log past the retention
# MAGIC window (default 7 days) — fine for a mature pipeline, but a poor default to automate this early,
# MAGIC since it removes your ability to `RESTORE`/time-travel past that window while you're still
# MAGIC actively debugging the pipeline. Run it manually (`VACUUM <table>`) once the pipeline's been
# MAGIC stable for a while, or add it here deliberately later with an explicit retention policy.