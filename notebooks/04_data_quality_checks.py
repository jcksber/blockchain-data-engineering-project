# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04 - Data Quality Checks
# MAGIC Runs between Silver and Gold in the Job DAG. We're orchestrating plain notebooks with Lakeflow
# MAGIC Jobs (not Lakeflow Declarative Pipelines), so there's no built-in `@expect(...)` decorator here --
# MAGIC this is the hand-rolled equivalent: a list of checks, each returns pass/fail + detail, and the
# MAGIC notebook raises (failing the Job task) if anything failed. That failure blocks the downstream
# MAGIC `gold_aggregates` task via `depends_on` and triggers the job's `email_notifications.on_failure`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

CHECK_DATES = date_range(BACKFILL_START_DATE, BACKFILL_END_DATE)

results = []  # (check_name, passed: bool, detail: str)

def record(name: str, passed: bool, detail: str = ""):
    results.append((name, passed, detail))
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {name}" + (f" -- {detail}" if detail else ""))

# COMMAND ----------

# MAGIC %md ### 1. No null primary keys, no duplicate primary keys

# COMMAND ----------

PK_CHECKS = [
    ("eth", "blocks", ["hash"]),
    ("eth", "transactions", ["hash"]),
    ("eth", "token_transfers", ["transaction_hash", "log_index"]),
    ("btc", "blocks", ["hash"]),
    ("btc", "transactions", ["hash"]),
]

for chain, table, pk_cols in PK_CHECKS:
    df = spark.table(silver_table(chain, table)).filter(F.col("date").isin(CHECK_DATES))
    total = df.count()
    if total == 0:
        record(f"{chain}.{table}: has data for this window", False, "0 rows in Silver for the configured date range")
        continue

    null_pk = df.filter(" OR ".join(f"{c} IS NULL" for c in pk_cols)).count()
    record(f"{chain}.{table}: no null primary key ({', '.join(pk_cols)})", null_pk == 0, f"{null_pk} rows with a null PK column")

    dup_pk = total - df.dropDuplicates(pk_cols).count()
    record(f"{chain}.{table}: no duplicate primary key", dup_pk == 0, f"{dup_pk} duplicate rows on {pk_cols}")

# COMMAND ----------

# MAGIC %md ### 2. Every processed date has at least one row per table (a silent empty day usually means an upstream read failure, not a real zero-activity day on these chains)

# COMMAND ----------

for chain, table, _ in PK_CHECKS:
    df = spark.table(silver_table(chain, table)).filter(F.col("date").isin(CHECK_DATES))
    present_dates = {r["date"] for r in df.select("date").distinct().collect()}
    missing = sorted(set(CHECK_DATES) - present_dates)
    record(f"{chain}.{table}: no missing dates in window", len(missing) == 0, f"missing: {missing}" if missing else "")

# COMMAND ----------

# MAGIC %md ### 3. Value sanity ranges (post-Silver-filter, these should already hold -- this catches a regression in the filters themselves)

# COMMAND ----------

eth_tx = spark.table(silver_table("eth", "transactions")).filter(F.col("date").isin(CHECK_DATES))
bad_eth_value = eth_tx.filter(F.col("value_eth") < 0).count()
record("eth.transactions: no negative value_eth", bad_eth_value == 0, f"{bad_eth_value} rows")

btc_tx = spark.table(silver_table("btc", "transactions")).filter(F.col("date").isin(CHECK_DATES))
bad_btc_value = btc_tx.filter(F.col("value_btc") < 0).count()
record("btc.transactions: no negative value_btc", bad_btc_value == 0, f"{bad_btc_value} rows")

eth_blocks = spark.table(silver_table("eth", "blocks")).filter(F.col("date").isin(CHECK_DATES))
bad_utilization = eth_blocks.filter((F.col("gas_used_pct") < 0) | (F.col("gas_used_pct") > 100)).count()
record("eth.blocks: gas_used_pct within [0, 100]", bad_utilization == 0, f"{bad_utilization} rows")

# COMMAND ----------

# MAGIC %md ### Verdict

# COMMAND ----------

failures = [r for r in results if not r[1]]
print(f"\n{len(results) - len(failures)}/{len(results)} checks passed.")

if failures:
    detail = "\n".join(f"  - {name}: {d}" for name, _, d in failures)
    raise RuntimeError(f"{len(failures)} data quality check(s) failed:\n{detail}")