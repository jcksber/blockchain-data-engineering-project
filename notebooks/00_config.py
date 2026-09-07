# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 00 - Configuration
# MAGIC Shared config for the blockchain data engineering pipeline (Bronze -> Silver -> Gold).
# MAGIC
# MAGIC **Data source:** [AWS Public Blockchain Data](https://registry.opendata.aws/aws-public-blockchain/)
# MAGIC A third-party, no-auth S3 bucket (`s3://aws-public-blockchain`) maintained by AWS. We only have
# MAGIC read access to it, and we do NOT own it, so two things follow:
# MAGIC 1. Reads must use **anonymous S3A credentials**, scoped to this one bucket only (see below) so it
# MAGIC    doesn't interfere with your workspace's normal (IAM-role-based) S3 access to your own storage.
# MAGIC 2. We can't set up S3 event notifications on someone else's bucket, so Auto Loader's efficient
# MAGIC    "file notification" mode isn't available here -- only "directory listing" mode is, and listing
# MAGIC    the *entire* history of Bitcoin (2009-) / Ethereum (2015-) on every run doesn't scale or stay
# MAGIC    cheap. Instead, this pipeline explicitly targets a date range per run (see widgets below) and
# MAGIC    reads that range as a bounded batch job -- which is also the more honest "how would you actually
# MAGIC    run this in production" answer for the exam's Cost & Performance Optimization objectives.

# COMMAND ----------

from datetime import date, timedelta

# COMMAND ----------

# MAGIC %md ### Widgets (parameterize for Databricks Jobs / Asset Bundles later)

# COMMAND ----------

dbutils.widgets.text("catalog", "blockchain_project", "Unity Catalog catalog name")
dbutils.widgets.text("backfill_start_date", (date.today() - timedelta(days=30)).isoformat(), "Backfill start date (YYYY-MM-DD)")
dbutils.widgets.text("backfill_end_date", (date.today() - timedelta(days=1)).isoformat(), "Backfill end date (YYYY-MM-DD, inclusive)")
dbutils.widgets.text("whale_eth_threshold", "100", "ETH whale threshold (native ETH)")
dbutils.widgets.text("whale_btc_threshold", "10", "BTC whale threshold (native BTC)")
dbutils.widgets.text("whale_sol_threshold", "1000", "SOL whale threshold (native SOL)")

CATALOG = dbutils.widgets.get("catalog")
BACKFILL_START_DATE = dbutils.widgets.get("backfill_start_date")
BACKFILL_END_DATE = dbutils.widgets.get("backfill_end_date")
WHALE_ETH_THRESHOLD = float(dbutils.widgets.get("whale_eth_threshold"))
WHALE_BTC_THRESHOLD = float(dbutils.widgets.get("whale_btc_threshold"))
WHALE_SOL_THRESHOLD = float(dbutils.widgets.get("whale_sol_threshold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cost note
# MAGIC Default is a rolling **30-day** window -- enough data to be a real, non-trivial dataset (millions of
# MAGIC Ethereum transactions, hundreds of thousands of Bitcoin transactions) without scanning years of
# MAGIC history on a personal/portfolio budget. Widen `backfill_start_date` once you've confirmed cluster
# MAGIC sizing and cost are where you want them.

# COMMAND ----------

# DBTITLE 1,Cell 6
SOURCE_BUCKET = "aws-public-blockchain"
SOURCE_PREFIX = "v1.0"

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Public S3 bucket access (no explicit credentials needed on Serverless)
# Spark automatically uses anonymous credentials for public buckets

# COMMAND ----------

for schema in (BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

# COMMAND ----------

# MAGIC %md ### Path + date-range helpers

# COMMAND ----------

def date_range(start_iso: str, end_iso: str):
    """Inclusive list of ISO date strings between start and end."""
    start = date.fromisoformat(start_iso)
    end = date.fromisoformat(end_iso)
    if end < start:
        raise ValueError(f"end_date {end_iso} is before start_date {start_iso}")
    days = (end - start).days
    return [(start + timedelta(days=i)).isoformat() for i in range(days + 1)]


def source_path(chain: str, table: str, day: str) -> str:
    """s3a path for one chain/table/day partition, e.g. eth/transactions/date=2026-08-01."""
    return f"s3a://{SOURCE_BUCKET}/{SOURCE_PREFIX}/{chain}/{table}/date={day}/"


def bronze_table(chain: str, table: str) -> str:
    return f"{CATALOG}.{BRONZE_SCHEMA}.{chain}_{table}"


def silver_table(chain: str, table: str) -> str:
    return f"{CATALOG}.{SILVER_SCHEMA}.{chain}_{table}"


def gold_table(name: str) -> str:
    return f"{CATALOG}.{GOLD_SCHEMA}.{name}"