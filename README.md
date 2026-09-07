# Blockchain Lakehouse
#### *Author: Jack Kasbeer*

A Databricks data engineering pipeline that ingests public Bitcoin and Ethereum on-chain data and
builds it into a medallion-architecture lakehouse — orchestrated, monitored, and deployed via CI/CD.
Part of a 2-month cert + portfolio project; it exercises Data Engineer Professional exam objectives
across both phases and produces the feature source (`gold.whale_transactions`) for the Month 2
anomaly-detection modeling project.

- **Phase 1** (`notebooks/`): Bronze → Silver → Gold. See below.
- **Phase 2** (`databricks.yml`, `.github/workflows/deploy.yml`, `04_data_quality_checks.py`,
  `05_maintenance_optimize.py`): orchestration, data quality gating, cost/performance maintenance,
  and CI/CD. See "Phase 2" section below.

## Data source

[AWS Public Blockchain Data](https://registry.opendata.aws/aws-public-blockchain/) — Bitcoin,
Ethereum, and 9 other chains, pre-transformed into partitioned Parquet by AWS, free, no AWS account
required. We use:

| Chain | Tables |
|---|---|
| Ethereum | `blocks`, `transactions`, `token_transfers` |
| Bitcoin | `blocks`, `transactions` (nested inputs/outputs) |

Schema reference: [AWS digital-assets-on-aws schema docs](https://github.com/aws-solutions-library-samples/guidance-for-digital-assets-on-aws/blob/main/analytics/consumer/schema/).

## Why not pure Auto Loader?

Auto Loader's efficient mode relies on the bucket owner publishing S3 event notifications — not
possible on a bucket we don't own. The alternative (directory-listing mode) would mean re-listing
years of blockchain history on every run. This pipeline instead does an explicit, bounded batch
read per date partition, which is both cheaper and the more defensible production answer if asked
about it in an interview.

## Setup

1. **Unity Catalog:** the notebooks create the catalog/schemas themselves (`00_config.py`), but you
   need `CREATE CATALOG` permission on your metastore, or point `catalog` at one that already exists.
2. **S3 access:** no credentials needed — `00_config.py` sets anonymous, bucket-scoped S3A
   credentials so this doesn't touch your workspace's normal cloud storage access.
3. **Cluster:** any general-purpose cluster with Unity Catalog access (a small single-node cluster
   is enough for the default 30-day backfill window).
4. **Import into Databricks:** these are `.py` files with Databricks notebook markers
   (`# Databricks notebook source`) — import the `notebooks/` folder via Repos (Git folder) or
   Workspace → Import, and they'll render as normal notebooks with cells.

## Running it

Run in order: `00_config` → `01_bronze_backfill` → `02_silver_transform` → `03_gold_aggregates`
(`00_config` is also `%run` from within the other three, so you mainly need to run 01→02→03).

Adjust the `backfill_start_date` / `backfill_end_date` widgets to control cost — default is a
rolling 30 days.

**Bronze is incremental, not a full re-scan every run.** `01_bronze_backfill` checks the max `date`
already landed in each Bronze table and only processes dates after that watermark, up to
`backfill_end_date`. First run does the full configured window; every run after that (e.g. a daily
scheduled Job) only touches new dates and skips everything already ingested.

**Silver/Gold rebuild the configured window on every run, not full history** — every Bronze/Silver
read in `02_silver_transform.py` / `03_gold_aggregates.py` is filtered to the window before any
transformation. This isn't just an optimization: their writes use `replaceWhere date IN (window)`,
and Delta requires every row being written to satisfy that predicate — without the filter, the write
throws as soon as Bronze/Silver holds any history outside the current window, which happens from the
second run onward once `01`'s watermark starts accumulating dates. Giving Silver/Gold their own
watermark (only rebuild rows actually affected by new Bronze data) is a reasonable future
optimization, but isn't required for correctness the way the window filter is.

## Flagged assumption to verify

`btc.transactions.output_value` / `fee` are documented as `double` with no stated unit. This project
assumes native BTC (standard blockchain-etl convention), not satoshis — sanity-check
`avg(output_value)` once you have real data and flip `BTC_VALUE_DIVISOR` in
`02_silver_transform.py` if it turns out to be satoshis (`1e8`).

---

## Phase 2 — Orchestration, monitoring, CI/CD

### What's new
- **`databricks.yml`** — a Databricks Asset Bundle defining two Lakeflow Jobs:
  - `blockchain_pipeline` (daily, 06:00): `bronze_backfill` → `silver_transform` →
    `data_quality_checks` → `gold_aggregates`, chained with `depends_on` so a failure anywhere
    blocks everything downstream instead of writing partial/bad Gold data.
  - `blockchain_maintenance` (weekly, Sunday 08:00): `OPTIMIZE ... ZORDER BY` on the tables that
    actually get point-looked-up downstream (wallet/address columns) — kept separate from the daily
    job since compaction is a cost concern, not a correctness one.
  - Both use **serverless compute** (no `job_cluster_key`/`new_cluster` on any task) — no
    autoscale/node-type tuning needed for a pipeline this size.
  - `email_notifications.on_failure` on both jobs, wired to the `notification_email` bundle
    variable (blank by default — set it before deploying, see Setup below).
- **`04_data_quality_checks.py`** — runs between Silver and Gold. Not a Lakeflow Declarative
  Pipelines `@expect(...)` (this pipeline uses plain notebooks orchestrated by Jobs, not DLT) —
  a hand-rolled equivalent: null/duplicate primary-key checks, missing-date detection, and value
  sanity ranges, all across every table. Raises (failing the task, blocking Gold, triggering the
  email) if anything fails.
- **`05_maintenance_optimize.py`** — the weekly `OPTIMIZE`/`ZORDER` job. Deliberately does *not*
  run `VACUUM` (see the note in that file for why).
- **`.github/workflows/deploy.yml`** — validates the bundle on every PR, deploys to `prod` on
  merge to `main`. Needs `DATABRICKS_HOST` and `DATABRICKS_TOKEN` (a service principal token) as
  GitHub repo secrets.

### Deploying it
1. Install the [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) locally (`databricks bundle` commands run through it).
2. Fill in `host:` under both `dev` and `prod` targets in `databricks.yml` with your workspace URL.
3. Set the `notification_email` variable — either edit the default in `databricks.yml`, or pass it per-deploy: `databricks bundle deploy -t dev --var="notification_email=you@example.com"`.
4. `databricks bundle validate -t dev` then `databricks bundle deploy -t dev` to try it locally before wiring up CI.
5. For CI/CD: create a Databricks service principal, generate a token for it, add `DATABRICKS_HOST`/`DATABRICKS_TOKEN` as GitHub Actions secrets on the repo.

### Exam objectives this phase exercises
Data Engineer Professional: Debugging & Deploying (Asset Bundles, task `depends_on` chaining,
git-based CI/CD), Monitoring & Alerting (failure notifications, the DQ-check task itself), Cost &
Performance Optimization (serverless vs. manual cluster sizing, `OPTIMIZE`/`ZORDER` on a separate
cadence from correctness-critical work).

## What's next (Month 2)

`gold.whale_transactions` (unified schema across both chains) is the feature source for the
anomaly-detection project — the next phase of this project.

## Exam objectives Phase 1 exercises

Data Engineer Professional: Developing Code (Python/SQL), Data Ingestion & Acquisition, Data
Transformation/Cleansing/Quality, Data Modelling, Cost & Performance Optimization (bounded batch
reads vs. naive full-history scans, idempotent partition overwrites).

Not yet covered anywhere in this project: Data Governance (row/column-level security, Unity Catalog
lineage/tags).
