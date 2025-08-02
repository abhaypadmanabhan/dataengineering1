# 🏗️ DBT Models for Liquidity Forecasting

This repo contains the DBT implementation for the Real-Time Liquidity Forecasting project.

---

## 🧱 Purpose
Transform raw Plaid transaction data in BigQuery into analytics-ready tables for Looker dashboards.

---

## 🔗 Project Context
This is part of the larger [Liquidity Forecasting Pipeline](https://github.com/abhaypadmanabhan/liquidity-pipeline). That repo contains Plaid ingestion, Pub/Sub pipeline, and visualization.

---

## 🧪 DBT Structure
- `stg_plaid_transactions.sql`: Stages raw data
- `int_cashflow_summary.sql`: Intermediate model to calculate net flow
- `mart_liquidity_metrics.sql`: Final model feeding dashboards

---

## ⚙️ DBT Setup
1. Clone repo
2. Set your profiles.yml
3. Run:
```bash
dbt deps
dbt seed
dbt run
dbt test
