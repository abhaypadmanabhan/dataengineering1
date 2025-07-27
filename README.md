# Liquidity Forecasting — dbt Project

This dbt project implements the transformation layer for the Real-Time Liquidity Forecasting pipeline. It models raw Plaid transactions and forecast events into staged views, intermediate tables, fact tables, and policy-aware report tables.

Branch: `olist_branch`  
Repo: https://github.com/abhaypadmanabhan/dataengineering1/tree/olist_branch

---

## Table of Contents

- [Project Structure](#project-structure)  
- [Prerequisites](#prerequisites)  
- [Configuration](#configuration)  
- [Models](#models)  
  - [Sources](#sources)  
  - [Staging (stg)](#staging-stg)  
  - [Intermediate (int)](#intermediate-int)  
  - [Facts (fct)](#facts-fct)  
  - [Reports (rpt)](#reports-rpt)  
- [Tests](#tests)  
- [How to Run](#how-to-run)  
- [Documentation](#documentation)  
- [CI/CD Suggestions](#cicd-suggestions)  

---

## Project Structure
├── models/
│   ├── liquidity_forecasting/
│   │   ├── sources/
│   │   │   └── liquidity_forecasting_sources.yml
│   │   ├── staging/
│   │   │   ├── stg_liquidity_forecasting__transactions.sql
│   │   │   ├── stg_liquidity_forecasting__forecast_plan.sql
│   │   │   └── stg_liquidity_forecasting__forecast_events.sql
│   │   ├── intermediate/
│   │   │   ├── int_liquidity_forecasting__actual_cash_daily.sql
│   │   │   └── int_liquidity_forecasting__forecast_cash_daily.sql
│   │   ├── marts/
│   │   │   ├── fct/
│   │   │   │   ├── fct_liquidity_forecasting__cash_daily_combined.sql
│   │   │   │   ├── fct_liquidity_forecasting__cash_projection.sql
│   │   │   │   └── fct_liquidity_forecasting__liquidity_kpis.sql
│   │   │   └── rpt/
│   │   │       └── rpt_liquidity_forecasting__cash_alerts.sql
│   └── schema.yml      # model configurations & tests
├── dbt_project.yml
└── profiles.yml.sample

---

## Prerequisites

- **dbt Core** `>=1.5.0`  
- **Python** `>=3.8` (if using `dbt-python` packages)  
- **BigQuery** account with:
  - Dataset for raw tables (`liquidity_forecasting`)  
  - Dataset for dbt outputs (e.g. `dbt_<user>`)  
- **dbt profile** configured for BigQuery

---

## Configuration

1. Copy `profiles.yml.sample` → `~/.dbt/profiles.yml`  
2. Update the `liquidity_forecasting` profile:
   
   liquidity_forecasting:
     target: dev
     outputs:
       dev:
         type: bigquery
         method: service-account
         project: your-gcp-project
         dataset: dbt_youruser
         keyfile: /path/to/your-service-account.json
         threads: 4
         timeout_seconds: 300
         location: US

	3.	Confirm connectivity:
dbt debug --profile liquidity_forecasting

Models

Sources

Defined in models/liquidity_forecasting/sources/liquidity_forecasting_sources.yml
	•	transactions_raw: Plaid transaction feed
	•	forecast_events_raw: Pub/Sub–streamed forecast events
	•	cfg_opening_balances, cfg_alert_policies: config tables

Staging (stg)

Clean and cast raw fields to typed views:
	•	stg_liquidity_forecasting__transactions
	•	stg_liquidity_forecasting__forecast_plan
	•	stg_liquidity_forecasting__forecast_events

Intermediate (int)

Aggregate daily cash deltas:
	•	int_liquidity_forecasting__actual_cash_daily
	•	int_liquidity_forecasting__forecast_cash_daily

Facts (fct)

Combine and project cashflow:
	•	fct_liquidity_forecasting__cash_daily_combined
	•	fct_liquidity_forecasting__cash_projection
	•	fct_liquidity_forecasting__liquidity_kpis

Reports (rpt)

Policy-aware alerts:
	•	rpt_liquidity_forecasting__cash_alerts

⸻

Tests
	•	uniqueness and not_null on primary keys and required fields
	•	relationships between staging sources and config tables

Tests
	•	uniqueness and not_null on primary keys and required fields
	•	relationships between staging sources and config tables

Run all tests:
dbt test --select liquidity_forecasting.*




