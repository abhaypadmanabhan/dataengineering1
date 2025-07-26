{{ config(
    materialized='view'
) }}

WITH src AS (
  SELECT *
  FROM {{ source('liquidity_forecasting', 'transactions_raw') }}
)

SELECT
  actual_id,
  business_id,
  account_id,
  account_name,
  source_system,
  cashflow_type,
  direction,
  amount,
  currency,
  post_date,
  authorized_date,
  merchant_name,
  original_name,
  category_l1,
  category_l2,
  payment_channel,
  transaction_type,
  pending,
  ingest_ts
FROM src
