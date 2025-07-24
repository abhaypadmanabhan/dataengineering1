-- models/liquidity_forecasting/marts/mart_liquidity_forecasting__daily_spend.sql

SELECT
  date,
  SUM(amount) AS total_daily_spend,
  COUNT(*) AS transaction_count,
  COUNT(DISTINCT merchant_name) AS unique_merchants
FROM {{ ref('stg_liquidity_forcasting__transactions') }}
GROUP BY date
ORDER BY date