{{ config(materialized='table') }}

WITH base AS (
  SELECT
    business_id,
    DATE(post_date) AS date_day,
    CASE
      WHEN UPPER(direction) = 'OUTFLOW' THEN -amount
      ELSE amount
    END AS signed_amount
  FROM {{ ref('stg_liquidity_forecasting__transactions') }}
  WHERE post_date IS NOT NULL
)

SELECT
  business_id,
  date_day,
  SUM(signed_amount) AS actual_cash_change
FROM base
GROUP BY 1, 2