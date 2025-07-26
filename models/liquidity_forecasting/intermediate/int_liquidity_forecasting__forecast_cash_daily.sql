{{ config(materialized='table') }}

WITH src AS (
  SELECT
    business_id,
    due_date                                  AS date_day,
    scenario,
    signed_amount,
    signed_amount_prob
  FROM {{ ref('int_liquidity_forecasting__forecast_events_latest') }}
)

SELECT
  business_id,
  date_day,
  scenario,
  SUM(signed_amount)       AS forecast_cash_change,
  SUM(signed_amount_prob)  AS forecast_cash_change_prob
FROM src
GROUP BY 1,2,3