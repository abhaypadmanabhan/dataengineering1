{{ config(
    materialized='table',
    partition_by={'field': 'date_day', 'data_type': 'date'},
    cluster_by=['business_id', 'scenario']
) }}

WITH date_spine AS (
  SELECT date_day
  FROM {{ ref('dim_liquidity_forecasting__date') }}
),

businesses AS (
  SELECT DISTINCT business_id FROM {{ ref('int_liquidity_forecasting__actual_cash_daily') }}
  UNION DISTINCT
  SELECT DISTINCT business_id FROM {{ ref('int_liquidity_forecasting__forecast_cash_daily') }}
),

scenarios AS (
  SELECT 'base' AS scenario
  UNION DISTINCT
  SELECT scenario
  FROM {{ ref('int_liquidity_forecasting__forecast_cash_daily') }}
  WHERE scenario IS NOT NULL
),

grid AS (
  SELECT DISTINCT
    b.business_id,
    d.date_day,
    s.scenario
  FROM businesses b
  CROSS JOIN {{ ref('dim_liquidity_forecasting__date') }} d
  CROSS JOIN scenarios s
),

actuals AS (
  SELECT
    business_id,
    date_day,
    actual_cash_change
  FROM {{ ref('int_liquidity_forecasting__actual_cash_daily') }}
),

forecasts AS (
  SELECT
    business_id,
    date_day,
    scenario,
    forecast_cash_change,
    forecast_cash_change_prob
  FROM {{ ref('int_liquidity_forecasting__forecast_cash_daily') }}
)

SELECT DISTINCT
  g.business_id,
  g.date_day,
  g.scenario,
  COALESCE(a.actual_cash_change, 0)          AS actual_cash_change,
  COALESCE(f.forecast_cash_change, 0)        AS forecast_cash_change,
  COALESCE(f.forecast_cash_change_prob, 0)   AS forecast_cash_change_prob
FROM grid g
LEFT JOIN actuals   a USING (business_id, date_day)
LEFT JOIN forecasts f USING (business_id, date_day, scenario)