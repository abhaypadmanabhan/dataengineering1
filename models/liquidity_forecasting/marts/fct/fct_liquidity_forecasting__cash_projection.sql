{{ config(
    materialized='table',
    partition_by={'field': 'date_day', 'data_type': 'date'},
    cluster_by=['business_id', 'scenario']
) }}

WITH base AS (
  SELECT
    business_id,
    date_day,
    scenario,
    actual_cash_change,
    forecast_cash_change
  FROM {{ ref('fct_liquidity_forecasting__cash_daily_combined') }}
),

-- <<< INLINE OPENING BALANCES >>> 
-- Change these numbers/dates as needed. Keep one row per business_id.
ob AS (
  SELECT
    business_id,
    opening_balance_date,
    opening_balance_amount
  FROM {{ source('liquidity_forecasting', 'cfg_opening_balances') }}
),

joined AS (
  SELECT
    b.business_id,
    b.date_day,
    b.scenario,
    COALESCE(b.actual_cash_change, 0)   AS actual_cash_change,
    COALESCE(b.forecast_cash_change, 0) AS forecast_cash_change,
    o.opening_balance_amount,
    o.opening_balance_date
  FROM base b
  LEFT JOIN ob o USING (business_id)
),

calc AS (
  SELECT
    business_id,
    date_day,
    scenario,
    actual_cash_change,
    forecast_cash_change,
    opening_balance_amount,
    -- cumulative actual-only balance (start from opening balance)
    opening_balance_amount
      + SUM(actual_cash_change) OVER (
          PARTITION BY business_id
          ORDER BY date_day
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS balance_actual,

    -- cumulative forecast-only delta
    SUM(forecast_cash_change) OVER (
      PARTITION BY business_id, scenario
      ORDER BY date_day
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS forecast_cum_change
  FROM joined
)

SELECT
  business_id,
  date_day,
  scenario,
  actual_cash_change,
  forecast_cash_change,
  opening_balance_amount,
  balance_actual,
  balance_actual + forecast_cum_change AS balance_projected
FROM calc