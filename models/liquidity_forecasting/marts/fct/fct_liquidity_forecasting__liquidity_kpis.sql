{{ config(materialized='view') }}

{# ---- Tunables (override with --vars) ---- #}
{% set days_cash_window   = var('days_cash_window', 30) %}
{% set min_cash_threshold = var('min_cash_threshold', 5000.0) %}
{% set kpi_scenario       = var('kpi_scenario', 'base') %}
{% set hide_inactive_days = var('hide_inactive_days', true) %}

WITH bounds AS (
  SELECT
    LEAST(
      (SELECT MIN(date_day) FROM {{ ref('int_liquidity_forecasting__actual_cash_daily') }}),
      (SELECT MIN(date_day) FROM {{ ref('int_liquidity_forecasting__forecast_cash_daily') }})
    ) AS min_d,
    GREATEST(
      (SELECT MAX(date_day) FROM {{ ref('int_liquidity_forecasting__actual_cash_daily') }}),
      (SELECT MAX(date_day) FROM {{ ref('int_liquidity_forecasting__forecast_cash_daily') }})
    ) AS max_d
),

proj AS (
  SELECT p.*
  FROM {{ ref('fct_liquidity_forecasting__cash_projection') }} p
  CROSS JOIN bounds b
  WHERE p.scenario = '{{ kpi_scenario }}'
    AND p.date_day BETWEEN b.min_d AND b.max_d
),

deltas AS (
  SELECT
    business_id,
    date_day,
    scenario,
    actual_cash_change,
    forecast_cash_change
  FROM {{ ref('fct_liquidity_forecasting__cash_daily_combined') }}
  CROSS JOIN bounds b
  WHERE scenario = '{{ kpi_scenario }}'
    AND date_day BETWEEN b.min_d AND b.max_d
),

-- Outflows only (for rolling DOCH calculation)
flows AS (
  SELECT
    business_id,
    date_day,
    scenario,
    CASE WHEN forecast_cash_change < 0 THEN ABS(forecast_cash_change) ELSE 0 END AS outflow
  FROM deltas
),

-- Backward-looking 30-day avg
avg_outflow_raw AS (
  SELECT
    business_id,
    date_day,
    scenario,
    AVG(outflow) OVER (
      PARTITION BY business_id
      ORDER BY date_day
      ROWS BETWEEN {{ days_cash_window }} PRECEDING AND CURRENT ROW
    ) AS avg_daily_outflow
  FROM flows
),

-- Fill-forward non-null avg to avoid NULL DOCH
avg_outflow AS (
  SELECT
    business_id,
    date_day,
    scenario,
    COALESCE(
      avg_daily_outflow,
      LAST_VALUE(avg_daily_outflow IGNORE NULLS) OVER (
        PARTITION BY business_id
        ORDER BY date_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ),
      0
    ) AS avg_daily_outflow_filled
  FROM avg_outflow_raw
),

joined AS (
  SELECT
    p.business_id,
    p.date_day,
    p.scenario,
    p.balance_actual,
    p.balance_projected,
    d.actual_cash_change,
    d.forecast_cash_change,
    a.avg_daily_outflow_filled AS avg_daily_outflow,
    {{ min_cash_threshold }}   AS min_cash_threshold
  FROM proj p
  LEFT JOIN deltas     d USING (business_id, date_day, scenario)
  LEFT JOIN avg_outflow a USING (business_id, date_day, scenario)
),

final AS (
  SELECT
    *,
    -- in the final SELECT
    SAFE_DIVIDE(balance_projected, GREATEST(avg_daily_outflow, 1.0)) AS est_days_cash_on_hand,
    balance_projected < min_cash_threshold                       AS threshold_breach_flag,
    (actual_cash_change - forecast_cash_change)                  AS forecast_vs_actual_var,
    SAFE_DIVIDE(
      (actual_cash_change - forecast_cash_change),
      NULLIF(ABS(forecast_cash_change), 0)
    )                                                            AS variance_pct
  FROM joined
)

SELECT *
FROM final
{% if hide_inactive_days %}
WHERE (ABS(actual_cash_change) > 0.00001 OR ABS(forecast_cash_change) > 0.00001)
{% endif %}