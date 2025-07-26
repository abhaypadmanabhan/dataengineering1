{{ config(materialized='view') }}

WITH src AS (
  SELECT *
  FROM {{ source('liquidity_forecasting', 'cfg_alert_policies') }}
  WHERE active_flag = TRUE
)

SELECT
  business_id,
  COALESCE(scenario, 'base') AS scenario,
  CAST(min_cash_threshold AS NUMERIC)        AS min_cash_threshold,
  CAST(warn_days AS INT64)                   AS warn_days,
  as_of_date_override                        AS as_of_date_override,
  COALESCE(currency, 'USD')                  AS currency,
  TIMESTAMP(updated_at)                      AS updated_at_ts
FROM src