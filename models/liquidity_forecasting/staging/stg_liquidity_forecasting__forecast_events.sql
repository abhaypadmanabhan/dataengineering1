{{ config(materialized='view') }}

WITH src AS (
  SELECT *
  FROM {{ source('liquidity_forecasting', 'forecast_events_raw') }}
)

SELECT
  event_id,
  event_type,
  event_status,
  forecast_id,
  business_id,
  cashflow_type,
  direction,
  amount,
  currency,
  -- Parse “stringy” dates/timestamps if you want typed fields
  SAFE.PARSE_DATE('%Y-%m-%d', due_date)                      AS due_date,
  probability,
  scenario,
  cost_center,
  department,
  gl_account,
  counterparty,
  note,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', created_at)       AS created_at_ts,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S.%f', updated_at)    AS updated_at_ts,
  TIMESTAMP(ingest_ts)                                        AS ingest_ts_ts,
  source_system,
  version
FROM src
