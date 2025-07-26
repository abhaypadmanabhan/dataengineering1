{{ config(materialized='table') }}

WITH base AS (
  SELECT
    business_id,
    forecast_id,
    event_id,
    event_type,
    event_status,
    version,
    ingest_ts_ts          AS ingest_ts,        -- <- use the casted timestamp from staging
    due_date,                                     -- already DATE
    direction,
    amount,
    probability,
    currency,
    scenario,
    cost_center,
    department,
    gl_account,
    counterparty,
    note,
    created_at_ts,
    updated_at_ts
  FROM {{ ref('stg_liquidity_forecasting__forecast_events') }}
  WHERE due_date IS NOT NULL
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY forecast_id
      ORDER BY version DESC, ingest_ts DESC
    ) AS rn
  FROM base
)

SELECT
  business_id,
  forecast_id,
  event_id,
  event_type,
  event_status,
  version,
  ingest_ts,
  due_date,
  direction,
  amount,
  probability,
  CASE WHEN UPPER(direction) = 'OUTFLOW' THEN -amount ELSE amount END               AS signed_amount,
  CASE WHEN UPPER(direction) = 'OUTFLOW' THEN -amount ELSE amount END * probability AS signed_amount_prob,
  currency,
  scenario,
  cost_center,
  department,
  gl_account,
  counterparty,
  note,
  created_at_ts,
  updated_at_ts
FROM ranked
WHERE rn = 1
  AND UPPER(event_status) != 'CANCELLED'