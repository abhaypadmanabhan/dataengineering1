{{ config(materialized='view') }}

WITH src AS (
  SELECT * FROM {{ source('liquidity_forecasting','forecast_plan_raw') }}
),

casted AS (
  SELECT
    forecast_id,
    business_id,
    source_system,
    UPPER(cashflow_type)    AS cashflow_type,
    UPPER(direction)        AS direction,
    CAST(amount AS NUMERIC) AS amount,
    UPPER(currency)         AS currency,
    CAST(due_date AS DATE)  AS due_date,
    CAST(expected_post_date AS DATE) AS expected_post_date,
    recurrence_rule,
    parent_recurring_id,
    counterparty_name,
    counterparty_id,
    category,
    CAST(probability AS FLOAT64) AS probability,
    scenario,
    UPPER(status)           AS status,
    cost_center,
    department,
    gl_account,

    -- created_at / updated_at came in as STRING (YYYY-MM-DD); convert to TIMESTAMP.
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',
      CASE
        WHEN created_at IS NULL THEN NULL
        WHEN REGEXP_CONTAINS(created_at, r'\d{4}-\d{2}-\d{2} \d') THEN created_at
        ELSE created_at || ' 00:00:00'
      END
    ) AS created_at_ts,

    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S',
      CASE
        WHEN updated_at IS NULL THEN NULL
        WHEN REGEXP_CONTAINS(updated_at, r'\d{4}-\d{2}-\d{2} \d') THEN updated_at
        ELSE updated_at || ' 00:00:00'
      END
    ) AS updated_at_ts,

    ingest_ts,

    direction = 'INFLOW' AS is_inflow,
    status    != 'PLANNED' AS is_changed
  FROM src
)

SELECT * FROM casted