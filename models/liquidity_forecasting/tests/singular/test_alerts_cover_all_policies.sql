-- Fails if any business/scenario in policies does NOT appear in alerts
WITH pol AS (
  SELECT business_id, scenario
  FROM {{ ref('stg_liquidity_forecasting__cfg_alert_policies') }}
),
alerts AS (
  SELECT DISTINCT business_id, scenario
  FROM {{ ref('rpt_liquidity_forecasting__cash_alerts') }}
)
SELECT p.*
FROM pol p
LEFT JOIN alerts a
  USING (business_id, scenario)
WHERE a.business_id IS NULL