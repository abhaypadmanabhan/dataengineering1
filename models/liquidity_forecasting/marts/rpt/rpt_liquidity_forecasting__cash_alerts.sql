{{ config(materialized='view') }}

-- Pull business-specific policy instead of hard-coded vars
WITH pol AS (
  SELECT
    business_id,
    scenario,
    min_cash_threshold,
    warn_days,
    COALESCE(as_of_date_override, CURRENT_DATE()) AS as_of_date
  FROM {{ ref('stg_liquidity_forecasting__cfg_alert_policies') }}
),

kpi AS (
  SELECT *
  FROM {{ ref('fct_liquidity_forecasting__liquidity_kpis') }}
),

-- Scope KPI rows using policy (multi-tenant-friendly)
kpi_scoped AS (
  SELECT k.*
  FROM kpi k
  JOIN pol p
    ON k.business_id = p.business_id
   AND k.scenario    = p.scenario
),

-- All breach days under threshold
breaches AS (
  SELECT
    ks.business_id,
    ks.scenario,
    ks.date_day AS breach_date
  FROM kpi_scoped ks
  JOIN pol p USING (business_id, scenario)
  WHERE ks.balance_projected < p.min_cash_threshold
),

-- Next breach date attached to each day
next_breach AS (
  SELECT
    ks.business_id,
    ks.scenario,
    ks.date_day,
    ks.balance_projected,
    ks.est_days_cash_on_hand,
    ks.avg_daily_outflow,
    ks.actual_cash_change,
    ks.forecast_cash_change,
    ks.threshold_breach_flag,
    p.min_cash_threshold,
    p.warn_days,
    p.as_of_date,
    b.breach_date AS next_breach_date
  FROM kpi_scoped ks
  JOIN pol p USING (business_id, scenario)
  LEFT JOIN breaches b
    ON b.business_id = ks.business_id
   AND b.scenario    = ks.scenario
   AND b.breach_date >= ks.date_day
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ks.business_id, ks.scenario, ks.date_day
    ORDER BY b.breach_date
  ) = 1
),

annotated AS (
  SELECT
    business_id,
    scenario,
    date_day,
    balance_projected,
    est_days_cash_on_hand,
    avg_daily_outflow,
    actual_cash_change,
    forecast_cash_change,
    threshold_breach_flag,
    min_cash_threshold,
    warn_days,
    as_of_date,
    (balance_projected < min_cash_threshold) AS breach_today,
    next_breach_date,
    DATE_DIFF(next_breach_date, date_day, DAY) AS days_until_breach
  FROM next_breach
),

-- Step 1: Assign severity
scored_stage AS (
  SELECT
    *,
    CASE
      WHEN breach_today OR threshold_breach_flag THEN 'RED'
      WHEN next_breach_date IS NOT NULL AND days_until_breach <= warn_days THEN 'AMBER'
      ELSE 'GREEN'
    END AS severity
  FROM annotated
),

-- Step 2: Mark first day of a red streak
scored AS (
  SELECT
    *,
    CASE
      WHEN (breach_today OR threshold_breach_flag)
       AND COALESCE(
             LAG(breach_today OR threshold_breach_flag)
             OVER (PARTITION BY business_id, scenario ORDER BY date_day),
             FALSE
           ) = FALSE
      THEN TRUE ELSE FALSE
    END AS start_of_breach
  FROM scored_stage
),

-- Step 3: Build message text
scored_msg AS (
  SELECT
    *,
    CONCAT(
      'Biz ', business_id,
      CASE
        WHEN breach_today OR threshold_breach_flag THEN ' breached today'
        WHEN severity = 'AMBER' THEN CONCAT(' will breach in ', CAST(days_until_breach AS STRING), ' days')
        ELSE ' is healthy'
      END,
      '. Balance: $', FORMAT('%.2f', balance_projected),
      ', DOCH: ', FORMAT('%.1f', est_days_cash_on_hand)
    ) AS alert_message
  FROM scored
),

-- One RED per biz/scenario (first future one)
red_one AS (
  SELECT *
  FROM scored_msg
  WHERE severity = 'RED'
    AND date_day >= as_of_date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY business_id, scenario
    ORDER BY date_day
  ) = 1
),

-- All AMBER rows in the future window
amber_rows AS (
  SELECT *
  FROM scored_msg
  WHERE severity = 'AMBER'
    AND date_day >= as_of_date
),

-- Businesses that already have an alert row
biz_with_alerts AS (
  SELECT DISTINCT business_id, scenario FROM red_one
  UNION DISTINCT
  SELECT DISTINCT business_id, scenario FROM amber_rows
),

-- Fallback GREEN rows: for any biz with NO alerts, show their lowest future balance day
fallback_green AS (
  SELECT
    sm.business_id,
    sm.scenario,
    sm.date_day,
    'GREEN'                          AS severity,
    sm.balance_projected,
    sm.est_days_cash_on_hand,
    sm.avg_daily_outflow,
    sm.actual_cash_change,
    sm.forecast_cash_change,
    sm.threshold_breach_flag,
    FALSE                            AS breach_today,
    FALSE                            AS start_of_breach,
    sm.next_breach_date,
    sm.days_until_breach,
    sm.min_cash_threshold,
    sm.warn_days,
    sm.as_of_date,
    CONCAT(
      'Biz ', sm.business_id,
      ' is healthy. Lowest projected balance (future): $',
      FORMAT('%.2f', sm.balance_projected),
      ', DOCH: ', FORMAT('%.1f', sm.est_days_cash_on_hand)
    ) AS alert_message
  FROM scored_msg sm
  LEFT JOIN biz_with_alerts a
    ON sm.business_id = a.business_id
   AND sm.scenario    = a.scenario
  WHERE sm.severity = 'GREEN'
    AND sm.date_day >= sm.as_of_date
    AND a.business_id IS NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY sm.business_id, sm.scenario
    ORDER BY sm.balance_projected ASC, sm.date_day ASC
  ) = 1
)

-- Final union
SELECT
  business_id,
  scenario,
  date_day,
  severity,
  balance_projected,
  est_days_cash_on_hand,
  avg_daily_outflow,
  actual_cash_change,
  forecast_cash_change,
  threshold_breach_flag,
  breach_today,
  start_of_breach,
  next_breach_date,
  days_until_breach,
  min_cash_threshold,
  warn_days,
  as_of_date,
  alert_message
FROM red_one

UNION ALL

SELECT
  business_id,
  scenario,
  date_day,
  severity,
  balance_projected,
  est_days_cash_on_hand,
  avg_daily_outflow,
  actual_cash_change,
  forecast_cash_change,
  threshold_breach_flag,
  breach_today,
  start_of_breach,
  next_breach_date,
  days_until_breach,
  min_cash_threshold,
  warn_days,
  as_of_date,
  alert_message
FROM amber_rows

UNION ALL

SELECT
  business_id,
  scenario,
  date_day,
  severity,
  balance_projected,
  est_days_cash_on_hand,
  avg_daily_outflow,
  actual_cash_change,
  forecast_cash_change,
  threshold_breach_flag,
  breach_today,
  start_of_breach,
  next_breach_date,
  days_until_breach,
  min_cash_threshold,
  warn_days,
  as_of_date,
  alert_message
FROM fallback_green