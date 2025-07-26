{{ config(materialized='table') }}

{#-- Set default bounds; override with --vars if you want #}
{% set start_date = var('calendar_start_date', '2024-01-01') %}
{% set end_date   = var('calendar_end_date', '2027-12-31') %}

WITH dates AS (
  SELECT
    d AS date_day
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      DATE('{{ start_date }}'),
      DATE('{{ end_date }}')
    )
  ) AS d
)

SELECT
  date_day,
  EXTRACT(YEAR    FROM date_day)        AS year,
  EXTRACT(QUARTER FROM date_day)        AS quarter,
  EXTRACT(MONTH   FROM date_day)        AS month,
  EXTRACT(DAY     FROM date_day)        AS day,
  FORMAT_DATE('%Y-%m', date_day)        AS year_month,
  FORMAT_DATE('%G-W%V', date_day)       AS iso_year_week
FROM dates