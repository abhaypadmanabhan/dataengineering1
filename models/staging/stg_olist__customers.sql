{{ config(materialized='view') }}
with raw as (
select * from {{ source('olist','customers') }}
)
select
customer_id,
customer_unique_id,
customer_zip_code_prefix,
customer_city,
customer_state
from raw
where customer_id is not null