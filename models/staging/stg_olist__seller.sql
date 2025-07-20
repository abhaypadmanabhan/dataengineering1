{{ config(materialized='view') }}
with raw as (
select * from {{ source('olist','sellers') }}
)
select
seller_id,
cast(seller_zip_code_prefix as int64) as seller_zip_code_prefix,
seller_city,
seller_state
from raw
where seller_id is not null