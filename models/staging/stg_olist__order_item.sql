{{ config(materialized='view') }} -- FIX: Added missing opening curly brace
with raw as (
select * from {{ source('olist','items') }}
)
select
order_id,
cast(order_item_id as int64) as order_item_id,
product_id,
seller_id,
-- FIX: Removed the extra 'L' from shipping_limit_dateL to match schema
cast(shipping_limit_date as timestamp) as shipping_limit_date,
cast(price as float64) as price,
cast(freight_value as float64) as freight_value
from raw
where order_id is not null