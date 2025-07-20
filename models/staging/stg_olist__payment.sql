{{ config(materialized='view') }}
with raw as (
select * from {{ source('olist','order_payments') }}
)
select
order_id,
cast(payment_sequential as int64) as payment_sequential,
payment_type,
cast(payment_installments as int64) as payment_installments,
cast(payment_value as float64) as payment_value
from raw
where order_id is not null