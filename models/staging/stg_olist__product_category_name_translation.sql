{{ config(materialized='view') }}

with raw as (
    -- CHANGE THIS LINE: Use 'product_catagory_name_translation'
    -- to match the actual BigQuery table name and your sources.yml
    select * from {{ source('olist','product_catagory_name_translation') }}
)

select
    string_field_0 as product_category_name,
    string_field_1 as product_category_name_english
from raw
where string_field_0 is not null