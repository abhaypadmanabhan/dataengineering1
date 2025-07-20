{{ config(materialized='view') }}

with raw as (
    select * from {{ source('olist','products') }}
)

select
    product_id,
    product_category_name,
    -- Corrected typo from `product_name_length` to `product_name_lenght`
    cast(product_name_lenght as int64) as product_name_lenght,
    -- Corrected typo from `product_description_length` to `product_description_lenght`
    cast(product_description_lenght as int64) as product_description_lenght,
    cast(product_photos_qty as int64) as product_photos_qty,
    -- Schema specifies INTEGER, so cast to INT64, not FLOAT64
    cast(product_weight_g as int64) as product_weight_g,
    -- Schema specifies INTEGER, so cast to INT64, not FLOAT64
    cast(product_length_cm as int64) as product_length_cm,
    -- Schema specifies INTEGER, so cast to INT64, not FLOAT64
    cast(product_height_cm as int64) as product_height_cm,
    -- Schema specifies INTEGER, so cast to INT64, not FLOAT64
    cast(product_width_cm as int64) as product_width_cm
from raw
where product_id is not null