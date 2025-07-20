{{ config(materialized='view') }}
with raw as (
select * from {{ source('olist','geolocations') }}
)
select
cast(geolocation_zip_code_prefix as int64) as geolocation_zip_code_prefix,
cast(geolocation_lat as float64) as geolocation_lat,
cast(geolocation_lng as float64) as geolocation_lng,
geolocation_city,
geolocation_state
from raw
where geolocation_zip_code_prefix is not null