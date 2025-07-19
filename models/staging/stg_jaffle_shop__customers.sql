with
source as (
    select * from {{ source('jaffle_shop','Customers') }}
)
select * from source