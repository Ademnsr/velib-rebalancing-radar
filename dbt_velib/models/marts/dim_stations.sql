with stations as (

    select * from {{ ref('stg_stations') }}

)

select
    station_id,
    station_code,
    station_name,
    latitude,
    longitude,
    capacity

from stations