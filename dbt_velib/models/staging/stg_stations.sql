with source as (

    select * from {{ source('raw', 'stations') }}

),

renamed as (

    select
        station_id,
        stationcode as station_code,
        name as station_name,
        lat as latitude,
        lon as longitude,
        capacity

    from source

),

deduplicated as (

    select distinct *
    from renamed

)

select * from deduplicated