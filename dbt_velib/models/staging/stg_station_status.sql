with source as (

    select * from {{ source('raw', 'station_status_snapshots') }}

),

renamed as (

    select
        station_id,
        num_bikes_available,
        num_docks_available,
        is_installed,
        is_renting,
        is_returning,
        last_reported

    from source

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by station_id, last_reported
            order by last_reported
        ) as row_num

    from renamed

)

select
    station_id,
    num_bikes_available,
    num_docks_available,
    is_installed,
    is_renting,
    is_returning,
    last_reported

from deduplicated
where row_num = 1