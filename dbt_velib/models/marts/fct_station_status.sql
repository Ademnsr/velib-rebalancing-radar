{{
    config(
        materialized='incremental',
        unique_key=['station_id', 'last_reported']
    )
}}

with source as (

    select * from {{ ref('stg_station_status') }}

)

select
    station_id,
    num_bikes_available,
    num_docks_available,
    is_installed,
    is_renting,
    is_returning,
    last_reported,
    case
        when num_bikes_available = 0 then true
        else false
    end as est_vide,
    case
        when num_docks_available = 0 then true
        else false
    end as est_pleine

from source

{% if is_incremental() %}

    where last_reported > (select max(last_reported) from {{ this }})

{% endif %}