with fct as (

    select * from {{ ref('fct_station_status') }}

),

dim as (

    select * from {{ ref('dim_stations') }}

),

agregat_par_station as (

    select
        station_id,
        count(*) as nb_photos_total,
        sum(case when est_vide then 1 else 0 end) as nb_photos_vide,
        sum(case when est_pleine then 1 else 0 end) as nb_photos_pleine

    from fct
    group by station_id
    having count(*) >= 10

),

pourcentages as (

    select
        station_id,
        nb_photos_total,
        round(100.0 * nb_photos_vide / nb_photos_total, 2) as pct_temps_vide,
        round(100.0 * nb_photos_pleine / nb_photos_total, 2) as pct_temps_pleine

    from agregat_par_station

),

score as (

    select
        station_id,
        nb_photos_total,
        pct_temps_vide,
        pct_temps_pleine,
        round(0.7 * pct_temps_vide + 0.3 * pct_temps_pleine, 2) as score_criticite

    from pourcentages

)

select
    dim.station_id,
    dim.station_name,
    dim.station_code,
    dim.latitude,
    dim.longitude,
    dim.capacity,
    score.nb_photos_total,
    score.pct_temps_vide,
    score.pct_temps_pleine,
    score.score_criticite

from score
inner join dim on score.station_id = dim.station_id
order by score.score_criticite desc