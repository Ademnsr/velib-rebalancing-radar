{{ config(severity = 'warn') }}

-- Ce test echoue si le nombre de velos + places libres
-- s'ecarte trop de la capacite officielle de la station
-- Marge de tolerance : 2. Avertissement (pas erreur bloquante) car
-- des incoherences reelles existent sur certaines stations, probablement
-- liees a des donnees de referentiel obsoletes cote API source.
select
    f.station_id,
    f.last_reported,
    f.num_bikes_available,
    f.num_docks_available,
    d.capacity
from {{ ref('fct_station_status') }} f
inner join {{ ref('dim_stations') }} d
    on f.station_id = d.station_id
where abs((f.num_bikes_available + f.num_docks_available) - d.capacity) > 2