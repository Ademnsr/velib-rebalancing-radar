-- Ce test echoue si des lignes ont le meme
-- couple station_id + last_reported dans fct_station_status
select
    station_id,
    last_reported,
    count(*) as nb_lignes
from {{ ref('fct_station_status') }}
group by station_id, last_reported
having count(*) > 1