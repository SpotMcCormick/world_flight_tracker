SELECT * FROM dev_env.stg_flight_data;



CREATE VIEW dev_env.dm_flight_data AS
SELECT
    id AS load_id,
    upload_dt, -- This is your ingestion time
    -- Convert Unix seconds to readable Timestamp with Time Zone
    to_timestamp((flight_row->>3)::double precision) AS time_position,
    to_timestamp((flight_row->>4)::double precision) AS last_contact,
    (flight_row->>0) AS icao24,
    (flight_row->>1) AS callsign,
    (flight_row->>2) AS origin_country,
    (flight_row->>5)::numeric AS longitude,
    (flight_row->>6)::numeric AS latitude,
    (flight_row->>7)::numeric AS baro_altitude,
    (flight_row->>8)::boolean AS on_ground,
    (flight_row->>9)::numeric AS velocity,
    (flight_row->>10)::numeric AS true_track,
    (flight_row->>11)::numeric AS vertical_rate,
    (flight_row->>13)::numeric AS geo_altitude,
    (flight_row->>14) AS squawk,
    (flight_row->>15)::boolean AS spi,
    (flight_row->>16)::int AS position_source
FROM (
    SELECT 
        id, 
        upload_dt, 
        jsonb_array_elements(data->'states') AS flight_row
    FROM dev_env.stg_flight_data
) AS flattened_subquery;


create view dev_env.dm_latest_flight_data AS
WITH ranked_flights AS (
    SELECT 
      *,
        RANK() OVER (ORDER BY load_id DESC) as rn
    FROM dev_env.dm_flight_data
)
SELECT * 
FROM ranked_flights 
WHERE rn = 1; 

;

select * from dev_env.dm_latest_flight_data
;
select count(*)
from dm_flight_data