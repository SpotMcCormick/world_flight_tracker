SELECT * FROM dev_env.stg_flight_data
ORDER BY upload_dt DESC

;
drop view dm_flight_data
;



CREATE OR REPLACE VIEW dev_env.dm_flight_data AS
SELECT
    id AS load_id,
    uploaded_at, -- Ensure this column exists in the source table
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
        uploaded_at, -- Changed from upload_dt to match your outer SELECT
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

-- 1555577
select date(uploaded_at) as upload_date, count(*) as flight_count
from dm_flight_data
group by date(uploaded_at)
order by date(uploaded_at)
;

SELECT DISTINCT 
    origin_country, 
    time_position::DATE AS fly_date, 
    EXTRACT(HOUR FROM time_position) AS fly_hour, 
    COUNT(DISTINCT icao24) AS fly_count,
    ROUND(AVG(velocity), 2) AS hourly_avg_velocity,
    ROUND(AVG(baro_altitude), 2) AS hourly_avg_altitude
FROM dm_flight_data

WHERE 1=1
    AND time_position >= date_trunc('hour', NOW()) - INTERVAL '1 hour'
    AND time_position < date_trunc('hour', NOW())
    AND on_ground = FALSE
GROUP BY origin_country, DATE(time_position), EXTRACT(HOUR FROM time_position)
ORDER BY fly_hour, origin_country

;
-- 2026-05-08 13:34:27.272131-04
select min(uploaded_at)
from dm_flight_data;


select max(uploaded_at) 
from dev_env.dm_flight_data;
;

select * from dev_env.dm_latest_flight_data;


DELETE FROM dev_env.stg_flight_data WHERE uploaded_at < NOW() - INTERVAL '3 days';

SELECT definition FROM pg_views WHERE schemaname = 'dev_env' AND viewname = 'dm_flight_data';   

