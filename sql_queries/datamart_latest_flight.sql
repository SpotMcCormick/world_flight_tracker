
CREATE OR REPLACE VIEW dev_env.dm_latest_flight_data AS
WITH ranked_flights AS (
    SELECT 
      *,
        RANK() OVER (ORDER BY load_id DESC) as indx
    FROM dev_env.dm_flight_data
)
SELECT * 
FROM ranked_flights 
WHERE indx = 1; 