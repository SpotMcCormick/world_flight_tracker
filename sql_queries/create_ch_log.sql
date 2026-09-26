CREATE TABLE IF NOT EXISTS dev_world_flight_tracker.log
(
    logged_at DateTime,
    level     String,
    message   String,
    logger    String,
    func_name String,
    line_no   UInt32,
    pathname  String
)
ENGINE = MergeTree
ORDER BY logged_at;

ALTER TABLE dev_world_flight_tracker.log ADD COLUMN run_id String;


CREATE OR REPLACE VIEW mart_latest_flight as (
    with watermark as (
        select max(uploaded_at) max_date from mart_flight_data
    )
    SELECT *
    FROM mart_flight_data d 
    JOIN watermark ON max_date = uploaded_at
)


select * from dev_world_flight_tracker.mart_latest_flight