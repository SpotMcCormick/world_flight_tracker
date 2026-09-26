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