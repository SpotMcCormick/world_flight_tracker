-- select *
-- from dev_env.dm_latest_flight_data
-- order by uploaded_at desc
-- limit 1
-- ;

-- select *
-- from dev_env.stg_flight_data;

-- select max(uploaded_at)
-- from dev_env.dm_flight_data
-- limit 10; 

-- select *
-- from test_dev.hmda.stg_hmda;

-- --    SELECT 
-- --         id, 
-- --         data,
-- --         uploaded_at, -- Changed from upload_dt to match your outer SELECT
-- --         (flight_row->>0)  
-- --     FROM dev_env.stg_flight_data,
-- --     LATERAL jsonb_array_elements(data->'states') AS flight_row
-- --     where id = 74460
-- --     limit 10;



-- CREATE TABLE IF NOT EXISTS s3_ingestion_log (
--     id            bigserial PRIMARY KEY,
--     target_table  text NOT NULL,
--     file_key      text NOT NULL,
--     etag          text NOT NULL,
--     file_size     bigint,
--     status        text NOT NULL,          -- 'SUCCESS' | 'FAILED'
--     error_message text,
--     loaded_at     timestamptz NOT NULL DEFAULT now(),
--     UNIQUE (target_table, file_key, etag)
-- );

-- CREATE OR REPLACE FUNCTION ingest_s3(
--     p_target_table text,     -- e.g. 'public.orders'
--     p_columns      text,     -- comma-separated, e.g. 'order_id,customer,amount'
--     p_s3_path      text,     -- e.g. 's3://my-bucket/orders/'
--     p_file_type    text DEFAULT 'all'   -- 'all' | 'csv' | 'json' | 'parquet'
-- )
-- RETURNS TABLE(files_processed integer, files_skipped integer, files_failed integer)
-- LANGUAGE plpython3u
-- AS $function$
-- import boto3
-- import os
-- import csv
-- import json
-- import io
-- import re
-- import pyarrow.parquet as pq

-- s3 = boto3.client('s3')

-- if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$', p_target_table):
--     raise ValueError("Invalid target table name: " + p_target_table)

-- target_cols = [c.strip() for c in p_columns.split(',') if c.strip()]
-- if not target_cols:
--     raise ValueError("p_columns must list at least one column")
-- for c in target_cols:
--     if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', c):
--         raise ValueError("Invalid column name: " + c)

-- clean_path = re.sub(r'^s3://', '', p_s3_path.strip())
-- parts = clean_path.split('/', 1)
-- bucket_name = parts[0]
-- prefix = parts[1] if len(parts) > 1 else ''

-- requested_type = p_file_type.lower().strip().lstrip('.')

-- col_list = ", ".join('"{}"'.format(c) for c in target_cols)
-- record_cols = ", ".join('r."{}"'.format(c) for c in target_cols)

-- # jsonb_populate_record casts each field to the TARGET TABLE's real
-- # column type automatically -- avoids hand-rolled/incorrect type casting
-- plan_insert = plpy.prepare(
--     'INSERT INTO {tbl} ({cols}) '
--     'SELECT {rcols} FROM jsonb_populate_record(NULL::{tbl}, $1::jsonb) r'
--     .format(tbl=p_target_table, cols=col_list, rcols=record_cols),
--     ["jsonb"]
-- )

-- plan_log = plpy.prepare(
--     "INSERT INTO s3_ingestion_log "
--     "(target_table, file_key, etag, file_size, status, error_message) "
--     "VALUES ($1, $2, $3, $4, $5, $6) "
--     "ON CONFLICT (target_table, file_key, etag) DO UPDATE SET "
--     "  status = EXCLUDED.status, file_size = EXCLUDED.file_size, "
--     "  error_message = EXCLUDED.error_message, loaded_at = now()",
--     ["text", "text", "text", "bigint", "text", "text"]
-- )

-- plan_check = plpy.prepare(
--     "SELECT 1 FROM s3_ingestion_log "
--     "WHERE target_table = $1 AND file_key = $2 AND etag = $3 AND status = 'SUCCESS' LIMIT 1",
--     ["text", "text", "text"]
-- )


-- def rows_from(ext, raw_bytes):
--     if ext == 'json':
--         payload = json.loads(raw_bytes.decode('utf-8'))
--         return payload if isinstance(payload, list) else [payload]
--     if ext == 'ndjson':
--         return [json.loads(l) for l in raw_bytes.decode('utf-8').splitlines() if l.strip()]
--     if ext == 'csv':
--         return list(csv.DictReader(io.StringIO(raw_bytes.decode('utf-8'))))
--     if ext == 'parquet':
--         return pq.read_table(io.BytesIO(raw_bytes)).to_pylist()
--     raise ValueError("Unsupported extension: " + ext)


-- paginator = s3.get_paginator('list_objects_v2')
-- pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

-- processed = 0
-- skipped = 0
-- failed = 0

-- for page in pages:
--     for obj in page.get('Contents', []):
--         file_key = obj['Key']
--         if file_key.endswith('/'):
--             continue

--         ext = os.path.splitext(file_key)[1].lower().lstrip('.')
--         if ext not in ('json', 'ndjson', 'csv', 'parquet'):
--             continue
--         if requested_type != 'all':
--             if requested_type == 'json' and ext not in ('json', 'ndjson'):
--                 continue
--             elif requested_type != 'json' and ext != requested_type:
--                 continue

--         etag = obj['ETag'].strip('"')

--         if len(plpy.execute(plan_check, [p_target_table, file_key, etag])) > 0:
--             skipped += 1
--             continue

--         try:
--             with plpy.subtransaction():
--                 response = s3.get_object(Bucket=bucket_name, Key=file_key)
--                 raw_bytes = response['Body'].read()
--                 rows = rows_from(ext, raw_bytes)

--                 for row in rows:
--                     filtered = {c: row.get(c) for c in target_cols}
--                     plpy.execute(plan_insert, [json.dumps(filtered, default=str)])

--                 plpy.execute(plan_log, [p_target_table, file_key, etag, obj['Size'], 'SUCCESS', None])
--                 processed += 1
--         except Exception as e:
--             plpy.warning("failed to ingest {}: {}".format(file_key, str(e)))
--             try:
--                 with plpy.subtransaction():
--                     plpy.execute(plan_log, [p_target_table, file_key, etag, obj.get('Size'), 'FAILED', str(e)])
--             except Exception:
--                 pass
--             failed += 1

-- return [{'files_processed': processed, 'files_skipped': skipped, 'files_failed': failed}]
-- $function$;


CREATE SCHEMA IF NOT EXISTS external_storage;

CREATE TABLE external_storage.test_stage (
    load_id     bigserial PRIMARY KEY,
    ingest_time timestamptz NOT NULL DEFAULT now(),
    data        jsonb NOT NULL
);

SELECT * FROM ingest_s3(
    'external_storage.test_stage',
    'data',
    's3://world-flight-tracker/flights/2026/',
    'json'
);


CREATE DATABASE dev_world_flight_tracker;CREATE TABLE test (
    id UInt64 DEFAULT generateSerialID('my_counter'),
    data String
) ORDER BY id;   

CREATE TABLE dev_world_flight_tracker.stg_flight_data
(
    load_id UInt64 DEFAULT generateSerialID('dev_stage_load_id'),
    ingest_time DateTime64(3) DEFAULT now64(3),
    data JSON
)
ENGINE = MergeTree
ORDER BY load_id;
;
select * from dev_world_flight_tracker.stg_flight_data;


show create table default.mv_dm_flight_data;

CREATE OR REPLACE MATERIALIZED VIEW dev_world_flight_tracker.mv_mart_flight_data
TO dev_world_flight_tracker.mart_flight_data
(
    `load_id` UInt64,
    `uploaded_at` DateTime64(3),
    `time_position` Nullable(DateTime),
    `last_contact` Nullable(DateTime),
    `icao24` Nullable(String),
    `callsign` Nullable(String),
    `origin_country` Nullable(String),
    `longitude` Nullable(Float64),
    `latitude` Nullable(Float64),
    `baro_altitude` Nullable(Float64),
    `on_ground` Nullable(Bool),
    `velocity` Nullable(Float64),
    `true_track` Nullable(Float64),
    `vertical_rate` Nullable(Float64),
    `geo_altitude` Nullable(Float64),
    `squawk` Nullable(String),
    `spi` Nullable(Bool),
    `position_source` Nullable(Int32)
)
AS SELECT
    load_id,
    ingest_time AS uploaded_at,
    fromUnixTimestamp(toUInt32OrZero(flight_row[4])) AS time_position,
    fromUnixTimestamp(toUInt32OrZero(flight_row[5])) AS last_contact,
    flight_row[1] AS icao24,
    flight_row[2] AS callsign,
    flight_row[3] AS origin_country,
    toFloat64OrNull(flight_row[6]) AS longitude,
    toFloat64OrNull(flight_row[7]) AS latitude,
    toFloat64OrNull(flight_row[8]) AS baro_altitude,
    toBool(toUInt8OrZero(flight_row[9])) AS on_ground,
    toFloat64OrNull(flight_row[10]) AS velocity,
    toFloat64OrNull(flight_row[11]) AS true_track,
    toFloat64OrNull(flight_row[12]) AS vertical_rate,
    toFloat64OrNull(flight_row[14]) AS geo_altitude,
    flight_row[15] AS squawk,
    toBool(toUInt8OrZero(flight_row[16])) AS spi,
    toInt32OrNull(flight_row[17]) AS position_source
FROM dev_world_flight_tracker.stg_flight_data
ARRAY JOIN JSONExtract(
    data,
    'states',
    'Array(Array(String))'
) AS flight_row;



DESCRIBE TABLE dev_world_flight_tracker.stg_flight_data;


show create table default.mv_dm_flight_data;

SELECT
    data
FROM dev_world_flight_tracker.stg_flight_data
LIMIT 1;


SELECT
    load_id,
    ingest_time,
    data.states
FROM dev_world_flight_tracker.stg_flight_data
LIMIT 1;

SELECT
    load_id,
    ingest_time,
    JSONExtractArrayRaw(data, 'states')[1] AS flight_row
FROM dev_world_flight_tracker.stg_flight_data
LIMIT 1;

show create table default.dev_env_stg_flight_data;

CREATE OR REPLACE MATERIALIZED VIEW dev_world_flight_tracker.mv_mart_flight_data
TO dev_world_flight_tracker.mart_flight_data
(
    `id` Int64,
    `uploaded_at` DateTime64(6),
    `time_position` Nullable(DateTime64(6)),
    `last_contact` Nullable(DateTime64(6)),
    `icao24` String,
    `callsign` String,
    `origin_country` String,
    `longitude` Nullable(Float64),
    `latitude` Nullable(Float64),
    `baro_altitude` Nullable(Float64),
    `on_ground` Nullable(Bool),
    `velocity` Nullable(Float64),
    `true_track` Nullable(Float64),
    `vertical_rate` Nullable(Float64),
    `geo_altitude` Nullable(Float64),
    `squawk` Nullable(String),
    `spi` Nullable(Bool),
    `position_source` Nullable(Int32),
    `_peerdb_is_deleted` UInt8,
    `_peerdb_version` UInt64
)
AS SELECT
    toInt64(load_id) AS id,
    toDateTime64(ingest_time, 6) AS uploaded_at,
    fromUnixTimestamp(toUInt32OrZero(flight_row[4])) AS time_position,
    fromUnixTimestamp(toUInt32OrZero(flight_row[5])) AS last_contact,
    flight_row[1] AS icao24,
    flight_row[2] AS callsign,
    flight_row[3] AS origin_country,
    toFloat64OrNull(flight_row[6]) AS longitude,
    toFloat64OrNull(flight_row[7]) AS latitude,
    toFloat64OrNull(flight_row[8]) AS baro_altitude,
    toBool(toUInt8OrZero(flight_row[9])) AS on_ground,
    toFloat64OrNull(flight_row[10]) AS velocity,
    toFloat64OrNull(flight_row[11]) AS true_track,
    toFloat64OrNull(flight_row[12]) AS vertical_rate,
    toFloat64OrNull(flight_row[14]) AS geo_altitude,
    flight_row[15] AS squawk,
    toBool(toUInt8OrZero(flight_row[16])) AS spi,
    toInt32OrNull(flight_row[17]) AS position_source,
    0 AS _peerdb_is_deleted,
    0 AS _peerdb_version
FROM dev_world_flight_tracker.stg_flight_data
ARRAY JOIN JSONExtract(
    toString(data),
    'states',
    'Array(Array(Nullable(String)))'
) AS flight_row;


select * from mart_flight_data
;
SELECT count()
FROM dev_world_flight_tracker.stg_flight_data;


INSERT INTO dev_world_flight_tracker.mart_flight_data
(
    id,
    uploaded_at,
    time_position,
    last_contact,
    icao24,
    callsign,
    origin_country,
    longitude,
    latitude,
    baro_altitude,
    on_ground,
    velocity,
    true_track,
    vertical_rate,
    geo_altitude,
    squawk,
    spi,
    position_source,
    _peerdb_is_deleted,
    _peerdb_version
)
SELECT
    toInt64(load_id) AS id,
    toDateTime64(ingest_time, 6) AS uploaded_at,
    fromUnixTimestamp(toUInt32OrZero(flight_row[4])) AS time_position,
    fromUnixTimestamp(toUInt32OrZero(flight_row[5])) AS last_contact,
    flight_row[1] AS icao24,
    flight_row[2] AS callsign,
    flight_row[3] AS origin_country,
    toFloat64OrNull(flight_row[6]) AS longitude,
    toFloat64OrNull(flight_row[7]) AS latitude,
    toFloat64OrNull(flight_row[8]) AS baro_altitude,
    toBool(toUInt8OrZero(flight_row[9])) AS on_ground,
    toFloat64OrNull(flight_row[10]) AS velocity,
    toFloat64OrNull(flight_row[11]) AS true_track,
    toFloat64OrNull(flight_row[12]) AS vertical_rate,
    toFloat64OrNull(flight_row[14]) AS geo_altitude,
    flight_row[15] AS squawk,
    toBool(toUInt8OrZero(flight_row[16])) AS spi,
    toInt32OrNull(flight_row[17]) AS position_source,
    0 AS _peerdb_is_deleted,
    0 AS _peerdb_version
FROM dev_world_flight_tracker.stg_flight_data
ARRAY JOIN JSONExtract(
    toString(data),
    'states',
    'Array(Array(Nullable(String)))'
) AS flight_row;

ALTER TABLE dev_world_flight_tracker.mart_flight_data
RENAME COLUMN id TO load_id;

SELECT
    table,
    name,
    type,
    default_kind,
    default_expression
FROM system.columns
WHERE database = 'dev_world_flight_tracker'
ORDER BY table, position;

SHOW CREATE TABLE dev_world_flight_tracker.mart_flight_data;

DROP VIEW dev_world_flight_tracker.mv_mart_flight_data;


DROP TABLE dev_world_flight_tracker.mart_flight_data;

CREATE TABLE dev_world_flight_tracker.mart_flight_data
(
    `load_id` UInt64,
    `uploaded_at` DateTime64(6),
    `time_position` Nullable(DateTime64(6)),
    `last_contact` Nullable(DateTime64(6)),
    `icao24` String,
    `callsign` String,
    `origin_country` String,
    `longitude` Nullable(Float64),
    `latitude` Nullable(Float64),
    `baro_altitude` Nullable(Float64),
    `on_ground` Nullable(Bool),
    `velocity` Nullable(Float64),
    `true_track` Nullable(Float64),
    `vertical_rate` Nullable(Float64),
    `geo_altitude` Nullable(Float64),
    `squawk` Nullable(String),
    `spi` Nullable(Bool),
    `position_source` Nullable(Int32)
)
ENGINE = MergeTree
ORDER BY (load_id)
SETTINGS index_granularity = 8192;

DROP VIEW IF EXISTS dev_world_flight_tracker.mv_mart_flight_data;


CREATE OR REPLACE MATERIALIZED VIEW dev_world_flight_tracker.mv_mart_flight_data
TO dev_world_flight_tracker.mart_flight_data
POPULATE
AS
SELECT
    toUInt64(load_id) AS load_id,
    toDateTime64(ingest_time, 6) AS uploaded_at,
    toDateTime64(toUInt32OrZero(flight_row[4]), 6) AS time_position,
    toDateTime64(toUInt32OrZero(flight_row[5]), 6) AS last_contact,
    flight_row[1] AS icao24,
    flight_row[2] AS callsign,
    flight_row[3] AS origin_country,
    toFloat64OrNull(flight_row[6]) AS longitude,
    toFloat64OrNull(flight_row[7]) AS latitude,
    toFloat64OrNull(flight_row[8]) AS baro_altitude,
    toBool(toUInt8OrZero(flight_row[9])) AS on_ground,
    toFloat64OrNull(flight_row[10]) AS velocity,
    toFloat64OrNull(flight_row[11]) AS true_track,
    toFloat64OrNull(flight_row[12]) AS vertical_rate,
    toFloat64OrNull(flight_row[14]) AS geo_altitude,
    flight_row[15] AS squawk,
    toBool(toUInt8OrZero(flight_row[16])) AS spi,
    toInt32OrNull(flight_row[17]) AS position_source
FROM dev_world_flight_tracker.stg_flight_data
ARRAY JOIN JSONExtract(
    toString(data),
    'states',
    'Array(Array(Nullable(String)))'
) AS flight_row;


select * from dev_world_flight_tracker.log
order by logged_at desc