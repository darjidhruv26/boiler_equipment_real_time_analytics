-- # Overview
-- This document outlines the database schema for the industrial equipment real-time analytics project.
-- The schema is designed for ClickHouse and includes tables for equipment hierarchy, sensor metadata,
-- time-series data, and aggregated data.

-- ## Equipment Tree
-- The `equipment_tree` table stores the hierarchical structure of industrial equipment.

CREATE TABLE equipment_tree
(
    equipment_id String, -- Unique identifier for the equipment
    parent_id String, --  Identifier of the parent equipment in the hierarchy
    name String, --  Name of the equipment
    type String, --  Type of equipment (e.g., 'Boiler', 'Turbine')
    path String, --  Full path in the equipment hierarchy
    level UInt8, --  Level in the hierarchy (root is 0)
    plant String, --  Plant location
    area String, --  Area within the plant
    status String, --  Operational status of the equipment
    created_at DateTime DEFAULT now() --  Timestamp of the record creation
)
ENGINE = MergeTree
ORDER BY (path, equipment_id);


-- ## Tag Metadata
-- The `tag_metadata` table stores metadata about each sensor tag.

CREATE TABLE tag_metadata
(
    pi_point_id UInt32, --  Unique identifier for the PI point
    tag_name String, --  Name of the tag
    equipment_id String, --  Identifier of the associated equipment
    path String, --  Path in the equipment hierarchy
    unit String, --  Unit of measurement
    description String, --  Description of the tag
    data_type String, --  Data type of the tag value (e.g., 'Float64', 'Int32')
    scan_rate UInt32, --  Data collection frequency in milliseconds
    kafka_topic String, --  Kafka topic to which the tag data is published
    enabled UInt8 DEFAULT 1, --  Flag indicating whether the tag is enabled for data collection
    created_at DateTime DEFAULT now() --  Timestamp of the record creation
)
ENGINE = MergeTree
ORDER BY (pi_point_id);

-- ## Tag Timeseries Data
-- The `tag_timeseries` table stores the raw time-series data for each sensor tag.

CREATE TABLE tag_timeseries
(
    event_time DateTime64(3), --  Timestamp of the event with millisecond precision
    pi_point_id UInt32, --  Identifier of the associated PI point
    equipment_id String, --  Identifier of the associated equipment
    value Float64, --  Sensor value
    quality UInt8, --  Data quality code
    kafka_topic String, --  Kafka topic from which the data was ingested
    partition_id UInt16, --  Kafka partition ID
    ingest_time DateTime DEFAULT now() --  Timestamp of data ingestion
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (pi_point_id, event_time)
SETTINGS index_granularity = 8192;

-- ## Tag Aggregation (1-minute)
-- The `tag_1min` table stores the aggregated time-series data at 1-minute intervals.

CREATE TABLE tag_1min
(
    minute DateTime, --  Start time of the 1-minute interval
    pi_point_id UInt32, --  Identifier of the associated PI point
    avg_value Float64, --  Average value during the interval
    min_value Float64, --  Minimum value during the interval
    max_value Float64, --  Maximum value during the interval
    bad_quality UInt32 --  Count of data points with non-zero quality during the interval
)
ENGINE = MergeTree
PARTITION BY toDate(minute)
ORDER BY (pi_point_id, minute);

-- Note: A materialized view is used to populate this table from the `tag_timeseries` table.