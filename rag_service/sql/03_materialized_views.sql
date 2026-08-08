-- Materialized view to create hierarchical context automatically
USE industrial_analytics;
CREATE MATERIALIZED VIEW IF NOT EXISTS equipment_context_mv
TO rag_context
AS
SELECT
    equipment_id,
    'hierarchy' AS context_type,
    concat(
        'Equipment: ', name, '\n',
        'Type: ', type, '\n',
        'Level: ', toString(level), '\n',
        'Path: ', path, '\n',
        'Location: ', plant, '-', area, '\n',
        'Status: ', status
    ) AS content,
    arrayMap(x -> CAST(0.0 AS Float32), range(384)) AS embedding, -- Will be populated by external embedding service
    tuple(created_at, created_at) AS time_range,
    ['equipment', type, status] AS tags,
    toJSONString(map(
        'parent_id', parent_id,
        'level', toString(level),
        'plant', plant,
        'area', area
    )) AS metadata,
    created_at
FROM equipment_tree;

-- Create context from recent time-series data
CREATE MATERIALIZED VIEW IF NOT EXISTS timeseries_context_mv
TO rag_context
AS
SELECT
    t.equipment_id,
    'time_series' AS context_type,
    concat(
        'Tag: ', m.tag_name, '\n',
        'Description: ', m.description, '\n',
        'Unit: ', m.unit, '\n',
        'Recent Values: ',
        arrayStringConcat(
            arrayMap(x -> toString(x), groupArray(10)(t.value)), ', '
        )
    ) AS content,
    arrayMap(x -> CAST(0.0 AS Float32), range(384)) AS embedding,
    tuple(now() - INTERVAL 1 HOUR, now()) AS time_range,
    [m.tag_name, m.data_type] AS tags,
    toJSONString(map(
        'pi_point_id', toString(t.pi_point_id),
        'unit', m.unit,
        'scan_rate', toString(m.scan_rate)
    )) AS metadata,
    now() AS created_at
FROM tag_timeseries t
JOIN tag_metadata m ON t.pi_point_id = m.pi_point_id
WHERE t.event_time > now() - INTERVAL 1 HOUR
GROUP BY t.equipment_id, m.tag_name, m.description, m.unit, m.data_type, t.pi_point_id, m.scan_rate;

-- Creating a materialized view for hourly carbon impact
CREATE MATERIALIZED VIEW IF NOT EXISTS carbon_hourly_mv
ENGINE = MergeTree
ORDER BY (event_hour, equipment_id)
AS SELECT
    toStartOfHour(event_time) AS event_hour,
    equipment_id,
    -- Assuming a tag 'energy_kw' and a tag 'production_units' exists
    avg(value) AS avg_power_kw,  -- This should ideally filter specifically by energy tags
    sum(value) AS total_production, -- This should ideally filter specifically by production tags
    (avg(value) * 1.0 / sum(value)) AS carbon_per_unit -- Placeholder, requires real emission factor
FROM tag_timeseries
WHERE pi_point_id IN (SELECT pi_point_id FROM tag_metadata WHERE tag_name LIKE '%_POWER' OR tag_name LIKE '%_FLOW') 
GROUP BY event_hour, equipment_id;
