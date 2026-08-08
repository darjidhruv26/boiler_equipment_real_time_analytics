USE industrial_analytics;

CREATE MATERIALIZED VIEW IF NOT EXISTS carbon_context_mv
TO rag_context
AS
SELECT
    t.equipment_id,
    'carbon_metrics' AS context_type,
    concat(
        'Carbon Emission Report for ', t.equipment_id, '\n',
        'Average Power Consumption: ', toString(avg(t.value)), ' kW\n',
        'Estimated Carbon Intensity: ', toString(avg(t.value) * 0.428), ' kg CO2 (using standard grid emission factor)\n',
        'Status: Active Monitoring'
    ) AS content,
    arrayMap(x -> CAST(0.0 AS Float32), range(384)) AS embedding,
    tuple(now() - INTERVAL 1 HOUR, now()) AS time_range,
    ['carbon emission', 'co2', 'power', 'sustainability', 'footprint'] AS tags,
    '{}' AS metadata,
    now() AS created_at
FROM tag_timeseries t
JOIN tag_metadata m ON t.pi_point_id = m.pi_point_id
WHERE m.tag_name LIKE '%_CURRENT' OR m.tag_name LIKE '%_POWER'
GROUP BY t.equipment_id;
