-- ## Tag Aggregation Materialized View (1-minute)
-- The `tag_1min_mv` materialized view automatically aggregates data from the `tag_timeseries` table into the `tag_1min` table.

CREATE MATERIALIZED VIEW tag_1min_mv
TO tag_1min
AS
SELECT
    toStartOfMinute(event_time) AS minute,
    pi_point_id,
    avg(value) AS avg_value,
    min(value) AS min_value,
    max(value) AS max_value,
    countIf(quality != 0) AS bad_quality
FROM tag_timeseries
GROUP BY
    minute,
    pi_point_id;

-- Note: After creation, this view will automatically update the `tag_1min` table with aggregated data from the `tag_timeseries` table.
-- The name of this file has been created for the clarity of view.