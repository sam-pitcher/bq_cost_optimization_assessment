WITH baseQuery AS (
SELECT
project_id,
period_start,
SUM(slot_ms)/1000 AS total_slots_this_second
FROM
-- `{INFOSCHEMA_PROJECT_NAME}.region-eu.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_FOLDER`
`{DESTINATION_PROJECT_NAME}.{DESTINATION_DATASET_NAME}`.timeline_metrics_{INFOSCHEMA_PROJECT_NAME_FOR_TABLE}
WHERE
-- job_type = "QUERY" AND
-- period_start BETWEEN TIMESTAMP_SUB(current_timestamp, INTERVAL {DAYS_AGO} day) AND current_timestamp
period_start BETWEEN "{START_DATE}" and "{END_DATE}"
{PROJECT_WHERE_CLAUSE}

GROUP BY
project_id,
period_start),

percentiles AS (
SELECT
project_id,
ROUND(PERCENTILE_CONT(total_slots_this_second, 0.50) OVER(PARTITION BY project_id), 0) AS percentile50,
ROUND(PERCENTILE_CONT(total_slots_this_second, 0.70) OVER(PARTITION BY project_id), 0) AS percentile70,
ROUND(PERCENTILE_CONT(total_slots_this_second, 0.90) OVER(PARTITION BY project_id), 0) AS percentile90,
ROUND(PERCENTILE_CONT(total_slots_this_second, 0.93) OVER(PARTITION BY project_id), 0) AS percentile93,
ROUND(PERCENTILE_CONT(total_slots_this_second, 0.94) OVER(PARTITION BY project_id), 0) AS percentile94,
ROUND(PERCENTILE_CONT(total_slots_this_second, 0.95) OVER(PARTITION BY project_id), 0) AS percentile95,
ROUND(PERCENTILE_CONT(total_slots_this_second, 0.96) OVER(PARTITION BY project_id), 0) AS percentile96,
ROUND(PERCENTILE_CONT(total_slots_this_second, 0.98) OVER(PARTITION BY project_id), 0) AS percentile98,
ROUND(PERCENTILE_CONT(total_slots_this_second, 0.99) OVER(PARTITION BY project_id), 0) AS percentile99
FROM
baseQuery
),

slot_info AS (
SELECT
project_id,
ROUND(max(total_slots_this_second), 1) as peak_slots,
ROUND(avg(total_slots_this_second), 1) as average_slots,
ROUND(sum(total_slots_this_second), 1) as total_slots_this_second,
ROUND(sum(total_slots_this_second) / 3600 * 0.044 ,2) as standard_capacity_cost
FROM baseQuery
GROUP BY project_id
)

SELECT
percentiles.project_id,
percentile50,
percentile70,
percentile90,
percentile93,
percentile94,
percentile95,
percentile96,
percentile98,
percentile99,
slot_info.peak_slots,
slot_info.average_slots,
slot_info.total_slots_this_second,
slot_info.standard_capacity_cost
FROM percentiles
LEFT JOIN slot_info on percentiles.project_id = slot_info.project_id
GROUP BY
1,2,3,4,5,6,7,8,9,10,11,12,13,14
ORDER BY 1