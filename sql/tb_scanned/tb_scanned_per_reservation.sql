WITH
baseQuery AS (
SELECT
reservation_id,
COUNT(DISTINCT job_id) AS query_count,
SUM(total_slot_ms) / 1000 AS total_slot_sec,
SUM(total_bytes_processed * 1e-12) total_processed_tb,
FROM
-- `{PROJECT_NAME}.region-eu.INFORMATION_SCHEMA.JOBS_BY_FOLDER`
`{DESTINATION_PROJECT_NAME}.{DESTINATION_DATASET_NAME}`.job_metrics_{INFOSCHEMA_PROJECT_NAME_FOR_TABLE}
WHERE
-- creation_time BETWEEN TIMESTAMP_SUB(current_timestamp, INTERVAL {DAYS_AGO} day) AND current_timestamp
creation_time BETWEEN "{START_DATE}" and "{END_DATE}"
{PROJECT_WHERE_CLAUSE}
-- AND job_type = 'QUERY'
-- AND statement_type <> 'SCRIPT'
-- AND total_slot_ms > 0
GROUP BY
1
)
SELECT
reservation_id,

-- SUM(query_count) AS total_queries,
-- ROUND(SUM(query_count)/({DAYS_AGO}*24), 0) AS avg_queries_per_hour,
ROUND(SUM(query_count)/{DAYS_AGO}, 0) AS avg_queries_per_day,

ROUND(SUM(total_processed_tb), 2) AS total_tb_processed,
-- ROUND(SUM(total_processed_tb)/({DAYS_AGO}*24), 2) AS total_tb_processed_per_hour,
ROUND(SUM(total_processed_tb)/{DAYS_AGO}, 2) AS total_tb_processed_per_day,
ROUND(SUM(total_processed_tb)/{DAYS_AGO}*30.0 * 6.25, 0) AS approx_monthly_on_demand_cost,

FROM
baseQuery
GROUP BY
1
ORDER BY
2 DESC