WITH
baseQuery AS (
SELECT
reservation_id,
project_id,
COUNT(DISTINCT EXTRACT(date from creation_time)) AS day_count,
COUNT(DISTINCT job_id) AS query_count,
SUM(total_slot_ms) / 1000 AS total_slot_sec,
SUM(total_bytes_processed * 1e-12) total_processed_tb,
FROM
-- `{INFOSCHEMA_PROJECT_NAME}.region-eu.INFORMATION_SCHEMA.JOBS_BY_FOLDER`
`{DESTINATION_PROJECT_NAME}.{DESTINATION_DATASET_NAME}`.job_metrics_{INFOSCHEMA_PROJECT_NAME_FOR_TABLE}
WHERE
-- creation_time BETWEEN TIMESTAMP_SUB(current_timestamp, INTERVAL {DAYS_AGO} day) AND current_timestamp
creation_time BETWEEN "{START_DATE}" and "{END_DATE}"
{PROJECT_WHERE_CLAUSE}
-- AND job_type = 'QUERY'
-- AND statement_type <> 'SCRIPT'
-- AND total_slot_ms > 0

GROUP BY
1,2
)
SELECT
reservation_id,
project_id,
-- SUM(day_count) AS day_count,
SUM(query_count) AS total_queries,
ROUND(SUM(query_count)/DATE_DIFF("{END_DATE}", "{START_DATE}", day), 0) AS avg_queries_per_day,

ROUND(SUM(total_processed_tb / (DATE_DIFF("{END_DATE}", "{START_DATE}", day)/30.0)) * 6.25, 0) AS approx_30day_on_demand_cost,
ROUND(SUM((total_slot_sec / (DATE_DIFF("{END_DATE}", "{START_DATE}", day)/30.0)) / 3600.0) * 0.044, 0) AS approx_30day_standard_cost,
ROUND(SUM((total_slot_sec / (DATE_DIFF("{END_DATE}", "{START_DATE}", day)/30.0)) / 3600.0) * 0.066, 0) AS approx_30day_enterprise_cost,
ROUND(SUM((total_slot_sec / (DATE_DIFF("{END_DATE}", "{START_DATE}", day)/30.0)) / 3600.0) * 0.0396, 0) AS approx_30day_enterprise_cost_3yr,

FROM
baseQuery
GROUP BY
1,2
-- HAVING avg_queries_per_day > {QUERIES_PER_DAY}
ORDER BY
4 desc

