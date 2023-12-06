with a as (
SELECT
project_id,
period_start,
SUM(slot_ms)/1000 AS total_slots
FROM
-- `{INFOSCHEMA_PROJECT_NAME}.region-eu.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_FOLDER`
-- `{CONTROL_PROJECT_NAME}.{DATASET_NAME}`.timeline_metrics
`{CONTROL_PROJECT_NAME}.{DATASET_NAME}`.{INFOSCHEMA_PROJECT_NAME_FOR_TABLE}_timeline_metrics
WHERE
-- job_type = "QUERY"
-- AND
period_start BETWEEN TIMESTAMP_SUB(current_timestamp, INTERVAL {DAYS_AGO} day) AND current_timestamp
{PROJECT_WHERE_CLAUSE}
GROUP BY
1,2
ORDER BY
3 desc),

b as (
select
project_id,
{slot_buckets_sql},
count(*) as total_seconds_in_bucket

from a
group by 1,2
order by 1)
select *,
round(safe_divide(total_seconds_in_bucket, sum(total_seconds_in_bucket) over(partition by project_id)), 6) as pct
from b
order by 1