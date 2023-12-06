WITH
  baseQuery AS (
  SELECT
    project_id,
    job_id,
    period_start AS period_ts_local,
    COUNT(DISTINCT job_id) AS query_count,
    SUM(slot_ms) / 1000 AS total_slot_sec,
    SUM(total_bytes_processed * 1e-12) AS total_processed_tb
  FROM
    `{CONTROL_PROJECT_NAME}.{DATASET_NAME}`.{INFOSCHEMA_PROJECT_NAME_FOR_TABLE}_timeline_metrics_by_job
  WHERE 1=1
    {PERIOD_START_WHERE_CLAUSE}
    {PROJECT_WHERE_CLAUSE}
  GROUP BY
    1,
    2,
    3)    
SELECT
  baseQuery.project_id,
  baseQuery.job_id,
  jobs_table.time_secs AS duration,
  jobs_table.query_time_secs AS query_duration,
  user_email,
  ROUND(SUM(total_slot_sec), 2) AS totalSlotUsage,
  MAX(total_slot_sec) AS peakSlotUsage,
  AVG(total_slot_sec) AS avgPeriodSlotUsage,
  ROUND(SUM(total_processed_tb), 2) AS totalProcessedTb,
FROM
  baseQuery
  INNER JOIN `{CONTROL_PROJECT_NAME}.{DATASET_NAME}`.{INFOSCHEMA_PROJECT_NAME_FOR_TABLE}_job_metrics as jobs_table ON jobs_table.job_id=baseQuery.job_id
GROUP BY
  1,
  2,
  3,
  4,
  5
ORDER BY
    query_duration DESC
