CREATE OR REPLACE TABLE `{CONTROL_PROJECT_NAME}.{DATASET_NAME}`.{INFOSCHEMA_PROJECT_NAME_FOR_TABLE}_timeline_metrics_by_job
PARTITION BY
  DATE(period_start)
CLUSTER BY
   project_id AS
SELECT
  period_start,
  reservation_id,
  project_id,
  job_id,
  user_email,
  SUM(period_slot_ms) slot_ms,
  SUM(CASE WHEN state = 'PENDING' THEN 1 ELSE 0 END) AS pending_job_count,
  SUM(CASE WHEN state = 'RUNNING' THEN 1 ELSE 0 END) AS running_job_count,
  SUM(total_bytes_processed) total_bytes_processed,
  SUM(period_shuffle_ram_usage_ratio) period_shuffle_ram_usage_ratio,
  SUM(period_estimated_runnable_units) period_estimated_runnable_units
FROM `{INFOSCHEMA_PROJECT_NAME}`.`region-eu`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_FOLDER jbp
WHERE
  job_creation_time BETWEEN TIMESTAMP_SUB(current_timestamp, INTERVAL 365 day) AND current_timestamp
    AND jbp.statement_type != 'SCRIPT'
    AND job_type = 'QUERY'
GROUP BY
1,2,3,4,5
;
