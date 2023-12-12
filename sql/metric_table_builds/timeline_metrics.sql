CREATE OR REPLACE TABLE `{DESTINATION_PROJECT_NAME}.{DESTINATION_DATASET_NAME}`.timeline_metrics_{INFOSCHEMA_PROJECT_NAME_FOR_TABLE}
PARTITION BY
  DATE(period_start)
CLUSTER BY
   project_id AS
SELECT
  period_start,
  reservation_id,
  project_id,
  user_email,
  SUM(period_slot_ms) slot_ms,
  SUM(CASE WHEN state = 'PENDING' THEN 1 ELSE 0 END) AS pending_job_count,
  SUM(CASE WHEN state = 'RUNNING' THEN 1 ELSE 0 END) AS running_job_count,
  SUM(total_bytes_processed) total_bytes_processed,
  SUM(period_shuffle_ram_usage_ratio) period_shuffle_ram_usage_ratio,
  SUM(period_estimated_runnable_units) period_estimated_runnable_units
FROM `{INFOSCHEMA_PROJECT_NAME}`.`region-{REGION}`.INFORMATION_SCHEMA.JOBS_TIMELINE{INFO_TABLE_SUFFIX} jbp
WHERE
  job_creation_time BETWEEN TIMESTAMP_SUB(current_timestamp, INTERVAL 365 day) AND current_timestamp
    AND jbp.statement_type != 'SCRIPT'
    AND job_type = 'QUERY'
GROUP BY
1,2,3,4
;
