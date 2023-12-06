CREATE OR REPLACE TABLE `{CONTROL_PROJECT_NAME}.{DATASET_NAME}`.{INFOSCHEMA_PROJECT_NAME_FOR_TABLE}_job_metrics
PARTITION BY
    DATE(creation_time)
CLUSTER BY
    user_email
AS
SELECT
    creation_time,
    reservation_id,
    project_id,
    user_email,
    job_id,
    error_result,
    statement_type,
    labels,
    total_slot_ms,
    total_slot_ms/1000.0 as total_slot_seconds,
    TIMESTAMP_DIFF(jbp.end_time,jbp.creation_time, SECOND) time_secs,
    TIMESTAMP_DIFF(jbp.end_time,jbp.start_time, SECOND) query_time_secs,
    total_bytes_processed,
    (
        SELECT
            SUM(stage.shuffle_output_bytes)
        FROM
        UNNEST(job_stages) stage
    ) bytes_shuffled,
    (
        SELECT
            SUM(stage.shuffle_output_bytes_spilled)
        FROM
        UNNEST(job_stages) stage
    ) bytes_spilled
FROM `{INFOSCHEMA_PROJECT_NAME}`.`region-eu`.INFORMATION_SCHEMA.JOBS_BY_FOLDER jbp
WHERE
    creation_time BETWEEN TIMESTAMP_SUB(current_timestamp, INTERVAL 365 day) AND current_timestamp
    AND jbp.statement_type != 'SCRIPT'
    AND job_type = 'QUERY'
;
