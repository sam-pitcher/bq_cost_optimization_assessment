SELECT
    project_id,
    job_id,
    period_start,
    SUM(slot_ms/1000.0) as slots,
    SUM(total_bytes_processed) as total_bytes_processed
FROM `{CONTROL_PROJECT_NAME}.{DATASET_NAME}`.{INFOSCHEMA_PROJECT_NAME_FOR_TABLE}_timeline_metrics_by_job
WHERE 1=1
    {PERIOD_START_WHERE_CLAUSE}
    {PROJECT_WHERE_CLAUSE}
GROUP BY 1,2,3
ORDER BY 3,2 ASC