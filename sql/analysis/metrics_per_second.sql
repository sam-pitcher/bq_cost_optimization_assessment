SELECT
    project_id,
    job_id,
    period_start,
    SUM(slot_ms/1000.0) as slots,
    SUM(total_bytes_processed) as total_bytes_processed
FROM `{DESTINATION_PROJECT_NAME}.{DESTINATION_DATASET_NAME}`.timeline_metrics_by_job_{INFOSCHEMA_PROJECT_NAME_FOR_TABLE}
WHERE
    period_start BETWEEN "{START_DATE}" and "{END_DATE}"
    -- {PERIOD_START_WHERE_CLAUSE}
    {PROJECT_WHERE_CLAUSE}
GROUP BY 1,2,3
ORDER BY 3,2 ASC