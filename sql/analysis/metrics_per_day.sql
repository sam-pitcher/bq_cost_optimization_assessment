
SELECT
    DATE(creation_time) as day,
    -- reservation_id,
    project_id,
    -- user_email,
    -- job_id,
    -- error_result,
    -- statement_type,
    -- labels,
    SUM(total_slot_ms) as slot_ms,
    SUM(query_time_secs) as total_query_time,
    SUM(total_bytes_processed) as total_bytes_processed
FROM `{DESTINATION_PROJECT_NAME}.{DESTINATION_DATASET_NAME}`.job_metrics_{INFOSCHEMA_PROJECT_NAME_FOR_TABLE}
WHERE
    -- creation_time BETWEEN TIMESTAMP_SUB(current_timestamp, INTERVAL 365 day) AND current_timestamp
    creation_time BETWEEN "{START_DATE}" and "{END_DATE}"
    {PROJECT_WHERE_CLAUSE}
GROUP BY 1,2
ORDER BY 1,2