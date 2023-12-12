CREATE OR REPLACE TABLE `{DESTINATION_PROJECT_NAME}.{DESTINATION_DATASET_NAME}`.job_stages_{INFOSCHEMA_PROJECT_NAME_FOR_TABLE}
PARTITION BY
    DATE(creation_time)
CLUSTER BY
    job_id
AS
SELECT
    creation_time,
    project_id as project_id,
    job_id as job_id,
    job_stages
FROM
    `{INFOSCHEMA_PROJECT_NAME}`.`region-{REGION}`.INFORMATION_SCHEMA.JOBS{INFO_TABLE_SUFFIX} jbp
    ,UNNEST(job_stages) as job_stages
WHERE
    creation_time BETWEEN TIMESTAMP_SUB(current_timestamp, INTERVAL 365 day) AND current_timestamp
    AND jbp.statement_type != 'SCRIPT'
    AND job_type = 'QUERY'
;
