CREATE OR REPLACE TABLE `{CONTROL_PROJECT_NAME}.{DATASET_NAME}`.{INFOSCHEMA_PROJECT_NAME_FOR_TABLE}_job_stages
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
    `{INFOSCHEMA_PROJECT_NAME}`.`region-eu`.INFORMATION_SCHEMA.JOBS_BY_FOLDER jbp
    ,UNNEST(job_stages) as job_stages
WHERE
    creation_time BETWEEN TIMESTAMP_SUB(current_timestamp, INTERVAL 365 day) AND current_timestamp
    AND jbp.statement_type != 'SCRIPT'
    AND job_type = 'QUERY'
;
