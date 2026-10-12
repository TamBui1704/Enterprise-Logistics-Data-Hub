SELECT
    SERVICE_ID,
    SERVICE_CODE,
    SERVICE_NAME,
    IS_INTERNATIONAL,
    CREATED_AT,
    ingested_at
FROM delta.`s3a://logistics-lakehouse/silver/value_dim_services`

