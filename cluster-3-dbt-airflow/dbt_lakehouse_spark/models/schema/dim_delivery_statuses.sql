SELECT
    STATUS_ID,
    STATUS_CODE,
    STATUS_NAME,
    STATUS_GROUP,
    ingested_at
FROM delta.`s3a://logistics-lakehouse/silver/value_dim_delivery_statuses`

