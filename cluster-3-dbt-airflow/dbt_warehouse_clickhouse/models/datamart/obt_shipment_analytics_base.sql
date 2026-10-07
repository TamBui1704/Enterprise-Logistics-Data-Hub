{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(ingested_at)',
    incremental_strategy='append',
    partition_by='toYYYYMM(BOOKING_DATE)',
    order_by='(BOOKING_DATE, BOOKING_ID)',
    unique_key='BOOKING_ID'
) }}

/*
  ClickHouse dbt Model: OLAP OBT Table (One Big Table)
  Reads the clean Gold OBT dataset from S3 Gold Layer (created by dbt-spark in Delta format)
  and materializes incrementally into ClickHouse for high-performance BI & OLAP analytics.
  Uses ReplacingMergeTree to automatically deduplicate upserted records on merge or with FINAL.
*/

SELECT
    BOOKING_ID,
    ITEM_CODE,
    BOOKING_DATE,
    WEIGHT_GRAM,
    MAIN_FEE,
    SUR_FEE,
    DISCOUNT_AMOUNT,
    TOTAL_REVENUE,
    COST_AMOUNT,
    PROFIT_AMOUNT,
    CREATED_AT,
    UPDATED_AT,
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CUSTOMER_TYPE,
    CUSTOMER_PROVINCE,
    CUSTOMER_REGION,
    SERVICE_ID,
    SERVICE_CODE,
    SERVICE_NAME,
    IS_INTERNATIONAL,
    SENDING_POS_CODE,
    SENDING_POS_NAME,
    SENDING_PROVINCE,
    SENDING_REGION,
    RECEIVING_POS_CODE,
    RECEIVING_POS_NAME,
    RECEIVING_PROVINCE,
    RECEIVING_REGION,
    STATUS_ID,
    STATUS_CODE,
    STATUS_NAME,
    STATUS_GROUP,
    ingested_at,
    now() AS olap_loaded_at
FROM deltaLake(
    minio_gold, 
    url='http://minio-lakehouse:9000/logistics-lakehouse/gold/datamart/obt_shipment_analytics/'
)

{% if is_incremental() %}
WHERE ingested_at >= (
    SELECT coalesce(max(ingested_at), toDateTime('1970-01-01 00:00:00')) 
    FROM {{ this }}
)
{% endif %}
