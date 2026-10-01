{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(BOOKING_ID, CREATED_AT)'
) }}

SELECT
    BOOKING_ID,
    ITEM_CODE,
    BOOKING_DATE,
    CUSTOMER_ID,
    SERVICE_ID,
    SENDING_POS_CODE,
    RECEIVING_POS_CODE,
    WEIGHT_GRAM,
    WEIGHT_TIER_ID,
    ROUTING_TYPE_ID,
    MAIN_FEE,
    SUR_FEE,
    DISCOUNT_AMOUNT,
    TOTAL_REVENUE,
    COST_AMOUNT,
    STATUS_ID,
    CREATED_AT,
    UPDATED_AT
FROM s3('http://minio:9000/logistics-lakehouse/silver/value_shipment_bookings/*.parquet', 'minioadmin', 'minioadminpassword')
