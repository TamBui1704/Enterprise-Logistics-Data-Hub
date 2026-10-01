{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(STATUS_CODE, SENDING_PROVINCE, CREATED_AT)'
) }}

/*
  One Big Table (OBT) - Flat Analytical Data Mart
  Joins fact_shipment_bookings with dim_customers, dim_services,
  dim_pos_locations (sending & receiving) and dim_delivery_statuses
  Optimized for high-speed OLAP BI dashboards and Cube.dev semantic layer.
*/

SELECT
    -- Fact Core Metrics & Key Identifiers
    f.BOOKING_ID,
    f.ITEM_CODE,
    f.BOOKING_DATE,
    f.WEIGHT_GRAM,
    f.MAIN_FEE,
    f.SUR_FEE,
    f.DISCOUNT_AMOUNT,
    f.TOTAL_REVENUE,
    f.COST_AMOUNT,
    (f.TOTAL_REVENUE - f.COST_AMOUNT) AS PROFIT_AMOUNT,
    f.CREATED_AT,
    f.UPDATED_AT,

    -- Customer Dimension Details
    c.CUSTOMER_ID,
    c.CUSTOMER_NAME,
    c.CUSTOMER_TYPE,
    c.PROVINCE AS CUSTOMER_PROVINCE,
    c.REGION AS CUSTOMER_REGION,

    -- Service Dimension Details
    s.SERVICE_ID,
    s.SERVICE_CODE,
    s.SERVICE_NAME,
    s.IS_INTERNATIONAL,

    -- Sending POS Location Details
    sp.POS_CODE AS SENDING_POS_CODE,
    sp.POS_NAME AS SENDING_POS_NAME,
    sp.PROVINCE_NAME AS SENDING_PROVINCE,
    sp.REGION AS SENDING_REGION,

    -- Receiving POS Location Details
    rp.POS_CODE AS RECEIVING_POS_CODE,
    rp.POS_NAME AS RECEIVING_POS_NAME,
    rp.PROVINCE_NAME AS RECEIVING_PROVINCE,
    rp.REGION AS RECEIVING_REGION,

    -- Delivery Status Details
    st.STATUS_ID,
    st.STATUS_CODE,
    st.STATUS_NAME,
    st.STATUS_GROUP,

    now() AS obt_ingested_at

FROM {{ ref('fact_shipment_bookings') }} f
LEFT JOIN {{ ref('dim_customers') }} c ON f.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN {{ ref('dim_services') }} s ON f.SERVICE_ID = s.SERVICE_ID
LEFT JOIN {{ ref('dim_pos_locations') }} sp ON f.SENDING_POS_CODE = sp.POS_CODE
LEFT JOIN {{ ref('dim_pos_locations') }} rp ON f.RECEIVING_POS_CODE = rp.POS_CODE
LEFT JOIN {{ ref('dim_delivery_statuses') }} st ON f.STATUS_ID = st.STATUS_ID
