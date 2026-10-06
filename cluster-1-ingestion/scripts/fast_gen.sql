SET SERVEROUTPUT ON;
DECLARE
  v_batch_size CONSTANT NUMBER := 50000;
  v_total_batches CONSTANT NUMBER := 20; -- 20 * 50,000 = 1,000,000 for a quick 1M test first
  v_start TIMESTAMP;
BEGIN
  v_start := SYSTIMESTAMP;
  -- Xoá dữ liệu cũ
  EXECUTE IMMEDIATE 'TRUNCATE TABLE debezium.delivery_remunerations';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE debezium.return_remunerations';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE debezium.shipment_event_trackings';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE debezium.shipment_bookings';

  FOR b IN 0..(v_total_batches-1) LOOP
    INSERT INTO debezium.shipment_bookings (
        booking_id, item_code, booking_date, customer_id, service_id, 
        sending_pos_code, receiving_pos_code, weight_gram, weight_tier_id, 
        routing_type_id, main_fee, sur_fee, discount_amount, total_revenue, 
        cost_amount, status_id
    )
    SELECT 
        'BK-' || RAWTOHEX(SYS_GUID()), 
        item_code, b_date, cust, serv, send_pos, recv_pos, w, wt, rt,
        mf, sf, da, (mf+sf-da), (mf+sf-da)*0.65, st
    FROM (
        SELECT 
            'EA' || TO_CHAR(100000000 + b * v_batch_size + LEVEL) || 'VN' AS item_code,
            SYSTIMESTAMP - NUMTODSINTERVAL(MOD(ABS(DBMS_RANDOM.RANDOM), 365), 'DAY') AS b_date,
            'CUST-' || LPAD(MOD(ABS(DBMS_RANDOM.RANDOM), 200) + 1, 4, '0') AS cust,
            'SERV-0' || (MOD(ABS(DBMS_RANDOM.RANDOM), 4) + 1) AS serv,
            'POS-' || LPAD(MOD(ABS(DBMS_RANDOM.RANDOM), 200) + 1, 4, '0') AS send_pos,
            'POS-' || LPAD(MOD(ABS(DBMS_RANDOM.RANDOM), 200) + 1, 4, '0') AS recv_pos,
            MOD(ABS(DBMS_RANDOM.RANDOM), 15000) + 100 AS w,
            'WT-0' || (MOD(ABS(DBMS_RANDOM.RANDOM), 6) + 1) AS wt,
            'RT-0' || (MOD(ABS(DBMS_RANDOM.RANDOM), 4) + 1) AS rt,
            MOD(ABS(DBMS_RANDOM.RANDOM), 235000) + 15000 AS mf,
            MOD(ABS(DBMS_RANDOM.RANDOM), 23500) + 1500 AS sf,
            0 AS da,
            'STAT-0' || (MOD(ABS(DBMS_RANDOM.RANDOM), 7) + 1) AS st
        FROM DUAL CONNECT BY LEVEL <= v_batch_size
    );

    COMMIT;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Done');
END;
/
EXIT;
