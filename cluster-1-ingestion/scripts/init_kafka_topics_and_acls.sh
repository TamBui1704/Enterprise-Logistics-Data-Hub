#!/bin/bash
set -e

ADMIN_CFG="/opt/kafka/config/admin-client.properties"
BOOTSTRAP="kafka:29092"

echo "⏳ Đang đợi Kafka Broker sẵn sàng trên $BOOTSTRAP..."
until /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" > /dev/null 2>&1; do
    echo "  Kafka chưa sẵn sàng, chờ 2s..."
    sleep 2
done

echo "✅ Kafka Broker sẵn sàng. Bắt đầu tạo Topics..."

TOPICS=(
    "cdc_logistics_oracle.DEBEZIUM.DIM_CUSTOMERS"
    "cdc_logistics_oracle.DEBEZIUM.DIM_SERVICES"
    "cdc_logistics_oracle.DEBEZIUM.DIM_POS_LOCATIONS"
    "cdc_logistics_oracle.DEBEZIUM.DIM_WEIGHT_TIERS"
    "cdc_logistics_oracle.DEBEZIUM.DIM_ROUTING_TYPES"
    "cdc_logistics_oracle.DEBEZIUM.DIM_DELIVERY_STATUSES"
    "cdc_logistics_oracle.DEBEZIUM.DIM_FAILURE_REASONS"
    "cdc_logistics_oracle.DEBEZIUM.SHIPMENT_BOOKINGS"
    "cdc_logistics_oracle.DEBEZIUM.SHIPMENT_EVENT_TRACKINGS"
    "cdc_logistics_oracle.DEBEZIUM.DELIVERY_REMUNERATIONS"
    "cdc_logistics_oracle.DEBEZIUM.RETURN_REMUNERATIONS"
    "schema-changes.logistics.dim"
    "schema-changes.logistics.fact"
)

for t in "${TOPICS[@]}"; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
        --create --if-not-exists --topic "$t" --partitions 3 --replication-factor 1
    echo "  ✔ Topic $t đã sẵn sàng."
done

# Connect internal storage topics
CONNECT_TOPICS=("my_connect_configs" "my_connect_offsets" "my_connect_statuses")
for ct in "${CONNECT_TOPICS[@]}"; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
        --create --if-not-exists --topic "$ct" --partitions 3 --replication-factor 1 --config cleanup.policy=compact
    echo "  ✔ Connect Topic $ct đã sẵn sàng."
done

echo "🔒 Thiết lập Kafka ACLs (Access Control Lists)..."

# 1. Phân quyền User:debezium_user (CDC Producer & Connect Worker)
echo "  [ACL] Cấp quyền cho debezium_user..."
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
    --add --allow-principal User:debezium_user --operation All --topic cdc_ --resource-pattern-type PREFIXED
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
    --add --allow-principal User:debezium_user --operation All --topic schema-changes --resource-pattern-type PREFIXED
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
    --add --allow-principal User:debezium_user --operation All --topic my_connect_ --resource-pattern-type PREFIXED
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
    --add --allow-principal User:debezium_user --operation All --group 1 --resource-pattern-type PREFIXED
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
    --add --allow-principal User:debezium_user --operation All --group connect- --resource-pattern-type PREFIXED
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
    --add --allow-principal User:debezium_user --operation All --group cdc_ --resource-pattern-type PREFIXED
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
    --add --allow-principal User:debezium_user --operation Describe --cluster

# 2. Phân quyền User:spark_user (Streaming Consumer đọc dữ liệu nạp Delta Lake)
echo "  [ACL] Cấp quyền cho spark_user..."
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
    --add --allow-principal User:spark_user --operation Read --operation Describe --topic cdc_logistics_oracle --resource-pattern-type PREFIXED
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
    --add --allow-principal User:spark_user --operation Read --operation Describe --group spark- --resource-pattern-type PREFIXED
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
    --add --allow-principal User:spark_user --operation Read --operation Describe --group * --resource-pattern-type LITERAL
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CFG" \
    --add --allow-principal User:spark_user --operation Describe --cluster

echo "🎉 Hoàn tất khởi tạo Topics và phân quyền Kafka ACLs thành công!"
