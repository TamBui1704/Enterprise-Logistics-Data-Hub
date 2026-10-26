#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
DEBEZIUM_DIR="$BASE_DIR/debezium"

CONNECT_URL="http://localhost:8083"
AUTH="debezium_admin:AdminSecretKey2026!"

echo "⏳ Đang kiểm tra Debezium REST API tại $CONNECT_URL..."
until curl -s -u "$AUTH" "$CONNECT_URL/connectors" > /dev/null 2>&1; do
    echo "  Debezium chưa sẵn sàng, chờ 3s..."
    sleep 3
done

echo "✅ Debezium REST API đã sẵn sàng!"

echo "📦 Đăng ký Dim Connector (oracle-logistics-dim-connector)..."
curl -s -X POST "$CONNECT_URL/connectors" \
  -u "$AUTH" \
  -H "Content-Type: application/json" \
  -d @"$DEBEZIUM_DIR/register-dim-connector.json" | grep -v '^{}' || true

echo ""
echo "📦 Đăng ký Fact Connector (oracle-logistics-fact-connector)..."
curl -s -X POST "$CONNECT_URL/connectors" \
  -u "$AUTH" \
  -H "Content-Type: application/json" \
  -d @"$DEBEZIUM_DIR/register-fact-connector.json" | grep -v '^{}' || true

echo ""
echo "⏳ Chờ 5s để connectors khởi tạo..."
sleep 5

echo "🔍 Kiểm tra trạng thái Connectors:"
curl -s -u "$AUTH" "$CONNECT_URL/connectors?expand=status" | jq . || curl -s -u "$AUTH" "$CONNECT_URL/connectors?expand=status"

echo ""
echo "🎉 Hoàn tất đăng ký Debezium Connectors!"
